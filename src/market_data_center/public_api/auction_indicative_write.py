"""Bounded asynchronous persistence for live auction indicative fetches."""

from collections.abc import Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from json import dumps
from logging import getLogger
from pathlib import Path, PurePosixPath
from threading import BoundedSemaphore
from uuid import UUID, uuid4

from sqlalchemy import Engine, text

from market_data_center.domain.auction_indicative import CallAuctionIndicativeDetailRecord
from market_data_center.raw_store import LocalRawStore

LOGGER = getLogger(__name__)

PERSIST = text("""
select api_v1.persist_call_auction_indicative_details(
 p_ingestion_id=>:ingestion_id,p_raw_id=>:raw_id,p_symbol=>:symbol,
 p_trade_date=>:trade_date,p_fetched_at=>:fetched_at,p_input_hash=>:input_hash,
 p_object_path=>:object_path,p_content_sha256=>:content_sha256,p_byte_size=>:byte_size,
 p_source_row_count=>:source_row_count,p_records=>cast(:records as jsonb)
) as payload
""")


class AuctionIndicativePersistenceError(RuntimeError):
    pass


class AuctionIndicativePersistenceQueueFull(AuctionIndicativePersistenceError):
    pass


@dataclass(frozen=True, slots=True)
class PreparedAuctionIndicativePersistence:
    ingestion_id: UUID
    raw_id: UUID
    symbol: str
    trade_date: date
    fetched_at: datetime
    input_hash: str
    object_path: str
    content_sha256: str
    byte_size: int
    source_row_count: int
    records_payload: str


class AuctionIndicativeApiPersistence:
    def __init__(self, engine: Engine, raw_store: LocalRawStore, raw_root: Path) -> None:
        self._engine = engine
        self._raw_store = raw_store
        self._raw_root = raw_root.resolve()

    def prepare(
        self,
        *,
        symbol: str,
        trade_date: date,
        fetched_at: datetime,
        raw_rows: Sequence[Mapping[str, str]],
        records: Sequence[CallAuctionIndicativeDetailRecord],
    ) -> PreparedAuctionIndicativePersistence:
        canonical = dumps(list(raw_rows), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        input_hash = sha256(canonical.encode()).hexdigest()
        ingestion_id, raw_id = uuid4(), uuid4()
        try:
            stored = self._raw_store.write_jsonl(
                provider="eastmoney",
                dataset="call_auction_indicative_detail",
                partition_date=trade_date,
                ingestion_id=ingestion_id,
                rows=raw_rows,
                schema_version="eastmoney.call_auction_indicative_detail.v1",
            )
        except OSError as error:
            raise AuctionIndicativePersistenceError("auction Raw persistence failed") from error
        payload = dumps(
            [
                {
                    "source_sequence": record.source_sequence,
                    "observed_at": record.observed_at.isoformat(),
                    "indicative_price": str(record.indicative_price),
                    "displayed_volume_shares": record.displayed_volume_shares,
                    "source_display_classification": record.source_display_classification.value,
                }
                for record in records
            ],
            separators=(",", ":"),
        )
        return PreparedAuctionIndicativePersistence(
            ingestion_id=ingestion_id,
            raw_id=raw_id,
            symbol=symbol,
            trade_date=trade_date,
            fetched_at=fetched_at,
            input_hash=input_hash,
            object_path=stored.object_path,
            content_sha256=stored.content_sha256,
            byte_size=stored.byte_size,
            source_row_count=stored.row_count,
            records_payload=payload,
        )

    def commit(self, prepared: PreparedAuctionIndicativePersistence) -> Mapping[str, object]:
        try:
            with self._engine.begin() as connection:
                row = (
                    connection.execute(
                        PERSIST,
                        {
                            "ingestion_id": prepared.ingestion_id,
                            "raw_id": prepared.raw_id,
                            "symbol": prepared.symbol,
                            "trade_date": prepared.trade_date,
                            "fetched_at": prepared.fetched_at,
                            "input_hash": prepared.input_hash,
                            "object_path": prepared.object_path,
                            "content_sha256": prepared.content_sha256,
                            "byte_size": prepared.byte_size,
                            "source_row_count": prepared.source_row_count,
                            "records": prepared.records_payload,
                        },
                    )
                    .mappings()
                    .one()
                )
        except Exception as error:
            # Keep the immutable Raw object for bounded operational recovery.  It is not exposed
            # as successfully registered and no database partial write survives the transaction.
            raise AuctionIndicativePersistenceError(
                "auction database persistence failed"
            ) from error
        result = row["payload"]
        if not isinstance(result, Mapping):
            raise AuctionIndicativePersistenceError("auction persistence returned invalid metadata")
        self._validate_result(result, prepared)
        if result["outcome"] == "reused":
            self.discard(prepared)
        return result

    def discard(self, prepared: PreparedAuctionIndicativePersistence) -> None:
        path = self._raw_root.joinpath(*PurePosixPath(prepared.object_path).parts).resolve()
        if path.is_relative_to(self._raw_root):
            path.unlink(missing_ok=True)

    @staticmethod
    def _validate_result(
        result: Mapping[str, object], prepared: PreparedAuctionIndicativePersistence
    ) -> None:
        try:
            outcome = result["outcome"]
            version = result["version"]
            UUID(str(result["ingestion_id"]))
            UUID(str(result["raw_id"]))
            result_hash = result["input_hash"]
        except (KeyError, TypeError, ValueError) as error:
            raise AuctionIndicativePersistenceError(
                "auction persistence returned invalid metadata"
            ) from error
        if (
            outcome not in {"created", "reused"}
            or not isinstance(version, int)
            or isinstance(version, bool)
            or version < 1
            or result_hash != prepared.input_hash
        ):
            raise AuctionIndicativePersistenceError("auction persistence returned invalid metadata")


class AuctionIndicativePersistenceQueue:
    """One writer and one waiting slot; no unbounded background accumulation."""

    def __init__(self, persistence: AuctionIndicativeApiPersistence) -> None:
        self._persistence = persistence
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="auction-persist")
        self._capacity = BoundedSemaphore(2)

    def prepare(
        self,
        *,
        symbol: str,
        trade_date: date,
        fetched_at: datetime,
        raw_rows: Sequence[Mapping[str, str]],
        records: Sequence[CallAuctionIndicativeDetailRecord],
    ) -> PreparedAuctionIndicativePersistence:
        return self._persistence.prepare(
            symbol=symbol,
            trade_date=trade_date,
            fetched_at=fetched_at,
            raw_rows=raw_rows,
            records=records,
        )

    def submit(self, prepared: PreparedAuctionIndicativePersistence) -> None:
        if not self._capacity.acquire(blocking=False):
            raise AuctionIndicativePersistenceQueueFull("auction persistence queue is full")
        try:
            future = self._executor.submit(self._persistence.commit, prepared)
        except Exception as error:
            self._capacity.release()
            raise AuctionIndicativePersistenceError(
                "auction persistence task could not be queued"
            ) from error
        future.add_done_callback(self._completed)

    def discard(self, prepared: PreparedAuctionIndicativePersistence) -> None:
        self._persistence.discard(prepared)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)

    def _completed(self, future: Future[Mapping[str, object]]) -> None:
        self._capacity.release()
        error = future.exception()
        if error is not None:
            LOGGER.error(
                "asynchronous auction persistence failed; immutable Raw retained for recovery",
                exc_info=(type(error), error, error.__traceback__),
            )
