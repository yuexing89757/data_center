"""Bounded asynchronous persistence for live THS board-index history."""

import base64
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from json import dumps
from logging import getLogger
from pathlib import Path, PurePosixPath
from struct import pack
from threading import BoundedSemaphore
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import Engine, text

from market_data_center.providers.akshare_ths import THSBoardDailyBarLiveBatch
from market_data_center.raw_store import LocalRawStore

LOGGER = getLogger(__name__)
SHANGHAI = ZoneInfo("Asia/Shanghai")

PERSIST = text("""
select api_v1.persist_board_index_daily_bars_live(
 p_ingestion_id=>:ingestion_id,p_raw_id=>:raw_id,p_fetched_at=>:fetched_at,
 p_input_hash=>:input_hash,p_object_path=>:object_path,
 p_content_sha256=>:content_sha256,p_byte_size=>:byte_size,
 p_source_row_count=>:source_row_count,p_source_years=>cast(:source_years as jsonb),
 p_records=>cast(:records as jsonb)
) as payload
""")


class BoardIndexPersistenceError(RuntimeError):
    pass


class BoardIndexPersistenceQueueFull(BoardIndexPersistenceError):
    pass


@dataclass(frozen=True, slots=True)
class PreparedBoardIndexPersistence:
    ingestion_id: UUID
    raw_id: UUID
    fetched_at: datetime
    input_hash: str
    object_path: str
    content_sha256: str
    byte_size: int
    source_row_count: int
    source_years_payload: str
    records_payload: str


class BoardIndexApiPersistence:
    def __init__(self, engine: Engine, raw_store: LocalRawStore, raw_root: Path) -> None:
        self._engine = engine
        self._raw_store = raw_store
        self._raw_root = raw_root.resolve()

    def prepare(self, batch: THSBoardDailyBarLiveBatch) -> PreparedBoardIndexPersistence:
        if not batch.payloads or len(batch.records) < 34:
            raise BoardIndexPersistenceError("THS live batch is incomplete")
        digest = sha256()
        raw_rows: list[Mapping[str, str]] = []
        for payload in batch.payloads:
            digest.update(pack(">Q", len(payload.content)))
            digest.update(payload.content)
            raw_rows.append(
                {
                    "year": str(payload.year),
                    "url": payload.url,
                    "encoding": "base64",
                    "payload_base64": base64.b64encode(payload.content).decode("ascii"),
                    "fetched_at": payload.fetched_at.isoformat(),
                }
            )

        ingestion_id, raw_id = uuid4(), uuid4()
        partition_date = batch.fetched_at.astimezone(SHANGHAI).date()
        try:
            stored = self._raw_store.write_jsonl(
                provider="akshare_ths",
                dataset="board_index_daily_bar",
                partition_date=partition_date,
                ingestion_id=ingestion_id,
                rows=raw_rows,
                schema_version="akshare_ths.board_index_daily_bar.live.v1",
            )
        except OSError as error:
            raise BoardIndexPersistenceError("board-index Raw persistence failed") from error

        records_payload = dumps(
            [
                {
                    "board_id": record.board_id,
                    "trade_date": record.trade_date.isoformat(),
                    "market": record.market.value,
                    "open": str(record.open),
                    "high": str(record.high),
                    "low": str(record.low),
                    "close": str(record.close),
                    "volume": record.volume,
                    "amount": str(record.amount),
                    "source_code": record.source_code,
                }
                for record in batch.records
            ],
            separators=(",", ":"),
        )
        return PreparedBoardIndexPersistence(
            ingestion_id=ingestion_id,
            raw_id=raw_id,
            fetched_at=batch.fetched_at,
            input_hash=digest.hexdigest(),
            object_path=stored.object_path,
            content_sha256=stored.content_sha256,
            byte_size=stored.byte_size,
            source_row_count=stored.row_count,
            source_years_payload=dumps(
                [payload.year for payload in batch.payloads], separators=(",", ":")
            ),
            records_payload=records_payload,
        )

    def commit(self, prepared: PreparedBoardIndexPersistence) -> Mapping[str, object]:
        try:
            with self._engine.begin() as connection:
                row = (
                    connection.execute(
                        PERSIST,
                        {
                            "ingestion_id": prepared.ingestion_id,
                            "raw_id": prepared.raw_id,
                            "fetched_at": prepared.fetched_at,
                            "input_hash": prepared.input_hash,
                            "object_path": prepared.object_path,
                            "content_sha256": prepared.content_sha256,
                            "byte_size": prepared.byte_size,
                            "source_row_count": prepared.source_row_count,
                            "source_years": prepared.source_years_payload,
                            "records": prepared.records_payload,
                        },
                    )
                    .mappings()
                    .one()
                )
        except Exception as error:
            raise BoardIndexPersistenceError("board-index database persistence failed") from error
        result = row["payload"]
        if not isinstance(result, Mapping):
            raise BoardIndexPersistenceError("board-index persistence returned invalid metadata")
        self._validate_result(result, prepared)
        if result["outcome"] == "reused":
            self.discard(prepared)
        return result

    def discard(self, prepared: PreparedBoardIndexPersistence) -> None:
        path = self._raw_root.joinpath(*PurePosixPath(prepared.object_path).parts).resolve()
        if path.is_relative_to(self._raw_root):
            path.unlink(missing_ok=True)

    @staticmethod
    def _validate_result(
        result: Mapping[str, object], prepared: PreparedBoardIndexPersistence
    ) -> None:
        try:
            outcome = result["outcome"]
            result_ingestion_id = UUID(str(result["ingestion_id"]))
            result_raw_id = UUID(str(result["raw_id"]))
            result_hash = result["input_hash"]
        except (KeyError, TypeError, ValueError) as error:
            raise BoardIndexPersistenceError(
                "board-index persistence returned invalid metadata"
            ) from error
        if outcome not in {"created", "reused"} or result_hash != prepared.input_hash:
            raise BoardIndexPersistenceError("board-index persistence returned invalid metadata")
        if outcome == "created" and (
            result_ingestion_id != prepared.ingestion_id or result_raw_id != prepared.raw_id
        ):
            raise BoardIndexPersistenceError("board-index persistence returned invalid metadata")


class BoardIndexPersistenceQueue:
    """One writer and one waiting slot; no unbounded background accumulation."""

    def __init__(self, persistence: BoardIndexApiPersistence) -> None:
        self._persistence = persistence
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="board-persist")
        self._capacity = BoundedSemaphore(2)

    def prepare(self, batch: THSBoardDailyBarLiveBatch) -> PreparedBoardIndexPersistence:
        return self._persistence.prepare(batch)

    def submit(self, prepared: PreparedBoardIndexPersistence) -> None:
        if not self._capacity.acquire(blocking=False):
            raise BoardIndexPersistenceQueueFull("board-index persistence queue is full")
        try:
            future = self._executor.submit(self._persistence.commit, prepared)
        except Exception as error:
            self._capacity.release()
            raise BoardIndexPersistenceError(
                "board-index persistence task could not be queued"
            ) from error
        future.add_done_callback(self._completed)

    def discard(self, prepared: PreparedBoardIndexPersistence) -> None:
        self._persistence.discard(prepared)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)

    def _completed(self, future: Future[Mapping[str, object]]) -> None:
        self._capacity.release()
        error = future.exception()
        if error is not None:
            LOGGER.error(
                "asynchronous board-index persistence failed; immutable Raw retained",
                exc_info=(type(error), error, error.__traceback__),
            )
