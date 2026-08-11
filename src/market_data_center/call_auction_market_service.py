"""Collect one complete Shanghai/SZSE morning call-auction market snapshot."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from types import TracebackType
from typing import Protocol, Self
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    QualitySeverity,
    QualityStatus,
    RawFileFormat,
    RawManifest,
)
from market_data_center.domain.realtime_quote import (
    CallAuctionMarketSnapshotRecord,
    FiveLevelQuoteSnapshotRecord,
    RealtimeQuoteFinding,
    validate_realtime_quotes,
)
from market_data_center.providers.contracts import RealtimeQuoteFetch
from market_data_center.raw_store import StoredRawObject

SHANGHAI_ZONE = ZoneInfo("Asia/Shanghai")
WINDOW_START = time(9, 25)
REQUEST_CUTOFF = time(9, 29, 30)
WINDOW_END = time(9, 30)
MAX_ENDPOINT_ATTEMPTS = 2


class CallAuctionMarketPersistence(Protocol):
    def is_trading_day(self, trade_date: date) -> bool: ...

    def listed_sse_szse_stock_symbols(self) -> list[str]: ...

    def create_ingestion_run(self, run: IngestionRun) -> None: ...

    def commit_call_auction_market_attempt(
        self,
        run: IngestionRun,
        records: Sequence[CallAuctionMarketSnapshotRecord],
        manifest: RawManifest,
        quality_results: Sequence[QualityResult],
    ) -> None: ...


class CallAuctionMarketRawStore(Protocol):
    def write_jsonl(
        self,
        *,
        provider: str,
        dataset: str,
        partition_date: date,
        ingestion_id: UUID,
        rows: Sequence[Mapping[str, str]],
        schema_version: str,
    ) -> StoredRawObject: ...


class ManagedRealtimeQuoteProvider(Protocol):
    source_code: str

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    def fetch_five_level_quotes(
        self, symbols: Sequence[str], *, deadline: datetime | None = None
    ) -> RealtimeQuoteFetch: ...


@dataclass(frozen=True, slots=True)
class CallAuctionMarketCollectionSummary:
    status: str
    attempts: int
    expected_rows: int
    accepted_rows: int
    rejected_rows: int
    ingestion_id: UUID


class CallAuctionMarketSnapshotService:
    def __init__(
        self,
        *,
        persistence: CallAuctionMarketPersistence,
        raw_store: CallAuctionMarketRawStore,
        quote_endpoints: Sequence[tuple[str, int]],
        provider_factory: Callable[[tuple[str, int]], ManagedRealtimeQuoteProvider],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._persistence = persistence
        self._raw_store = raw_store
        self._quote_endpoints = tuple(quote_endpoints[:MAX_ENDPOINT_ATTEMPTS])
        self._provider_factory = provider_factory
        self._clock = clock

    def collect(self, trade_date: date) -> CallAuctionMarketCollectionSummary:
        now = self._clock()
        _validate_collection_time(now, trade_date)
        if not self._persistence.is_trading_day(trade_date):
            raise ValueError(f"{trade_date.isoformat()} is not a CN_A_SHARE trading day")
        universe = tuple(self._persistence.listed_sse_szse_stock_symbols())
        _validate_universe(universe)
        if not self._quote_endpoints:
            raise ValueError("call-auction market collection requires a quote endpoint")

        deadline = datetime.combine(trade_date, REQUEST_CUTOFF, SHANGHAI_ZONE).astimezone(UTC)
        expected = set(universe)
        last_summary: CallAuctionMarketCollectionSummary | None = None
        for attempt_number, endpoint in enumerate(self._quote_endpoints, start=1):
            attempt_time = self._clock()
            if attempt_number > 1 and attempt_time >= deadline:
                break
            run = _running_attempt(trade_date, endpoint, len(universe), attempt_time)
            self._persistence.create_ingestion_run(run)
            provider_error: str | None = None
            try:
                with self._provider_factory(endpoint) as provider:
                    fetch = provider.fetch_five_level_quotes(universe, deadline=deadline)
            except Exception as error:
                provider_error = type(error).__name__
                fetch = RealtimeQuoteFetch(
                    raw_rows=(),
                    records=(),
                    requested_symbols=universe,
                    failed_symbols=universe,
                    schema_version="pytdx_hq.security_quotes.v1",
                )

            stored = self._raw_store.write_jsonl(
                provider=ProviderCode.PYTDX_HQ.value,
                dataset=DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT.value,
                partition_date=trade_date,
                ingestion_id=run.ingestion_id,
                rows=fetch.raw_rows,
                schema_version=fetch.schema_version,
            )
            manifest = _manifest(run.ingestion_id, stored)
            response_counts = Counter(record.symbol for record in fetch.records)
            duplicate_symbols = {symbol for symbol, count in response_counts.items() if count > 1}
            out_of_window_symbols = {
                record.symbol
                for record in fetch.records
                if not _in_observation_window(record, trade_date)
            }
            candidate_records = tuple(
                record
                for record in fetch.records
                if record.symbol not in duplicate_symbols
                and record.symbol not in out_of_window_symbols
            )
            validation = validate_realtime_quotes(
                candidate_records,
                known_symbols=expected,
                known_stock_symbols=expected,
                now=self._clock(),
            )
            records = tuple(_to_market_record(record, trade_date) for record in validation.accepted)
            missing_symbols = expected - {record.symbol for record in records}
            dataset_rejected_symbols = missing_symbols | duplicate_symbols | out_of_window_symbols
            rejected_rows = validation.rejected_rows + len(dataset_rejected_symbols)
            succeeded = rejected_rows == 0
            quality_results = _quality_results(
                run.ingestion_id,
                validation.findings,
                missing_symbols,
                duplicate_symbols,
                out_of_window_symbols,
                provider_error,
            )
            completed = replace(
                run,
                status=(IngestionStatus.SUCCEEDED if succeeded else IngestionStatus.PARTIAL),
                finished_at=self._clock(),
                fetched_rows=max(len(fetch.requested_symbols), len(records) + rejected_rows),
                accepted_rows=len(records),
                rejected_rows=rejected_rows,
                error_summary=(
                    None
                    if succeeded
                    else (
                        f"{provider_error}: quote endpoint attempt failed"
                        if provider_error is not None
                        else f"{rejected_rows} expected symbol response(s) were rejected or missing"
                    )
                ),
            )
            self._persistence.commit_call_auction_market_attempt(
                completed, records, manifest, quality_results
            )
            last_summary = CallAuctionMarketCollectionSummary(
                completed.status.value,
                attempt_number,
                len(universe),
                len(records),
                rejected_rows,
                completed.ingestion_id,
            )
            if succeeded:
                return last_summary
        if last_summary is None:  # pragma: no cover - first attempt always starts inside the window
            raise RuntimeError("call-auction market collection did not start an attempt")
        return last_summary


def _validate_collection_time(now: datetime, trade_date: date) -> None:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("call-auction collection clock must be timezone-aware")
    local = now.astimezone(SHANGHAI_ZONE)
    if (
        local.date() != trade_date
        or not WINDOW_START <= local.time().replace(tzinfo=None) < WINDOW_END
    ):
        raise ValueError("call-auction market collection must run in [09:25,09:30) Asia/Shanghai")
    if local.time().replace(tzinfo=None) >= REQUEST_CUTOFF:
        raise ValueError("call-auction market request cutoff has passed")


def _validate_universe(universe: tuple[str, ...]) -> None:
    if not universe:
        raise ValueError("call-auction market universe must not be empty")
    if universe != tuple(sorted(set(universe))):
        raise ValueError("call-auction market universe must be sorted and unique")
    if any(not symbol.startswith(("SSE:", "SZSE:")) for symbol in universe):
        raise ValueError("call-auction market universe supports SSE/SZSE only")


def _running_attempt(
    trade_date: date,
    endpoint: tuple[str, int],
    expected_rows: int,
    now: datetime,
) -> IngestionRun:
    return IngestionRun(
        ingestion_id=uuid4(),
        provider_code=ProviderCode.PYTDX_HQ,
        dataset_code=DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT,
        status=IngestionStatus.RUNNING,
        requested_at=now,
        started_at=now,
        request_params={
            "trade_date": trade_date.isoformat(),
            "endpoint": f"{endpoint[0]}:{endpoint[1]}",
            "expected_rows": expected_rows,
        },
    )


def _manifest(ingestion_id: UUID, stored: StoredRawObject) -> RawManifest:
    return RawManifest(
        uuid4(),
        ingestion_id,
        stored.object_path,
        RawFileFormat.JSONL,
        stored.content_sha256,
        stored.byte_size,
        stored.row_count,
        stored.schema_version,
    )


def _to_market_record(
    quote: FiveLevelQuoteSnapshotRecord, trade_date: date
) -> CallAuctionMarketSnapshotRecord:
    return CallAuctionMarketSnapshotRecord(
        symbol=quote.symbol,
        trade_date=trade_date,
        observed_at=quote.observed_at,
        source_code=quote.source_code,
        last_price=quote.last_price,
        previous_close=quote.previous_close,
        high_price=quote.high,
        low_price=quote.low,
        cumulative_volume=quote.cumulative_volume,
        cumulative_amount=quote.cumulative_amount,
    )


def _in_observation_window(record: FiveLevelQuoteSnapshotRecord, trade_date: date) -> bool:
    observed = record.observed_at.astimezone(SHANGHAI_ZONE)
    return (
        observed.date() == trade_date
        and WINDOW_START <= observed.time().replace(tzinfo=None) < WINDOW_END
    )


def _quality_results(
    ingestion_id: UUID,
    findings: Sequence[RealtimeQuoteFinding],
    missing_symbols: set[str] | None = None,
    duplicate_symbols: set[str] | None = None,
    out_of_window_symbols: set[str] | None = None,
    provider_error: str | None = None,
) -> tuple[QualityResult, ...]:
    results = [
        QualityResult(
            uuid4(),
            ingestion_id,
            DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT,
            "call_auction_market.missing_symbol",
            QualitySeverity.ERROR,
            QualityStatus.FAILED,
            "provider did not return an acceptable explicit response for an expected symbol",
            {"symbol": symbol},
        )
        for symbol in sorted(missing_symbols or set())
    ]
    results.extend(
        QualityResult(
            uuid4(),
            ingestion_id,
            DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT,
            "call_auction_market.duplicate_symbol",
            QualitySeverity.ERROR,
            QualityStatus.FAILED,
            "provider returned more than one response for a symbol",
            {"symbol": symbol},
        )
        for symbol in sorted(duplicate_symbols or set())
    )
    results.extend(
        QualityResult(
            uuid4(),
            ingestion_id,
            DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT,
            "call_auction_market.observation_window",
            QualitySeverity.ERROR,
            QualityStatus.FAILED,
            "quote observed_at is outside [09:25,09:30) Asia/Shanghai on the trade date",
            {"symbol": symbol},
        )
        for symbol in sorted(out_of_window_symbols or set())
    )
    if provider_error is not None:
        results.append(
            QualityResult(
                uuid4(),
                ingestion_id,
                DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT,
                "call_auction_market.provider_error",
                QualitySeverity.ERROR,
                QualityStatus.FAILED,
                "quote endpoint attempt failed before returning a complete response",
                details={"error_type": provider_error},
            )
        )
    results.extend(
        QualityResult(
            uuid4(),
            ingestion_id,
            DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT,
            finding.rule_code,
            finding.severity,
            QualityStatus.FAILED,
            finding.message,
            finding.natural_key,
        )
        for finding in findings
    )
    return tuple(results)
