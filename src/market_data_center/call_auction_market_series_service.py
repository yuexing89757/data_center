"""Collect deterministic full-market source snapshots throughout the call auction."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from json import dumps
from time import sleep
from types import TracebackType
from typing import Protocol, Self
from uuid import UUID, uuid4

from market_data_center.domain.call_auction_market_series import (
    SERIES_CADENCE_SECONDS,
    MarketSeriesRound,
    MarketSeriesSession,
    MarketSeriesSnapshotRecord,
    MarketSeriesStatus,
    series_slots,
    universe_hash,
)
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
    FiveLevelQuoteSnapshotRecord,
    RealtimeQuoteFinding,
    validate_realtime_quotes,
)
from market_data_center.providers.contracts import (
    RealtimeQuoteFetch,
    RealtimeQuoteNormalizationError,
)
from market_data_center.raw_store import StoredRawObject

MAX_ENDPOINT_ATTEMPTS = 2
CALL_AUCTION_MARKET_SERIES_RAW_SCHEMA_VERSION = (
    "market_data_center.call_auction_market_series.raw.v1"
)


class CallAuctionMarketSeriesPersistence(Protocol):
    def is_trading_day(self, trade_date: date) -> bool: ...

    def listed_sse_szse_stock_symbols(self) -> list[str]: ...

    def load_recovery_universe(self, trade_date: date) -> tuple[str, ...] | None: ...

    def create_session(self, session: MarketSeriesSession) -> None: ...

    def start_round(self, round_state: MarketSeriesRound) -> None: ...

    def create_ingestion_run(self, run: IngestionRun) -> None: ...

    def commit_attempt(
        self,
        run: IngestionRun,
        records: Sequence[MarketSeriesSnapshotRecord],
        manifest: RawManifest,
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def finish_round(self, round_summary: MarketSeriesRound) -> None: ...

    def finish_session(self, session_id: UUID, finished_at: datetime) -> MarketSeriesSession: ...


class CallAuctionMarketSeriesRawStore(Protocol):
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
class CallAuctionMarketSeriesSummary:
    status: str
    expected_rows: int
    accepted_rows: int
    rejected_rows: int
    session_id: UUID


class CallAuctionMarketSeriesService:
    def __init__(
        self,
        *,
        persistence: CallAuctionMarketSeriesPersistence,
        raw_store: CallAuctionMarketSeriesRawStore,
        quote_endpoints: Sequence[tuple[str, int]],
        provider_factory: Callable[[tuple[str, int]], ManagedRealtimeQuoteProvider],
        retry_budget_seconds: float,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        if retry_budget_seconds < 0:
            raise ValueError("retry_budget_seconds must not be negative")
        self._persistence = persistence
        self._raw_store = raw_store
        self._quote_endpoints = tuple(quote_endpoints[:MAX_ENDPOINT_ATTEMPTS])
        self._provider_factory = provider_factory
        self._retry_budget = timedelta(seconds=retry_budget_seconds)
        self._clock = clock
        self._sleeper = sleeper

    def collect(self, trade_date: date, workflow_run_id: UUID) -> CallAuctionMarketSeriesSummary:
        now = _utc_clock_sample(self._clock)
        if not self._persistence.is_trading_day(trade_date):
            raise ValueError(f"{trade_date.isoformat()} is not a CN_A_SHARE trading day")
        if not self._quote_endpoints:
            raise ValueError("call-auction market series requires a quote endpoint")
        recovered = self._persistence.load_recovery_universe(trade_date)
        universe = (
            recovered
            if recovered is not None
            else tuple(self._persistence.listed_sse_szse_stock_symbols())
        )
        universe_hash_value = universe_hash(universe)
        slots = series_slots(trade_date)
        session = MarketSeriesSession(
            session_id=uuid4(),
            workflow_run_id=workflow_run_id,
            trade_date=trade_date,
            window_start=slots[0],
            window_end=slots[-1] + timedelta(seconds=SERIES_CADENCE_SECONDS),
            cadence_seconds=SERIES_CADENCE_SECONDS,
            expected_rounds=len(slots),
            universe_symbols=universe,
            universe_count=len(universe),
            universe_hash=universe_hash_value,
            status=MarketSeriesStatus.RUNNING,
            started_at=now,
        )
        self._persistence.create_session(session)

        for sample_seq, scheduled_at in enumerate(slots):
            deadline = scheduled_at + timedelta(seconds=SERIES_CADENCE_SECONDS)
            running_round = MarketSeriesRound(
                session_id=session.session_id,
                sample_seq=sample_seq,
                scheduled_at=scheduled_at,
                collected_at=None,
                status=MarketSeriesStatus.RUNNING,
                attempt_count=0,
                expected_quotes=len(universe),
                successful_quotes=0,
                failed_quotes=0,
                selected_ingestion_id=None,
            )
            self._persistence.start_round(running_round)
            now = _utc_clock_sample(self._clock)
            if now >= deadline:
                self._persistence.finish_round(
                    replace(
                        running_round,
                        collected_at=now,
                        status=MarketSeriesStatus.FAILED,
                        failed_quotes=len(universe),
                        error_summary="missed_sampling_round",
                    )
                )
                continue
            if now < scheduled_at:
                self._sleeper((scheduled_at - now).total_seconds())
            completed_round = self._collect_round(
                session,
                running_round,
                deadline,
            )
            self._persistence.finish_round(completed_round)

        finished = self._persistence.finish_session(
            session.session_id, _utc_clock_sample(self._clock)
        )
        expected_rows = finished.universe_count * finished.expected_rounds
        return CallAuctionMarketSeriesSummary(
            finished.status.value,
            expected_rows,
            finished.successful_quotes,
            finished.failed_quotes,
            finished.session_id,
        )

    def _collect_round(
        self,
        session: MarketSeriesSession,
        round_state: MarketSeriesRound,
        deadline: datetime,
    ) -> MarketSeriesRound:
        last_attempt: _AttemptSummary | None = None
        for attempt_number, endpoint in enumerate(self._quote_endpoints, start=1):
            if _utc_clock_sample(self._clock) >= deadline:
                break
            if (
                attempt_number > 1
                and _utc_clock_sample(self._clock) + self._retry_budget >= deadline
            ):
                break
            last_attempt = self._attempt(
                session,
                round_state,
                endpoint,
                deadline,
                attempt_number,
            )
            if last_attempt.succeeded:
                break
        if last_attempt is None:
            return replace(
                round_state,
                collected_at=_utc_clock_sample(self._clock),
                status=MarketSeriesStatus.FAILED,
                failed_quotes=round_state.expected_quotes,
                error_summary="round_deadline_reached",
            )
        return replace(
            round_state,
            collected_at=_utc_clock_sample(self._clock),
            status=(
                MarketSeriesStatus.SUCCEEDED
                if last_attempt.succeeded
                else MarketSeriesStatus.PARTIAL
            ),
            attempt_count=last_attempt.attempt_number,
            successful_quotes=last_attempt.accepted_rows,
            failed_quotes=round_state.expected_quotes - last_attempt.accepted_rows,
            selected_ingestion_id=last_attempt.ingestion_id,
            error_summary=(None if last_attempt.succeeded else "incomplete_quote_response"),
        )

    def _attempt(
        self,
        session: MarketSeriesSession,
        round_state: MarketSeriesRound,
        endpoint: tuple[str, int],
        deadline: datetime,
        attempt_number: int,
    ) -> _AttemptSummary:
        started_at = _utc_clock_sample(self._clock)
        run = IngestionRun(
            ingestion_id=uuid4(),
            provider_code=ProviderCode.PYTDX_HQ,
            dataset_code=DatasetCode.CALL_AUCTION_MARKET_SERIES,
            status=IngestionStatus.RUNNING,
            requested_at=started_at,
            started_at=started_at,
            request_params={
                "trade_date": session.trade_date.isoformat(),
                "session_id": str(session.session_id),
                "sample_seq": round_state.sample_seq,
                "scheduled_at": round_state.scheduled_at.isoformat(),
                "endpoint": f"{endpoint[0]}:{endpoint[1]}",
                "expected_rows": session.universe_count,
            },
        )
        self._persistence.create_ingestion_run(run)
        provider_error: str | None = None
        try:
            with self._provider_factory(endpoint) as provider:
                fetch = provider.fetch_five_level_quotes(
                    session.universe_symbols,
                    deadline=deadline,
                )
        except Exception as error:
            provider_error = type(error).__name__
            fetch = RealtimeQuoteFetch(
                raw_rows=(),
                records=(),
                requested_symbols=session.universe_symbols,
                failed_symbols=session.universe_symbols,
                schema_version="pytdx_hq.security_quotes.v1",
                raw_observed_at=(),
            )

        expected = set(session.universe_symbols)
        counts = Counter(record.symbol for record in fetch.records)
        duplicates = {symbol for symbol, count in counts.items() if count > 1}
        outside = {
            record.symbol
            for record in fetch.records
            if not round_state.scheduled_at
            <= record.observed_at
            < round_state.scheduled_at + timedelta(seconds=SERIES_CADENCE_SECONDS)
        }
        candidates = tuple(
            record
            for record in fetch.records
            if record.symbol not in duplicates and record.symbol not in outside
        )
        validation = validate_realtime_quotes(
            candidates,
            known_symbols=expected,
            known_stock_symbols=expected,
            now=_utc_clock_sample(self._clock),
        )
        records = tuple(
            _to_snapshot(record, session, round_state) for record in validation.accepted
        )
        missing = expected - {record.symbol for record in records}
        requested_mismatch = fetch.requested_symbols != session.universe_symbols
        raw_cardinality = len(fetch.raw_rows) != len(fetch.records)
        succeeded = (
            not requested_mismatch
            and provider_error is None
            and not fetch.normalization_errors
            and not fetch.failed_symbols
            and not duplicates
            and not outside
            and not missing
            and not raw_cardinality
            and validation.rejected_rows == 0
            and len(records) == session.universe_count
        )
        stored = self._raw_store.write_jsonl(
            provider=ProviderCode.PYTDX_HQ.value,
            dataset=DatasetCode.CALL_AUCTION_MARKET_SERIES.value,
            partition_date=session.trade_date,
            ingestion_id=run.ingestion_id,
            rows=_raw_envelopes(fetch, session.session_id, round_state),
            schema_version=CALL_AUCTION_MARKET_SERIES_RAW_SCHEMA_VERSION,
        )
        quality_results = _quality_results(
            run.ingestion_id,
            validation.findings,
            missing,
            duplicates,
            outside,
            provider_error,
            requested_mismatch,
            raw_cardinality,
            fetch.normalization_errors,
        )
        completed = replace(
            run,
            status=IngestionStatus.SUCCEEDED if succeeded else IngestionStatus.PARTIAL,
            finished_at=_utc_clock_sample(self._clock),
            fetched_rows=stored.row_count,
            accepted_rows=len(records),
            rejected_rows=max(stored.row_count - len(records), 0),
            error_summary=None if succeeded else "incomplete_quote_response",
        )
        self._persistence.commit_attempt(
            completed,
            records,
            _manifest(run.ingestion_id, stored),
            quality_results,
        )
        return _AttemptSummary(
            attempt_number=attempt_number,
            ingestion_id=run.ingestion_id,
            accepted_rows=len(records),
            succeeded=succeeded,
        )


@dataclass(frozen=True, slots=True)
class _AttemptSummary:
    attempt_number: int
    ingestion_id: UUID
    accepted_rows: int
    succeeded: bool


def _to_snapshot(
    quote: FiveLevelQuoteSnapshotRecord,
    session: MarketSeriesSession,
    round_state: MarketSeriesRound,
) -> MarketSeriesSnapshotRecord:
    return MarketSeriesSnapshotRecord(
        symbol=quote.symbol,
        trade_date=session.trade_date,
        session_id=session.session_id,
        sample_seq=round_state.sample_seq,
        scheduled_at=round_state.scheduled_at,
        observed_at=quote.observed_at,
        source_code=quote.source_code,
        last_price=quote.last_price,
        previous_close=quote.previous_close,
        high_price=quote.high,
        low_price=quote.low,
        cumulative_volume=quote.cumulative_volume,
        cumulative_amount=quote.cumulative_amount,
    )


def _raw_envelopes(
    fetch: RealtimeQuoteFetch,
    session_id: UUID,
    round_state: MarketSeriesRound,
) -> tuple[Mapping[str, str], ...]:
    return tuple(
        {
            "session_id": str(session_id),
            "sample_seq": str(round_state.sample_seq),
            "scheduled_at": round_state.scheduled_at.isoformat(),
            "worker_observed_at": fetch.raw_observed_at[index].isoformat(),
            "provider_schema_version": fetch.schema_version,
            "provider_raw_json": dumps(
                dict(fetch.raw_rows[index]),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        for index in range(len(fetch.raw_rows))
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


def _quality_results(
    ingestion_id: UUID,
    findings: Sequence[RealtimeQuoteFinding],
    missing: set[str],
    duplicates: set[str],
    outside: set[str],
    provider_error: str | None,
    requested_mismatch: bool,
    raw_cardinality: bool,
    normalization_errors: Sequence[RealtimeQuoteNormalizationError],
) -> tuple[QualityResult, ...]:
    results = [
        _quality(ingestion_id, "missing_symbol", "expected symbol is missing", {"symbol": symbol})
        for symbol in sorted(missing)
    ]
    results.extend(
        _quality(
            ingestion_id,
            "duplicate_symbol",
            "provider returned a duplicate symbol",
            {"symbol": symbol},
        )
        for symbol in sorted(duplicates)
    )
    results.extend(
        _quality(
            ingestion_id,
            "observation_window",
            "observation is outside its round",
            {"symbol": symbol},
        )
        for symbol in sorted(outside)
    )
    if provider_error is not None:
        results.append(
            _quality(
                ingestion_id,
                "provider_error",
                "quote endpoint attempt failed",
                details={"error_type": provider_error},
            )
        )
    if requested_mismatch:
        results.append(
            _quality(ingestion_id, "requested_symbols", "provider changed requested symbols")
        )
    if raw_cardinality:
        results.append(_quality(ingestion_id, "raw_record_cardinality", "Raw and records differ"))
    results.extend(
        _quality(
            ingestion_id,
            "normalization_error",
            "provider row could not be normalized",
            ({"symbol": error.symbol} if error.symbol is not None else None),
            {"raw_row_index": error.raw_row_index, "reason": error.reason},
        )
        for error in normalization_errors
    )
    results.extend(
        QualityResult(
            uuid4(),
            ingestion_id,
            DatasetCode.CALL_AUCTION_MARKET_SERIES,
            finding.rule_code,
            finding.severity,
            QualityStatus.FAILED,
            finding.message,
            finding.natural_key,
        )
        for finding in findings
    )
    return tuple(results)


def _quality(
    ingestion_id: UUID,
    suffix: str,
    message: str,
    natural_key: Mapping[str, object] | None = None,
    details: Mapping[str, object] | None = None,
) -> QualityResult:
    return QualityResult(
        uuid4(),
        ingestion_id,
        DatasetCode.CALL_AUCTION_MARKET_SERIES,
        f"call_auction_market_series.{suffix}",
        QualitySeverity.ERROR,
        QualityStatus.FAILED,
        message,
        natural_key,
        details or {},
    )


def _utc_clock_sample(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("call-auction market series clock must be timezone-aware")
    return value.astimezone(UTC)
