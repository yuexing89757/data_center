from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from json import loads
from threading import Event, Thread, current_thread
from typing import Self
from uuid import UUID, uuid4

from market_data_center.call_auction_market_series_service import (
    CALL_AUCTION_MARKET_SERIES_RAW_SCHEMA_VERSION,
    CallAuctionMarketSeriesService,
    CallAuctionMarketSeriesSummary,
    _series_values,
)
from market_data_center.call_auction_market_series_writer import CapturedRound
from market_data_center.domain.call_auction_market_series import (
    MarketSeriesRound,
    MarketSeriesSession,
    MarketSeriesSnapshotRecord,
    MarketSeriesStatus,
    MarketSeriesValueSemantics,
    series_slots,
)
from market_data_center.domain.ingestion import IngestionRun, QualityResult
from market_data_center.domain.realtime_quote import (
    FiveLevelQuoteSnapshotRecord,
    OrderBookLevel,
    QuoteStatus,
)
from market_data_center.domain.records import Market
from market_data_center.providers.contracts import RealtimeQuoteFetch
from market_data_center.raw_store import StoredRawObject

TRADE_DATE = date(2026, 8, 17)
SLOTS = series_slots(TRADE_DATE)
UNIVERSE = ("SSE:600000", "SZSE:000001")
ENDPOINTS = (("first.quote", 7709), ("second.quote", 7709))


class MutableClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class FakePersistence:
    def __init__(
        self,
        recovery_universe: tuple[str, ...] | None = None,
        *,
        fail_sequences: set[int] | None = None,
    ) -> None:
        self.recovery_universe = recovery_universe
        self.fail_sequences = fail_sequences or set()
        self.core_universe_calls = 0
        self.session: MarketSeriesSession | None = None
        self.captured_rounds: list[CapturedRound] = []
        self.rounds: list[MarketSeriesRound] = []
        self.completed_runs: list[IngestionRun] = []
        self.records: list[tuple[MarketSeriesSnapshotRecord, ...]] = []
        self.quality: list[QualityResult] = []
        self.persistence_thread_ids: list[int | None] = []
        self.finish_error_summary: str | None = None

    def is_trading_day(self, trade_date: date) -> bool:
        return trade_date == TRADE_DATE

    def listed_sse_szse_stock_symbols(self) -> list[str]:
        self.core_universe_calls += 1
        return list(UNIVERSE)

    def load_recovery_universe(self, trade_date: date) -> tuple[str, ...] | None:
        assert trade_date == TRADE_DATE
        return self.recovery_universe

    def create_session(self, session: MarketSeriesSession) -> None:
        self.session = session

    def persist_captured_round(self, captured: CapturedRound) -> None:
        self.persistence_thread_ids.append(current_thread().ident)
        if captured.completed_round.sample_seq in self.fail_sequences:
            raise RuntimeError("database unavailable")
        self.captured_rounds.append(captured)
        self.rounds.append(captured.completed_round)
        for attempt in captured.attempts:
            self.completed_runs.append(attempt.run)
            self.records.append(attempt.records)
            self.quality.extend(attempt.quality_results)

    def finish_session(
        self,
        session_id: UUID,
        finished_at: datetime,
        error_summary: str | None = None,
    ) -> MarketSeriesSession:
        assert self.session is not None and self.session.session_id == session_id
        self.finish_error_summary = error_summary
        successful = sum(item.status is MarketSeriesStatus.SUCCEEDED for item in self.rounds)
        partial = sum(item.status is MarketSeriesStatus.PARTIAL for item in self.rounds)
        explicit_failed = sum(item.status is MarketSeriesStatus.FAILED for item in self.rounds)
        missing = self.session.expected_rounds - len(self.rounds)
        failed = explicit_failed + missing
        successful_quotes = sum(item.successful_quotes for item in self.rounds)
        failed_quotes = sum(item.failed_quotes for item in self.rounds) + missing * len(UNIVERSE)
        status = (
            MarketSeriesStatus.SUCCEEDED
            if successful == 32
            else MarketSeriesStatus.PARTIAL
            if successful_quotes or partial
            else MarketSeriesStatus.FAILED
        )
        self.session = replace(
            self.session,
            status=status,
            finished_at=finished_at,
            successful_rounds=successful,
            partial_rounds=partial,
            failed_rounds=failed,
            successful_quotes=successful_quotes,
            failed_quotes=failed_quotes,
            error_summary=error_summary or ("missed_sampling_rounds" if missing else None),
        )
        return self.session


class BlockingPersistence(FakePersistence):
    def __init__(self) -> None:
        super().__init__()
        self.writer_started = Event()
        self.release_writer = Event()

    def persist_captured_round(self, captured: CapturedRound) -> None:
        if captured.completed_round.sample_seq == 0:
            self.writer_started.set()
            if not self.release_writer.wait(timeout=5):
                raise TimeoutError("test did not release Writer")
        super().persist_captured_round(captured)


class FakeRawStore:
    def __init__(
        self,
        *,
        clock: MutableClock | None = None,
        delayed_writes: Mapping[int, float] | None = None,
        failed_writes: set[int] | None = None,
    ) -> None:
        self.clock = clock
        self.delayed_writes = delayed_writes or {}
        self.failed_writes = failed_writes or set()
        self.write_attempts = 0
        self.rows: list[tuple[Mapping[str, str], ...]] = []
        self.schema_versions: list[str] = []

    def write_jsonl(
        self,
        *,
        provider: str,
        dataset: str,
        partition_date: date,
        ingestion_id: UUID,
        rows: Sequence[Mapping[str, str]],
        schema_version: str,
    ) -> StoredRawObject:
        del provider, dataset, partition_date, ingestion_id
        write_index = self.write_attempts
        self.write_attempts += 1
        if write_index in self.failed_writes:
            raise OSError("Raw unavailable")
        if self.clock is not None:
            self.clock.sleep(self.delayed_writes.get(write_index, 0))
        captured = tuple(rows)
        self.rows.append(captured)
        self.schema_versions.append(schema_version)
        return StoredRawObject(
            "raw/series.jsonl", "0" * 64, 0, len(captured), "jsonl", schema_version
        )


class FakeProvider(AbstractContextManager["FakeProvider"]):
    source_code = "pytdx_hq"

    def __init__(
        self,
        behavior: str | Exception,
        clock: MutableClock,
        requested: list[tuple[str, ...]],
        deadlines: list[datetime | None],
        all_provider_calls: Event,
    ) -> None:
        self.behavior = behavior
        self.clock = clock
        self.requested = requested
        self.deadlines = deadlines
        self.all_provider_calls = all_provider_calls

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        return None

    def fetch_five_level_quotes(
        self, symbols: Sequence[str], *, deadline: datetime | None = None
    ) -> RealtimeQuoteFetch:
        requested = tuple(symbols)
        self.requested.append(requested)
        if len(self.requested) >= 32:
            self.all_provider_calls.set()
        self.deadlines.append(deadline)
        if isinstance(self.behavior, Exception):
            raise self.behavior
        returned = requested if self.behavior in {"full", "invalid_source"} else requested[:1]
        records = tuple(_quote(symbol, self.clock()) for symbol in returned)
        if self.behavior == "invalid_source":
            records = (
                *records[:-1],
                replace(records[-1], source_code="unexpected"),
            )
        return RealtimeQuoteFetch(
            raw_rows=tuple({"symbol": symbol} for symbol in returned),
            records=records,
            requested_symbols=requested,
            failed_symbols=tuple(symbol for symbol in requested if symbol not in returned),
            schema_version="pytdx_hq.security_quotes.v1",
            raw_observed_at=tuple(record.observed_at for record in records),
        )


class FakeProviderFactory:
    def __init__(self, clock: MutableClock, behaviors: Sequence[str | Exception] = ()) -> None:
        self.clock = clock
        self.behaviors = list(behaviors)
        self.endpoints: list[tuple[str, int]] = []
        self.requested: list[tuple[str, ...]] = []
        self.deadlines: list[datetime | None] = []
        self.all_provider_calls = Event()

    def __call__(self, endpoint: tuple[str, int]) -> FakeProvider:
        self.endpoints.append(endpoint)
        behavior = self.behaviors.pop(0) if self.behaviors else "full"
        return FakeProvider(
            behavior,
            self.clock,
            self.requested,
            self.deadlines,
            self.all_provider_calls,
        )


def _quote(symbol: str, observed_at: datetime) -> FiveLevelQuoteSnapshotRecord:
    bids = tuple(
        OrderBookLevel(level, Decimal("10") - Decimal(level) / Decimal("100"), 100)
        for level in range(1, 6)
    )
    asks = tuple(
        OrderBookLevel(level, Decimal("10") + Decimal(level) / Decimal("100"), 100)
        for level in range(1, 6)
    )
    return FiveLevelQuoteSnapshotRecord(
        symbol=symbol,
        market=Market.CN_A_SHARE,
        observed_at=observed_at,
        source_timestamp=None,
        quote_status=QuoteStatus.TRADING,
        last_price=Decimal("10.00"),
        previous_close=Decimal("9.90"),
        open=Decimal("10.00"),
        high=None,
        low=None,
        cumulative_volume=100,
        cumulative_amount=Decimal("1000.00"),
        bid_levels=bids,
        ask_levels=asks,
        source_code="pytdx_hq",
    )


def _service(
    persistence: FakePersistence,
    clock: MutableClock,
    factory: FakeProviderFactory,
    raw_store: FakeRawStore,
    *,
    retry_budget_seconds: float = 5,
) -> CallAuctionMarketSeriesService:
    return CallAuctionMarketSeriesService(
        persistence=persistence,
        raw_store=raw_store,
        quote_endpoints=ENDPOINTS,
        provider_factory=factory,
        retry_budget_seconds=retry_budget_seconds,
        clock=clock,
        sleeper=clock.sleep,
    )


def test_series_values_use_bid1_before_0925_and_source_trade_at_0925() -> None:
    quote = _quote("SSE:600000", SLOTS[29])
    assert _series_values(quote, SLOTS[29]) == (
        Decimal("9.99"),
        100,
        Decimal("999.00"),
        MarketSeriesValueSemantics.AUCTION_INDICATIVE,
    )
    assert _series_values(quote, SLOTS[30]) == (
        Decimal("10.00"),
        100,
        Decimal("1000.00"),
        MarketSeriesValueSemantics.OPENING_TRADE,
    )


def test_series_values_keep_all_bid1_values_missing_together() -> None:
    quote = _quote("SSE:600000", SLOTS[0])
    quote = replace(
        quote,
        bid_levels=tuple(OrderBookLevel(level, None, None) for level in range(1, 6)),
    )
    assert _series_values(quote, SLOTS[0]) == (
        None,
        None,
        None,
        MarketSeriesValueSemantics.AUCTION_INDICATIVE,
    )


def test_collects_thirty_two_exact_rounds_and_raw_lineage() -> None:
    clock = MutableClock(SLOTS[0])
    persistence = FakePersistence()
    raw_store = FakeRawStore()
    factory = FakeProviderFactory(clock)

    summary = _service(persistence, clock, factory, raw_store).collect(TRADE_DATE, uuid4())

    assert summary.status == "succeeded"
    assert [item.sample_seq for item in persistence.rounds] == list(range(32))
    assert all(item.status is MarketSeriesStatus.SUCCEEDED for item in persistence.rounds)
    assert factory.requested == [UNIVERSE] * 32
    assert factory.deadlines[-1] == datetime(2026, 8, 17, 1, 25, 40, tzinfo=UTC)
    assert (summary.expected_rows, summary.accepted_rows, summary.rejected_rows) == (64, 64, 0)
    assert raw_store.schema_versions == [CALL_AUCTION_MARKET_SERIES_RAW_SCHEMA_VERSION] * 32
    first_raw = raw_store.rows[0][0]
    first_attempt = persistence.captured_rounds[0].attempts[0]
    assert CALL_AUCTION_MARKET_SERIES_RAW_SCHEMA_VERSION == (
        "market_data_center.call_auction_market_series.raw.v2"
    )
    assert first_raw["ingestion_id"] == str(first_attempt.run.ingestion_id)
    assert first_raw["trade_date"] == TRADE_DATE.isoformat()
    assert first_raw["session_id"] == str(summary.session_id)
    assert first_raw["sample_seq"] == "0"
    assert first_raw["scheduled_at"] == SLOTS[0].isoformat()
    assert first_raw["endpoint"] == "first.quote:7709"
    assert first_raw["attempt_number"] == "1"
    assert first_raw["worker_observed_at"] == SLOTS[0].isoformat()
    assert first_raw["provider_schema_version"] == "pytdx_hq.security_quotes.v1"
    assert loads(first_raw["provider_raw_json"]) == {"symbol": "SSE:600000"}
    first_snapshot = persistence.records[0][0]
    assert first_snapshot.batch_code == "091500"
    assert first_snapshot.bid_levels == _quote("SSE:600000", SLOTS[0]).bid_levels
    assert first_snapshot.ask_levels == _quote("SSE:600000", SLOTS[0]).ask_levels


def test_partial_attempt_retries_entire_universe_on_second_endpoint() -> None:
    clock = MutableClock(SLOTS[0])
    persistence = FakePersistence()
    raw_store = FakeRawStore()
    factory = FakeProviderFactory(clock, ("partial", "full"))

    summary = _service(persistence, clock, factory, raw_store).collect(TRADE_DATE, uuid4())

    assert factory.endpoints[:2] == list(ENDPOINTS)
    assert factory.requested[:2] == [UNIVERSE, UNIVERSE]
    assert [run.status.value for run in persistence.completed_runs[:2]] == [
        "partial",
        "succeeded",
    ]
    assert persistence.rounds[0].attempt_count == 2
    assert persistence.rounds[0].selected_ingestion_id == persistence.completed_runs[1].ingestion_id
    assert summary.status == "succeeded"


def test_retry_reserves_the_previous_complete_attempt_duration() -> None:
    clock = MutableClock(SLOTS[0])
    persistence = FakePersistence()
    raw_store = FakeRawStore(clock=clock, delayed_writes={0: 16})
    factory = FakeProviderFactory(clock, ("partial",))

    summary = _service(
        persistence,
        clock,
        factory,
        raw_store,
        retry_budget_seconds=2,
    ).collect(TRADE_DATE, uuid4())

    assert factory.endpoints[:2] == [ENDPOINTS[0], ENDPOINTS[0]]
    assert persistence.rounds[0].status is MarketSeriesStatus.PARTIAL
    assert persistence.rounds[0].attempt_count == 1
    assert summary.status == "partial"


def test_record_conversion_error_preserves_raw_and_rejects_only_that_record() -> None:
    clock = MutableClock(SLOTS[0])
    persistence = FakePersistence()
    raw_store = FakeRawStore()
    factory = FakeProviderFactory(clock, ("invalid_source", "full"))

    summary = _service(persistence, clock, factory, raw_store).collect(TRADE_DATE, uuid4())

    first_run = persistence.completed_runs[0]
    assert len(raw_store.rows[0]) == 2
    assert (
        first_run.status.value,
        first_run.fetched_rows,
        first_run.accepted_rows,
        first_run.rejected_rows,
    ) == ("partial", 2, 1, 1)
    assert [record.symbol for record in persistence.records[0]] == ["SSE:600000"]
    failed = next(
        result
        for result in persistence.quality
        if result.rule_code == "call_auction_market_series.domain_record"
    )
    assert failed.natural_key == {"symbol": "SZSE:000001"}
    assert persistence.rounds[0].status is MarketSeriesStatus.SUCCEEDED
    assert summary.status == "succeeded"


def test_recovery_universe_is_reused_and_elapsed_slots_are_failed_without_provider_calls() -> None:
    clock = MutableClock(SLOTS[2])
    persistence = FakePersistence(recovery_universe=UNIVERSE)
    raw_store = FakeRawStore()
    factory = FakeProviderFactory(clock)

    summary = _service(persistence, clock, factory, raw_store).collect(TRADE_DATE, uuid4())

    assert persistence.core_universe_calls == 0
    assert [item.status for item in persistence.rounds[:2]] == [
        MarketSeriesStatus.FAILED,
        MarketSeriesStatus.FAILED,
    ]
    assert factory.requested == [UNIVERSE] * 30
    assert summary.status == "partial"
    assert (summary.accepted_rows, summary.rejected_rows) == (60, 4)


def test_slow_writer_does_not_delay_any_of_the_thirty_two_captures() -> None:
    clock = MutableClock(SLOTS[0])
    persistence = BlockingPersistence()
    factory = FakeProviderFactory(clock)
    results: list[CallAuctionMarketSeriesSummary] = []
    errors: list[BaseException] = []

    def collect() -> None:
        try:
            results.append(
                _service(persistence, clock, factory, FakeRawStore()).collect(
                    TRADE_DATE, uuid4()
                )
            )
        except BaseException as error:
            errors.append(error)

    collection = Thread(target=collect, name="test-auction-series-collector")
    collection.start()
    try:
        assert factory.all_provider_calls.wait(timeout=5)
        assert factory.requested == [UNIVERSE] * 32
        assert persistence.writer_started.is_set()
        assert collection.is_alive()
    finally:
        persistence.release_writer.set()
        collection.join(timeout=5)

    assert not collection.is_alive()
    assert errors == []
    assert results[0].status == "succeeded"


def test_writer_failure_preserves_later_capture_and_marks_session_partial() -> None:
    clock = MutableClock(SLOTS[0])
    persistence = FakePersistence(fail_sequences={1})
    raw_store = FakeRawStore()
    factory = FakeProviderFactory(clock)

    summary = _service(persistence, clock, factory, raw_store).collect(TRADE_DATE, uuid4())

    assert factory.requested == [UNIVERSE] * 32
    assert raw_store.write_attempts == 32
    assert [item.completed_round.sample_seq for item in persistence.captured_rounds] == [
        0,
        *range(2, 32),
    ]
    assert persistence.finish_error_summary == "writer_persistence_error:RuntimeError"
    assert summary.status == "partial"


def test_raw_failure_marks_only_that_round_failed_and_continues_capture() -> None:
    clock = MutableClock(SLOTS[0])
    persistence = FakePersistence()
    raw_store = FakeRawStore(failed_writes={0})
    factory = FakeProviderFactory(clock)

    summary = _service(persistence, clock, factory, raw_store).collect(TRADE_DATE, uuid4())

    assert factory.requested == [UNIVERSE] * 32
    assert raw_store.write_attempts == 32
    assert len(raw_store.rows) == 31
    assert persistence.captured_rounds[0].attempts == ()
    assert persistence.rounds[0].status is MarketSeriesStatus.FAILED
    assert persistence.rounds[0].error_summary == "raw_persistence_error"
    assert summary.status == "partial"
