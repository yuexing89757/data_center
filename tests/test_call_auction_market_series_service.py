from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from json import loads
from typing import Self
from uuid import UUID, uuid4

from market_data_center.call_auction_market_series_service import (
    CALL_AUCTION_MARKET_SERIES_RAW_SCHEMA_VERSION,
    CallAuctionMarketSeriesService,
)
from market_data_center.domain.call_auction_market_series import (
    MarketSeriesRound,
    MarketSeriesSession,
    MarketSeriesSnapshotRecord,
    MarketSeriesStatus,
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
    def __init__(self, recovery_universe: tuple[str, ...] | None = None) -> None:
        self.recovery_universe = recovery_universe
        self.core_universe_calls = 0
        self.session: MarketSeriesSession | None = None
        self.rounds: list[MarketSeriesRound] = []
        self.created_runs: list[IngestionRun] = []
        self.completed_runs: list[IngestionRun] = []
        self.records: list[tuple[MarketSeriesSnapshotRecord, ...]] = []
        self.quality: list[QualityResult] = []

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

    def start_round(self, round_state: MarketSeriesRound) -> None:
        self.rounds.append(round_state)

    def create_ingestion_run(self, run: IngestionRun) -> None:
        self.created_runs.append(run)

    def commit_attempt(
        self,
        run: IngestionRun,
        records: Sequence[MarketSeriesSnapshotRecord],
        manifest: object,
        quality_results: Sequence[QualityResult],
    ) -> None:
        assert manifest.row_count >= len(records)  # type: ignore[attr-defined]
        self.completed_runs.append(run)
        self.records.append(tuple(records))
        self.quality.extend(quality_results)

    def finish_round(self, round_summary: MarketSeriesRound) -> None:
        assert self.rounds[-1].sample_seq == round_summary.sample_seq
        self.rounds[-1] = round_summary

    def finish_session(self, session_id: UUID, finished_at: datetime) -> MarketSeriesSession:
        assert self.session is not None and self.session.session_id == session_id
        successful = sum(item.status is MarketSeriesStatus.SUCCEEDED for item in self.rounds)
        partial = sum(item.status is MarketSeriesStatus.PARTIAL for item in self.rounds)
        failed = 32 - successful - partial
        successful_quotes = sum(item.successful_quotes for item in self.rounds)
        failed_quotes = 64 - successful_quotes
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
        )
        return self.session


class FakeRawStore:
    def __init__(self) -> None:
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
    ) -> None:
        self.behavior = behavior
        self.clock = clock
        self.requested = requested
        self.deadlines = deadlines

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        return None

    def fetch_five_level_quotes(
        self, symbols: Sequence[str], *, deadline: datetime | None = None
    ) -> RealtimeQuoteFetch:
        requested = tuple(symbols)
        self.requested.append(requested)
        self.deadlines.append(deadline)
        if isinstance(self.behavior, Exception):
            raise self.behavior
        returned = requested if self.behavior == "full" else requested[:1]
        records = tuple(_quote(symbol, self.clock()) for symbol in returned)
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

    def __call__(self, endpoint: tuple[str, int]) -> FakeProvider:
        self.endpoints.append(endpoint)
        behavior = self.behaviors.pop(0) if self.behaviors else "full"
        return FakeProvider(behavior, self.clock, self.requested, self.deadlines)


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
        high=Decimal("10.00"),
        low=Decimal("10.00"),
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
) -> CallAuctionMarketSeriesService:
    return CallAuctionMarketSeriesService(
        persistence=persistence,
        raw_store=raw_store,
        quote_endpoints=ENDPOINTS,
        provider_factory=factory,
        retry_budget_seconds=5,
        clock=clock,
        sleeper=clock.sleep,
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
    assert first_raw["sample_seq"] == "0"
    assert first_raw["scheduled_at"] == SLOTS[0].isoformat()
    assert loads(first_raw["provider_raw_json"]) == {"symbol": "SSE:600000"}


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
