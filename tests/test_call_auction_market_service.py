from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Self

import pytest

from market_data_center.call_auction_market_service import (
    CallAuctionMarketSnapshotService,
)
from market_data_center.domain.ingestion import IngestionRun, IngestionStatus, QualityResult
from market_data_center.domain.realtime_quote import (
    FiveLevelQuoteSnapshotRecord,
    OrderBookLevel,
    QuoteStatus,
)
from market_data_center.domain.records import Market
from market_data_center.providers.contracts import ProviderError, RealtimeQuoteFetch
from market_data_center.raw_store import StoredRawObject

TRADE_DATE = date(2026, 8, 12)
COLLECTION_TIME = datetime(2026, 8, 12, 1, 26, tzinfo=UTC)
EXPECTED_UNIVERSE = ("SSE:600000", "SZSE:000001")
ENDPOINTS = (("first.quote", 7709), ("second.quote", 7709))


def _levels(side: str) -> tuple[OrderBookLevel, ...]:
    if side == "bid":
        prices = ("10.00", "9.99", "9.98", "9.97", "9.96")
    else:
        prices = ("10.02", "10.03", "10.04", "10.05", "10.06")
    return tuple(
        OrderBookLevel(level, Decimal(price), level * 100)
        for level, price in enumerate(prices, start=1)
    )


def _quote(symbol: str, **changes: object) -> FiveLevelQuoteSnapshotRecord:
    values: dict[str, object] = {
        "symbol": symbol,
        "market": Market.CN_A_SHARE,
        "observed_at": COLLECTION_TIME,
        "source_timestamp": None,
        "quote_status": QuoteStatus.TRADING,
        "last_price": Decimal("10.10"),
        "previous_close": Decimal("9.90"),
        "open": Decimal("10.10"),
        "high": Decimal("10.10"),
        "low": Decimal("10.10"),
        "cumulative_volume": 12_300,
        "cumulative_amount": Decimal("124230.00"),
        "bid_levels": _levels("bid"),
        "ask_levels": _levels("ask"),
        "source_code": "pytdx_hq",
    }
    values.update(changes)
    return FiveLevelQuoteSnapshotRecord(**values)  # type: ignore[arg-type]


def _empty_quote(symbol: str) -> FiveLevelQuoteSnapshotRecord:
    empty_levels = tuple(OrderBookLevel(level, None, None) for level in range(1, 6))
    return _quote(
        symbol,
        quote_status=QuoteStatus.UNKNOWN,
        last_price=None,
        previous_close=None,
        open=None,
        high=None,
        low=None,
        cumulative_volume=None,
        cumulative_amount=None,
        bid_levels=empty_levels,
        ask_levels=empty_levels,
    )


def _fetch(*records: FiveLevelQuoteSnapshotRecord) -> RealtimeQuoteFetch:
    return RealtimeQuoteFetch(
        raw_rows=tuple({"symbol": record.symbol} for record in records),
        records=records,
        requested_symbols=EXPECTED_UNIVERSE,
        failed_symbols=(),
        schema_version="pytdx_hq.security_quotes.v1",
    )


class FakePersistence:
    def __init__(self, universe: Sequence[str] = EXPECTED_UNIVERSE) -> None:
        self.universe = list(universe)
        self.requested_universe: tuple[str, ...] = ()
        self.created_runs: list[IngestionRun] = []
        self.committed_runs: list[IngestionRun] = []
        self.committed_records: list[FiveLevelQuoteSnapshotRecord] = []
        self.committed_quality: list[QualityResult] = []
        self.manifest_row_counts: list[int] = []

    def is_trading_day(self, trade_date: date) -> bool:
        return trade_date == TRADE_DATE

    def listed_sse_szse_stock_symbols(self) -> list[str]:
        self.requested_universe = tuple(self.universe)
        return list(self.universe)

    def create_ingestion_run(self, run: IngestionRun) -> None:
        self.created_runs.append(run)

    def commit_call_auction_market_attempt(
        self,
        run: IngestionRun,
        records: Sequence[object],
        manifest: object,
        quality_results: Sequence[QualityResult],
    ) -> None:
        self.committed_runs.append(run)
        self.committed_records.extend(records)  # type: ignore[arg-type]
        self.committed_quality.extend(quality_results)
        self.manifest_row_counts.append(manifest.row_count)  # type: ignore[attr-defined]


class FakeRawStore:
    def __init__(self) -> None:
        self.rows: list[tuple[Mapping[str, str], ...]] = []

    def write_jsonl(
        self,
        *,
        provider: str,
        dataset: str,
        partition_date: date,
        ingestion_id: object,
        rows: Sequence[Mapping[str, str]],
        schema_version: str,
    ) -> StoredRawObject:
        del provider, dataset, partition_date, ingestion_id
        captured = tuple(rows)
        self.rows.append(captured)
        return StoredRawObject(
            "raw/attempt.jsonl",
            "0" * 64,
            0,
            len(captured),
            "jsonl",
            schema_version,
        )


class FakeProvider(AbstractContextManager["FakeProvider"]):
    source_code = "pytdx_hq"

    def __init__(
        self,
        fetch: RealtimeQuoteFetch | Exception,
        requested_symbols: list[tuple[str, ...]],
        deadlines: list[datetime | None],
        after_fetch: Callable[[], None] | None = None,
    ) -> None:
        self.fetch = fetch
        self.requested_symbols = requested_symbols
        self.deadlines = deadlines
        self.after_fetch = after_fetch

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        return None

    def fetch_five_level_quotes(
        self, symbols: Sequence[str], *, deadline: datetime | None = None
    ) -> RealtimeQuoteFetch:
        self.requested_symbols.append(tuple(symbols))
        self.deadlines.append(deadline)
        if self.after_fetch is not None:
            self.after_fetch()
        if isinstance(self.fetch, Exception):
            raise self.fetch
        return self.fetch


class FakeProviderFactory:
    def __init__(
        self,
        scripted_fetches: Sequence[RealtimeQuoteFetch | Exception],
        after_fetch: Callable[[], None] | None = None,
    ) -> None:
        self.scripted_fetches = list(scripted_fetches)
        self.after_fetch = after_fetch
        self.endpoints: list[tuple[str, int]] = []
        self.requested_symbols: list[tuple[str, ...]] = []
        self.deadlines: list[datetime | None] = []

    def __call__(self, endpoint: tuple[str, int]) -> FakeProvider:
        self.endpoints.append(endpoint)
        return FakeProvider(
            self.scripted_fetches.pop(0),
            self.requested_symbols,
            self.deadlines,
            self.after_fetch,
        )


class MutableClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current


def _service(
    persistence: FakePersistence,
    provider_factory: FakeProviderFactory,
    raw_store: FakeRawStore | None = None,
    clock: Callable[[], datetime] = lambda: COLLECTION_TIME,
) -> CallAuctionMarketSnapshotService:
    return CallAuctionMarketSnapshotService(
        persistence=persistence,
        raw_store=raw_store or FakeRawStore(),
        quote_endpoints=ENDPOINTS,
        provider_factory=provider_factory,
        clock=clock,
    )


def test_single_endpoint_complete_collection_succeeds() -> None:
    persistence = FakePersistence()
    provider_factory = FakeProviderFactory(
        [_fetch(*(_quote(symbol) for symbol in EXPECTED_UNIVERSE))]
    )

    summary = _service(persistence, provider_factory).collect(TRADE_DATE)

    assert summary.status == "succeeded"
    assert summary.attempts == 1
    assert summary.expected_rows == 2
    assert summary.accepted_rows == 2
    assert summary.rejected_rows == 0
    assert summary.ingestion_id == persistence.committed_runs[0].ingestion_id
    assert persistence.requested_universe == EXPECTED_UNIVERSE
    assert persistence.committed_runs[0].status is IngestionStatus.SUCCEEDED
    assert persistence.committed_records[0].high_price == Decimal("10.10")
    assert persistence.committed_records[0].low_price == Decimal("10.10")
    assert provider_factory.endpoints == [("first.quote", 7709)]
    assert provider_factory.requested_symbols == [EXPECTED_UNIVERSE]
    assert provider_factory.deadlines == [datetime(2026, 8, 12, 1, 29, 30, tzinfo=UTC)]
    assert persistence.manifest_row_counts == [2]


def test_partial_attempt_restarts_complete_universe_on_second_endpoint() -> None:
    persistence = FakePersistence()
    provider_factory = FakeProviderFactory(
        [
            _fetch(_quote(EXPECTED_UNIVERSE[0])),
            _fetch(*(_quote(symbol) for symbol in EXPECTED_UNIVERSE)),
        ]
    )

    summary = _service(persistence, provider_factory).collect(TRADE_DATE)

    assert summary.status == "succeeded"
    assert summary.attempts == 2
    assert provider_factory.requested_symbols == [EXPECTED_UNIVERSE, EXPECTED_UNIVERSE]
    assert [run.status for run in persistence.committed_runs] == [
        IngestionStatus.PARTIAL,
        IngestionStatus.SUCCEEDED,
    ]
    assert persistence.committed_runs[0].ingestion_id != persistence.committed_runs[1].ingestion_id
    assert provider_factory.endpoints == [
        ("first.quote", 7709),
        ("second.quote", 7709),
    ]
    assert [record.symbol for record in persistence.committed_records] == [
        "SSE:600000",
        "SSE:600000",
        "SZSE:000001",
    ]
    assert [result.rule_code for result in persistence.committed_quality].count(
        "call_auction_market.missing_symbol"
    ) == 1


def test_two_incomplete_endpoints_leave_latest_attempt_partial() -> None:
    persistence = FakePersistence()
    provider_factory = FakeProviderFactory(
        [_fetch(_quote("SSE:600000")), _fetch(_quote("SZSE:000001"))]
    )

    summary = _service(persistence, provider_factory).collect(TRADE_DATE)

    assert summary.status == "partial"
    assert summary.attempts == 2
    assert summary.expected_rows == 2
    assert summary.accepted_rows == 1
    assert summary.rejected_rows == 1
    assert [run.status for run in persistence.committed_runs] == [
        IngestionStatus.PARTIAL,
        IngestionStatus.PARTIAL,
    ]
    assert provider_factory.requested_symbols == [EXPECTED_UNIVERSE, EXPECTED_UNIVERSE]


def test_duplicate_response_is_rejected_with_dataset_quality_result() -> None:
    persistence = FakePersistence()
    provider_factory = FakeProviderFactory(
        [
            _fetch(
                _quote("SSE:600000"),
                _quote("SSE:600000"),
                _quote("SZSE:000001"),
            ),
            _fetch(_quote("SSE:600000")),
        ]
    )

    summary = _service(persistence, provider_factory).collect(TRADE_DATE)

    assert summary.status == "partial"
    assert persistence.committed_runs[0].status is IngestionStatus.PARTIAL
    assert [record.symbol for record in persistence.committed_records[:1]] == ["SZSE:000001"]
    duplicate_results = [
        result
        for result in persistence.committed_quality
        if result.rule_code == "call_auction_market.duplicate_symbol"
    ]
    assert len(duplicate_results) == 1
    assert duplicate_results[0].dataset_code.value == "call_auction_market_snapshot"


def test_out_of_window_observation_is_quality_failure_not_service_error() -> None:
    persistence = FakePersistence()
    before_window = datetime(2026, 8, 12, 1, 24, 59, tzinfo=UTC)
    provider_factory = FakeProviderFactory(
        [
            _fetch(
                _quote("SSE:600000", observed_at=before_window),
                _quote("SZSE:000001"),
            ),
            _fetch(
                _quote("SSE:600000", observed_at=before_window),
                _quote("SZSE:000001"),
            ),
        ]
    )

    summary = _service(persistence, provider_factory).collect(TRADE_DATE)

    assert summary.status == "partial"
    assert summary.accepted_rows == 1
    assert summary.rejected_rows == 1
    assert all(record.symbol == "SZSE:000001" for record in persistence.committed_records)
    window_results = [
        result
        for result in persistence.committed_quality
        if result.rule_code == "call_auction_market.observation_window"
    ]
    assert len(window_results) == 2
    assert all(
        result.dataset_code.value == "call_auction_market_snapshot" for result in window_results
    )


def test_explicit_empty_quote_is_a_valid_missing_value_fact() -> None:
    persistence = FakePersistence()
    provider_factory = FakeProviderFactory(
        [_fetch(_empty_quote("SSE:600000"), _quote("SZSE:000001"))]
    )

    summary = _service(persistence, provider_factory).collect(TRADE_DATE)

    assert summary.status == "succeeded"
    assert summary.accepted_rows == 2
    empty_fact = persistence.committed_records[0]
    assert empty_fact.symbol == "SSE:600000"
    assert empty_fact.last_price is None
    assert empty_fact.cumulative_volume is None


def test_unknown_response_is_rejected_but_all_raw_rows_are_manifested() -> None:
    persistence = FakePersistence()
    raw_store = FakeRawStore()
    fetch = _fetch(
        _quote("SSE:600000"),
        _quote("SZSE:000001"),
        _quote("SSE:600001"),
    )
    provider_factory = FakeProviderFactory([fetch, fetch])

    summary = _service(persistence, provider_factory, raw_store).collect(TRADE_DATE)

    assert summary.status == "partial"
    assert summary.accepted_rows == 2
    assert summary.rejected_rows == 1
    assert persistence.manifest_row_counts == [3, 3]
    assert [len(rows) for rows in raw_store.rows] == [3, 3]
    assert any(
        result.rule_code == "realtime_quote.unknown_symbol"
        and result.dataset_code.value == "call_auction_market_snapshot"
        for result in persistence.committed_quality
    )


def test_service_defensively_rejects_bse_in_persistence_universe() -> None:
    persistence = FakePersistence(["BSE:920000", *EXPECTED_UNIVERSE])
    provider_factory = FakeProviderFactory([])

    try:
        _service(persistence, provider_factory).collect(TRADE_DATE)
    except ValueError as error:
        assert "SSE/SZSE only" in str(error)
    else:  # pragma: no cover - assertion branch
        raise AssertionError("BSE universe must be rejected")

    assert persistence.created_runs == []
    assert provider_factory.endpoints == []


@pytest.mark.parametrize(
    "current",
    [
        datetime(2026, 8, 12, 1, 24, 59, tzinfo=UTC),
        datetime(2026, 8, 12, 1, 30, tzinfo=UTC),
    ],
)
def test_service_rejects_collection_outside_shanghai_window(current: datetime) -> None:
    persistence = FakePersistence()
    provider_factory = FakeProviderFactory([])

    with pytest.raises(ValueError, match=r"\[09:25,09:30\)"):
        _service(persistence, provider_factory, clock=lambda: current).collect(TRADE_DATE)

    assert persistence.requested_universe == ()
    assert persistence.created_runs == []


def test_service_rejects_non_trading_date_before_freezing_universe() -> None:
    persistence = FakePersistence()
    provider_factory = FakeProviderFactory([])
    non_trading_date = date(2026, 8, 13)
    current = datetime(2026, 8, 13, 1, 26, tzinfo=UTC)

    with pytest.raises(ValueError, match="not a CN_A_SHARE trading day"):
        _service(persistence, provider_factory, clock=lambda: current).collect(non_trading_date)

    assert persistence.requested_universe == ()


def test_no_second_attempt_starts_at_request_cutoff() -> None:
    persistence = FakePersistence()
    clock = MutableClock(COLLECTION_TIME)
    provider_factory = FakeProviderFactory(
        [_fetch(_quote("SSE:600000"))],
        after_fetch=lambda: setattr(clock, "current", datetime(2026, 8, 12, 1, 29, 30, tzinfo=UTC)),
    )

    summary = _service(persistence, provider_factory, clock=clock).collect(TRADE_DATE)

    assert summary.status == "partial"
    assert summary.attempts == 1
    assert provider_factory.endpoints == [("first.quote", 7709)]
    assert provider_factory.requested_symbols == [EXPECTED_UNIVERSE]


def test_no_initial_attempt_starts_at_request_cutoff() -> None:
    persistence = FakePersistence()
    provider_factory = FakeProviderFactory([])
    cutoff = datetime(2026, 8, 12, 1, 29, 30, tzinfo=UTC)

    with pytest.raises(ValueError, match="request cutoff"):
        _service(persistence, provider_factory, clock=lambda: cutoff).collect(TRADE_DATE)

    assert persistence.created_runs == []
    assert provider_factory.endpoints == []


def test_two_endpoint_errors_are_preserved_as_partial_attempts() -> None:
    persistence = FakePersistence()
    raw_store = FakeRawStore()
    provider_factory = FakeProviderFactory(
        [ProviderError("first unavailable"), ProviderError("second unavailable")]
    )

    summary = _service(persistence, provider_factory, raw_store).collect(TRADE_DATE)

    assert summary.status == "partial"
    assert summary.attempts == 2
    assert summary.accepted_rows == 0
    assert summary.rejected_rows == len(EXPECTED_UNIVERSE)
    assert [run.status for run in persistence.committed_runs] == [
        IngestionStatus.PARTIAL,
        IngestionStatus.PARTIAL,
    ]
    assert persistence.manifest_row_counts == [0, 0]
    assert [len(rows) for rows in raw_store.rows] == [0, 0]
    assert [
        result.rule_code
        for result in persistence.committed_quality
        if result.rule_code == "call_auction_market.provider_error"
    ] == ["call_auction_market.provider_error"] * 2
