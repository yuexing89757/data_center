from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy.engine import Engine

import market_data_center.snapshot_collector as snapshot_collector
from market_data_center.domain import (
    FiveLevelQuoteSnapshotRecord,
    Market,
    OrderBookLevel,
    QuoteStatus,
)
from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
)
from market_data_center.snapshot_collector import (
    LIMIT_UP_POOL_CODE,
    EodQuoteSnapshotUnavailable,
    _eod_quality_results,
    _finish_run,
    _limit_up_symbols,
    _to_eod_records,
    collect_eod_quotes,
)


@dataclass(frozen=True, slots=True)
class RecordedCall:
    statement: str
    parameters: dict[str, object]


class RecordingResult:
    def __init__(
        self,
        *,
        scalar: str | None = None,
        rows: tuple[tuple[str], ...] = (),
    ) -> None:
        self.scalar = scalar
        self.rows = rows

    def scalar_one_or_none(self) -> str | None:
        return self.scalar

    def all(self) -> list[tuple[str]]:
        return list(self.rows)


class RecordingConnection:
    def __init__(self, engine: "RecordingEngine") -> None:
        self.engine = engine

    def __enter__(self) -> "RecordingConnection":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def execute(self, statement, parameters) -> RecordingResult:
        self.engine.calls.append(RecordedCall(str(statement), dict(parameters)))
        if len(self.engine.calls) == 1:
            return RecordingResult(scalar=self.engine.snapshot_id)
        return RecordingResult(rows=tuple((symbol,) for symbol in self.engine.symbols))


class RecordingEngine:
    def __init__(self, *, snapshot_id: str, symbols: tuple[str, ...]) -> None:
        self.snapshot_id = snapshot_id
        self.symbols = symbols
        self.calls: list[RecordedCall] = []

    def connect(self) -> RecordingConnection:
        return RecordingConnection(self)


def _levels(*prices: str, volume: int) -> tuple[OrderBookLevel, ...]:
    return tuple(
        OrderBookLevel(level, Decimal(price), volume) for level, price in enumerate(prices, start=1)
    )


def _quote() -> FiveLevelQuoteSnapshotRecord:
    return FiveLevelQuoteSnapshotRecord(
        symbol="SSE:600000",
        market=Market.CN_A_SHARE,
        observed_at=datetime(2026, 8, 10, 7, 10, tzinfo=UTC),
        source_timestamp=None,
        quote_status=QuoteStatus.TRADING,
        last_price=Decimal("11.00"),
        previous_close=Decimal("10.00"),
        open=Decimal("10.50"),
        high=Decimal("11.00"),
        low=Decimal("10.40"),
        cumulative_volume=1_000_000,
        cumulative_amount=Decimal("10900000"),
        bid_levels=_levels("11.00", "10.99", "10.98", "10.97", "10.96", volume=500),
        ask_levels=_levels("11.01", "11.02", "11.03", "11.04", "11.05", volume=300),
        source_code="pytdx_hq",
    )


def test_snapshot_conversion_preserves_decimal_units() -> None:
    quote = _quote()

    eod = _to_eod_records((quote,), date(2026, 8, 10), {quote.symbol: Decimal("11.00")})[0]

    assert eod.bid1_volume == 500
    assert eod.seal_amount == Decimal("5500.00")


class FinalizationPersistence:
    def __init__(self, result: int) -> None:
        self.result = result
        self.trade_dates: list[date] = []

    def finalize_call_auction_snapshot(self, trade_date: date) -> int:
        self.trade_dates.append(trade_date)
        return self.result


def test_call_auction_finalization_delegates_to_database_only() -> None:
    persistence = FinalizationPersistence(result=2)

    written = snapshot_collector.finalize_call_auction(
        cast(Engine, object()),
        date(2026, 8, 12),
        persistence_factory=lambda _engine: persistence,
    )

    assert written == 2
    assert persistence.trade_dates == [date(2026, 8, 12)]


def test_call_auction_symbols_use_exact_date_limit_up_pool_only() -> None:
    trade_date = date(2026, 8, 11)
    engine = RecordingEngine(snapshot_id="pool-up", symbols=("SSE:600000", "SZSE:000001"))

    symbols = _limit_up_symbols(cast(Engine, engine), trade_date)

    assert symbols == ["SSE:600000", "SZSE:000001"]
    assert engine.calls[0].parameters == {"code": LIMIT_UP_POOL_CODE, "d": trade_date}
    assert "basis_trade_date = :d" in engine.calls[0].statement
    assert "status = 'ready'" in engine.calls[0].statement
    assert engine.calls[1].parameters == {"snapshot_id": "pool-up"}


def test_snapshot_ingestion_run_finishes_partial_with_counts() -> None:
    now = datetime(2026, 8, 10, tzinfo=UTC)
    running = IngestionRun(
        ingestion_id=uuid4(),
        provider_code=ProviderCode.PYTDX_HQ,
        dataset_code=DatasetCode.EOD_QUOTE_SNAPSHOT,
        status=IngestionStatus.RUNNING,
        requested_at=now,
        started_at=now,
    )

    finished = _finish_run(
        running,
        fetched_rows=3,
        accepted_rows=2,
        rejected_rows=1,
        error_summary="one missing quote",
    )

    assert finished.status is IngestionStatus.PARTIAL
    assert finished.finished_at is not None
    assert (finished.fetched_rows, finished.accepted_rows, finished.rejected_rows) == (3, 2, 1)


def test_eod_collection_rejects_historical_date_before_database_or_network_access() -> None:
    with pytest.raises(EodQuoteSnapshotUnavailable, match="cannot backfill"):
        collect_eod_quotes(
            object(),  # type: ignore[arg-type]
            date(2026, 8, 10),
            clock=lambda: datetime(2026, 8, 11, 13, 10, tzinfo=UTC),
        )


def test_eod_missing_expected_symbol_is_blocking_quality_result() -> None:
    ingestion_id = uuid4()

    results = _eod_quality_results(ingestion_id, {"SSE:600000"}, ())

    assert len(results) == 1
    assert results[0].ingestion_id == ingestion_id
    assert results[0].dataset_code is DatasetCode.EOD_QUOTE_SNAPSHOT
    assert results[0].rule_code == "eod_quote.missing_symbol"
    assert results[0].status.value == "failed"
