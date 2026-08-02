from contextlib import contextmanager
from dataclasses import replace
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from market_data_center.domain.records import Exchange, SecurityStatus, SecurityType, TradeStatus
from market_data_center.domain.stock_pool import (
    MAINBOARD_LIMIT_DOWN_POOL,
    MAINBOARD_LIMIT_UP_POOL,
    StockPoolBuildInput,
    StockPoolCandidate,
)
from market_data_center.stock_pool_calculator import calculate_mainboard_stock_pools
from market_data_center.stock_pool_service import StockPoolService

BASIS = date(2026, 7, 31)
EFFECTIVE = date(2026, 8, 3)


def _candidate(
    symbol: str = "SSE:600000",
    *,
    previous_close: str = "10.05",
    close: str = "11.06",
) -> StockPoolCandidate:
    price = Decimal(close)
    return StockPoolCandidate(
        symbol=symbol,
        code=symbol.split(":")[1],
        exchange=Exchange(symbol.split(":")[0]),
        security_type=SecurityType.STOCK,
        security_status=SecurityStatus.LISTED,
        ipo_date=date(2000, 1, 1),
        listing_trading_day_number=1000,
        prior_five_bar_count=5,
        trade_status=TradeStatus.TRADING,
        previous_close=Decimal(previous_close),
        open=price,
        high=price,
        low=price,
        close=price,
        is_st=None,
        daily_bar_ingestion_id=uuid4(),
        indicator_ingestion_id=uuid4(),
        free_float_turnover_rate_pct=Decimal("2.5"),
        free_float_shares=1_000_000,
        circulating_market_value=Decimal("11060000"),
    )


def test_calculator_builds_symmetric_pools_with_exchange_rounding() -> None:
    up = _candidate()
    down = _candidate("SZSE:000001", close="9.05")

    output = calculate_mainboard_stock_pools(StockPoolBuildInput(BASIS, EFFECTIVE, (down, up)))

    limits = {record.symbol: record for record in output.daily_price_limits}
    assert limits[up.symbol].upper_limit == Decimal("11.06")
    assert limits[up.symbol].lower_limit == Decimal("9.05")
    assert {(event.symbol, event.direction.value) for event in output.events} == {
        (up.symbol, "up"),
        (down.symbol, "down"),
    }
    assert {(member.pool_code, member.symbol) for member in output.members} == {
        (MAINBOARD_LIMIT_UP_POOL, up.symbol),
        (MAINBOARD_LIMIT_DOWN_POOL, down.symbol),
    }
    assert output.rejected_count == 0


def test_current_rule_treats_st_and_regular_mainboard_as_ten_percent() -> None:
    regular = _candidate()
    st = replace(_candidate("SSE:600001"), is_st=True)

    output = calculate_mainboard_stock_pools(StockPoolBuildInput(BASIS, EFFECTIVE, (regular, st)))

    assert {record.limit_ratio for record in output.daily_price_limits} == {Decimal("0.10")}


@pytest.mark.parametrize(
    ("change", "rule_code"),
    [
        ({"symbol": "SSE:688001", "code": "688001"}, "stock_pool.unsupported_board"),
        ({"listing_trading_day_number": 5}, "stock_pool.no_limit_initial_listing_stage"),
        ({"prior_five_bar_count": 4}, "stock_pool.unproven_continuous_listing_stage"),
        ({"indicator_ingestion_id": None}, "stock_pool.missing_daily_indicator"),
    ],
)
def test_uncertain_rule_or_input_is_quality_failed_and_excluded(
    change: dict[str, object], rule_code: str
) -> None:
    candidate = replace(_candidate(), **change)

    output = calculate_mainboard_stock_pools(StockPoolBuildInput(BASIS, EFFECTIVE, (candidate,)))

    assert output.members == ()
    assert output.rejected_count == 1
    assert output.findings[0].rule_code == rule_code


def test_ohlc_outside_calculated_limits_is_rejected_instead_of_guessed() -> None:
    candidate = replace(_candidate(), high=Decimal("11.07"))

    output = calculate_mainboard_stock_pools(StockPoolBuildInput(BASIS, EFFECTIVE, (candidate,)))

    assert output.daily_price_limits == ()
    assert output.findings[0].rule_code == "stock_pool.ohlc_outside_price_limit"


def test_pre_2026_rule_date_is_explicitly_unsupported() -> None:
    with pytest.raises(ValueError, match="predates"):
        calculate_mainboard_stock_pools(
            StockPoolBuildInput(date(2026, 7, 3), date(2026, 7, 6), (_candidate(),))
        )


def test_input_hash_is_deterministic_independent_of_candidate_order() -> None:
    first = _candidate()
    second = _candidate("SZSE:000001", close="9.05")

    left = calculate_mainboard_stock_pools(StockPoolBuildInput(BASIS, EFFECTIVE, (first, second)))
    right = calculate_mainboard_stock_pools(StockPoolBuildInput(BASIS, EFFECTIVE, (second, first)))

    assert left.input_hash == right.input_hash


class MemoryStockPoolPersistence:
    def __init__(self, source: StockPoolBuildInput) -> None:
        self.source = source
        self.run = None
        self.snapshots = ()

    @contextmanager
    def build_lock(self, basis_trade_date: date):  # type: ignore[no-untyped-def]
        assert basis_trade_date == self.source.basis_trade_date
        yield

    def load_build_input(self, basis_trade_date: date):  # type: ignore[no-untyped-def]
        assert basis_trade_date == self.source.basis_trade_date
        return self.source, {"basis_trade_date": basis_trade_date.isoformat()}

    def succeeded_calculation_id(self, basis_trade_date: date, input_hash: str):  # type: ignore[no-untyped-def]
        del basis_trade_date, input_hash
        return None

    def create_calculation_run(self, run):  # type: ignore[no-untyped-def]
        self.run = run

    def next_snapshot_version(self, pool_code: str, effective_date: date) -> int:
        del pool_code, effective_date
        return 1

    def commit_build(self, run, output, snapshots):  # type: ignore[no-untyped-def]
        self.run = run
        self.output = output
        self.snapshots = snapshots

    def fail_calculation_run(self, run):  # type: ignore[no-untyped-def]
        self.run = run


def test_service_commits_two_immutable_pool_snapshots_in_one_calculation() -> None:
    source = StockPoolBuildInput(
        BASIS,
        EFFECTIVE,
        (_candidate(), _candidate("SZSE:000001", close="9.05")),
    )
    persistence = MemoryStockPoolPersistence(source)

    summary = StockPoolService(
        persistence,  # type: ignore[arg-type]
    ).build(BASIS)

    assert summary.status == "succeeded"
    assert summary.member_count == 2
    assert {snapshot.pool_code for snapshot in persistence.snapshots} == {
        MAINBOARD_LIMIT_UP_POOL,
        MAINBOARD_LIMIT_DOWN_POOL,
    }
    assert all(snapshot.version == 1 for snapshot in persistence.snapshots)
