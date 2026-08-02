from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain import (
    Market,
    PriceLimitStatus,
    StockDailyIndicatorSnapshotRecord,
    validate_stock_daily_indicators,
)


def _record(**changes: object) -> StockDailyIndicatorSnapshotRecord:
    record = StockDailyIndicatorSnapshotRecord(
        symbol="SSE:600000",
        trade_date=date(2026, 7, 31),
        market=Market.CN_A_SHARE,
        close=Decimal("10.50"),
        turnover_rate_pct=Decimal("1.25"),
        free_float_turnover_rate_pct=Decimal("1.60"),
        volume_ratio=Decimal("1.12"),
        pe=Decimal("8.5"),
        pe_ttm=Decimal("8.2"),
        pb=Decimal("0.8"),
        ps=Decimal("2.1"),
        ps_ttm=Decimal("2.0"),
        dividend_yield_pct=Decimal("3.2"),
        dividend_yield_ttm_pct=Decimal("3.3"),
        total_shares=10_000_000,
        circulating_shares=8_000_000,
        free_float_shares=6_000_000,
        total_market_value=Decimal("105000000"),
        circulating_market_value=Decimal("84000000"),
        price_limit_status=PriceLimitStatus.RISE,
        source_code="tushare",
    )
    return replace(record, **changes)


def test_record_preserves_percentage_points_and_share_layers() -> None:
    record = _record()

    assert record.turnover_rate_pct == Decimal("1.25")
    assert record.free_float_shares == 6_000_000
    assert record.price_limit_status is PriceLimitStatus.RISE


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"turnover_rate_pct": Decimal("-0.1")}, "must not be negative"),
    ],
)
def test_record_rejects_impossible_values(changes: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _record(**changes)


def test_validation_deduplicates_identical_records() -> None:
    record = _record()

    result = validate_stock_daily_indicators(
        [record, record],
        known_symbols={record.symbol},
        known_trading_dates={record.trade_date},
    )

    assert result.accepted == (record,)
    assert result.findings == ()


def test_validation_rejects_conflicts_and_unknown_references() -> None:
    record = _record()
    conflict = replace(record, pe=Decimal("9.1"))

    conflict_result = validate_stock_daily_indicators(
        [record, conflict],
        known_symbols={record.symbol},
        known_trading_dates={record.trade_date},
    )
    unknown_result = validate_stock_daily_indicators(
        [record], known_symbols=set(), known_trading_dates={record.trade_date}
    )

    assert conflict_result.rejected_rows == 2
    assert conflict_result.findings[0].rule_code.endswith("conflicting_duplicate")
    assert unknown_result.rejected_rows == 1
    assert unknown_result.findings[0].rule_code.endswith("unknown_symbol")


@pytest.mark.parametrize(
    ("changes", "rule_code"),
    [
        ({"free_float_shares": 9_000_000}, "invalid_share_order"),
        ({"circulating_shares": 11_000_000}, "invalid_share_order"),
        ({"circulating_market_value": Decimal("106000000")}, "invalid_market_value_order"),
    ],
)
def test_validation_rejects_invalid_cross_field_order(
    changes: dict[str, object], rule_code: str
) -> None:
    record = _record(**changes)

    result = validate_stock_daily_indicators(
        [record], known_symbols={record.symbol}, known_trading_dates={record.trade_date}
    )

    assert result.accepted == ()
    assert result.rejected_rows == 1
    assert result.findings[0].rule_code.endswith(rule_code)


def test_validation_blocks_a_snapshot_below_historical_coverage_floor() -> None:
    record = _record()

    result = validate_stock_daily_indicators(
        [record],
        known_symbols={record.symbol},
        known_trading_dates={record.trade_date},
        minimum_accepted_rows=2,
    )

    assert result.accepted == ()
    assert result.rejected_rows == 1
    assert result.findings[-1].rule_code.endswith("incomplete_market_snapshot")
