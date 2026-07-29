from datetime import date
from decimal import Decimal

from market_data_center.domain import (
    DailyBarRecord,
    Market,
    TradeStatus,
    ValidationRule,
    validate_daily_bars,
)


def _bar(*, symbol: str = "SSE:600000", volume: int | None = 100) -> DailyBarRecord:
    return DailyBarRecord(
        symbol=symbol,
        trade_date=date(2026, 7, 27),
        market=Market.CN_A_SHARE,
        open=Decimal("10.00"),
        high=Decimal("11.00"),
        low=Decimal("9.50"),
        close=Decimal("10.50"),
        previous_close=Decimal("9.90"),
        volume=volume,
        amount=Decimal("1050.00"),
        trade_status=TradeStatus.TRADING,
        is_st=False,
        source_code="baostock",
    )


def test_reference_failures_block_daily_bar_write() -> None:
    findings = validate_daily_bars(
        [_bar(symbol="SSE:999999")],
        known_symbols={"SSE:600000"},
        known_trading_dates={date(2026, 7, 28)},
    )

    assert {finding.rule_code for finding in findings} == {
        ValidationRule.UNKNOWN_SECURITY,
        ValidationRule.NON_TRADING_DATE,
    }
    assert all(finding.blocks_core_write for finding in findings)


def test_identical_duplicate_natural_key_is_still_rejected() -> None:
    record = _bar()

    findings = validate_daily_bars(
        [record, record],
        known_symbols={record.symbol},
        known_trading_dates={record.trade_date},
    )

    assert [finding.rule_code for finding in findings] == [ValidationRule.DUPLICATE_NATURAL_KEY]
    assert findings[0].blocks_core_write
