from datetime import date

import pytest

from market_data_center.domain import (
    Market,
    TradingDayRecord,
    calculate_trading_day_links,
)


def test_calculator_links_trading_days_across_weekend() -> None:
    records = [
        TradingDayRecord(Market.CN_A_SHARE, date(2026, 7, 24), True, "baostock"),
        TradingDayRecord(Market.CN_A_SHARE, date(2026, 7, 25), False, "baostock"),
        TradingDayRecord(Market.CN_A_SHARE, date(2026, 7, 26), False, "baostock"),
        TradingDayRecord(Market.CN_A_SHARE, date(2026, 7, 27), True, "baostock"),
    ]

    calculated = calculate_trading_day_links(records)

    assert calculated[1].previous_trading_day == date(2026, 7, 24)
    assert calculated[1].next_trading_day == date(2026, 7, 27)
    assert calculated[3].previous_trading_day == date(2026, 7, 24)


def test_calculator_rejects_natural_day_gap() -> None:
    records = [
        TradingDayRecord(Market.CN_A_SHARE, date(2026, 7, 24), True, "baostock"),
        TradingDayRecord(Market.CN_A_SHARE, date(2026, 7, 26), False, "baostock"),
    ]

    with pytest.raises(ValueError, match="every natural day"):
        calculate_trading_day_links(records)


def test_calendar_uses_persisted_boundaries_for_incremental_window() -> None:
    records = [
        TradingDayRecord(Market.CN_A_SHARE, date(2026, 7, 27), True, "baostock"),
        TradingDayRecord(Market.CN_A_SHARE, date(2026, 7, 28), True, "baostock"),
    ]

    calculated = calculate_trading_day_links(
        records,
        previous_trading_day=date(2026, 7, 24),
        next_trading_day=date(2026, 7, 29),
    )

    assert calculated[0].previous_trading_day == date(2026, 7, 24)
    assert calculated[-1].next_trading_day == date(2026, 7, 29)
