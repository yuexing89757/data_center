"""Pure trading-calendar calculations."""

from collections.abc import Sequence
from datetime import date, timedelta
from itertools import pairwise

from market_data_center.domain.entities import CalculatedTradingDay
from market_data_center.domain.records import TradingDayRecord


def calculate_trading_day_links(
    records: Sequence[TradingDayRecord],
    *,
    previous_trading_day: date | None = None,
    next_trading_day: date | None = None,
) -> list[CalculatedTradingDay]:
    """Add nearest previous/next trading dates to a complete natural-day sequence."""
    ordered = sorted(records, key=lambda record: record.trade_date)
    if not ordered:
        return []

    markets = {record.market for record in ordered}
    if len(markets) != 1:
        raise ValueError("calendar calculation accepts exactly one market")
    dates = [record.trade_date for record in ordered]
    if len(set(dates)) != len(dates):
        raise ValueError("calendar dates must be unique")
    for previous, current in pairwise(dates):
        if current != previous + timedelta(days=1):
            raise ValueError("calendar must contain every natural day in the requested range")

    if previous_trading_day is not None and previous_trading_day >= dates[0]:
        raise ValueError("previous trading-day boundary must precede the calendar range")
    if next_trading_day is not None and next_trading_day <= dates[-1]:
        raise ValueError("next trading-day boundary must follow the calendar range")

    previous_links: list[date | None] = []
    for record in ordered:
        previous_links.append(previous_trading_day)
        if record.is_trading_day:
            previous_trading_day = record.trade_date

    next_links: list[date | None] = [None] * len(ordered)
    for index in range(len(ordered) - 1, -1, -1):
        record = ordered[index]
        next_links[index] = next_trading_day
        if record.is_trading_day:
            next_trading_day = record.trade_date

    return [
        CalculatedTradingDay(
            market=record.market,
            trade_date=record.trade_date,
            is_trading_day=record.is_trading_day,
            previous_trading_day=previous_links[index],
            next_trading_day=next_links[index],
            source_code=record.source_code,
        )
        for index, record in enumerate(ordered)
    ]
