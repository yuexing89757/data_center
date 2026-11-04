"""Pure fixed-calendar five-day share-volume averages; no source unit conversion."""

from dataclasses import asdict
from decimal import Decimal, localcontext
from hashlib import sha256
from json import dumps
from re import fullmatch

from market_data_center.domain.records import TradeStatus
from market_data_center.domain.stock_volume_average import (
    ALGORITHM_VERSION,
    VolumeAverageRecord,
    VolumeCalculation,
    VolumeWindowInput,
)


def calculate_stock_volume_average(source: VolumeWindowInput) -> VolumeCalculation:
    dates = source.trading_dates
    if len(dates) != 5 or tuple(sorted(set(dates))) != dates or dates[-1] != source.trade_date:
        raise ValueError("exact target date and five distinct ordered trading days are required")
    if not 0 < len(source.stocks) <= 10_000:
        raise ValueError("stock universe must contain 1 to 10000 stocks")
    symbols = [stock.symbol for stock in source.stocks]
    if len(set(symbols)) != len(symbols):
        raise ValueError("duplicate stock input")
    records = []
    canonical_stocks = []
    for stock in sorted(source.stocks, key=lambda item: item.symbol):
        if not fullmatch(r"(?:SSE|SZSE|BSE):[0-9]{6}", stock.symbol):
            raise ValueError("standard stock symbol required")
        if stock.ipo_date > source.trade_date or (
            stock.delisting_date is not None and stock.delisting_date < source.trade_date
        ):
            raise ValueError("stock is outside its lifecycle")
        days = {day.trade_date: day for day in stock.days}
        if len(days) != len(stock.days) or not set(days) <= set(dates):
            raise ValueError("duplicate or out-of-window daily input")
        total = covered = 0
        reasons = set()
        for observation in stock.days:
            if observation.volume is not None and (
                type(observation.volume) is not int or observation.volume < 0
            ):
                raise ValueError("volume must be nonnegative integral shares or None")
            if observation.volume is not None and (
                not observation.certified or not observation.present
            ):
                raise ValueError("unadmitted quantities must be masked")
            if not isinstance(observation.trade_status, TradeStatus):
                raise ValueError("invalid trade status")
        for trade_date in dates:
            day = days.get(trade_date)
            if trade_date < stock.ipo_date:
                reasons.add("insufficient_listing_history")
            elif day is None or not day.present:
                reasons.add("missing_daily_bar")
            elif day.trade_status is TradeStatus.SUSPENDED:
                reasons.add("suspended")
            elif not day.certified:
                reasons.add("quantity_units_unverified")
            elif day.volume is None:
                reasons.add("missing_volume")
            else:
                total += day.volume
                covered += 1
        average = None
        if covered == 5:
            # The ambient Decimal context must not round an integer share total.
            with localcontext() as context:
                context.prec = max(28, len(str(total)) + 2)
                average = Decimal(total) / Decimal(5)
            if average >= Decimal(10) ** 27:
                raise ValueError("average exceeds numeric(28,1) storage range")
        records.append(
            VolumeAverageRecord(
                stock.symbol, source.trade_date, dates[0], average, covered, tuple(sorted(reasons))
            )
        )
        canonical_stocks.append(
            {
                "symbol": stock.symbol,
                "ipo_date": stock.ipo_date,
                "delisting_date": stock.delisting_date,
                "days": [
                    asdict(days[d]) if d in days else {"trade_date": d, "present": False}
                    for d in dates
                ],
            }
        )
    signature = dumps(
        {
            "algorithm_version": ALGORITHM_VERSION,
            "trade_date": source.trade_date,
            "trading_dates": dates,
            "stocks": canonical_stocks,
        },
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )
    return VolumeCalculation(tuple(records), sha256(signature.encode()).hexdigest())
