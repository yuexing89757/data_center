from dataclasses import replace
from datetime import date
from decimal import Decimal, localcontext

import pytest

from market_data_center.domain.records import TradeStatus
from market_data_center.domain.stock_volume_average import (
    StockVolumeInput,
    VolumeDay,
    VolumeWindowInput,
)
from market_data_center.stock_volume_average_calculator import calculate_stock_volume_average

DAYS = tuple(
    date.fromisoformat(d)
    for d in ("2026-10-29", "2026-10-30", "2026-11-02", "2026-11-03", "2026-11-04")
)


def source(volumes=(100, 200, 300, 400, 501)):
    return VolumeWindowInput(
        DAYS[-1],
        DAYS,
        (
            StockVolumeInput(
                "SSE:600000",
                date(1999, 11, 10),
                None,
                tuple(VolumeDay(day, volume) for day, volume in zip(DAYS, volumes, strict=True)),
            ),
        ),
    )


def test_exact_five_market_days_include_today_and_keep_decimal_shares():
    calculation = calculate_stock_volume_average(source())
    row = calculation.records[0]
    assert row.volume_average_5d == Decimal("300.2")
    assert row.trade_date == DAYS[-1]
    assert row.window_start_date == DAYS[0]
    assert row.covered_days == 5
    assert row.quality_reasons == ()
    assert calculation == calculate_stock_volume_average(source())


def test_zero_is_a_real_observation_and_arithmetic_ignores_decimal_context():
    assert calculate_stock_volume_average(source((0, 0, 0, 0, 0))).records[0].volume_average_5d == 0
    with localcontext() as context:
        context.prec = 3
        result = calculate_stock_volume_average(source((9_223_372_036_854_775_807,) * 5))
    assert result.records[0].volume_average_5d == Decimal("9223372036854775807")


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"present": False, "volume": None}, "missing_daily_bar"),
        ({"volume": None}, "missing_volume"),
        ({"certified": False, "volume": None}, "quantity_units_unverified"),
        ({"trade_status": TradeStatus.SUSPENDED}, "suspended"),
    ],
)
def test_incomplete_window_does_not_skip_days_or_fill_zero(change, reason):
    original = source()
    stock = original.stocks[0]
    stock = replace(stock, days=(replace(stock.days[0], **change), *stock.days[1:]))
    row = calculate_stock_volume_average(replace(original, stocks=(stock,))).records[0]
    assert row.volume_average_5d is None
    assert row.covered_days == 4
    assert row.quality_reasons == (reason,)


def test_new_listing_does_not_publish_a_shorter_window():
    original = source()
    stock = replace(original.stocks[0], ipo_date=DAYS[2], days=original.stocks[0].days[2:])
    row = calculate_stock_volume_average(replace(original, stocks=(stock,))).records[0]
    assert row.volume_average_5d is None
    assert row.covered_days == 3
    assert "insufficient_listing_history" in row.quality_reasons


@pytest.mark.parametrize("volume", [-1, Decimal("1.5"), True, float("nan")])
def test_non_integral_or_negative_quantity_is_a_hard_failure(volume):
    with pytest.raises(ValueError):
        calculate_stock_volume_average(source((volume, 1, 2, 3, 4)))


def test_duplicate_stocks_or_days_and_wrong_window_are_rejected():
    original = source()
    with pytest.raises(ValueError):
        calculate_stock_volume_average(replace(original, stocks=original.stocks * 2))
    with pytest.raises(ValueError):
        calculate_stock_volume_average(replace(original, trading_dates=DAYS[:-1]))
    duplicate = replace(original.stocks[0], days=original.stocks[0].days * 2)
    with pytest.raises(ValueError):
        calculate_stock_volume_average(replace(original, stocks=(duplicate,)))
    out_of_window = replace(original.stocks[0], days=(VolumeDay(date(2026, 10, 28), 1),))
    with pytest.raises(ValueError):
        calculate_stock_volume_average(replace(original, stocks=(out_of_window,)))
