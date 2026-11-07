from dataclasses import replace
from datetime import date, datetime, timedelta

import pytest

from market_data_center.domain.stock_max_minute_volume import MinuteVolumeBar, MinuteVolumeSource
from market_data_center.stock_max_minute_volume_calculator import calculate_minute_volume_peak

DAY = date(2026, 11, 3)


def source():
    bars = tuple(
        MinuteVolumeBar(datetime(2026, 11, 3, hour, minute) + timedelta(minutes=i), 0)
        for hour, minute in ((9, 31), (13, 1))
        for i in range(120)
    )
    return MinuteVolumeSource("SSE:600327", DAY, bars, (), {"history_boundary_reached": True}, ())


@pytest.mark.parametrize("index", [0, 27, 239])
def test_peak_preserves_first_last_native_labels(index):
    item = source()
    bars = list(item.bars)
    bars[index] = replace(bars[index], volume_shares=7260200)
    result = calculate_minute_volume_peak(replace(item, bars=tuple(bars)))
    assert result.max_minute_volume_shares == 7260200
    assert result.peak_at == bars[index].timestamp
    assert result.expected_count == result.observed_count == 240
    assert not result.quality_reasons


def test_true_zero_and_earliest_tie_are_deterministic():
    item = source()
    result = calculate_minute_volume_peak(replace(item, bars=tuple(reversed(item.bars))))
    assert result.max_minute_volume_shares == 0
    assert result.peak_at == datetime(2026, 11, 3, 9, 31)


@pytest.mark.parametrize(
    "kind,reason",
    [
        ("missing", "missing_minute_bar"),
        ("duplicate", "duplicate_minute_bar"),
        ("lunch", "unexpected_minute_bar"),
        ("date", "source_date_mismatch"),
        ("negative", "invalid_minute_volume"),
        ("float", "invalid_minute_volume"),
        ("bool", "invalid_minute_volume"),
        ("overflow", "invalid_minute_volume"),
        ("boundary", "history_boundary_not_reached"),
        ("source", "quantity_units_unverified"),
    ],
)
def test_invalid_input_never_returns_partial_peak(kind, reason):
    item = source()
    bars = list(item.bars)
    if kind == "missing":
        bars.pop()
    elif kind == "duplicate":
        bars[-1] = bars[0]
    elif kind == "lunch":
        bars[0] = replace(bars[0], timestamp=datetime(2026, 11, 3, 12, 0))
    elif kind == "date":
        bars[0] = replace(bars[0], timestamp=bars[0].timestamp - timedelta(days=1))
    elif kind == "boundary":
        item = replace(item, metadata={"history_boundary_reached": False})
    elif kind == "source":
        item = replace(item, quality_reasons=("quantity_units_unverified",))
    else:
        value = {"negative": -1, "float": 1.0, "bool": True, "overflow": 2**63}[kind]
        bars[0] = replace(bars[0], volume_shares=value)
    result = calculate_minute_volume_peak(replace(item, bars=tuple(bars)))
    assert result.max_minute_volume_shares is None
    assert result.peak_at is None
    assert reason in result.quality_reasons
