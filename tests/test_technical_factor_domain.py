from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain.records import Market
from market_data_center.domain.stock_technical_factor import (
    COMMON_FACTOR_NAMES,
    FACTOR_CATALOG_VERSION,
    TECHNICAL_FACTOR_NAMES,
    StockTechnicalFactorSnapshotRecord,
    validate_technical_factor_snapshot,
)


def snapshot():
    values = dict.fromkeys(TECHNICAL_FACTOR_NAMES, Decimal("-1.25"))
    return StockTechnicalFactorSnapshotRecord(
        "SZSE:000001",
        date(2026, 10, 9),
        Market.CN_A_SHARE,
        "tushare",
        FACTOR_CATALOG_VERSION,
        values,
        values,
        values,
        dict.fromkeys(COMMON_FACTOR_NAMES, 0),
    )


def test_full_catalog_immutable_and_signed():
    record = snapshot()
    assert len(record.unadjusted) == 74
    assert len(record.common) == 4
    assert record.unadjusted["macd"] == Decimal("-1.25")
    with pytest.raises(TypeError):
        record.unadjusted["macd"] = Decimal(0)


@pytest.mark.parametrize(
    "changes",
    [
        {"symbol": "SZSE:1"},
        {"market": "SSE"},
        {"source_code": ""},
        {"factor_catalog_version": "unknown"},
        {"unadjusted": {}},
        {"common": dict.fromkeys(COMMON_FACTOR_NAMES, -1)},
        {"common": dict.fromkeys(COMMON_FACTOR_NAMES, True)},
        {"unadjusted": dict.fromkeys(TECHNICAL_FACTOR_NAMES, Decimal("NaN"))},
        {"unadjusted": dict.fromkeys(TECHNICAL_FACTOR_NAMES, 1.2)},
    ],
)
def test_invalid_snapshot_rejected(changes):
    with pytest.raises(ValueError):
        replace(snapshot(), **changes)


def validate(records, **changes):
    options = {
        "trade_date": date(2026, 10, 9),
        "known_symbols": {"SZSE:000001"},
        "known_trading_dates": {date(2026, 10, 9)},
        "minimum_accepted_rows": 1,
    }
    options.update(changes)
    return validate_technical_factor_snapshot(records, **options)


def test_validation_deduplicates_equal_keys():
    record = snapshot()
    assert validate([record, record]) == (record,)


@pytest.mark.parametrize(
    "changes",
    [
        {"known_symbols": set()},
        {"known_trading_dates": set()},
        {"trade_date": date(2026, 10, 8)},
        {"minimum_accepted_rows": 2},
        {"minimum_accepted_rows": -1},
    ],
)
def test_validation_blocks_entire_invalid_snapshot(changes):
    with pytest.raises(ValueError):
        validate([snapshot()], **changes)


def test_validation_blocks_conflicts_empty_and_limit():
    record = snapshot()
    conflict = replace(record, common=dict.fromkeys(COMMON_FACTOR_NAMES, 1))
    for records in ([], [record] * 10000, [record, conflict]):
        with pytest.raises(ValueError):
            validate(records)
