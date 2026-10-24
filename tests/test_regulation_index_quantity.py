"""Independent index units must reach adaptation, replay metadata and certification."""

from dataclasses import replace
from datetime import date
from decimal import Decimal, localcontext
from uuid import uuid4

import pytest
from test_quantity_certification import manifest

from market_data_center.domain.ingestion import DatasetCode
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.quantity_units import (
    quantity_group_rules,
    validate_quantity_metadata,
)
from market_data_center.providers.tushare import normalize_tushare_raw
from market_data_center.quantity_certification import build_quantity_certificates

PARAMS = {"source_symbol": "000688.SH", "start_date": "20261023", "end_date": "20261023"}
SCHEMA = "tushare.index_daily.v1"


def row(volume="0.37", amount="123456789012345.6789"):
    return {
        "ts_code": "000688.SH",
        "trade_date": "20261023",
        "open": "100",
        "high": "102",
        "low": "99",
        "close": "101",
        "pre_close": "100",
        "vol": volume,
        "amount": amount,
    }


@pytest.mark.parametrize("volume,expected", [("0.37", 37), ("0", 0), ("", None)])
def test_index_units_remain_exact_with_small_decimal_context(volume, expected):
    with localcontext() as context:
        context.prec = 6
        record = normalize_tushare_raw(DatasetCode.DAILY_BAR, SCHEMA, [row(volume)], PARAMS)[0]
    assert record.volume == expected
    assert record.amount == Decimal("123456789012345678.9")
    assert record.trade_date == date(2026, 10, 23)


@pytest.mark.parametrize("value", ["0.001", "-1", "NaN", "Infinity", "bad", True])
def test_index_rejects_invalid_or_fractional_share_quantity(value):
    with pytest.raises(ProviderError):
        normalize_tushare_raw(DatasetCode.DAILY_BAR, SCHEMA, [row(value)], PARAMS)


@pytest.mark.parametrize("value", ["-1", "NaN", "Infinity", "bad"])
def test_index_rejects_invalid_amount(value):
    with pytest.raises(ProviderError):
        normalize_tushare_raw(DatasetCode.DAILY_BAR, SCHEMA, [row(amount=value)], PARAMS)


def test_index_contract_is_separate_from_stock_and_binds_both_certificates():
    for group, field, unit in (
        ("daily_bar.volume", "vol", "lot"),
        ("daily_bar.amount", "amount", "thousand_cny"),
    ):
        rules = quantity_group_rules(
            provider="tushare", schema_version=SCHEMA, field_group=group, instrument_type="index"
        )
        assert len(rules) == 1
        identity, rule = rules[0]
        assert identity == ("tushare", "index_daily", SCHEMA, "index", field)
        assert rule.source_unit == unit
        with pytest.raises(ProviderError):
            quantity_group_rules(provider="tushare", schema_version=SCHEMA, field_group=group)
    raw = manifest(SCHEMA)
    certificates = build_quantity_certificates(
        ingestion_id=uuid4(), provider="tushare", dataset="daily_bar", manifest=raw
    )
    assert {item["field_group"] for item in certificates} == {
        "daily_bar.volume",
        "daily_bar.amount",
    }
    assert all(item["status"] == "certified" for item in certificates)
    with pytest.raises(ProviderError):
        build_quantity_certificates(
            ingestion_id=uuid4(),
            provider="tushare",
            dataset="daily_bar",
            manifest=replace(raw, schema_version="tushare.index_daily.unknown"),
        )


def test_index_metadata_checks_independent_amount_evidence_and_rejects_conflicts():
    metadata = {
        "source_quantity_unit": "lot",
        "standard_quantity_unit": "share",
        "volume_lot_size": 100,
        "source_amount_unit": "thousand_cny",
        "standard_amount_unit": "cny",
    }
    validate_quantity_metadata(provider="tushare", schema_version=SCHEMA, request_params=metadata)
    for key, invalid in (("source_quantity_unit", "share"), ("source_amount_unit", "cny")):
        with pytest.raises(ProviderError):
            validate_quantity_metadata(
                provider="tushare", schema_version=SCHEMA, request_params={**metadata, key: invalid}
            )


@pytest.mark.parametrize("symbol", ["000001.SH", "600000.SH", "399006.SZ"])
def test_index_replay_does_not_expand_the_regulation_allowlist(symbol):
    with pytest.raises(ProviderError):
        normalize_tushare_raw(
            DatasetCode.DAILY_BAR,
            SCHEMA,
            [{**row(), "ts_code": symbol}],
            {**PARAMS, "source_symbol": symbol},
        )
