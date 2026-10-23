from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain.ingestion import DatasetCode
from market_data_center.providers.akshare import _map_share_capital, normalize_akshare_raw
from market_data_center.providers.akshare_limit_down import _map_row as limit_down_source
from market_data_center.providers.akshare_limit_up import _decimal as limit_up_amount
from market_data_center.providers.akshare_limit_up import _map_row as limit_up_source
from market_data_center.providers.akshare_ths import _map_daily_bar as ths_daily_bar
from market_data_center.providers.auction_sources import _bar
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx import _binary32_amount, normalize_pytdx_raw
from market_data_center.providers.pytdx_hq import _lots_to_shares as hq_lots
from market_data_center.providers.pytdx_hq import _source_identity as hq_identity
from market_data_center.providers.quantity_units import quantity_group_rules
from market_data_center.providers.tencent_quote import _lots_to_shares as tencent_lots
from market_data_center.providers.tencent_quote import _source_symbol as tencent_identity
from market_data_center.providers.tushare import _map_cb_daily_bar
from market_data_center.providers.tushare_technical_factor import (
    TECHNICAL_FACTOR_FIELDS,
    normalize_technical_factor_raw,
)


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_akshare_daily_raw_normalizes_lots_exactly(version: str) -> None:
    row = {
        "日期": "2026-10-22",
        "股票代码": "600000",
        "开盘": "10",
        "收盘": "10",
        "最高": "10",
        "最低": "10",
        "成交量": "0.37",
        "成交额": "370",
    }
    result = normalize_akshare_raw(DatasetCode.DAILY_BAR, f"akshare.daily_bar.{version}", [row], {})
    assert result[0].volume == 37
    assert row["成交量"] == "0.37"


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_remote_daily_raw_always_outputs_shares(version: str) -> None:
    row = {
        "date": "20261022",
        "price_scale": "1",
        "open": "10",
        "close": "10",
        "high": "10",
        "low": "10",
        "volume": "0.37",
        "amount": "370",
    }
    result = normalize_pytdx_raw(
        DatasetCode.DAILY_BAR,
        f"pytdx.remote_daily_bar.{version}",
        [row],
        {"source_symbol": "sh.600000"},
    )
    assert result[0].volume == 37
    assert row["volume"] == "0.37"


@pytest.mark.parametrize("value", ["0.001", "-1", "NaN", "Infinity", "922337203685477580.8"])
def test_convertible_bond_volume_never_truncates_or_uses_stock_lots(value: str) -> None:
    with pytest.raises(ProviderError):
        _map_cb_daily_bar({"ts_code": "113001.SH", "trade_date": "20261022", "vol": value})


def test_convertible_bond_volume_is_ten_bonds_per_lot() -> None:
    result = _map_cb_daily_bar({"ts_code": "113001.SH", "trade_date": "20261022", "vol": "0.1"})
    assert result.volume == 1


@pytest.mark.parametrize("factor,value,expected", [(100, "0.37", 37), (1, "37", 37)])
def test_source_kline_odd_lots_become_integral_shares(
    factor: int, value: str, expected: int
) -> None:
    assert _bar(["2026-10-22", "10", "10", "10", "10", value], factor)["volume_shares"] == expected


@pytest.mark.parametrize(
    "factor,value", [(100, "0.001"), (1, "0.5"), (100, "92233720368547758.08")]
)
def test_source_kline_fractional_or_overflow_shares_block(factor: int, value: str) -> None:
    with pytest.raises((ProviderError, ValueError)):
        _bar(["2026-10-22", "10", "10", "10", "10", value], factor)


def test_ths_unknown_quantity_contract_blocks_standard_record() -> None:
    with pytest.raises(ProviderError, match="quantity unit contract"):
        ths_daily_bar(
            {
                "日期": "2026-10-22",
                "开盘价": "10",
                "收盘价": "10",
                "最高价": "10",
                "最低价": "10",
                "成交量": "100",
                "成交额": "1000",
            }
        )


def test_technical_factor_unknown_output_scaling_blocks_even_empty_leaves() -> None:
    row = dict(dict.fromkeys(TECHNICAL_FACTOR_FIELDS), ts_code="600000.SH", trade_date="20261022")
    with pytest.raises(ProviderError, match="quantity unit contract"):
        normalize_technical_factor_raw([row], "tushare.stock_technical_factor.v1")


@pytest.mark.parametrize("value", [True, -1, 92233720368547759])
def test_tdx_quote_integer_protocol_rejects_invalid_or_overflow(value: object) -> None:
    with pytest.raises((ProviderError, ValueError)):
        hq_lots(value)


def test_tencent_quote_integer_protocol_rejects_overflow() -> None:
    with pytest.raises((ProviderError, ValueError)):
        tencent_lots("92233720368547759")


def test_binary32_amount_preserves_sub_yuan_value_without_float_or_rounding() -> None:
    assert _binary32_amount(0x3FA00000) == Decimal("1.25")
    assert _binary32_amount(1) == Decimal(
        "1.40129846432481707092372958328991613128026194187651577175706828388979108268586060148663818836212158203125e-45"
    )


@pytest.mark.parametrize("bits", [0x7F800000, 0x7FC00000, 0xBF800000])
def test_binary32_amount_rejects_invalid_source_value(bits: int) -> None:
    with pytest.raises(ProviderError):
        _binary32_amount(bits)


@pytest.mark.parametrize("version,unit", [("v1", "share"), ("v2", "lot")])
def test_series_archive_units_follow_envelope_version(version: str, unit: str) -> None:
    rules = quantity_group_rules(
        provider="pytdx_hq",
        schema_version=f"market_data_center.call_auction_market_series.raw.{version}",
        field_group="call_auction_market_series.quantities",
    )
    assert len(rules) == 11
    assert {rule.source_unit for _, rule in rules} == {unit}
    if version == "v2":
        assert all(identity[4].startswith("provider_raw_json.") for identity, _ in rules)


def test_source_limit_up_funds_reject_nonfinite_amount() -> None:
    with pytest.raises(ProviderError):
        limit_up_amount("Infinity")


@pytest.mark.parametrize("symbol", ["SSE:000001", "SSE:113001", "SZSE:399001", "SZSE:123001"])
def test_quote_stock_contract_does_not_accept_index_or_bond(symbol: str) -> None:
    for identity in (hq_identity, tencent_identity):
        with pytest.raises(ProviderError):
            identity(symbol)


def test_source_down_pool_unknown_money_unit_blocks_standard_observation() -> None:
    with pytest.raises(ProviderError, match="quantity unit contract"):
        limit_down_source({"代码": "600000", "封单资金": "123.45"}, date(2026, 10, 22))


@pytest.mark.parametrize("amount", ["123.45", "0", "9007199254740993.01", None, ""])
def test_source_up_funds_are_cny_without_scaling(amount: str | None) -> None:
    record = limit_up_source({"代码": "600000", "封板资金": amount}, date(2026, 10, 22))
    assert record.source_reported_sealed_funds_cny == (
        None if amount in (None, "") else Decimal(amount)
    )


@pytest.mark.parametrize("amount", ["-1", "Infinity", "-Infinity", "bad"])
def test_source_up_funds_reject_invalid_amounts(amount: str) -> None:
    with pytest.raises(ProviderError):
        limit_up_source({"代码": "600000", "封板资金": amount}, date(2026, 10, 22))


def test_source_up_funds_certificate_is_scoped_to_verified_schema() -> None:
    rules = quantity_group_rules(
        provider="akshare",
        schema_version="akshare_eastmoney.current_day_limit_up_pool.v1",
        field_group="today_limit_up.source_observation.quantities",
    )
    assert len(rules) == 1
    assert rules[0][1].source_unit == "cny"
    with pytest.raises(ProviderError):
        quantity_group_rules(
            provider="akshare",
            schema_version="akshare_eastmoney.current_day_limit_up_pool.v2",
            field_group="today_limit_up.source_observation.quantities",
        )


def test_gbjg_unknown_quantity_unit_blocks_standard_capital() -> None:
    with pytest.raises(ProviderError, match="quantity unit contract"):
        _map_share_capital({"变更日期": "2026-10-22", "总股本": "100"}, "SSE:600000")


@pytest.mark.parametrize(
    "schema,group",
    [
        ("akshare.capital.v1", "capital.shares"),
        (
            "akshare_eastmoney.current_day_limit_down_pool.v1",
            "today_limit_down.source_observation.quantities",
        ),
    ],
)
def test_unknown_source_group_cannot_issue_certificate(schema: str, group: str) -> None:
    with pytest.raises(ProviderError, match="quantity unit contract"):
        quantity_group_rules(provider="akshare", schema_version=schema, field_group=group)


def test_bond_volume_and_amount_have_independent_proof_and_unknown_remaining_size() -> None:
    identity = {
        "provider": "tushare",
        "schema_version": "tushare.convertible_bond_daily_bar.v1",
        "instrument_type": "convertible_bond",
    }
    rules = quantity_group_rules(**identity, field_group="convertible_bond_daily_bar.volume")
    assert len(rules) == 1
    assert rules[0][0][4] == "vol"
    assert rules[0][1].source_unit == "bond_lot"
    amount_rules = quantity_group_rules(**identity, field_group="convertible_bond_daily_bar.amount")
    assert len(amount_rules) == 1
    assert amount_rules[0][0][4] == "amount"
    assert amount_rules[0][1].source_unit == "ten_thousand_cny"
    for group in (
        "convertible_bond_daily_bar.remain_size",
        "convertible_bond.daily_bar.quantities",
        "convertible_bond.bond.quantities",
    ):
        with pytest.raises(ProviderError, match="quantity unit contract"):
            quantity_group_rules(**identity, field_group=group)
