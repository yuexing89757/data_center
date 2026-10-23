from dataclasses import dataclass, replace
from uuid import uuid4

import pytest

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.providers import quantity_units
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.quantity_units import quantity_group_rules
from market_data_center.quantity_certification import (
    build_quantity_certificates,
    check_standard_quantities,
)


def manifest(schema: str = "pytdx.remote_daily_bar.v2") -> RawManifest:
    return RawManifest(
        raw_id=uuid4(),
        ingestion_id=uuid4(),
        object_path="synthetic/input.jsonl",
        file_format=RawFileFormat.JSONL,
        content_sha256="a" * 64,
        byte_size=1,
        row_count=1,
        schema_version=schema,
    )


def test_new_stock_batch_certificate_binds_raw_and_normalizer() -> None:
    raw = manifest()
    certificates = build_quantity_certificates(
        ingestion_id=raw.ingestion_id, provider="pytdx", dataset="daily_bar", manifest=raw
    )
    assert {item["field_group"] for item in certificates} == {
        "daily_bar.volume",
        "daily_bar.amount",
    }
    assert all(item["status"] == "certified" for item in certificates)
    changed = build_quantity_certificates(
        ingestion_id=raw.ingestion_id,
        provider="pytdx",
        dataset="daily_bar",
        manifest=replace(raw, content_sha256="b" * 64),
    )
    assert certificates[0]["evidence_hash"] != changed[0]["evidence_hash"]


def test_unknown_quantity_schema_cannot_be_certified() -> None:
    raw = manifest("pytdx.remote_daily_bar.unknown")
    with pytest.raises(ProviderError, match="quantity"):
        build_quantity_certificates(
            ingestion_id=raw.ingestion_id, provider="pytdx", dataset="daily_bar", manifest=raw
        )


@pytest.mark.parametrize(
    "provider,schema,field,unit",
    [
        ("akshare", "akshare.daily_bar.v1", "成交额", "cny"),
        ("akshare", "akshare.daily_bar.v2", "成交额", "cny"),
        ("pytdx", "pytdx.remote_daily_bar.v1", "amount", "cny"),
        ("pytdx", "pytdx.remote_daily_bar.v2", "amount", "cny"),
        ("pytdx", "pytdx.local_daily_bar.v1", "amount", "cny"),
        ("pytdx", "pytdx.local_daily_bar.v2", "amount", "cny"),
        ("pytdx", "pytdx.local_daily_bar.v3", "amount", "cny"),
        ("baostock", "baostock.daily_bar.v1", "amount", "cny"),
        ("tushare", "tushare.daily_bar.v1", "amount", "thousand_cny"),
    ],
)
def test_daily_bar_amount_has_its_own_field_evidence(provider, schema, field, unit):
    amounts = quantity_group_rules(
        provider=provider, schema_version=schema, field_group="daily_bar.amount"
    )
    volumes = quantity_group_rules(
        provider=provider, schema_version=schema, field_group="daily_bar.volume"
    )
    assert len(amounts) == 1
    assert amounts[0][0][4] == field
    assert amounts[0][1].source_unit == unit
    assert {identity[4] for identity, _ in volumes} <= {"volume", "vol", "成交量"}
    assert not {identity for identity, _ in amounts} & {identity for identity, _ in volumes}


def test_volume_only_registration_cannot_issue_amount_certificate(monkeypatch):
    monkeypatch.setattr(
        quantity_units,
        "QUANTITY_UNIT_RULES",
        {
            identity: rule
            for identity, rule in quantity_units.QUANTITY_UNIT_RULES.items()
            if identity[4] not in {"amount", "成交额"}
        },
    )
    raw = manifest()
    with pytest.raises(ProviderError, match="quantity unit contract"):
        build_quantity_certificates(
            ingestion_id=raw.ingestion_id, provider="pytdx", dataset="daily_bar", manifest=raw
        )


def test_rights_only_capital_proof_does_not_depend_on_unknown_gbjg():
    raw = manifest("akshare.capital.v1")
    certificates = build_quantity_certificates(
        ingestion_id=raw.ingestion_id, provider="akshare", dataset="capital", manifest=raw
    )
    assert {certificate["field_group"] for certificate in certificates} == {
        "capital.rights_issue.quantities"
    }
    rules = quantity_group_rules(
        provider="akshare",
        schema_version=raw.schema_version,
        field_group="capital.rights_issue.quantities",
    )
    assert len(rules) == 1
    assert rules[0][0][1:] == (
        "stock_history_dividend_detail",
        "akshare.capital.v1",
        "stock",
        "基准股本",
    )
    with pytest.raises(ProviderError, match="quantity unit contract"):
        quantity_group_rules(
            provider="akshare", schema_version=raw.schema_version, field_group="capital.shares"
        )


def test_future_source_endpoint_needs_explicit_group_membership(monkeypatch):
    rules = dict(quantity_units.QUANTITY_UNIT_RULES)
    for field, unit in (("volume", "lot"), ("amount", "cny")):
        rules["pytdx", "future_daily_endpoint", "pytdx.future_daily_bar.v1", "stock", field] = (
            quantity_units.QuantityUnitRule(unit, "synthetic future source")
        )
    monkeypatch.setattr(quantity_units, "QUANTITY_UNIT_RULES", rules)
    raw = manifest("pytdx.future_daily_bar.v1")
    with pytest.raises(ProviderError, match="quantity unit contract"):
        build_quantity_certificates(
            ingestion_id=raw.ingestion_id, provider="pytdx", dataset="daily_bar", manifest=raw
        )


def test_same_endpoint_extra_fields_do_not_expand_existing_proof(monkeypatch):
    rules = dict(quantity_units.QUANTITY_UNIT_RULES)
    rules["pytdx", "get_security_bars", "pytdx.remote_daily_bar.v2", "stock", "future_amount"] = (
        quantity_units.QuantityUnitRule("cny", "future field")
    )
    monkeypatch.setattr(quantity_units, "QUANTITY_UNIT_RULES", rules)
    for group, field in (("daily_bar.volume", "volume"), ("daily_bar.amount", "amount")):
        proof = quantity_group_rules(
            provider="pytdx", schema_version="pytdx.remote_daily_bar.v2", field_group=group
        )
        assert len(proof) == 1
        assert proof[0][0][4] == field


@pytest.mark.parametrize(
    "dataset,schema,expected",
    [
        ("convertible_bond", "tushare.convertible_bond.v1", {"convertible_bond.bond.quantities"}),
        (
            "convertible_bond_daily_bar",
            "tushare.convertible_bond_daily_bar.v1",
            {"convertible_bond_daily_bar.volume", "convertible_bond_daily_bar.amount"},
        ),
    ],
)
def test_bond_certificates_prove_only_independent_documented_fields(dataset, schema, expected):
    raw = manifest(schema)
    certificates = build_quantity_certificates(
        ingestion_id=raw.ingestion_id, provider="tushare", dataset=dataset, manifest=raw
    )
    assert {certificate["field_group"] for certificate in certificates} == expected
    assert all(certificate["status"] == "certified" for certificate in certificates)


@dataclass
class Fact:
    volume: object
    cumulative_volume: object = None


@pytest.mark.parametrize("value", [True, 0.5, -1, 2**63])
def test_standard_fact_quantity_is_an_exact_bounded_integer(value: object) -> None:
    with pytest.raises(ProviderError, match="quantity"):
        check_standard_quantities((Fact(value),))


@pytest.mark.parametrize("value", [None, 0, 37])
def test_null_zero_and_odd_share_quantities_preserved(value: object) -> None:
    fact = Fact(value)
    check_standard_quantities((fact,))
    assert fact.volume is value


@dataclass
class HoldingChange:
    hold_change: object


@pytest.mark.parametrize("value", [None, 0, -37, -(2**63), 2**63 - 1])
def test_signed_share_change_is_preserved(value):
    check_standard_quantities((HoldingChange(value),))


@pytest.mark.parametrize("value", [True, 0.5, 2**63, -(2**63) - 1])
def test_signed_share_change_is_exact_and_bounded(value):
    with pytest.raises(ProviderError, match="signed integral shares"):
        check_standard_quantities((HoldingChange(value),))
