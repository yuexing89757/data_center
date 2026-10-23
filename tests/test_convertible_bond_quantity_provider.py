from datetime import UTC, date, datetime
from decimal import Decimal, localcontext
from unittest.mock import Mock
from uuid import uuid4

import pytest

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    RawFileFormat,
    RawManifest,
)
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.quantity_units import validate_quantity_metadata
from market_data_center.providers.tushare import (
    CB_BASIC_FIELDS,
    CB_DAILY_FIELDS,
    TushareProvider,
    normalize_tushare_raw,
)
from market_data_center.quantity_certification import quantity_certificates


class Client:
    def __init__(self, row):
        self.row = row

    def query(self, api_name, *, params, fields):
        return [self.row]


def daily_row(**updates):
    return (
        dict(
            dict.fromkeys(CB_DAILY_FIELDS, ""),
            ts_code="113001.SH",
            trade_date="20261022",
            vol="0.1",
        )
        | updates
    )


@pytest.mark.parametrize(
    "source,expected",
    [
        (None, None),
        ("", None),
        ("0", Decimal(0)),
        ("1.25", Decimal("12500")),
        ("12345678901234567890.123456789", Decimal("123456789012345678901234.56789")),
    ],
)
def test_bond_daily_amount_is_ten_thousand_cny_in_adapter_and_v1_replay(source, expected):
    row = daily_row(amount=source)
    batch = TushareProvider(Client(row)).fetch_convertible_bond_daily_bars(
        "SSE:113001", date(2026, 10, 22), date(2026, 10, 22)
    )
    with localcontext() as context:
        context.prec = 6
        assert batch.records[0].amount == expected
        replayed = normalize_tushare_raw(
            DatasetCode.CONVERTIBLE_BOND_DAILY_BAR,
            batch.schema_version,
            batch.raw_rows,
            batch.request_params,
        )
    assert replayed[0].amount == expected
    assert batch.records[0].volume == 1
    assert batch.records[0].remain_size is None
    assert batch.raw_rows[0]["amount"] == ("" if source is None else source)


@pytest.mark.parametrize("value", ["0", "1", "NaN", "-1"])
def test_nonmissing_unknown_remaining_size_blocks_new_and_v1_replay(value):
    batch = TushareProvider(
        Client(daily_row(amount="1", remain_size=value))
    ).fetch_convertible_bond_daily_bars("SSE:113001", date(2026, 10, 22), date(2026, 10, 22))
    with pytest.raises(ProviderError, match="quantity unit contract"):
        _ = batch.records
    with pytest.raises(ProviderError, match="quantity unit contract"):
        normalize_tushare_raw(
            DatasetCode.CONVERTIBLE_BOND_DAILY_BAR,
            batch.schema_version,
            batch.raw_rows,
            batch.request_params,
        )


def test_missing_remaining_size_is_not_required_or_fabricated():
    row = daily_row(amount="1.25")
    row.pop("remain_size", None)
    batch = TushareProvider(Client(row)).fetch_convertible_bond_daily_bars(
        "SSE:113001", date(2026, 10, 22), date(2026, 10, 22)
    )
    assert batch.records[0].remain_size is None


@pytest.mark.parametrize(
    "source,expected",
    [
        (None, None),
        ("0", Decimal(0)),
        ("12345678901234567890.123456789", Decimal("12345678901234567890.123456789")),
    ],
)
def test_basic_issue_size_is_already_cny_and_v1_replay_is_exact(source, expected):
    row = dict(
        dict.fromkeys(CB_BASIC_FIELDS, ""),
        ts_code="113001.SH",
        stock_ts_code="600000.SH",
        bond_short_name="synthetic",
        bond_full_name="synthetic bond",
        issue_size=source,
    )
    batch = TushareProvider(Client(row)).fetch_convertible_bonds()
    assert batch.records[0].issue_size == expected
    replayed = normalize_tushare_raw(
        DatasetCode.CONVERTIBLE_BOND, batch.schema_version, batch.raw_rows, batch.request_params
    )
    assert replayed[0].issue_size == expected


@pytest.mark.parametrize("source", ["NaN", "Infinity", "-1"])
def test_bond_money_rejects_nonfinite_or_negative_values(source):
    batch = TushareProvider(Client(daily_row(amount=source))).fetch_convertible_bond_daily_bars(
        "SSE:113001", date(2026, 10, 22), date(2026, 10, 22)
    )
    with pytest.raises(ProviderError):
        _ = batch.records
    with pytest.raises(ProviderError):
        normalize_tushare_raw(
            DatasetCode.CONVERTIBLE_BOND_DAILY_BAR,
            batch.schema_version,
            batch.raw_rows,
            batch.request_params,
        )


@pytest.mark.parametrize("basic", [False, True])
def test_bond_live_metadata_and_atomic_certificate_preparation_use_independent_money_rule(basic):
    if basic:
        row = dict(
            dict.fromkeys(CB_BASIC_FIELDS, ""),
            ts_code="113001.SH",
            stock_ts_code="600000.SH",
            bond_short_name="test",
            bond_full_name="synthetic",
            issue_size="12500",
        )
        batch = TushareProvider(Client(row)).fetch_convertible_bonds()
        dataset = DatasetCode.CONVERTIBLE_BOND
        expected = {"convertible_bond.bond.quantities"}
    else:
        batch = TushareProvider(Client(daily_row(amount="1.25"))).fetch_convertible_bond_daily_bars(
            "SSE:113001", date(2026, 10, 22), date(2026, 10, 22)
        )
        dataset = DatasetCode.CONVERTIBLE_BOND_DAILY_BAR
        expected = {"convertible_bond_daily_bar.volume", "convertible_bond_daily_bar.amount"}
    validate_quantity_metadata(
        provider="tushare", schema_version=batch.schema_version, request_params=batch.request_params
    )
    assert len(batch.records) == 1
    now = datetime.now(UTC)
    run = IngestionRun(
        uuid4(),
        ProviderCode.TUSHARE,
        dataset,
        IngestionStatus.SUCCEEDED,
        now,
        started_at=now,
        finished_at=now,
        request_params=batch.request_params,
        fetched_rows=1,
        accepted_rows=1,
    )
    manifest = RawManifest(
        uuid4(),
        run.ingestion_id,
        "synthetic/bond.jsonl",
        RawFileFormat.JSONL,
        "a" * 64,
        1,
        1,
        batch.schema_version,
    )
    certificates = quantity_certificates(Mock(), run, manifest)
    assert {certificate["field_group"] for certificate in certificates} == expected
    invalid = dict(batch.request_params, source_amount_unit="thousand_cny")
    with pytest.raises(ProviderError, match="metadata contradicts"):
        validate_quantity_metadata(
            provider="tushare", schema_version=batch.schema_version, request_params=invalid
        )
