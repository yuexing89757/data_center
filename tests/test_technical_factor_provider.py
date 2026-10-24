from datetime import date
from decimal import Decimal

import pytest

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.tushare_technical_factor import (
    SOURCE_FACTOR_PARAMETERS,
    TECHNICAL_FACTOR_FIELDS,
    TushareTechnicalFactorProvider,
    normalize_technical_factor_raw,
)


def source_row():
    return dict(dict.fromkeys(TECHNICAL_FACTOR_FIELDS), ts_code="000001.SZ", trade_date="20261009")


class Client:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def query(self, api_name, *, params, fields):
        self.calls.append((api_name, params, fields))
        return self.rows


def test_full_catalog_source_fields_and_raw_replay():
    row = source_row()
    row.update(
        ema_qfq_10="12.34567890123456789",
        kdj_qfq="-2.5",
        kdj_k_qfq="0",
        macd_hfq=Decimal("-0.2"),
        downdays="0",
        topdays="3.0",
        open="11.2",
    )
    client = Client([row])
    batch = TushareTechnicalFactorProvider(client).fetch_snapshot(date(2026, 10, 9))
    record = batch.records[0]
    assert batch.request_params["trade_date"] == "2026-10-09"
    assert len(TECHNICAL_FACTOR_FIELDS) == 261
    assert len(set(TECHNICAL_FACTOR_FIELDS)) == 261
    assert client.calls[0][:2] == ("stk_factor_pro", {"trade_date": "20261009"})
    assert record.forward_adjusted["ema_10"] == Decimal("12.34567890123456789")
    assert record.forward_adjusted["kdj_j"] == Decimal("-2.5")
    assert record.forward_adjusted["kdj_k"] == Decimal(0)
    assert record.backward_adjusted["macd"] == Decimal("-0.2")
    assert record.common["down_days"] == 0
    assert record.common["high_range_days"] == 3
    assert record.unadjusted["atr"] is None
    assert batch.raw_rows[0]["open"] == "11.2"
    assert normalize_technical_factor_raw(batch.raw_rows, batch.schema_version) == list(
        batch.records
    )


def test_catalog_preserves_basis_specific_default_parameters():
    assert len(SOURCE_FACTOR_PARAMETERS) == 222
    assert SOURCE_FACTOR_PARAMETERS["bbi_bfq"]["M4"] == 20
    assert SOURCE_FACTOR_PARAMETERS["bbi_hfq"]["M4"] == 21
    assert SOURCE_FACTOR_PARAMETERS["bbi_qfq"]["M4"] == 22
    assert SOURCE_FACTOR_PARAMETERS["ma_qfq_250"] == {"N": 250}
    assert SOURCE_FACTOR_PARAMETERS["macd_qfq"] == {"SHORT": 12, "LONG": 26, "M": 9}


@pytest.mark.parametrize(
    "field,value",
    [
        ("obv_bfq", "NaN"),
        ("emv_qfq", "oops"),
        ("maemv_hfq", "Infinity"),
        ("macd_qfq", "NaN"),
        ("macd_qfq", "Infinity"),
        ("macd_qfq", "oops"),
        ("macd_qfq", 1.2),
        ("macd_qfq", True),
        ("updays", "-1"),
        ("updays", "1.2"),
        ("ts_code", "invalid"),
        ("trade_date", "20260230"),
    ],
)
def test_invalid_cells_abort(field, value):
    row = source_row()
    row[field] = value
    with pytest.raises(ProviderError):
        batch = TushareTechnicalFactorProvider(Client([row])).fetch_snapshot(date(2026, 10, 9))
        _ = batch.records


def test_structure_date_duplicates_and_caps():
    row = source_row()
    for rows in (
        [],
        [row] * 10000,
        [dict(row, trade_date="20261008")],
        [{k: v for k, v in row.items() if k != "macd_qfq"}],
        [row, dict(row, macd_qfq="1")],
        [dict(row, obv_bfq="1"), dict(row, obv_bfq="2")],
    ):
        with pytest.raises(ProviderError):
            _ = (
                TushareTechnicalFactorProvider(Client(rows))
                .fetch_snapshot(date(2026, 10, 9))
                .records
            )
    batch = TushareTechnicalFactorProvider(Client([row, row])).fetch_snapshot(date(2026, 10, 9))
    assert len(batch.records) == 1
    with pytest.raises(ProviderError):
        normalize_technical_factor_raw(batch.raw_rows, "unknown")


def test_transient_retries_paced_and_permission_not_retried():
    class Flaky(Client):
        def query(self, api_name, *, params, fields):
            self.calls.append(api_name)
            if len(self.calls) < 3:
                raise TimeoutError("timeout")
            return self.rows

    sleeps = []
    client = Flaky([source_row()])
    provider = TushareTechnicalFactorProvider(client, sleeper=sleeps.append)
    assert provider.fetch_snapshot(date(2026, 10, 9)).records
    assert len(client.calls) == 3
    assert sleeps == [2, 2]

    class Denied(Client):
        def query(self, api_name, *, params, fields):
            self.calls.append(api_name)
            raise ProviderError("permission denied")

    denied = Denied([])
    with pytest.raises(ProviderError):
        TushareTechnicalFactorProvider(denied, sleeper=sleeps.append).fetch_snapshot(
            date(2026, 10, 9)
        )
    assert len(denied.calls) == 1


def test_retry_budget_and_next_request_are_bounded_and_paced():
    class TimedOut(Client):
        def query(self, api_name, *, params, fields):
            self.calls.append(api_name)
            raise TimeoutError("timeout")

    sleeps = []
    client = TimedOut([])
    with pytest.raises(ProviderError):
        TushareTechnicalFactorProvider(client, sleeper=sleeps.append).fetch_snapshot(
            date(2026, 10, 9)
        )
    assert len(client.calls) == 3
    assert sleeps == [2, 2]
    provider = TushareTechnicalFactorProvider(Client([source_row()]), sleeper=sleeps.append)
    provider.fetch_snapshot(date(2026, 10, 9))
    provider.fetch_snapshot(date(2026, 10, 9))
    assert sleeps == [2, 2, 2]


def test_default_requires_token(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    with pytest.raises(ProviderError, match="TUSHARE_TOKEN"):
        TushareTechnicalFactorProvider.default()


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf")])
def test_invalid_request_pacing_is_rejected(interval):
    with pytest.raises(ValueError):
        TushareTechnicalFactorProvider(Client([]), request_interval_seconds=interval)


def test_replay_rejects_malformed_structure_and_preserves_bse():
    with pytest.raises(ProviderError):
        normalize_technical_factor_raw([None], "tushare.stock_technical_factor.v1")
    row = source_row()
    row["ts_code"] = "920000.BJ"
    assert (
        normalize_technical_factor_raw([row], "tushare.stock_technical_factor.v1")[0].symbol
        == "BSE:920000"
    )


@pytest.mark.parametrize("field", ["macd_qfq", "ts_code", "open"])
def test_missing_source_field_retains_raw_before_normalization_fails(field):
    row = source_row()
    del row[field]
    batch = TushareTechnicalFactorProvider(Client([row])).fetch_snapshot(date(2026, 10, 9))
    assert len(batch.raw_rows) == 1
    assert field not in batch.raw_rows[0]
    with pytest.raises(ProviderError, match=f"missing fields: {field}"):
        _ = batch.records
    with pytest.raises(ProviderError, match=f"missing fields: {field}"):
        normalize_technical_factor_raw(batch.raw_rows, batch.schema_version)


@pytest.mark.parametrize(
    "value", ["202610 9", "\uff12\uff10\uff12\uff16\uff11\uff10\uff10\uff19", "20261009 "]
)
def test_source_date_requires_exact_ascii_digits(value):
    row = dict(source_row(), trade_date=value)
    batch = TushareTechnicalFactorProvider(Client([row])).fetch_snapshot(date(2026, 10, 9))
    assert batch.raw_rows[0]["trade_date"] == value
    with pytest.raises(ProviderError):
        _ = batch.records
    with pytest.raises(ProviderError):
        normalize_technical_factor_raw(batch.raw_rows, batch.schema_version)


def test_replay_deduplicates_equal_records():
    row = source_row()
    records = normalize_technical_factor_raw([row, dict(row)], "tushare.stock_technical_factor.v1")
    assert len(records) == 1
    assert records[0].symbol == "SZSE:000001"


@pytest.fixture
def synthetic_quantity_contract(monkeypatch):
    """Isolate parsing/retry tests; real unverified-source blocking has separate regressions."""
    monkeypatch.setattr(
        "market_data_center.providers.tushare_technical_factor.quantity_unit_rule",
        lambda **kwargs: None,
    )
