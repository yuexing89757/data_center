from datetime import date
from decimal import Decimal
from unittest.mock import Mock

import pytest

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.tushare_limit_down import (
    RAW_SCHEMA_VERSION,
    TushareCurrentDayLimitDownProvider,
    normalize_limit_down_raw,
)

DAY = date(2026, 10, 23)


def row(**changes):
    return dict(
        trade_date="20261023",
        ts_code="002388.SZ",
        name="新亚制程",
        fd_amount=Decimal("190983232.0"),
        first_time=None,
        last_time="92500",
        open_times=0,
        limit_times=Decimal("2.0"),
        limit="D",
        **changes,
    )


def batch_for(source):
    client = Mock()
    client.query.return_value = [source]
    return TushareCurrentDayLimitDownProvider(client).fetch_limit_down_pool(DAY), client


@pytest.mark.parametrize(
    "amount", [None, Decimal(0), Decimal("190983232.0"), Decimal("9007199254740993.125")]
)
def test_real_contract_exact_money_raw_and_replay(tmp_path, amount):
    from uuid import uuid4

    from market_data_center.domain.ingestion import RawFileFormat, RawManifest
    from market_data_center.raw_store import LocalRawStore

    source = row()
    source["fd_amount"] = amount
    batch, client = batch_for(source)
    item = batch.records[0]
    assert item.source_code == "tushare"
    assert item.source_reported_sealed_funds_cny == amount
    assert item.first_limit_down_at is None
    assert item.last_limit_down_at.strftime("%H:%M:%S") == "09:25:00"
    assert item.open_count == 0 and item.consecutive_limit_down_days == 2
    assert item.symbol == "SZSE:002388"
    assert client.query.call_args.args == ("limit_list_d",)
    assert client.query.call_args.kwargs["params"] == {"trade_date": "20261023", "limit_type": "D"}
    store = LocalRawStore(tmp_path)
    original_id = uuid4()
    stored = store.write_jsonl(
        provider="tushare",
        dataset="today_limit_down_source",
        partition_date=DAY,
        ingestion_id=original_id,
        rows=batch.raw_rows,
        schema_version=RAW_SCHEMA_VERSION,
    )
    manifest = RawManifest(
        uuid4(),
        original_id,
        stored.object_path,
        RawFileFormat.JSONL,
        stored.content_sha256,
        stored.byte_size,
        stored.row_count,
        stored.schema_version,
    )
    original = (tmp_path / stored.object_path).read_bytes()
    for _ in range(2):
        assert (
            tuple(normalize_limit_down_raw(store.read_jsonl(manifest), RAW_SCHEMA_VERSION, DAY))
            == batch.records
        )
    assert (tmp_path / stored.object_path).read_bytes() == original


@pytest.mark.parametrize(
    "field,value",
    [
        ("fd_amount", "0.00001"),
        ("fd_amount", "1e26"),
        ("open_times", "2147483648"),
        ("fd_amount", "NaN"),
        ("fd_amount", "Infinity"),
        ("fd_amount", "-1"),
        ("fd_amount", "oops"),
        ("fd_amount", 0.1),
        ("open_times", "0.5"),
        ("open_times", "-1"),
        ("limit_times", "0"),
        ("limit", "U"),
        ("trade_date", "20261022"),
        ("ts_code", "2388.SZ"),
        ("last_time", "153000"),
        ("first_time", "093000"),
    ],
)
def test_invalid_source_never_becomes_standard(field, value):
    source = row()
    source[field] = value
    with pytest.raises(ProviderError):
        batch, _ = batch_for(source)
        _ = batch.records


def test_unknown_schema_missing_fields_and_cap_are_rejected():
    source = row()
    source["fd_amount"] = "123"
    with pytest.raises(ProviderError):
        normalize_limit_down_raw([source], "unknown", DAY)
    del source["fd_amount"]
    with pytest.raises(ProviderError):
        batch, _ = batch_for(source)
        _ = batch.records
    with pytest.raises(ProviderError):
        normalize_limit_down_raw([row()] * 2500, RAW_SCHEMA_VERSION, DAY)


def test_empty_response_is_not_a_request_error():
    client = Mock()
    client.query.return_value = []
    assert TushareCurrentDayLimitDownProvider(client).fetch_limit_down_pool(DAY).records == ()


def test_permission_error_not_retried_and_transient_retry_bounded():
    client = Mock()
    client.query.side_effect = ProviderError("permission denied")
    with pytest.raises(ProviderError):
        TushareCurrentDayLimitDownProvider(client).fetch_limit_down_pool(DAY)
    assert client.query.call_count == 1
    client = Mock()
    client.query.side_effect = [TimeoutError(), [row()]]
    assert TushareCurrentDayLimitDownProvider(client).fetch_limit_down_pool(DAY).records
    assert client.query.call_count == 2
    client = Mock()
    client.query.side_effect = TimeoutError()
    with pytest.raises(ProviderError):
        TushareCurrentDayLimitDownProvider(client).fetch_limit_down_pool(DAY)
    assert client.query.call_count == 2


def test_service_records_tushare_provider_and_endpoint(tmp_path):
    from market_data_center.domain.ingestion import ProviderCode
    from market_data_center.domain.today_limit_down import TodayLimitDownDependencies, UpstreamState
    from market_data_center.raw_store import LocalRawStore
    from market_data_center.today_limit_down_service import TodayLimitDownFillService

    client = Mock()
    client.query.return_value = [row()]
    persistence = Mock()
    persistence.dependencies.return_value = TodayLimitDownDependencies(
        DAY, True, UpstreamState.SUCCEEDED, UpstreamState.SUCCEEDED, True
    )
    TodayLimitDownFillService(
        persistence=persistence,
        raw_store=LocalRawStore(tmp_path),
        provider_factory=lambda: TushareCurrentDayLimitDownProvider(client),
        provider_code=ProviderCode.TUSHARE,
    ).fill(DAY)
    run = persistence.commit_snapshot.call_args.kwargs["run"]
    assert run.provider_code is ProviderCode.TUSHARE
    assert (
        run.request_params["api_name"] == "limit_list_d" and run.request_params["limit_type"] == "D"
    )
    assert run.request_params["sealed_funds_unit"] == "cny"
    assert (
        persistence.commit_snapshot.call_args.kwargs["source_records"][0].source_code == "tushare"
    )
    assert "tushare" in str(next(tmp_path.rglob("*.jsonl")))


def test_service_rejects_mismatched_provider_lineage(tmp_path):
    from market_data_center.domain.ingestion import ProviderCode
    from market_data_center.domain.today_limit_down import TodayLimitDownDependencies, UpstreamState
    from market_data_center.raw_store import LocalRawStore
    from market_data_center.today_limit_down_service import TodayLimitDownFillService

    client = Mock()
    client.query.return_value = [row()]
    persistence = Mock()
    persistence.dependencies.return_value = TodayLimitDownDependencies(
        DAY, True, UpstreamState.SUCCEEDED, UpstreamState.SUCCEEDED, True
    )
    TodayLimitDownFillService(
        persistence=persistence,
        raw_store=LocalRawStore(tmp_path),
        provider_factory=lambda: TushareCurrentDayLimitDownProvider(client),
        provider_code=ProviderCode.AKSHARE,
    ).fill(DAY)
    persistence.commit_snapshot.assert_not_called()
    persistence.commit_failed.assert_called_once()
