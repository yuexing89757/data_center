"""Source facts, bounded reads and authenticated three-pool contracts."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from http.client import IncompleteRead
from pathlib import Path
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers import xuangubao_limit_up as existing_source
from market_data_center.providers.xuangubao_pools import (
    MAX_RESPONSE_BYTES,
    XuangubaoPoolsInvalid,
    XuangubaoPoolsProvider,
    XuangubaoPoolsUpstream,
)
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

PATH = "/api/v1/realtime/xuangubao/stock-pools"
KEY = "test-xuangubao-key-000000000000000"
NOW = datetime(2026, 10, 29, 9, tzinfo=UTC)
EVENT = int(datetime(2026, 10, 28, 2, tzinfo=UTC).timestamp())
ROW = {
    "symbol": "001234.SZ",
    "stock_chi_name": "测试股份",
    "change_percent": "-0.023456789",
    "limit_up_days": 0,
    "yesterday_limit_up_days": 2,
    "first_limit_up": EVENT,
    "last_limit_up": EVENT,
    "last_break_limit_up": EVENT,
    "yesterday_last_limit_up": EVENT - 86400,
    "surge_reason": {"related_plates": [{"plate_name": "测试题材"}]},
}


def provider(payload, calls):
    def request(req, timeout):
        calls.append(req)
        assert timeout <= 10
        if isinstance(payload, Exception):
            raise payload
        return payload if isinstance(payload, bytes) else json.dumps(payload).encode()

    return XuangubaoPoolsProvider(request_bytes=request, clock=lambda: NOW)


def api(payload, calls):
    return TestClient(
        create_app(
            settings=ApiSettings(
                _env_file=None,
                fastapi_database_url=SecretStr("unused"),
                fastapi_api_key=SecretStr(KEY),
            ),
            query_service=object(),
            auction_indicative_service=object(),
            xuangubao_pools_provider=provider(payload, calls),
        )
    )


def test_reversed_limit_up_events_return_upstream_error():
    row = {**ROW, "first_limit_up": EVENT + 3600, "last_limit_up": EVENT}
    response = api({"code": 20000, "data": [row]}, []).get(
        PATH, params={"pool_type": "limit_up"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502


@pytest.mark.parametrize(
    "kind,upstream",
    [
        ("limit_up", "limit_up"),
        ("broken_limit_up", "limit_up_broken"),
        ("previous_limit_up", "yesterday_limit_up"),
    ],
)
def test_three_pool_source_parameters_and_units(kind, upstream):
    calls = []
    page = provider({"code": 20000, "data": [ROW]}, calls).fetch(
        pool_type=kind, trade_date=date(2026, 10, 28)
    )
    assert len(calls) == 1
    assert urlsplit(calls[0].full_url).netloc == "flash-api.xuangubao.cn"
    assert parse_qs(urlsplit(calls[0].full_url).query) == {
        "pool_name": [upstream],
        "date": ["2026-10-28"],
    }
    item = page.items[0]
    assert item.symbol == "SZSE:001234" and item.code == "001234"
    assert item.change_pct == Decimal("-2.345678900")
    assert item.consecutive_limit_up_days == 0
    assert item.previous_consecutive_limit_up_days == 2
    assert item.topics == ("测试题材",)
    assert page.source_event_date == date(2026, 10, 28)
    assert page.date_verification == "event_dates_consistent"


@pytest.mark.parametrize("day", [None, date(2026, 10, 29)])
def test_latest_and_today_omit_source_date(day):
    calls = []
    row = ROW | {
        k: EVENT + 86400 for k in ("first_limit_up", "last_limit_up", "last_break_limit_up")
    }
    page = provider({"code": 20000, "data": [row]}, calls).fetch(
        pool_type="limit_up", trade_date=day
    )
    assert "date" not in parse_qs(urlsplit(calls[0].full_url).query)
    assert page.requested_date == day
    assert page.source_event_date == date(2026, 10, 29)


def test_missing_and_sentinel_values_do_not_become_zero_or_today():
    row = {
        "symbol": "600000.SS",
        "yesterday_last_limit_up": EVENT,
        "limit_up_days": -1,
        "yesterday_limit_up_days": None,
    }
    page = provider({"code": 20000, "data": [row]}, []).fetch(pool_type="previous_limit_up")
    item = page.items[0]
    assert item.change_pct is None and item.name is None
    assert item.consecutive_limit_up_days is None
    assert item.previous_consecutive_limit_up_days is None
    assert page.source_event_date is None and page.date_verification == "unverified"
    assert item.previous_last_limit_up_at is not None


@pytest.mark.parametrize(
    "change",
    [
        {"symbol": "600000.XX"},
        {"symbol": "600000.SZ"},
        {"limit_up_days": True},
        {"limit_up_days": "1.5"},
        {"change_percent": "NaN"},
        {"last_limit_up": -1},
        {"surge_reason": []},
        {"last_limit_up": EVENT + 86400},
        {"yesterday_last_limit_up": int(NOW.timestamp()) + 1},
    ],
)
def test_invalid_or_conflicting_source_fields_are_errors(change):
    with pytest.raises(XuangubaoPoolsUpstream):
        provider({"code": 20000, "data": [ROW | change]}, []).fetch(
            pool_type="limit_up", trade_date=date(2026, 10, 28)
        )


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"code": 400, "data": []},
        {"code": 20000, "data": None},
        {"code": 20000, "data": [ROW, ROW]},
        {"code": 20000, "data": [ROW] * 2001},
        b"not json",
        b"x" * (MAX_RESPONSE_BYTES + 1),
        TimeoutError("secret"),
        IncompleteRead(b"secret", 100),
    ],
    ids=[
        "missing",
        "business",
        "null",
        "duplicate",
        "rows",
        "json",
        "bytes",
        "timeout",
        "truncated",
    ],
)
def test_source_failures_are_sanitized(payload):
    response = api(payload, []).get(
        PATH, params={"pool_type": "limit_up"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502 and "secret" not in response.text


def test_explicit_date_mismatch_rejected_latest_stale_date_preserved():
    p = provider({"code": 20000, "data": [ROW]}, [])
    with pytest.raises(XuangubaoPoolsUpstream):
        p.fetch(pool_type="limit_up", trade_date=date(2026, 10, 27))
    assert p.fetch(pool_type="limit_up").source_event_date == date(2026, 10, 28)


def test_invalid_parameters_do_not_request_source():
    calls = []
    p = provider({}, calls)
    for params in [
        {"pool_type": "other"},
        {"pool_type": "limit_up", "trade_date": date(2026, 10, 30)},
    ]:
        with pytest.raises(XuangubaoPoolsInvalid):
            p.fetch(**params)
    assert calls == []


def test_api_auth_empty_response_timestamp_and_validation():
    calls = []
    client = api({"code": 20000, "data": []}, calls)
    assert client.get(PATH, params={"pool_type": "limit_up"}).status_code == 401
    for params in (
        {"pool_type": "bad"},
        {"pool_type": "limit_up", "trade_date": "2026-10-30"},
        {"pool_type": "limit_up", "trade_date": "2026-02-30"},
        {},
    ):
        assert client.get(PATH, params=params, headers={"X-API-Key": KEY}).status_code == 422
    assert calls == []
    data = client.get(PATH, params={"pool_type": "limit_up"}, headers={"X-API-Key": KEY}).json()
    assert data["items"] == [] and data["returned_count"] == 0
    assert data["observed_at"] == "2026-10-29 17:00:00"
    assert data["source_event_date"] is None and data["persisted"] is False


def test_api_decimal_and_timestamp_serialization_and_saved_contract():
    client = api({"code": 20000, "data": [ROW]}, [])
    response = client.get(
        PATH,
        params={"pool_type": "previous_limit_up", "trade_date": "2026-10-28"},
        headers={"X-API-Key": KEY},
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert Decimal(item["change_pct"]) == Decimal("-2.3456789")
    assert item["last_limit_up_at"] == "2026-10-28 10:00:00"
    schema = client.get("/openapi.json").json()
    saved = json.loads(
        (Path(__file__).parents[1] / "contracts/fastapi-openapi-v1.json").read_text(encoding="utf8")
    )
    assert saved["paths"][PATH] == schema["paths"][PATH]
    for name, model in schema["components"]["schemas"].items():
        if name.startswith("Xuangubao"):
            assert saved["components"]["schemas"][name] == model


def test_duplicate_json_keys_fail_at_provider_boundary():
    with pytest.raises(XuangubaoPoolsUpstream):
        provider(b'{"code":400,"code":20000,"data":[]}', []).fetch(pool_type="limit_up")


@pytest.mark.parametrize("pool", ["limit_up", "limit_up_broken", "yesterday_limit_up"])
def test_shared_transport_preserves_historical_query_and_total_deadline(monkeypatch, pool):
    raw, context, http, timer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    connect = MagicMock(return_value=raw)
    monkeypatch.setattr(existing_source, "_connect_http", connect)
    monkeypatch.setattr(existing_source.ssl, "create_default_context", lambda: context)
    monkeypatch.setattr(existing_source, "HTTPSConnection", MagicMock(return_value=http))
    monkeypatch.setattr(existing_source, "Timer", timer)
    monkeypatch.setattr(existing_source.time, "monotonic", lambda: 100)
    response = http.getresponse.return_value.__enter__.return_value
    response.status, response.read.return_value = 200, b"{}"
    target = f"/api/pool/detail?pool_name={pool}&date=2026-10-28"
    assert (
        existing_source._request_pool_bytes(Request("https://flash-api.xuangubao.cn" + target), 8)
        == b"{}"
    )
    http.request.assert_called_once_with("GET", target, headers={})
    connect.assert_called_once_with("flash-api.xuangubao.cn", 108)
    response.read.assert_called_once_with(existing_source.MAX_RESPONSE_BYTES + 1)
    assert timer.call_args.args[0] == 8


@pytest.mark.parametrize(
    "target",
    [
        "https://evil.test/api/pool/detail?pool_name=limit_up",
        "http://flash-api.xuangubao.cn/api/pool/detail?pool_name=limit_up",
        "https://flash-api.xuangubao.cn/api/pool/detail?pool_name=other",
        "https://flash-api.xuangubao.cn/api/pool/detail?pool_name=limit_up&url=evil",
        "https://flash-api.xuangubao.cn/api/pool/detail?pool_name=limit_up&pool_name=limit_up_broken",
        "https://flash-api.xuangubao.cn/api/pool/detail?pool_name=limit_up&date=2026-02-30",
    ],
)
def test_shared_transport_rejects_unapproved_target_before_connect(monkeypatch, target):
    connect = MagicMock()
    monkeypatch.setattr(existing_source, "_connect_http", connect)
    with pytest.raises(ValueError):
        existing_source._request_pool_bytes(Request(target), 8)
    connect.assert_not_called()
