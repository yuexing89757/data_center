"""Synthetic source observations; never load production credentials or a database."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from urllib.request import Request

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers import xuangubao_limit_up as source
from market_data_center.providers.xuangubao_limit_up import (
    MAX_RESPONSE_BYTES,
    XuangubaoLimitUpProvider,
    XuangubaoLimitUpUpstream,
)
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

PATH = "/api/v1/realtime/xuangubao/limit-up"
KEY = "test-xuangubao-key-0000000000000000"
NOW = datetime(2026, 10, 27, 16, 1, tzinfo=UTC)
STAMP = int(datetime(2026, 10, 27, 2, 30, tzinfo=UTC).timestamp())
ROW = {
    "symbol": "000678.SZ",
    "stock_chi_name": "Synthetic stock",
    "price": "12.340000000000000001",
    "change_percent": "0.100123456789012345",
    "turnover_ratio": 0,
    "first_limit_up": STAMP,
    "last_limit_up": STAMP + 60,
    "first_break_limit_up": 0,
    "limit_up_days": 2,
    "total_capital": "123456789.123456789",
    "surge_reason": {
        "symbol": "000678.SZ",
        "stock_reason": "Source opinion only",
        "related_plates": [{"plate_id": 123, "plate_name": "Source theme", "plate_reason": "Text"}],
    },
    "limit_timeline": {"items": [{"timestamp": STAMP, "status": 1}]},
}


def client_for(payload, calls):
    def request(req, timeout):
        calls.append(req)
        assert timeout == 8
        if isinstance(payload, Exception):
            raise payload
        return payload if isinstance(payload, bytes) else json.dumps(payload).encode()

    return TestClient(
        create_app(
            settings=ApiSettings(
                _env_file=None,
                fastapi_database_url=SecretStr("unused"),
                fastapi_api_key=SecretStr(KEY),
            ),
            query_service=object(),
            auction_indicative_service=object(),
            xuangubao_limit_up_provider=XuangubaoLimitUpProvider(
                request_bytes=request, clock=lambda: NOW
            ),
        )
    )


def envelope(rows=None):
    return {"code": 20000, "message": "OK", "data": [ROW] if rows is None else rows}


def test_authenticated_single_latest_read_preserves_source_date_and_precision():
    calls = []
    client = client_for(envelope(), calls)
    assert client.get(PATH).status_code == 401 and not calls
    response = client.get(PATH, headers={"X-API-Key": KEY})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["source_code"] == "xuangubao" and data["source_name"] == "选股宝"
    assert data["source_date"] == "2026-10-27"
    assert data["observed_at"] == "2026-10-28 00:01:00"
    assert data["date_status"] == "verified_from_events" and data["persisted"] is False
    row = data["items"][0]
    assert row["symbol"] == "SZSE:000678" and row["source_symbol"] == "000678.SZ"
    assert row["last_price"] == "12.340000000000000001"
    assert row["change_ratio"] == "0.100123456789012345"
    assert row["turnover_ratio"] == "0"
    assert row["first_limit_up_at"] == "2026-10-27 10:30:00"
    assert row["first_break_limit_up_at"] is None
    assert row["source_statistics"]["total_capital_value"] == "123456789.123456789"
    assert row["source_reason"]["text"] == "Source opinion only"
    assert row["source_reason"]["related_plates"][0]["source_id"] == "123"
    assert row["timeline"][0]["source_status"] == 1
    assert len(calls) == 1
    assert calls[0].full_url == "https://flash-api.xuangubao.cn/api/pool/detail?pool_name=limit_up"
    assert calls[0].method == "GET" and calls[0].data is None
    assert not {"cookie", "authorization"} & {k.lower() for k, _ in calls[0].header_items()}


@pytest.mark.parametrize("rows", [[], [ROW | {"first_limit_up": 0, "last_limit_up": None}]])
def test_unknown_date_is_not_filled_from_clock(rows):
    data = client_for(envelope(rows), []).get(PATH, headers={"X-API-Key": KEY}).json()
    assert data["source_date"] is None and data["date_status"] == "unknown"
    assert data["returned_count"] == len(rows)


@pytest.mark.parametrize(
    "change",
    [
        {"symbol": "678.SZ"},
        {"symbol": "000678.SS"},
        {"stock_chi_name": ""},
        {"price": "NaN"},
        {"price": -1},
        {"turnover_ratio": True},
        {"first_limit_up": -1},
        {"last_limit_up": STAMP - 1},
        {"last_limit_up": STAMP + 86400},
        {"first_limit_up": "1793068200.5"},
        {"limit_timeline": {"items": [{"timestamp": STAMP, "status": True}]}},
        {"surge_reason": {"symbol": "600000.SS", "stock_reason": "mismatch"}},
    ],
)
def test_invalid_rows_fail_the_whole_response(change):
    calls = []
    response = client_for(envelope([ROW | change]), calls).get(PATH, headers={"X-API-Key": KEY})
    assert response.status_code == 502 and len(calls) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"code": 20001, "message": "secret"},
        {"code": True, "data": []},
        {"code": 20000, "data": {}},
        envelope([ROW, ROW]),
        envelope([ROW] * 5001),
        b"not json secret",
        b"x" * (MAX_RESPONSE_BYTES + 1),
        TimeoutError("secret"),
    ],
    ids=[
        "source-error",
        "bad-code",
        "bad-list",
        "duplicates",
        "row-bound",
        "bad-json",
        "byte-bound",
        "timeout",
    ],
)
def test_errors_do_not_leak_or_retry(payload):
    calls = []
    response = client_for(payload, calls).get(PATH, headers={"X-API-Key": KEY})
    assert response.status_code == 502 and "secret" not in response.text and len(calls) == 1


def test_decimal_json_number_does_not_pass_through_float():
    body = json.dumps(envelope()).replace('"12.340000000000000001"', "12.340000000000000001")
    provider = XuangubaoLimitUpProvider(request_bytes=lambda *_: body.encode(), clock=lambda: NOW)
    assert provider.fetch()["items"][0]["last_price"] == Decimal("12.340000000000000001")


def test_timeout_validation():
    for timeout in (0, 11, float("nan")):
        with pytest.raises(ValueError):
            XuangubaoLimitUpProvider(timeout_seconds=timeout)


@pytest.mark.parametrize(
    "body",
    [b'{"code":20001,"code":20000,"data":[]}', b'{"code":20000,"data":[],"data":[]}'],
)
def test_duplicate_json_keys_are_rejected(body):
    provider = XuangubaoLimitUpProvider(request_bytes=lambda *_: body, clock=lambda: NOW)
    with pytest.raises(XuangubaoLimitUpUpstream):
        provider.fetch()


def test_date_evidence_must_cover_every_item_and_agree():
    second = ROW | {"symbol": "600000.SS", "surge_reason": None}

    def fetch(row):
        return XuangubaoLimitUpProvider(
            request_bytes=lambda *_: json.dumps(envelope([ROW, row])).encode(), clock=lambda: NOW
        ).fetch()

    assert fetch(second | {"first_limit_up": None, "last_limit_up": 0})["source_date"] is None
    with pytest.raises(XuangubaoLimitUpUpstream):
        fetch(second | {"first_limit_up": STAMP - 86400, "last_limit_up": STAMP - 86300})


@pytest.mark.parametrize(
    "symbol,standard", [("600000.SS", "SSE:600000"), ("920000.BJ", "BSE:920000")]
)
def test_exchange_mapping_and_unknown_source_status_preserved(symbol, standard):
    row = ROW | {
        "symbol": symbol,
        "surge_reason": None,
        "limit_timeline": {"items": [{"timestamp": 0, "status": 99}]},
        "is_new_stock": False,
        "new_stock_break_limit_up": True,
    }
    result = XuangubaoLimitUpProvider(
        request_bytes=lambda *_: json.dumps(envelope([row])).encode(), clock=lambda: NOW
    ).fetch()["items"][0]
    assert result["symbol"] == standard
    assert result["timeline"] == [{"at": None, "source_status": 99}]
    assert result["source_statistics"]["new_stock_flag"] == Decimal(0)
    assert result["source_statistics"]["new_stock_break_flag"] == Decimal(1)
    assert result["source_statistics"]["sell_lock_ratio"] is None


@pytest.mark.parametrize("status,oversize", [(200, False), (302, False), (200, True)])
def test_transport_fixed_target_tls_bounds_and_no_redirect(monkeypatch, status, oversize):
    raw = MagicMock()
    connect = MagicMock(return_value=raw)
    context = MagicMock()
    tls = context.wrap_socket.return_value
    http = MagicMock()
    response = http.getresponse.return_value.__enter__.return_value
    response.status = status
    response.read.return_value = b"x" * (MAX_RESPONSE_BYTES + 1) if oversize else b"{}"
    timer = MagicMock()
    monkeypatch.setattr(source, "_connect_http", connect)
    monkeypatch.setattr(source.ssl, "create_default_context", lambda: context)
    monkeypatch.setattr(source, "HTTPSConnection", MagicMock(return_value=http))
    monkeypatch.setattr(source, "Timer", timer)
    monkeypatch.setattr(source.time, "monotonic", lambda: 100)
    request = Request(source.SOURCE_URL, method="GET")
    if status == 200 and not oversize:
        assert source._request_bytes(request, 8) == b"{}"
    else:
        with pytest.raises(ValueError):
            source._request_bytes(request, 8)
    connect.assert_called_once_with("flash-api.xuangubao.cn", 108)
    context.wrap_socket.assert_called_once_with(
        raw.__enter__.return_value,
        server_hostname="flash-api.xuangubao.cn",
        do_handshake_on_connect=False,
    )
    tls.do_handshake.assert_called_once()
    http.request.assert_called_once_with("GET", "/api/pool/detail?pool_name=limit_up", headers={})
    response.read.assert_called_once_with(MAX_RESPONSE_BYTES + 1)
    assert timer.call_args.args[0] == 8
    timer.call_args.args[1]()  # Deadline abort closes the active socket, even for slow headers.
    tls.shutdown.assert_called_once()
    timer.return_value.cancel.assert_called_once()
    http.close.assert_called_once()
    tls.close.assert_called_once()


def test_transport_rejects_unknown_target_and_expired_deadline(monkeypatch):
    connect = MagicMock()
    monkeypatch.setattr(source, "_connect_http", connect)
    for url, method in [("https://example.invalid/", "GET"), (source.SOURCE_URL, "POST")]:
        with pytest.raises(ValueError):
            source._request_bytes(Request(url, method=method), 8)
    connect.assert_not_called()
    with pytest.raises(TimeoutError):
        source._remaining(0)


def test_contract_is_current():
    schema = client_for(envelope([]), []).get("/openapi.json").json()
    saved = json.loads(Path("contracts/fastapi-openapi-v1.json").read_text(encoding="utf8"))
    assert schema["paths"][PATH] == saved["paths"][PATH]
    for name, spec in schema["components"]["schemas"].items():
        if name.startswith("Xuangubao"):
            assert spec == saved["components"]["schemas"][name]
