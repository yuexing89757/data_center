"""Read-through alerts must be bounded, authenticated and independent of persistence."""

import json
from datetime import date
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers.kaipanla_alerts import (
    MAX_RESPONSE_BYTES,
    KaipanlaAlertsProvider,
    KaipanlaAlertsUpstream,
)
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

KEY = "test-kaipanla-api-key-00000000000000"
PREFIX = "/api/v1/realtime/kaipanla/alerts"


class Transport:
    def __init__(self, payload=None):
        self.payload = payload if payload is not None else {"errcode": "0", "List": []}
        self.calls = []

    def __call__(self, request, timeout):
        self.calls.append((request, timeout))
        return json.dumps(self.payload, ensure_ascii=False).encode()


def client(transport):
    # Any database or other live-service invocation would fail on these sentinels.
    app = create_app(
        settings=ApiSettings(
            _env_file=None,
            fastapi_database_url=SecretStr("unused"),
            fastapi_api_key=SecretStr(KEY),
        ),
        query_service=object(),
        auction_indicative_service=object(),
        kaipanla_alerts_provider=KaipanlaAlertsProvider(request_bytes=transport),
    )
    return TestClient(app)


def parameters(request):
    return parse_qs(request.data.decode() if request.data else urlsplit(request.full_url).query)


def test_severe_normalizes_dates_symbols_and_precise_decimals_without_persistence():
    transport = Transport(
        {
            "errcode": "0",
            "Day": "2026-09-24",
            "Time": 1790495221,
            "List_Today": [["001216", "示例股票", "10日100%", "0.1234567890123456789"]],
            "List_Tormorow": [],
        }
    )
    response = client(transport).get(PREFIX + "/severe", headers={"X-API-Key": KEY})
    assert response.status_code == 200
    body = response.json()
    assert body["trade_date"] == "2026-09-24"
    assert body["requested_date"] is None
    assert body["date_matches_request"] is None
    assert body["persisted"] is False
    assert body["source_timestamp"] == "2026-09-27 15:47:01"
    assert body["groups"][0]["items"][0]["symbol"] == "SZSE:001216"
    assert body["groups"][0]["items"][0]["source_values"][3] == "0.1234567890123456789"
    assert body["groups"][1]["key"] == "tomorrow"
    request, timeout = transport.calls[0]
    assert request.method == "GET"
    assert parameters(request)["a"] == ["GetPianLiZhi_W46"]
    assert 0 < timeout <= 10


@pytest.mark.parametrize(
    ("path", "query", "action", "host", "method", "extra"),
    [
        (
            "/severe",
            {"trade_date": "2026-09-23"},
            "GetYDTPZFPL_W46",
            "apphis",
            "GET",
            {"Day": "2026-09-23"},
        ),
        (
            "/severe/history",
            {"trade_date": "2026-09-23", "offset": 20, "filter": "suspended"},
            "GetYDTPZFPL_W46_HisAll",
            "apphis",
            "GET",
            {"Index": "20", "st": "20", "IsZT": "0", "Status": "1"},
        ),
        ("/hot", {}, "GetPianLiZhi_Hot", "apphwshhq", "GET", {}),
        (
            "/hot",
            {"trade_date": "2026-09-23"},
            "GetPianLiZhi_Hot_His",
            "apphis",
            "GET",
            {"Day": "2026-09-23"},
        ),
        ("/monitor", {}, "GetYDTP_ZDJK_Today", "apphwshhq", "POST", {}),
        ("/monitor/history", {}, "GetYDTP_ZDJK_His", "apphwshhq", "POST", {}),
        (
            "/monitor/history",
            {"before_date": "2026-09-23"},
            "GetYDTP_ZDJK_His",
            "apphis",
            "POST",
            {"Day": "2026-09-23"},
        ),
        ("/inquiries", {}, "GetYDTP_WXHJ_His", "apphwshhq", "POST", {}),
        (
            "/inquiries/history",
            {"offset": 20, "limit": 10},
            "GetYDTP_WXHJ_His",
            "apphis",
            "POST",
            {"Index": "20", "st": "10"},
        ),
        ("/legacy", {}, "GetPianLiZhi_Index", "apphwshhq", "POST", {"ZDJK_Type": "1"}),
        (
            "/legacy",
            {"trade_date": "2026-09-23", "triggered_only": True},
            "GetPianLiZhi_Index_W32",
            "apphis",
            "POST",
            {"IsZT": "1"},
        ),
        ("/multiple", {}, "GetPianLiZhi_Many", "apphwshhq", "POST", {}),
    ],
)
def test_routes_select_fixed_upstream_requests(path, query, action, host, method, extra):
    transport = Transport(
        {
            "errcode": "0",
            "List": [],
            "List_Today": [],
            "List_Tormorow": [],
            "List_His": [],
            "List_His_Total": 0,
            "ZDJKList": [],
            "WXHJList": [],
        }
    )
    response = client(transport).get(PREFIX + path, params=query, headers={"X-API-Key": KEY})
    assert response.status_code == 200, response.text
    request, _ = transport.calls[0]
    assert urlsplit(request.full_url).hostname == f"{host}.kaipanla.com"
    assert request.method == method
    params = parameters(request)
    assert params["a"] == [action]
    assert params["c"] == ["StockBidYiDong"]
    for key, value in extra.items():
        assert params[key] == [value]
    assert not {"Token", "UserID", "DeviceID"} & params.keys()
    if query.get("filter") != "suspended":
        assert "Status" not in params


def test_quote_refresh_uses_decimal_and_preserves_missing_codes():
    transport = Transport({"errcode": "0", "List": [["600127", 16.49, -1.55]]})
    response = client(transport).post(
        PREFIX + "/quotes/query",
        headers={"X-API-Key": KEY},
        json={"codes": ["600127", "001216", "600127"]},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["missing_codes"] == ["001216"]
    item = body["groups"][0]["items"][0]
    assert item["last_price"] == "16.49"
    assert item["change_pct"] == "-1.55"
    assert item["name"] is None
    assert parameters(transport.calls[0][0])["StockIDList"] == ["600127,001216"]


def test_legacy_mismatch_is_explicit_and_does_not_relabel_trade_date():
    provider = KaipanlaAlertsProvider(
        request_bytes=Transport({"errcode": "0", "Day": "2026-09-22", "List": []})
    )
    data = provider.fetch("legacy_history", trade_date=date(2026, 9, 23))
    assert data.trade_date == date(2026, 9, 22)
    assert data.date_matches_request is False


def test_page_total_and_next_offset_and_empty_last_page():
    transport = Transport(
        {
            "errcode": "0",
            "Day": "2026-09-23",
            "List_His": [["600000", "示例", "rule"]] * 20,
            "List_His_Total": 41,
        }
    )
    provider = KaipanlaAlertsProvider(request_bytes=transport)
    page = provider.fetch("severe_history_all", trade_date=date(2026, 9, 23), offset=20)
    assert page.total == 41
    assert page.next_offset == 40
    transport.payload["List_His"] = []
    assert (
        provider.fetch("severe_history_all", trade_date=date(2026, 9, 23), offset=60).next_offset
        is None
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"errcode": "403", "msg": "secret"},
        {},
        {"errcode": "0"},
        {"errcode": "0", "List": "wrong"},
        {"errcode": "0", "List": [["bad", "name"]]},
        {"errcode": "0", "List": [[123, "name"]]},
        {"errcode": "0", "Day": "not-a-date", "List": []},
        {"errcode": "0", "List": [["600000", "name", float("nan")]]},
    ],
)
def test_bad_source_data_is_safe_502_not_successful_empty_list(payload):
    response = client(Transport(payload)).get(PREFIX + "/hot", headers={"X-API-Key": KEY})
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"
    assert "secret" not in response.text


def test_transport_error_and_size_limit():
    def failed(request, timeout):
        raise OSError("credential-bearing upstream details")

    response = client(failed).get(PREFIX + "/hot", headers={"X-API-Key": KEY})
    assert response.status_code == 502
    assert "credential" not in response.text
    with pytest.raises(KaipanlaAlertsUpstream):
        KaipanlaAlertsProvider(request_bytes=lambda *_: b"x" * (MAX_RESPONSE_BYTES + 1)).fetch(
            "hot_current"
        )


def test_decimal_parser_never_round_trips_through_float():
    data = KaipanlaAlertsProvider(
        request_bytes=lambda *_: b'{"errcode":"0","List":[["600000",1.1234567890123456789,0]]}'
    ).fetch("quote_refresh", codes=("600000",))
    assert data.groups[0].items[0].last_price == Decimal("1.1234567890123456789")


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/severe", {"trade_date": "2026-02-30"}),
        ("/severe/history", {}),
        ("/severe/history", {"trade_date": "2026-09-23", "limit": 21}),
        ("/severe/history", {"trade_date": "2026-09-23", "offset": -1}),
        ("/severe/history", {"trade_date": "2026-09-23", "filter": "bad"}),
        ("/monitor/history", {"before_date": "wrong"}),
    ],
)
def test_invalid_requests_do_not_contact_upstream(path, params):
    transport = Transport()
    response = client(transport).get(PREFIX + path, params=params, headers={"X-API-Key": KEY})
    assert response.status_code == 422
    assert transport.calls == []


def test_auth_and_openapi_contract():
    transport = Transport()
    api = client(transport)
    assert api.get(PREFIX + "/hot").status_code == 401
    assert transport.calls == []
    schema = api.get("/openapi.json").json()
    routes = {p: v for p, v in schema["paths"].items() if p.startswith(PREFIX)}
    assert len(routes) == 10
    for path in routes.values():
        for operation in path.values():
            assert operation["tags"] == ["实时接口"]
            assert operation["security"] == [{"APIKeyHeader": []}]
            assert {"401", "422", "502"} <= operation["responses"].keys()
    assert not any("settings" in p for p in routes)


@pytest.mark.parametrize("codes", [[], ["123"], [600127], ["600127"] * 101])
def test_quote_request_bounds_do_not_contact_upstream(codes):
    transport = Transport()
    response = client(transport).post(
        PREFIX + "/quotes/query", json={"codes": codes}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 422
    assert transport.calls == []


@pytest.mark.parametrize("price", ["not-a-number", "NaN", "Infinity", -1])
def test_bad_quote_numbers_return_safe_source_error(price):
    transport = Transport({"errcode": "0", "List": [["600127", price, 0]]})
    response = client(transport).post(
        PREFIX + "/quotes/query", json={"codes": ["600127"]}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"


def test_monitor_and_inquiry_normalization_preserves_unknown_enum():
    monitor = KaipanlaAlertsProvider(
        request_bytes=Transport(
            {"errcode": "0", "List": [["600000", "示例", "2026-09-08", "2026-09-21", 2]]}
        )
    ).fetch("monitor_history_initial")
    item = monitor.groups[0].items[0]
    assert item.start_date == date(2026, 9, 8)
    assert item.end_date == date(2026, 9, 21)
    assert monitor.next_date == item.start_date
    assert item.source_values[-1] == 2
    inquiry = KaipanlaAlertsProvider(
        request_bytes=Transport(
            {
                "errcode": "0",
                "List": [["600000", "示例", "2026-08-28", "https://example.invalid/notice.pdf", 1]],
            }
        )
    ).fetch("inquiry_initial")
    assert inquiry.groups[0].items[0].announcement_date == date(2026, 8, 28)


def test_published_contract_matches_runtime_for_all_alert_routes():
    from pathlib import Path

    schema = client(Transport()).get("/openapi.json").json()
    saved = json.loads(
        (Path(__file__).parents[1] / "contracts" / "fastapi-openapi-v1.json").read_text(
            encoding="utf-8"
        )
    )
    for path, operation in schema["paths"].items():
        if path.startswith(PREFIX):
            assert saved["paths"][path] == operation
    for name, model in schema["components"]["schemas"].items():
        if name.startswith("Kaipanla"):
            assert saved["components"]["schemas"][name] == model
