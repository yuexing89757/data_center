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


@pytest.mark.parametrize("deviation", ["12.34567890123456789", "0", None])
def test_hot_preserves_deployed_rule_and_deviation_mapping(deviation):
    row = ["001216", "示例股票", "10日100%", "99", deviation, *([None] * 7)]
    response = client(Transport({"errcode": "0", "List": [row]})).get(
        PREFIX + "/hot", headers={"X-API-Key": KEY}
    )
    assert response.status_code == 200
    item = response.json()["groups"][0]["items"][0]
    assert item["rule_description"] == "10日100%"
    assert item["deviation_pct"] == deviation
    assert item["last_price"] is None
    assert item["change_pct"] is None


@pytest.mark.parametrize("rule,deviation", [(123, "1"), ("规则", "invalid")])
def test_hot_rejects_invalid_deployed_source_fields(rule, deviation):
    row = ["001216", "示例股票", rule, "99", deviation, *([None] * 7)]
    response = client(Transport({"errcode": "0", "List": [row]})).get(
        PREFIX + "/hot", headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502


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


def test_multiple_exposes_source_count_not_unconfirmed_numeric_enums():
    transport = Transport(
        {
            "errcode": "0",
            "Day": "2026-09-29",
            "List": [
                [
                    "001216",
                    "示例",
                    1,
                    "10日内3次异动个股",
                    2,
                    9,
                    "5.1",
                    "33.32",
                    "14.54",
                    "29.09",
                    0,
                ],
                [
                    "688137",
                    "示例二",
                    2,
                    "30日内偏离值临近200%",
                    5,
                    28,
                    "157.22",
                    "165.41",
                    "17.02",
                    "141.35",
                    0,
                ],
            ],
        }
    )
    response = client(transport).get(PREFIX + "/multiple", headers={"X-API-Key": KEY})
    assert response.status_code == 200
    first, second = response.json()["groups"][0]["items"]
    assert first["rule_description"] == "10日内3次异动个股"
    assert first["source_abnormal_count_10d"] == 3
    assert second["source_abnormal_count_10d"] is None
    assert first["deviation_pct"] == "5.1"
    assert first["expected_trigger_price"] == "33.32"
    assert first["expected_change_pct"] == "14.54"
    assert first["last_price"] == "29.09"
    assert first["change_pct"] == "0"


def test_severe_maps_trigger_pairs_by_group_not_deviation_space():
    row = [
        "001216",
        "示例",
        "10日100%",
        "3.74",
        "",
        "5.42",
        "29.56",
        "1.68",
        "未触发异动",
        "12.2",
        "板块",
        0,
        "30日200%",
        "32.64",
        0,
        0,
        0,
        "",
        "2026-09-29",
        "29.09",
    ]
    tomorrow = row.copy()
    tomorrow[5:7] = ["9.7", "155.06"]
    response = client(
        Transport(
            {"errcode": "0", "Day": "2026-09-29", "List_Today": [row], "List_Tormorow": [tomorrow]}
        )
    ).get(PREFIX + "/severe", headers={"X-API-Key": KEY})
    assert response.status_code == 200
    today_item = response.json()["groups"][0]["items"][0]
    next_item = response.json()["groups"][1]["items"][0]
    assert today_item["rule_description"] == "10日100%"
    assert today_item["trigger_status"] == "未触发异动"
    assert today_item["today_trigger_change_pct"] == "5.42"
    assert today_item["today_trigger_price"] == "29.56"
    assert today_item["tomorrow_trigger_change_pct"] is None
    assert today_item["tomorrow_trigger_price"] is None
    assert next_item["tomorrow_trigger_change_pct"] == "9.7"
    assert next_item["rule_description"] == "30日200%"
    assert next_item["tomorrow_trigger_price"] == "155.06"
    assert next_item["today_trigger_price"] is None
    assert next_item["trigger_status"] is None


@pytest.mark.parametrize("value", ["NaN", "bad", -1])
def test_named_trigger_price_rejects_invalid_values(value):
    row = [
        "001216",
        "示例",
        "10日100%",
        0,
        "",
        0,
        value,
        0,
        "",
        0,
        "",
        0,
        "",
        0,
        0,
        0,
        0,
        "",
        "2026-09-29",
        1,
    ]
    response = client(Transport({"errcode": "0", "List_Today": [row], "List_Tormorow": []})).get(
        PREFIX + "/severe", headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502


def test_unknown_layout_retains_raw_values_without_guessing_named_fields():
    response = client(
        Transport({"errcode": "0", "List": [["001216", "示例", 1, "10日内3次异动个股", 2]]})
    ).get(PREFIX + "/multiple", headers={"X-API-Key": KEY})
    assert response.status_code == 200
    item = response.json()["groups"][0]["items"][0]
    assert item["source_abnormal_count_10d"] is None
    assert item["expected_trigger_price"] is None


@pytest.mark.parametrize(
    "path,lists",
    [
        (
            "/severe",
            {
                "List_Today": [["001216", "示例", *([None] * 18)]],
                "List_Tormorow": [["001216", "示例", *([None] * 18)]],
            },
        ),
        ("/multiple", {"List": [["001216", "示例", *([None] * 9)]]}),
    ],
)
def test_confirmed_layout_keeps_missing_optional_fields_null(path, lists):
    response = client(Transport({"errcode": "0", **lists})).get(
        PREFIX + path, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 200
    for group in response.json()["groups"]:
        item = group["items"][0]
        assert item["code"] == "001216"
        assert item["rule_description"] is None
        assert item["trigger_status"] is None
        assert item["source_abnormal_count_10d"] is None
