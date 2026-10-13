"""Nine-in-one source adapters and authenticated request-time routes (synthetic fixtures)."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers.auction_sources import AuctionSourcesProvider
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

KEY = "test-source-api-key-000000000000000"
ROOT = "/api/v1/realtime"
NOW = datetime(2026, 10, 13, 1, 45, tzinfo=UTC)
QUOTE = {
    "f12": "600000",
    "f14": "Sample",
    "f2": "9.67",
    "f3": 0,
    "f17": "9.59",
    "f18": "9.54",
    "f5": 1234,
    "f6": "123456.70",
    "f8": "0.02",
    "f10": "1.20",
    "f20": "1000000",
    "f21": "900000",
    "f26": 19991110,
    "f100": "Bank",
    "f103": "Example",
}
BAR = ["2026-10-12", "9.59", "9.67", "9.70", "9.55", "1234"]
THEME = {
    "themeCode": "00647",
    "themeName": "Example",
    "f3": "9.92",
    "bf3": "1.13",
    "fex3": 1,
    "securityName": "Leader",
}

INDEX_FIELDS = ["0"] * 36
INDEX_FIELDS[:5] = ["1", "Index", "000001", "3000.01", "2990.00"]
INDEX_FIELDS[32:34] = ["0.33", "3002.00"]
INDEX = ('v_sh000001="' + "~".join(INDEX_FIELDS) + '";').encode()


def api(payload):
    calls = []

    def request(req, timeout):
        calls.append(req)
        assert 0 < timeout <= 10
        if isinstance(payload, Exception):
            raise payload
        return payload if isinstance(payload, bytes) else json.dumps(payload).encode()

    client = TestClient(
        create_app(
            settings=ApiSettings(
                _env_file=None,
                fastapi_database_url=SecretStr("unused"),
                fastapi_api_key=SecretStr(KEY),
            ),
            query_service=object(),
            auction_indicative_service=object(),
            auction_sources_provider=AuctionSourcesProvider(
                request_bytes=request, clock=lambda: NOW
            ),
        )
    )
    return client, calls


CASES = [
    (
        "/eastmoney/quotes?page=2&page_size=500&market=mainboard",
        {"rc": 0, "data": {"total": 501, "diff": [QUOTE]}},
        "clist/get",
    ),
    (
        "/eastmoney/limit-up?trade_date=2026-10-12",
        {
            "rc": 0,
            "data": {
                "qdate": 20261012,
                "tc": 1,
                "pool": [
                    {
                        "c": "600000",
                        "n": "Sample",
                        "lbc": 1,
                        "fbt": 92500,
                        "zbc": 0,
                        "fund": "100.12",
                        "hybk": "Bank",
                    }
                ],
            },
        },
        "getTopicZTPool",
    ),
    (
        "/eastmoney/limit-down?trade_date=2026-10-12",
        {"rc": 0, "data": {"qdate": 20261012, "tc": 0, "pool": []}},
        "getTopicDTPool",
    ),
    (
        "/eastmoney/broken-limit?trade_date=2026-10-12",
        {"rc": 0, "data": {"qdate": 20261012, "tc": 0, "pool": []}},
        "getTopicZBPool",
    ),
    (
        "/eastmoney/klines?symbol=SSE:600000&limit=10",
        {
            "rc": 0,
            "data": {
                "code": "600000",
                "klines": [",".join([*BAR, "123.45", "1.23", "0.83", "0.08", "0.02"])],
            },
        },
        "stock/kline/get",
    ),
    (
        "/tencent/klines?symbol=SSE:600000&limit=10&server=ifzq",
        {"code": 0, "data": {"sh600000": {"qfqday": [BAR]}}},
        "fqkline/get",
    ),
    (
        "/sina/klines?symbol=SSE:600000&limit=10",
        [
            {
                "day": BAR[0],
                "open": BAR[1],
                "close": BAR[2],
                "high": BAR[3],
                "low": BAR[4],
                "volume": "123400",
            }
        ],
        "getKLineData",
    ),
    (
        "/tencent/minute?symbol=SSE:600000",
        {
            "code": 0,
            "data": {
                "sh600000": {
                    "data": {
                        "date": "20261013",
                        "data": ["0930 10.00 10 10000", "0931 10.50 30 31000"],
                    }
                }
            },
        },
        "minute/query",
    ),
    (
        "/tencent/index",
        INDEX,
        "q=sh000001",
    ),
    ("/eastmoney/themes/today", {"code": 0, "data": [THEME]}, "getTodayChance"),
    (
        "/eastmoney/themes?page=2&page_size=10",
        {"code": 0, "data": {"list": [THEME], "total": 100}},
        "getThemeList",
    ),
    (
        "/eastmoney/themes/00647/stocks?page_size=10",
        {
            "code": 0,
            "data": {
                "stockNum": 1,
                "stockList": [
                    {
                        "securityCode": "600000",
                        "securityName": "Sample",
                        "f3": "0.83",
                        "f62": "100.12",
                        "f57": "9.67",
                        "keywordList": [{"introduction": "Source explanation"}],
                    }
                ],
            },
        },
        "getStockList",
    ),
]


@pytest.mark.parametrize(("path", "payload", "endpoint"), CASES)
def test_all_twelve_routes_are_authenticated_single_request_and_documented(path, payload, endpoint):
    client, calls = api(payload)
    assert client.get(ROOT + path).status_code == 401
    assert calls == []
    response = client.get(ROOT + path, headers={"X-API-Key": KEY})
    assert response.status_code == 200, response.text
    value = response.json()
    assert value["persisted"] is False
    assert value["observed_at"] == "2026-10-13 09:45:00"
    assert value["source_website"].startswith("https://")
    assert endpoint in calls[0].full_url and len(calls) == 1
    assert value["purpose"]
    assert calls[0].get_header("X-api-key") is None
    op = client.get("/openapi.json").json()["paths"][
        ROOT + path.split("?")[0].replace("00647", "{theme_code}")
    ]["get"]
    assert op["tags"] == ["实时接口"]
    assert "来源" in op["description"] and "示例" in op["description"]


def test_quote_units_missing_zero_and_paginated_upstream_query():
    client, calls = api({"rc": 0, "data": {"total": 501, "diff": [QUOTE | {"f10": "-"}]}})
    response = client.get(
        ROOT + "/eastmoney/quotes?page=2&page_size=500&market=mainboard", headers={"X-API-Key": KEY}
    ).json()
    row = response["items"][0]
    assert row["symbol"] == "SSE:600000"
    assert row["volume_shares"] == "123400"
    assert row["change_pct"] == "0" and row["volume_ratio"] is None
    query = parse_qs(urlsplit(calls[0].full_url).query)
    assert query["pn"] == ["2"] and query["pz"] == ["500"]
    assert query["fs"] == ["m:1+t:2,m:0+t:6"]


def test_minute_is_cumulative_not_repeatedly_accumulated():
    path, payload, _ = CASES[7]
    client, _ = api(payload)
    data = client.get(ROOT + path, headers={"X-API-Key": KEY}).json()
    assert data["trade_date"] == "2026-10-13"
    assert data["items"][1]["cumulative_volume_shares"] == "3000"
    assert data["items"][1]["cumulative_amount_cny"] == "31000"
    assert "avg_price" not in data["items"][1]


def test_index_change_is_not_high_price():
    client, _ = api(INDEX)
    value = client.get(ROOT + "/tencent/index", headers={"X-API-Key": KEY}).json()
    assert value["items"][0]["change_pct"] == "0.33"


def test_success_empty_quotes_is_not_an_upstream_failure():
    client, _ = api({"rc": 0, "data": {"diff": [], "total": 0}})
    response = client.get(ROOT + "/eastmoney/quotes", headers={"X-API-Key": KEY})
    assert response.status_code == 200 and response.json()["items"] == []


@pytest.mark.parametrize("rows", [[QUOTE, QUOTE], [QUOTE | {"f2": -1}], [QUOTE | {"f12": 600000}]])
def test_source_quote_identity_and_values_are_validated(rows):
    client, _ = api({"rc": 0, "data": {"diff": rows}})
    assert client.get(ROOT + "/eastmoney/quotes", headers={"X-API-Key": KEY}).status_code == 502


def test_decreasing_cumulative_minute_volume_is_rejected():
    payload = {
        "code": 0,
        "data": {
            "sh600000": {
                "data": {"date": "20261013", "data": ["0930 10 20 20000", "0931 10 10 10000"]}
            }
        },
    }
    client, _ = api(payload)
    assert (
        client.get(
            ROOT + "/tencent/minute?symbol=SSE:600000", headers={"X-API-Key": KEY}
        ).status_code
        == 502
    )


def test_tencent_unadjusted_fallback_is_explicit():
    client, _ = api({"code": 0, "data": {"sh600000": {"day": [BAR]}}})
    value = client.get(
        ROOT + "/tencent/klines?symbol=SSE:600000", headers={"X-API-Key": KEY}
    ).json()
    assert value["adjustment"] == "none"


@pytest.mark.parametrize(
    "rows", [[None], ["2026-10-14,10,10,10,10,100"], ["2026-10-12,10,10,9,11,100"]]
)
def test_invalid_or_future_eastmoney_bars_are_source_errors(rows):
    client, _ = api({"rc": 0, "data": {"klines": rows}})
    assert (
        client.get(
            ROOT + "/eastmoney/klines?symbol=SSE:600000", headers={"X-API-Key": KEY}
        ).status_code
        == 502
    )


def test_oversized_payload_is_a_source_error():
    client, _ = api(b" " * 8_000_001)
    assert client.get(ROOT + "/eastmoney/quotes", headers={"X-API-Key": KEY}).status_code == 502


def test_theme_list_board_change_is_bf3_not_leading_stock_change():
    path, payload, _ = CASES[10]
    client, _ = api(payload)
    row = client.get(ROOT + path, headers={"X-API-Key": KEY}).json()["items"][0]
    assert row["change_pct"] == "1.13" and row["leader_change_pct"] == "9.92"


def test_theme_post_body_examples_and_zero_net_flow():
    path, payload, _ = CASES[11]
    payload["data"]["stockList"][0]["f62"] = 0
    client, calls = api(payload)
    data = client.get(ROOT + path, headers={"X-API-Key": KEY}).json()
    assert data["items"][0]["main_net_amount_cny"] == "0"
    body = json.loads(calls[0].data)
    assert body["args"] == {
        "themeCode": "00647",
        "pageNum": 1,
        "pageSize": 10,
        "sort": -1,
        "sortField": "f3",
    }
    assert body["client"] == "web" and body["randomCode"] == "QMvrYSV2BSS4uDmf"
    assert calls[0].method == "POST"


@pytest.mark.parametrize(
    "payload",
    [
        OSError("SECRET"),
        b"bad json",
        {"rc": 1},
        {"rc": 0, "data": None},
        {"rc": 0, "data": {"diff": {}}},
        {"rc": 0, "data": {"diff": [QUOTE | {"f2": "NaN"}]}},
    ],
)
def test_bad_upstream_is_sanitized_502_without_mock_fallback(payload):
    client, calls = api(payload)
    r = client.get(ROOT + "/eastmoney/quotes", headers={"X-API-Key": KEY})
    assert r.status_code == 502 and len(calls) == 1
    assert r.json()["error"]["code"] == "upstream_error" and "SECRET" not in r.text


@pytest.mark.parametrize(
    "path",
    [
        "/eastmoney/quotes?page=0",
        "/eastmoney/quotes?page_size=6001",
        "/tencent/minute?symbol=BSE:920000",
        "/tencent/minute?symbol=https://evil.test",
        "/tencent/klines?symbol=SSE:600000&server=evil",
        "/eastmoney/themes/../stocks",
        "/eastmoney/limit-up?trade_date=2026-10-14",
    ],
)
def test_invalid_parameters_never_contact_source(path):
    client, calls = api({})
    r = client.get(ROOT + path, headers={"X-API-Key": KEY})
    assert r.status_code in (404, 422) and calls == []


def test_pool_does_not_silently_return_another_day():
    client, _ = api({"rc": 0, "data": {"qdate": 20261013, "tc": 0, "pool": []}})
    r = client.get(ROOT + "/eastmoney/limit-up?trade_date=2026-10-12", headers={"X-API-Key": KEY})
    assert r.status_code == 502


@pytest.mark.parametrize("index", [4, 5, 6])
def test_kline_source_units_are_normalized_without_price_times_volume_amount(index):
    path, payload, _ = CASES[index]
    client, _ = api(payload)
    row = client.get(ROOT + path, headers={"X-API-Key": KEY}).json()["items"][0]
    assert Decimal(row["volume_shares"]) == Decimal("123400")
    assert row["amount_cny"] == ("123.45" if index == 4 else None)
