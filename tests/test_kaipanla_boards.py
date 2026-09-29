"""Bounded board reads: source identity, forecasts, precision and public contracts."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers.kaipanla_boards import (
    MAX_RESPONSE_BYTES,
    KaipanlaBoardsProvider,
    KaipanlaBoardsUpstream,
)
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

BASE = "/api/v1/realtime/kaipanla/boards"
KEY = "test-board-api-key-000000000000000"
NOW = datetime(2026, 9, 29, 2, 0, tzinfo=UTC)
DAY = date(2026, 9, 28)
LABELS = ["2025年实际净利润", "2026年预测净利润", "2027年预测净利润", "2028年预测净利润"]


def board(code="803023"):
    return [code, "示例板块", 2898, "-2.00", "0.89", "188800000000", "-12", 0, 0, "1.2", 0, 0, "0"]


def stock(code="000739"):
    row = [None] * 51
    for i, value in {
        0: code,
        1: "示例股票",
        4: "示例概念",
        5: "26.81",
        6: "5.97",
        7: "485000000",
        10: "26500000000",
        11: "10",
        12: "-3",
        13: "7",
        17: "0",
        18: "2.5",
        21: "1.2",
        23: "2日2板",
        24: "龙一",
        28: "0",
        29: "1000",
        37: "30000000000",
        38: "29000000000",
        40: "3",
        44: "1042823529.123456789",
        45: "-20",
        46: "0",
        50: "-1",
    }.items():
        row[i] = value
    return row


class Transport:
    def __init__(self, rows=None, **changes):
        self.payload = dict(
            errcode="0", Day=[DAY.isoformat()], list=[board()] if rows is None else rows
        )
        self.payload.update(changes)
        self.calls = []

    def __call__(self, request, timeout):
        self.calls.append((request, timeout))
        return json.dumps(self.payload, ensure_ascii=False).encode()


def provider(transport):
    return KaipanlaBoardsProvider(request_bytes=transport, clock=lambda: NOW)


def client(transport):
    return TestClient(
        create_app(
            settings=ApiSettings(
                _env_file=None,
                fastapi_database_url=SecretStr("unused"),
                fastapi_api_key=SecretStr(KEY),
            ),
            query_service=object(),
            auction_indicative_service=object(),
            kaipanla_boards_provider=provider(transport),
        )
    )


@pytest.mark.parametrize("history", [False, True])
def test_ranking_routes_units_dates_paging_and_one_upstream_request(history):
    transport = Transport(Count=1)
    response = client(transport).get(
        BASE + ("/history" if history else ""),
        params={"offset": 60, "limit": 1, **({"trade_date": str(DAY)} if history else {})},
        headers={"X-API-Key": KEY},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["persisted"] is False and data["source_code"] == "kaipanla"
    assert data["observed_at"] == "2026-09-29 10:00:00"
    assert data["trade_date"] == str(DAY)
    assert data["requested_date"] == (str(DAY) if history else None)
    assert data["total"] is None and data["next_offset"] == 61
    assert data["items"][0]["main_net_amount_cny"] == "-12"
    assert data["items"][0]["large_order_net_amount_cny"] == "0"
    assert len(transport.calls) == 1
    req, timeout = transport.calls[0]
    query = parse_qs(urlsplit(req.full_url).query)
    assert req.method == "GET" and req.data is None and 0 < timeout <= 10
    assert urlsplit(req.full_url).hostname == (
        "apphis.longhuvip.com" if history else "apphq.longhuvip.com"
    )
    assert query["apiv"] == (["w26"] if history else ["w21"])
    assert query["a"] == ["RealRankingInfo"] and query["ZSType"] == ["7"]
    assert query["Index"] == ["60"] and query["st"] == ["1"]
    assert ("Date" in query) == history
    assert not {"Token", "UserID", "DeviceID"} & query.keys()


def test_historical_members_have_standard_symbols_and_dynamic_forecast_years():
    transport = Transport([stock(), stock("688244"), stock("920000")], ZB=LABELS)
    response = client(transport).get(
        BASE + "/803023/members/history",
        params={"trade_date": str(DAY)},
        headers={"X-API-Key": KEY},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["board_code"] == "803023" and data["forecast_years"] == [2026, 2027, 2028]
    assert [r["symbol"] for r in data["items"]] == ["SZSE:000739", "SSE:688244", "BSE:920000"]
    item = data["items"][0]
    assert item["height_label"] == "2日2板" and item["leader_label"] == "龙一"
    assert item["main_sell_amount_cny"] == "-3"
    assert item["forecast_status"] == "available"
    assert item["profit_forecasts"] == [
        {"year": 2026, "net_profit_cny": "1042823529.123456789"},
        {"year": 2027, "net_profit_cny": "-20"},
        {"year": 2028, "net_profit_cny": "0"},
    ]
    req = transport.calls[0][0]
    query = parse_qs(urlsplit(req.full_url).query)
    assert query["a"] == ["ZhiShuStockList_W8"] and query["apiv"] == ["w41"]
    assert query["PlateID"] == ["803023"] and query["Date"] == [str(DAY)]
    assert all(query[k] == ["0"] for k in ("IsZZ", "IsKZZType", "filterType", "TSZB", "TSZB_Type"))


def test_forecast_missing_years_and_zero_sentinel_are_explicit():
    row = stock()
    row[44:47] = [0, 0, 0]
    page = provider(Transport([row], ZB=LABELS)).members("803023", trade_date=DAY)
    assert page.items[0].forecast_status == "not_provided"
    assert [f.net_profit_cny for f in page.items[0].profit_forecasts] == [Decimal(0)] * 3
    page = provider(Transport([stock()], ZB=[])).members("803023", trade_date=DAY)
    assert page.forecast_years == () and page.items[0].profit_forecasts == ()
    assert page.items[0].forecast_status == "years_unverified"
    row = stock()[:25]
    page = provider(Transport([row], ZB=LABELS)).members("803023", trade_date=DAY)
    assert page.items[0].close_limit_bid_amount_cny is None
    assert all(f.net_profit_cny is None for f in page.items[0].profit_forecasts)


@pytest.mark.parametrize(
    "changes",
    [
        {"errcode": 1020},
        {"errcode": False},
        {"Day": None},
        {"Day": []},
        {"Day": ["2026-09-27"]},
        {"Day": "2026-02-30"},
        {"Day": "2026-09-30"},
        {"list": None},
        {"list": [board(), board()]},
        {"list": [["kaipanla.com"] * 13]},
        {"list": [["803023", "short"]]},
    ],
)
def test_bad_source_is_502_not_empty_success(changes):
    response = client(Transport(**changes)).get(
        BASE + "/history", params={"trade_date": str(DAY)}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"


@pytest.mark.parametrize(
    ("index", "value"),
    [(0, "123456"), (5, -1), (6, "NaN"), (7, True), (40, "1.5"), (44, "Infinity")],
)
def test_invalid_member_numbers_identifiers_and_counts_fail_whole_page(index, value):
    row = stock()
    row[index] = value
    with pytest.raises(KaipanlaBoardsUpstream):
        provider(Transport([row], ZB=LABELS)).members("803023", trade_date=DAY)


def test_decimal_json_precision_null_zero_and_short_page():
    row = board()
    row[2], row[3], row[5], row[6] = None, "--", 0, "0.123456789012345678901"
    body = Transport([row])(None, 8).replace(
        b'"0.123456789012345678901"', b"0.123456789012345678901"
    )
    page = provider(lambda *_: body).ranking()
    assert page.items[0].strength is None and page.items[0].change_pct is None
    assert page.items[0].amount_cny == Decimal(0)
    assert page.items[0].main_net_amount_cny == Decimal("0.123456789012345678901")
    assert page.next_offset is None
    assert provider(Transport([])).ranking().items == ()
    with pytest.raises(KaipanlaBoardsUpstream):
        provider(Transport([board(), board("803024")])).ranking(limit=1)


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/history", {}),
        ("/803023/members/history", {}),
        ("/history", {"trade_date": "2026-02-30"}),
        ("/history", {"trade_date": "2026-09-30"}),
        ("/803023/members/history", {"trade_date": "2026-09-29"}),
        ("/bad/members/history", {"trade_date": str(DAY)}),
        ("", {"limit": 0}),
        ("", {"limit": 61}),
        ("", {"offset": -1}),
        ("", {"offset": 10001}),
    ],
)
def test_invalid_queries_never_contact_source(path, params):
    transport = Transport()
    response = client(transport).get(BASE + path, params=params, headers={"X-API-Key": KEY})
    assert response.status_code == 422
    assert transport.calls == []


def test_transport_failure_bounds_and_no_detail_leak():
    def fail(*_):
        raise TimeoutError("private-secret-source-detail")

    response = client(fail).get(BASE, headers={"X-API-Key": KEY})
    assert response.status_code == 502 and "private-secret" not in response.text
    for body in (b"not JSON", b"x" * (MAX_RESPONSE_BYTES + 1)):
        with pytest.raises(KaipanlaBoardsUpstream):
            provider(lambda *_, content=body: content).ranking()


def test_three_authenticated_routes_and_saved_contract():
    transport = Transport()
    api = client(transport)
    paths = (BASE, BASE + "/history", BASE + "/{board_code}/members/history")
    for path in paths:
        assert (
            api.get(
                path.replace("{board_code}", "803023"), params={"trade_date": str(DAY)}
            ).status_code
            == 401
        )
    assert transport.calls == []
    schema = api.get("/openapi.json").json()
    saved = json.loads(
        (Path(__file__).parents[1] / "contracts/fastapi-openapi-v1.json").read_text(
            encoding="utf-8"
        )
    )
    for path in paths:
        operation = schema["paths"][path]["get"]
        assert operation["tags"] == ["实时接口"]
        assert operation["security"] == [{"APIKeyHeader": []}]
        assert {"401", "422", "502"} <= operation["responses"].keys()
        assert saved["paths"][path] == schema["paths"][path]
