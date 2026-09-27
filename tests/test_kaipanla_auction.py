"""Kaipanla auction pool units, date identity and source failure contracts."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers.kaipanla_auction import (
    MAX_RESPONSE_BYTES,
    KaipanlaAuctionProvider,
    KaipanlaAuctionUpstream,
)
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

ROUTE = "/api/v1/realtime/kaipanla/auction-pool"
KEY = "test-auction-api-key-00000000000000"
NOW = datetime(2026, 9, 27, 2, 0, tzinfo=UTC)


def row(code="001216"):
    result = [None] * 34
    for index, value in {
        0: code,
        1: "示例股票",
        4: "10.06",
        11: "示例板块",
        15: "3938419496",
        18: "9590788389",
        19: "10.0567",
        20: "-8055159",
        21: "0.23",
        22: "8994552",
    }.items():
        result[index] = value
    return result


class Transport:
    def __init__(self, payload=None):
        self.payload = (
            payload
            if payload is not None
            else {"errcode": "0", "day": "2026-09-24", "list": [row()]}
        )
        self.calls = []

    def __call__(self, request, timeout):
        self.calls.append((request, timeout))
        return json.dumps(self.payload, ensure_ascii=False).encode()


def provider(transport):
    return KaipanlaAuctionProvider(request_bytes=transport, clock=lambda: NOW)


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
            kaipanla_auction_provider=provider(transport),
        )
    )


def test_history_normalizes_named_fields_units_and_no_database_dependency():
    transport = Transport()
    response = client(transport).get(
        ROUTE, params={"trade_date": "2026-09-24"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["trade_date"] == body["requested_date"] == "2026-09-24"
    assert body["query_mode"] == "history"
    assert body["observed_at"] == "2026-09-27 10:00:00"
    assert body["persisted"] is False
    assert body["pool_scope"] == "kaipanla_auction_pool"
    item = body["items"][0]
    assert item["symbol"] == "SZSE:001216"
    assert item["change_pct"] == "10.06"
    assert item["actual_float_market_value_cny"] == "3938419496"
    assert item["limit_up_bid_amount_cny"] == "9590788389"
    assert item["auction_change_pct"] == "10.0567"
    assert item["auction_net_amount_cny"] == "-8055159"
    assert item["auction_turnover_rate_pct"] == "0.23"
    assert item["auction_amount_cny"] == "8994552"
    assert "source_values" not in item
    assert body["next_offset"] is None
    assert body["total"] is None
    req, timeout = transport.calls[0]
    assert req.method == "POST"
    assert urlsplit(req.full_url).hostname == "apphis.kaipanla.com"
    params = parse_qs(req.data.decode())
    assert params["c"] == ["HisHomeDingPan"]
    assert params["a"] == ["HisDaBanList"]
    assert params["Day"] == ["2026-09-24"]
    assert params["Is_st"] == ["1"]
    assert params["PidType"] == ["8"]
    assert params["Type"] == ["18"]
    assert params["Order"] == ["1"]
    assert 0 < timeout <= 10
    assert not {"Token", "UserID", "DeviceID"} & params.keys()


def test_current_keeps_actual_source_date_and_uses_hq():
    transport = Transport()
    data = provider(transport).fetch()
    assert data.requested_date is None
    assert data.trade_date == date(2026, 9, 24)
    assert data.query_mode == "current"
    req, _ = transport.calls[0]
    params = parse_qs(req.data.decode())
    assert urlsplit(req.full_url).hostname == "apphwshhq.kaipanla.com"
    assert params["c"] == ["HomeDingPan"]
    assert params["a"] == ["DaBanList"]
    assert "Day" not in params


def test_paging_and_st_filter_are_explicit_and_bounded():
    transport = Transport(
        {"errcode": 0, "day": "2026-09-24", "list": [row("000001"), row("600000")]}
    )
    data = provider(transport).fetch(
        trade_date=date(2026, 9, 24), offset=10, limit=2, exclude_st=False
    )
    assert data.offset == 10 and data.limit == 2 and data.returned_count == 2
    assert data.next_offset == 12 and data.total is None
    assert len(transport.calls) == 1
    params = parse_qs(transport.calls[0][0].data.decode())
    assert params["Index"] == ["10"] and params["st"] == ["2"] and params["Is_st"] == ["0"]


def test_empty_success_is_distinct_from_source_failure():
    data = provider(Transport({"errcode": "0", "day": "2026-09-24", "list": []})).fetch(
        trade_date=date(2026, 9, 24)
    )
    assert data.items == () and data.next_offset is None


@pytest.mark.parametrize(
    "payload",
    [
        {"errcode": "1", "message": "private upstream detail"},
        {},
        {"errcode": "0", "day": "2026-09-24"},
        {"errcode": "0", "day": "2026-09-24", "list": "bad"},
        {"errcode": "0", "day": "2026-09-23", "list": []},
        {"errcode": "0", "list": []},
        {"errcode": "0", "day": "2026-09-24", "list": [["kaipanla.com"] * 37]},
        {"errcode": "0", "day": "2026-09-24", "list": [row(), row()]},
        {"errcode": "0", "day": "2026-09-24", "list": [["600000", "too short"]]},
    ],
)
def test_upstream_failures_and_placeholders_are_not_successful_empty_results(payload):
    response = client(Transport(payload)).get(
        ROUTE, params={"trade_date": "2026-09-24"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"
    assert "private" not in response.text and "kaipanla.com" not in response.text


@pytest.mark.parametrize(
    ("index", "value"),
    [(0, 123), (0, "123"), (18, "NaN"), (22, -1), (21, -1), (15, "bad"), (11, {})],
)
def test_invalid_rows_fail_the_whole_page(index, value):
    item = row()
    item[index] = value
    with pytest.raises(KaipanlaAuctionUpstream):
        provider(Transport({"errcode": 0, "day": "2026-09-24", "list": [item]})).fetch()


def test_null_zero_and_decimal_precision_are_preserved():
    item = row()
    item[15], item[18], item[19], item[20], item[21], item[22] = (
        None,
        0,
        "--",
        "",
        0,
        "0.1234567890123456789",
    )
    result = (
        provider(Transport({"errcode": "0", "day": "2026-09-24", "list": [item]})).fetch().items[0]
    )
    assert result.actual_float_market_value_cny is None
    assert result.limit_up_bid_amount_cny == Decimal(0)
    assert result.auction_change_pct is None and result.auction_net_amount_cny is None
    assert result.auction_turnover_rate_pct == Decimal(0)
    assert result.auction_amount_cny == Decimal("0.1234567890123456789")


def test_json_decimal_values_do_not_round_trip_through_float():
    body = (
        json.dumps({"errcode": "0", "day": "2026-09-24", "list": [row()]})
        .replace('"10.0567"', "10.1234567890123456789")
        .encode()
    )
    item = provider(lambda *_: body).fetch().items[0]
    assert item.auction_change_pct == Decimal("10.1234567890123456789")


@pytest.mark.parametrize(
    "params",
    [
        {"trade_date": "2026-02-30"},
        {"trade_date": "2026-09-28"},
        {"limit": 0},
        {"limit": 31},
        {"offset": -1},
        {"offset": 10001},
        {"exclude_st": "bad"},
    ],
)
def test_bad_queries_do_not_contact_upstream(params):
    transport = Transport()
    response = client(transport).get(ROUTE, params=params, headers={"X-API-Key": KEY})
    assert response.status_code == 422
    assert transport.calls == []


def test_transport_and_response_limits():
    def fail(*_):
        raise TimeoutError("do not leak source detail")

    response = client(fail).get(ROUTE, headers={"X-API-Key": KEY})
    assert response.status_code == 502 and "detail" not in response.text
    with pytest.raises(KaipanlaAuctionUpstream):
        provider(lambda *_: b"x" * (MAX_RESPONSE_BYTES + 1)).fetch()
    with pytest.raises(KaipanlaAuctionUpstream):
        provider(lambda *_: b"not JSON").fetch()


def test_auth_docs_and_exported_contract():
    transport = Transport()
    api = client(transport)
    assert api.get(ROUTE).status_code == 401
    assert transport.calls == []
    schema = api.get("/openapi.json").json()
    operation = schema["paths"][ROUTE]["get"]
    assert operation["tags"] == ["实时接口"]
    assert operation["security"] == [{"APIKeyHeader": []}]
    assert {"401", "422", "502"} <= operation["responses"].keys()
    saved = json.loads(
        (Path(__file__).parents[1] / "contracts/fastapi-openapi-v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert saved["paths"][ROUTE] == schema["paths"][ROUTE]
