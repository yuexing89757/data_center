"""Historical Kaipanla market-emotion lists: source and public contracts."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers.kaipanla_market_history import (
    MAX_RESPONSE_BYTES,
    KaipanlaMarketHistoryInvalid,
    KaipanlaMarketHistoryProvider,
    KaipanlaMarketHistoryUpstream,
)
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

TRADE_DATE = date(2026, 9, 28)
NOW = datetime(2026, 9, 28, 17, tzinfo=UTC)
KEY = "test-market-history-api-key-000000000"
ROUTES = {
    "auction": "/api/v1/realtime/kaipanla/market-emotion/stocks/auction",
    "limit_up": "/api/v1/realtime/kaipanla/market-emotion/stocks/limit-up",
    "limit_down": "/api/v1/realtime/kaipanla/market-emotion/stocks/limit-down",
    "broken_limit_up": "/api/v1/realtime/kaipanla/market-emotion/stocks/broken-limit-up",
}


def row(kind: str, code: str = "001368") -> list[object]:
    result: list[object] = [None] * 34
    result[0], result[1] = code, "示例股票"
    if kind == "auction":
        result[11], result[18], result[19] = "半导体", "2404248.123456789", "10.0567"
        result[4], result[20] = "-2.35", "-8055159.123456789"
        result[15], result[21], result[22] = "1234567890.123456789", "0.23", "8994552.123456789"
    elif kind == "limit_up":
        result[6], result[9], result[16] = 1790578605, "首板", "海峡两岸"
        result[11], result[25] = "板块甲、板块乙", 1790578665
        result[23], result[8] = "99999999.123456789", "1234567.123456789"
        result[12], result[13] = "-987654.123456789", "23456789.123456789"
        result[14], result[15] = "3.95", "6884600000.123456789"
        result[24], result[27] = 1, 7
    elif kind == "limit_down":
        result[6], result[8], result[11] = 1790578293, "2404248", "光模块"
    else:
        result[4], result[6], result[7] = "17.06", 1790559894, 1790559912
    return result


class Transport:
    def __init__(self, payload: object):
        self.payload = payload
        self.calls: list[tuple[object, float]] = []

    def __call__(self, request: object, timeout: float) -> bytes:
        self.calls.append((request, timeout))
        return json.dumps(self.payload, ensure_ascii=False).encode()


def provider(transport: Transport) -> KaipanlaMarketHistoryProvider:
    return KaipanlaMarketHistoryProvider(request_bytes=transport, clock=lambda: NOW)


@pytest.mark.parametrize(
    ("kind", "pid_type", "sort_type"),
    [
        ("auction", "8", "18"),
        ("limit_up", "1", "6"),
        ("limit_down", "3", "6"),
        ("broken_limit_up", "2", "4"),
    ],
)
def test_fixed_history_request_and_typed_fields(kind, pid_type, sort_type):
    transport = Transport({"errcode": "0", "day": "2026-09-28", "list": [row(kind)]})
    page = provider(transport).fetch(kind=kind, trade_date=TRADE_DATE)
    request, timeout = transport.calls[0]
    params = parse_qs(request.data.decode("ascii"))
    assert request.method == "POST"
    assert urlsplit(request.full_url).hostname == "apphis.kaipanla.com"
    assert (params["PidType"], params["Type"], params["Order"]) == ([pid_type], [sort_type], ["1"])
    assert params["Day"] == ["2026-09-28"]
    assert params["Is_st"] == ["1"]
    assert not {"Token", "UserID", "DeviceID"} & params.keys()
    assert 0 < timeout <= 10
    assert page.list_type == kind
    assert page.source_code == "kaipanla" and page.persisted is False
    assert page.total is None and page.returned_count == 1
    item = page.items[0]
    assert item.symbol == "SZSE:001368"
    if kind == "auction":
        assert item.limit_up_bid_amount_cny == Decimal("2404248.123456789")
        assert item.auction_change_pct == Decimal("10.0567")
        assert item.change_pct == Decimal("-2.35")
        assert item.auction_net_amount_cny == Decimal("-8055159.123456789")
        assert item.actual_float_market_value_cny == Decimal("1234567890.123456789")
        assert item.auction_turnover_rate_pct == Decimal("0.23")
        assert item.auction_amount_cny == Decimal("8994552.123456789")
        assert set(vars(item)) == {
            "symbol",
            "code",
            "name",
            "board_name",
            "limit_up_bid_amount_cny",
            "auction_change_pct",
            "change_pct",
            "auction_net_amount_cny",
            "actual_float_market_value_cny",
            "auction_turnover_rate_pct",
            "auction_amount_cny",
        }
    elif kind == "limit_up":
        assert item.limit_up_at.isoformat() == "2026-09-28T14:56:45+08:00"
        assert item.status == "首板" and item.reason == "海峡两岸"
    elif kind == "limit_down":
        assert item.limit_down_at.isoformat() == "2026-09-28T14:51:33+08:00"
        assert item.sealed_amount_cny == Decimal("2404248")
    else:
        assert item.change_pct == Decimal("17.06")
        assert item.limit_up_at.isoformat() == "2026-09-28T09:44:54+08:00"
        assert item.opened_at.isoformat() == "2026-09-28T09:45:12+08:00"


@pytest.mark.parametrize(
    "payload",
    [
        {"errcode": "1", "day": "2026-09-28", "list": []},
        {"errcode": "0", "list": []},
        {"errcode": "0", "day": "2026-09-27", "list": []},
        {"errcode": "0", "day": "2026-09-28", "list": [["kaipanla.com"] * 34]},
        {"errcode": "0", "day": "2026-09-28", "list": [row("auction")] * 2},
    ],
)
def test_invalid_source_never_looks_like_empty_success(payload):
    with pytest.raises(KaipanlaMarketHistoryUpstream):
        provider(Transport(payload)).fetch(kind="auction", trade_date=TRADE_DATE)


@pytest.mark.parametrize(
    ("kind", "index", "value"),
    [
        ("auction", 18, "NaN"),
        ("auction", 18, -1),
        ("limit_down", 8, -1),
        ("limit_up", 6, 0),
        ("limit_up", 6, True),
        ("limit_up", 6, 1790492205),
        ("broken_limit_up", 7, "1790559912.5"),
    ],
)
def test_invalid_source_cells_fail_the_page(kind, index, value):
    source_row = row(kind)
    source_row[index] = value
    with pytest.raises(KaipanlaMarketHistoryUpstream):
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]})).fetch(
            kind=kind, trade_date=TRADE_DATE
        )


def test_null_zero_and_fractional_decimal_are_preserved():
    source_row = row("auction")
    source_row[11], source_row[18], source_row[19] = None, "0.1234567890123456789", 0
    item = (
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]}))
        .fetch(kind="auction", trade_date=TRADE_DATE)
        .items[0]
    )
    assert item.board_name is None
    assert item.limit_up_bid_amount_cny == Decimal("0.1234567890123456789")
    assert item.auction_change_pct == Decimal(0)


@pytest.mark.parametrize(
    ("code", "expected"),
    [("600000", "SSE:600000"), ("000001", "SZSE:000001"), ("920000", "BSE:920000")],
)
def test_exchange_prefixes(code, expected):
    item = (
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [row("auction", code)]}))
        .fetch(kind="auction", trade_date=TRADE_DATE)
        .items[0]
    )
    assert item.symbol == expected


def test_empty_and_full_page_paging():
    empty = provider(Transport({"errcode": 0, "day": "2026-09-28", "list": []})).fetch(
        kind="auction", trade_date=TRADE_DATE
    )
    assert empty.items == () and empty.next_offset is None
    rows = [row("auction", f"{index:06d}") for index in range(1, 31)]
    full = provider(Transport({"errcode": 0, "day": "2026-09-28", "list": rows})).fetch(
        kind="auction", trade_date=TRADE_DATE
    )
    assert full.returned_count == 30 and full.next_offset == 30
    short = provider(Transport({"errcode": 0, "day": "2026-09-28", "list": rows[:11]})).fetch(
        kind="auction", trade_date=TRADE_DATE
    )
    assert short.next_offset is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"trade_date": date(2026, 9, 29)},
        {"trade_date": date(2026, 9, 30)},
        {"offset": -1},
        {"offset": 10001},
        {"limit": 0},
        {"limit": 31},
    ],
)
def test_invalid_requests_do_not_contact_source(kwargs):
    transport = Transport({"errcode": 0, "day": "2026-09-28", "list": []})
    with pytest.raises(KaipanlaMarketHistoryInvalid):
        provider(transport).fetch(kind="auction", **{"trade_date": TRADE_DATE, **kwargs})
    assert not transport.calls


def test_oversized_or_overfull_page_is_source_failure():
    with pytest.raises(KaipanlaMarketHistoryUpstream):
        KaipanlaMarketHistoryProvider(
            request_bytes=lambda *_: b"x" * (MAX_RESPONSE_BYTES + 1), clock=lambda: NOW
        ).fetch(kind="auction", trade_date=TRADE_DATE)
    with pytest.raises(KaipanlaMarketHistoryUpstream):
        provider(
            Transport({"errcode": 0, "day": "2026-09-28", "list": [row("auction")] * 2})
        ).fetch(kind="auction", trade_date=TRADE_DATE, limit=1)


def client(transport: Transport) -> TestClient:
    return TestClient(
        create_app(
            settings=ApiSettings(
                _env_file=None,
                fastapi_database_url=SecretStr("unused"),
                fastapi_api_key=SecretStr(KEY),
            ),
            query_service=object(),
            auction_indicative_service=object(),
            kaipanla_market_history_provider=provider(transport),
        )
    )


@pytest.mark.parametrize("kind", ROUTES)
def test_history_routes_return_only_named_category_fields(kind):
    transport = Transport({"errcode": 0, "day": "2026-09-28", "list": [row(kind)]})
    response = client(transport).get(
        ROUTES[kind], params={"trade_date": "2026-09-28"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["source_code"] == "kaipanla"
    assert body["list_type"] == kind and body["persisted"] is False
    assert body["observed_at"] == "2026-09-29 01:00:00"
    assert body["requested_date"] == body["trade_date"] == "2026-09-28"
    assert body["total"] is None and body["returned_count"] == 1
    item = body["items"][0]
    if kind == "auction":
        assert item["limit_up_bid_amount_cny"] == "2404248.123456789"
        assert item["auction_change_pct"] == "10.0567"
        assert item["change_pct"] == "-2.35"
        assert item["auction_net_amount_cny"] == "-8055159.123456789"
        assert item["actual_float_market_value_cny"] == "1234567890.123456789"
        assert item["auction_turnover_rate_pct"] == "0.23"
        assert item["auction_amount_cny"] == "8994552.123456789"
        assert "limit_up_at" not in item
    elif kind == "limit_up":
        assert item["limit_up_at"] == "2026-09-28 14:56:45"
        assert item["status"] == "首板" and item["reason"] == "海峡两岸"
        assert item["board_name"] == "板块甲、板块乙"
        assert item["last_limit_up_at"] == "2026-09-28 14:57:45"
        assert item["sealed_amount_cny"] == "1234567.123456789"
        assert item["max_sealed_amount_cny"] == "99999999.123456789"
        assert item["main_net_amount_cny"] == "-987654.123456789"
        assert item["amount_cny"] == "23456789.123456789"
        assert item["actual_turnover_rate_pct"] == "3.95"
        assert item["actual_float_market_value_cny"] == "6884600000.123456789"
        assert item["is_resealed"] is True and item["reason_group_count"] == 7
        assert set(item) == {
            "symbol",
            "code",
            "name",
            "limit_up_at",
            "status",
            "reason",
            "board_name",
            "last_limit_up_at",
            "sealed_amount_cny",
            "max_sealed_amount_cny",
            "main_net_amount_cny",
            "amount_cny",
            "actual_turnover_rate_pct",
            "actual_float_market_value_cny",
            "is_resealed",
            "reason_group_count",
        }
    elif kind == "limit_down":
        assert item["limit_down_at"] == "2026-09-28 14:51:33"
        assert item["sealed_amount_cny"] == "2404248"
        assert "change_pct" not in item
    else:
        assert item["limit_up_at"] == "2026-09-28 09:44:54"
        assert item["opened_at"] == "2026-09-28 09:45:12"
        assert item["change_pct"] == "17.06"


@pytest.mark.parametrize("kind", ROUTES)
def test_api_rejects_bad_query_and_requires_key_without_contacting_source(kind):
    transport = Transport({"errcode": 0, "day": "2026-09-28", "list": []})
    api = client(transport)
    route = ROUTES[kind]
    assert api.get(route, params={"trade_date": "2026-09-28"}).status_code == 401
    for params in (
        {},
        {"trade_date": "2026-09-29"},
        {"trade_date": "2026-02-30"},
        {"limit": 31},
        {"offset": -1},
    ):
        assert api.get(route, params=params, headers={"X-API-Key": KEY}).status_code == 422
    assert transport.calls == []


def test_source_failure_is_sanitized_as_502():
    transport = Transport({"errcode": "1", "message": "private source detail"})
    response = client(transport).get(
        ROUTES["auction"], params={"trade_date": "2026-09-28"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"
    assert "private source detail" not in response.text


def test_authenticated_openapi_routes_match_checked_in_contract():
    schema = (
        client(Transport({"errcode": 0, "day": "2026-09-28", "list": []}))
        .get("/openapi.json")
        .json()
    )
    saved = json.loads(
        (Path(__file__).parents[1] / "contracts/fastapi-openapi-v1.json").read_text(
            encoding="utf-8"
        )
    )
    for route in ROUTES.values():
        operation = schema["paths"][route]["get"]
        assert operation["tags"] == ["实时接口"]
        assert operation["security"] == [{"APIKeyHeader": []}]
        assert {"401", "422", "502"} <= operation["responses"].keys()
        assert saved["paths"][route] == schema["paths"][route]


@pytest.mark.parametrize(
    ("index", "field"),
    [
        (15, "actual_float_market_value_cny"),
        (21, "auction_turnover_rate_pct"),
        (22, "auction_amount_cny"),
    ],
)
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("--", None),
        (0, Decimal(0)),
        ("0.1234567890123456789", Decimal("0.1234567890123456789")),
    ],
)
def test_auction_research_fields_preserve_missing_zero_and_precision(index, field, value, expected):
    source_row = row("auction")
    source_row[index] = value
    item = (
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]}))
        .fetch(kind="auction", trade_date=TRADE_DATE)
        .items[0]
    )
    assert getattr(item, field) == expected


@pytest.mark.parametrize("index", [15, 21, 22])
@pytest.mark.parametrize("value", [True, "-1", "NaN", "infinity", "not-a-number"])
def test_invalid_auction_research_numbers_fail_the_batch(index, value):
    source_row = row("auction")
    source_row[index] = value
    with pytest.raises(KaipanlaMarketHistoryUpstream):
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]})).fetch(
            kind="auction", trade_date=TRADE_DATE
        )


LIMIT_UP_NUMBERS = [
    (8, "sealed_amount_cny"),
    (23, "max_sealed_amount_cny"),
    (12, "main_net_amount_cny"),
    (13, "amount_cny"),
    (14, "actual_turnover_rate_pct"),
    (15, "actual_float_market_value_cny"),
]


@pytest.mark.parametrize(("index", "field"), LIMIT_UP_NUMBERS)
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("--", None),
        (0, Decimal(0)),
        ("0.123456789012345678901", Decimal("0.123456789012345678901")),
    ],
)
def test_limit_up_numbers_preserve_null_zero_and_precision(index, field, value, expected):
    source_row = row("limit_up")
    source_row[index] = value
    item = (
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]}))
        .fetch(kind="limit_up", trade_date=TRADE_DATE)
        .items[0]
    )
    assert getattr(item, field) == expected


@pytest.mark.parametrize(("index", "field"), LIMIT_UP_NUMBERS)
@pytest.mark.parametrize("value", [True, "NaN", "Infinity", "bad"])
def test_limit_up_invalid_numbers_return_sanitized_502(index, field, value):
    source_row = row("limit_up")
    source_row[index] = value
    response = client(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]})).get(
        ROUTES["limit_up"], params={"trade_date": "2026-09-28"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"
    assert field not in response.text


@pytest.mark.parametrize("index", [8, 23, 13, 14, 15])
def test_limit_up_unsigned_numbers_reject_negative_values(index):
    source_row = row("limit_up")
    source_row[index] = "-0.1"
    with pytest.raises(KaipanlaMarketHistoryUpstream):
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]})).fetch(
            kind="limit_up", trade_date=TRADE_DATE
        )


@pytest.mark.parametrize(
    ("value", "expected"), [(None, None), ("", None), ("--", None), (0, False), (1, True)]
)
def test_limit_up_resealed_flag_is_nullable_boolean(value, expected):
    source_row = row("limit_up")
    source_row[24] = value
    item = (
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]}))
        .fetch(kind="limit_up", trade_date=TRADE_DATE)
        .items[0]
    )
    assert item.is_resealed is expected


@pytest.mark.parametrize(
    ("index", "value"),
    [
        (24, True),
        (24, 2),
        (24, "1"),
        (24, -1),
        (27, True),
        (27, -1),
        (27, "2"),
        (27, "2.5"),
        (25, 0),
        (25, True),
        (25, 1790492205),
        (25, "1790578665"),
        (11, 123),
    ],
)
def test_limit_up_invalid_new_metadata_fails_the_page(index, value):
    source_row = row("limit_up")
    source_row[index] = value
    with pytest.raises(KaipanlaMarketHistoryUpstream):
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]})).fetch(
            kind="limit_up", trade_date=TRADE_DATE
        )


@pytest.mark.parametrize(
    ("value", "expected"), [(None, None), ("", None), ("--", None), (0, 0), (40, 40)]
)
def test_limit_up_group_count_uses_source_count_not_page_count(value, expected):
    source_row = row("limit_up")
    source_row[27] = value
    item = (
        provider(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]}))
        .fetch(kind="limit_up", trade_date=TRADE_DATE)
        .items[0]
    )
    assert item.reason_group_count == expected


def test_limit_up_legacy_short_row_has_null_tail_fields_and_ignores_unknown_slots():
    source_row = row("limit_up")[:23]
    source_row[10], source_row[17] = "not-consecutive-days", "unknown-source-value"
    response = client(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]})).get(
        ROUTES["limit_up"], params={"trade_date": "2026-09-28"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    for field in ("last_limit_up_at", "max_sealed_amount_cny", "is_resealed", "reason_group_count"):
        assert field in item and item[field] is None
    assert "unknown-source-value" not in response.text and "source_values" not in response.text


def test_limit_up_keeps_http_actual_turnover_value_and_does_not_use_duplicate_slot():
    source_row = row("limit_up")
    source_row[14], source_row[29] = "6.66", "6.64"
    response = client(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]})).get(
        ROUTES["limit_up"], params={"trade_date": "2026-09-28"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["actual_turnover_rate_pct"] == "6.66"


def test_limit_up_openapi_components_match_saved_contract():
    schema = (
        client(Transport({"errcode": 0, "day": "2026-09-28", "list": []}))
        .get("/openapi.json")
        .json()
    )
    saved = json.loads(
        (Path(__file__).parents[1] / "contracts/fastapi-openapi-v1.json").read_text(
            encoding="utf-8"
        )
    )
    model = schema["components"]["schemas"]["LimitUpHistoryItem"]
    assert model == saved["components"]["schemas"]["LimitUpHistoryItem"]
    props = model["properties"]
    assert "last_limit_up_at" in props and "is_resealed" in props and "reason_group_count" in props
    assert props["last_limit_up_at"]["example"] == "2026-10-23 14:54:09"


@pytest.mark.parametrize("value", [None, 0])
def test_limit_up_api_preserves_zero_versus_missing(value):
    source_row = row("limit_up")
    source_row[11], source_row[25] = None, None
    for index, _ in LIMIT_UP_NUMBERS:
        source_row[index] = value
    source_row[24], source_row[27] = value, value
    response = client(Transport({"errcode": 0, "day": "2026-09-28", "list": [source_row]})).get(
        ROUTES["limit_up"], params={"trade_date": "2026-09-28"}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["board_name"] is None and item["last_limit_up_at"] is None
    for _, field in LIMIT_UP_NUMBERS:
        assert item[field] == (None if value is None else "0")
    assert item["is_resealed"] is (None if value is None else False)
    assert item["reason_group_count"] == value
