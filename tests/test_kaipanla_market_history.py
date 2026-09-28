"""Historical Kaipanla market-emotion lists: source and public contracts."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest

from market_data_center.providers.kaipanla_market_history import (
    MAX_RESPONSE_BYTES,
    KaipanlaMarketHistoryInvalid,
    KaipanlaMarketHistoryProvider,
    KaipanlaMarketHistoryUpstream,
)

TRADE_DATE = date(2026, 9, 28)
NOW = datetime(2026, 9, 28, 17, tzinfo=UTC)


def row(kind: str, code: str = "001368") -> list[object]:
    result: list[object] = [None] * 34
    result[0], result[1] = code, "示例股票"
    if kind == "auction":
        result[11], result[18], result[19] = "半导体", "2404248.123456789", "10.0567"
    elif kind == "limit_up":
        result[6], result[9], result[16] = 1790578605, "首板", "海峡两岸"
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
        assert set(vars(item)) == {
            "symbol",
            "code",
            "name",
            "board_name",
            "limit_up_bid_amount_cny",
            "auction_change_pct",
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
