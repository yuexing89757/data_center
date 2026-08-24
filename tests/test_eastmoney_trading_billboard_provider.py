from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest

from market_data_center.domain import TradingBillboardSide
from market_data_center.providers import ProviderError
from market_data_center.providers.eastmoney_trading_billboard import (
    BUY_REPORT,
    MAX_PAGES,
    SCHEMA_VERSION,
    SELL_REPORT,
    SUMMARY_REPORT,
    EastmoneyTradingBillboardProvider,
    normalize_eastmoney_trading_billboard_raw,
)

TRADE_DATE = date(2026, 8, 17)


def _summary(
    event_id: str = "100396303",
    secucode: str = "000711.SZ",
    **changes: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "TRADE_ID": event_id,
        "SECUCODE": secucode,
        "SECURITY_CODE": secucode[:6],
        "TRADE_DATE": "2026-08-17 00:00:00",
        "CHANGE_TYPE": "106001",
        "EXPLANATION": "日涨幅偏离值达到7%的前5只证券",
        "CLOSE_PRICE": "12.34",
        "CHANGE_RATE": "9.98",
        "TURNOVERRATE": "18.25",
        "ACCUM_AMOUNT": "10000.00",
        "BILLBOARD_BUY_AMT": "600.00",
        "BILLBOARD_SELL_AMT": "400.00",
        "BILLBOARD_NET_AMT": "200.00",
        "BILLBOARD_DEAL_AMT": "1000.00",
        "DEAL_AMOUNT_RATIO": "10.00",
        "DEAL_NET_RATIO": "2.00",
        "FREE_MARKET_CAP": "50000.00",
    }
    row.update(changes)
    return row


def _seat(
    event_id: str,
    secucode: str,
    index: int,
    *,
    side: str,
    **changes: object,
) -> dict[str, object]:
    buy = Decimal("600") - Decimal(index * 10)
    sell = Decimal("100") + Decimal(index * 10)
    if side == "sell":
        buy, sell = sell, buy
    row: dict[str, object] = {
        "TRADE_ID": event_id,
        "SECUCODE": secucode,
        "TRADE_DATE": "2026-08-17 00:00:00",
        "OPERATEDEPT_CODE": "0" if index < 2 else f"8000000{index}",
        "OPERATEDEPT_NAME": "机构专用" if index < 2 else f"测试营业部{index}",
        "BUY": str(buy),
        "SELL": str(sell),
        "NET": str(buy - sell),
        "TOTAL_BUYRIO": "1.20",
        "TOTAL_SELLRIO": "0.20",
    }
    row.update(changes)
    return row


def _reports() -> dict[str, list[dict[str, object]]]:
    stock = _summary()
    bond = _summary("bond-event", "123213.SZ")
    return {
        SUMMARY_REPORT: [stock, bond],
        BUY_REPORT: [
            *[_seat("100396303", "000711.SZ", index, side="buy") for index in range(5)],
            _seat("bond-event", "123213.SZ", 0, side="buy"),
        ],
        SELL_REPORT: [
            *[_seat("100396303", "000711.SZ", index, side="sell") for index in range(5)],
            _seat("bond-event", "123213.SZ", 0, side="sell"),
        ],
    }


def _provider(
    reports: Mapping[str, list[dict[str, object]]] | None = None,
) -> tuple[EastmoneyTradingBillboardProvider, list[str]]:
    rows_by_report = reports or _reports()
    seen: list[str] = []

    def request(url: str, timeout: float) -> Mapping[str, object]:
        seen.append(url)
        assert timeout == 8.0
        query = parse_qs(urlparse(url).query)
        report = query["reportName"][0]
        page = int(query["pageNumber"][0])
        assert query["filter"] == ["(TRADE_DATE='2026-08-17')"]
        rows = rows_by_report[report]
        assert page == 1
        return {
            "success": True,
            "result": {"count": len(rows), "pages": 1, "data": rows},
        }

    return EastmoneyTradingBillboardProvider(request), seen


def test_adapter_builds_stock_aggregate_and_keeps_filtered_bond_raw() -> None:
    provider, seen = _provider()

    batch = provider.fetch_trading_billboard(TRADE_DATE)
    records = tuple(batch.records)

    assert len(seen) == 3
    assert len(records) == 1
    record = records[0]
    assert record.symbol == "SZSE:000711"
    assert record.buy_amount == Decimal("600.00")
    assert [seat.rank for seat in record.buy_seats] == [1, 2, 3, 4, 5]
    assert [seat.rank for seat in record.sell_seats] == [1, 2, 3, 4, 5]
    assert all(seat.side is TradingBillboardSide.BUY for seat in record.buy_seats)
    assert record.buy_seats[0].seat_code is None
    assert record.buy_seats[1].seat_code is None
    assert len(batch.raw_rows) == 14
    assert any("123213.SZ" in row["payload_json"] for row in batch.raw_rows)
    assert batch.schema_version == SCHEMA_VERSION
    assert (
        normalize_eastmoney_trading_billboard_raw(batch.raw_rows, batch.schema_version) == records
    )


@pytest.mark.parametrize(
    ("secucode", "expected"),
    [
        ("600000.SH", "SSE:600000"),
        ("000001.SZ", "SZSE:000001"),
        ("920000.BJ", "BSE:920000"),
    ],
)
def test_normalizer_maps_supported_stock_suffixes(secucode: str, expected: str) -> None:
    reports = _reports()
    reports[SUMMARY_REPORT] = [_summary(secucode=secucode)]
    reports[BUY_REPORT] = [_seat("100396303", secucode, index, side="buy") for index in range(5)]
    reports[SELL_REPORT] = [_seat("100396303", secucode, index, side="sell") for index in range(5)]
    provider, _ = _provider(reports)

    assert provider.fetch_trading_billboard(TRADE_DATE).records[0].symbol == expected


def test_equal_amount_seats_have_deterministic_tie_break_order() -> None:
    reports = _reports()
    reports[BUY_REPORT][0]["BUY"] = "600"
    reports[BUY_REPORT][1]["BUY"] = "600"
    reports[BUY_REPORT][0]["OPERATEDEPT_NAME"] = "机构专用B"
    reports[BUY_REPORT][1]["OPERATEDEPT_NAME"] = "机构专用A"
    provider, _ = _provider(reports)

    seats = provider.fetch_trading_billboard(TRADE_DATE).records[0].buy_seats

    assert [seat.seat_name for seat in seats[:2]] == ["机构专用A", "机构专用B"]


@pytest.mark.parametrize("payload", [{"success": False}, {"success": True}])
def test_adapter_rejects_unavailable_response(payload: Mapping[str, object]) -> None:
    provider = EastmoneyTradingBillboardProvider(lambda _url, _timeout: payload)

    with pytest.raises(ProviderError, match="unavailable"):
        provider.fetch_trading_billboard(TRADE_DATE)


@pytest.mark.parametrize(
    ("report", "field", "value", "message"),
    [
        (SUMMARY_REPORT, "TRADE_DATE", "2026-08-18", "date"),
        (SUMMARY_REPORT, "BILLBOARD_BUY_AMT", "bad", "decimal"),
        (SUMMARY_REPORT, "BILLBOARD_BUY_AMT", "NaN", "decimal"),
        (SUMMARY_REPORT, "TRADE_ID", None, "required"),
    ],
)
def test_normalizer_rejects_bad_required_source_values(
    report: str, field: str, value: object, message: str
) -> None:
    reports = _reports()
    reports[report][0][field] = value
    provider, _ = _provider(reports)

    with pytest.raises(ProviderError, match=message):
        tuple(provider.fetch_trading_billboard(TRADE_DATE).records)


def test_normalizer_rejects_missing_side_for_accepted_event() -> None:
    reports = _reports()
    reports[SELL_REPORT] = [row for row in reports[SELL_REPORT] if row["TRADE_ID"] != "100396303"]
    provider, _ = _provider(reports)

    with pytest.raises(ProviderError, match="one to five"):
        tuple(provider.fetch_trading_billboard(TRADE_DATE).records)


def test_raw_normalizer_rejects_unknown_schema_and_kind() -> None:
    provider, _ = _provider()
    batch = provider.fetch_trading_billboard(TRADE_DATE)

    with pytest.raises(ProviderError, match="schema"):
        normalize_eastmoney_trading_billboard_raw(batch.raw_rows, "unknown")
    malformed = ({**batch.raw_rows[0], "record_kind": "unknown"}, *batch.raw_rows[1:])
    with pytest.raises(ProviderError, match="record kind"):
        normalize_eastmoney_trading_billboard_raw(malformed, SCHEMA_VERSION)


def test_adapter_rejects_duplicate_pages_and_changing_counts() -> None:
    rows = _reports()

    def duplicate_page(url: str, _timeout: float) -> Mapping[str, object]:
        query = parse_qs(urlparse(url).query)
        report = query["reportName"][0]
        data = rows[report]
        return {"success": True, "result": {"count": len(data) * 2, "pages": 2, "data": data}}

    with pytest.raises(ProviderError, match="duplicate page"):
        EastmoneyTradingBillboardProvider(duplicate_page).fetch_trading_billboard(TRADE_DATE)

    calls = 0

    def changing_count(url: str, _timeout: float) -> Mapping[str, object]:
        nonlocal calls
        calls += 1
        query = parse_qs(urlparse(url).query)
        report = query["reportName"][0]
        page = int(query["pageNumber"][0])
        data = rows[report][:1]
        return {
            "success": True,
            "result": {"count": 2 if page == 1 else 3, "pages": 2, "data": data},
        }

    with pytest.raises(ProviderError, match="count changed"):
        EastmoneyTradingBillboardProvider(changing_count).fetch_trading_billboard(TRADE_DATE)
    assert calls == 2


def test_adapter_rejects_page_count_above_bound() -> None:
    payload = {
        "success": True,
        "result": {"count": MAX_PAGES + 1, "pages": MAX_PAGES + 1, "data": []},
    }
    provider = EastmoneyTradingBillboardProvider(lambda _url, _timeout: payload)

    with pytest.raises(ProviderError, match="page bound"):
        provider.fetch_trading_billboard(TRADE_DATE)


def test_network_failure_stops_after_two_attempts() -> None:
    attempts = 0

    def request(_url: str, _timeout: float) -> Mapping[str, object]:
        nonlocal attempts
        attempts += 1
        raise OSError("network unavailable")

    provider = EastmoneyTradingBillboardProvider(request)

    with pytest.raises(ProviderError, match="request failed"):
        provider.fetch_trading_billboard(TRADE_DATE)
    assert attempts == 2
