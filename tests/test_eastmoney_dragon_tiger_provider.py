from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest

from market_data_center.domain.dragon_tiger import (
    DragonTigerAmountPeriodBasis,
    DragonTigerWindowBasis,
)
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.eastmoney_dragon_tiger import (
    BUY_REPORT,
    SCHEMA_VERSION,
    SELL_REPORT,
    SUMMARY_REPORT,
    EastmoneyDragonTigerAdapter,
)
from market_data_center.providers.eastmoney_dragon_tiger_normalizer import (
    map_eastmoney_trigger_window,
    normalize_eastmoney_dragon_tiger_raw,
)

TRADE_DATE = date(2026, 8, 20)


def _summary(
    reason: str = "日价格涨幅偏离值达到7%", event_id: str = "event-1"
) -> dict[str, object]:
    return {
        "TRADE_ID": event_id,
        "SECUCODE": "600000.SH",
        "TRADE_DATE": "2026-08-20 00:00:00",
        "CHANGE_TYPE": "106001",
        "EXPLANATION": reason,
        "CLOSE_PRICE": "10.50",
        "CHANGE_RATE": "7.20",
        "TURNOVERRATE": "12.30",
        "ACCUM_AMOUNT": "1000",
        "BILLBOARD_BUY_AMT": "100",
        "BILLBOARD_SELL_AMT": "40",
    }


def _seat(
    code: str,
    name: str,
    buy: str | None,
    sell: str | None,
    event_id: str = "event-1",
) -> dict[str, object]:
    return {
        "TRADE_ID": event_id,
        "SECUCODE": "600000.SH",
        "TRADE_DATE": "2026-08-20 00:00:00",
        "OPERATEDEPT_CODE": code,
        "OPERATEDEPT_NAME": name,
        "BUY": buy,
        "SELL": sell,
        "NET": None if buy is None or sell is None else str(Decimal(buy) - Decimal(sell)),
        "TOTAL_BUYRIO": None,
        "TOTAL_SELLRIO": None,
    }


def _reports(reason: str = "日价格涨幅偏离值达到7%") -> dict[str, list[dict[str, object]]]:
    return {
        SUMMARY_REPORT: [_summary(reason)],
        BUY_REPORT: [
            _seat("100", "某证券营业部", "80", None),
            _seat("0", "机构专用", "20", None),
        ],
        SELL_REPORT: [
            _seat("100", "某证券营业部", "80", "40"),
            _seat("0", "机构专用", None, "10"),
        ],
    }


def _adapter(
    reports: Mapping[str, list[dict[str, object]]] | None = None,
) -> EastmoneyDragonTigerAdapter:
    rows_by_report = reports or _reports()

    def request(url: str, timeout: float) -> Mapping[str, object]:
        assert timeout == 8.0
        query = parse_qs(urlparse(url).query)
        report = query["reportName"][0]
        rows = rows_by_report[report]
        return {"success": True, "result": {"count": len(rows), "pages": 1, "data": rows}}

    return EastmoneyDragonTigerAdapter(request)


def _event(adapter: EastmoneyDragonTigerAdapter):
    return adapter.fetch_dragon_tiger(TRADE_DATE).normalization.events[0]


@pytest.mark.parametrize(
    ("reason", "basis", "sessions", "occurrences"),
    [
        ("连续三个交易日内涨幅偏离值累计达到20%", "MARKET_SESSIONS", 3, None),
        ("连续3个交易日内涨幅偏离值累计达到20%", "MARKET_SESSIONS", 3, None),
        (
            "有价格涨跌幅限制的连续10个交易日内收盘价格涨幅偏离值累计达到100%的证券",
            "MARKET_SESSIONS",
            10,
            None,
        ),
        ("连续10个交易日内4次出现同正向异常波动的证券", "MARKET_SESSIONS", 10, 4),
        (
            "北交所股票最近3个有成交的交易日以内收盘价涨跌幅偏离值累计达到+40%(-40%)",
            "SECURITY_TRADED_SESSIONS",
            3,
            None,
        ),
    ],
)
def test_maps_verified_trigger_windows(
    reason: str, basis: str, sessions: int, occurrences: int | None
) -> None:
    result = map_eastmoney_trigger_window(reason, TRADE_DATE)
    assert result.basis.value == basis
    assert result.session_count == sessions
    assert result.occurrence_count == occurrences


def test_adapter_keeps_amount_period_unspecified_when_only_trigger_is_known() -> None:
    event = _event(_adapter(_reports("连续10个交易日内4次出现同正向异常波动的证券")))

    assert event.trigger_window.session_count == 10
    assert event.amount_period.basis is DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED


def test_adapter_rejects_an_unknown_multi_day_period_with_a_stable_code() -> None:
    with pytest.raises(ProviderError, match="DT_PERIOD_MAPPING_UNSUPPORTED"):
        _event(_adapter(_reports("最近五个交易日涨幅累计达到30%")))


def test_adapter_merges_reliably_identified_cross_side_seats_by_code() -> None:
    event = _event(_adapter())

    reliable = next(trade for trade in event.seat_trades if trade.seat_source_key == "100")
    anonymous = [trade for trade in event.seat_trades if trade.seat_source_key is None]
    assert reliable.buy_amount == Decimal("80")
    assert reliable.sell_amount == Decimal("40")
    assert reliable.buy_rank == 1
    assert reliable.sell_rank == 1
    assert len(anonymous) == 2


def test_adapter_uses_reliable_code_even_when_the_name_changed() -> None:
    reports = _reports()
    reports[SELL_REPORT][0]["OPERATEDEPT_NAME"] = "更名后的证券营业部"

    reliable = [
        trade for trade in _event(_adapter(reports)).seat_trades if trade.seat_source_key == "100"
    ]

    assert len(reliable) == 1
    assert reliable[0].buy_rank == 1
    assert reliable[0].sell_rank == 1


def test_adapter_preserves_exact_duplicate_raw_and_filters_the_standard_fact() -> None:
    reports = _reports()
    reports[BUY_REPORT].append(dict(reports[BUY_REPORT][0]))

    batch = _adapter(reports).fetch_dragon_tiger(TRADE_DATE)
    result = batch.normalization

    assert len(batch.raw_rows) == 6
    assert len(result.events[0].seat_trades) == 3
    assert result.findings[0].rule_code == "DT_SOURCE_DUPLICATE_FILTERED"
    assert result.findings[0].filtered_count == 1


def test_adapter_filters_explicit_zero_activity_placeholder_but_keeps_raw() -> None:
    reports = _reports()
    zero = _seat("0", "深股通投资者", "0", "0")
    reports[BUY_REPORT].append(zero)

    batch = _adapter(reports).fetch_dragon_tiger(TRADE_DATE)
    result = batch.normalization

    assert any("深股通投资者" in row["payload_json"] for row in batch.raw_rows)
    assert all(
        not (trade.buy_amount == 0 and trade.sell_amount == 0)
        for trade in result.events[0].seat_trades
    )
    assert any(
        finding.rule_code == "DT_ZERO_ACTIVITY_PLACEHOLDER_FILTERED" for finding in result.findings
    )


def test_adapter_accepts_a_buy_only_disclosure() -> None:
    reports = _reports()
    reports[SELL_REPORT] = []

    event = _event(_adapter(reports))

    assert event.buy_disclosure_present is True
    assert event.sell_disclosure_present is False
    assert any(trade.buy_rank is not None for trade in event.seat_trades)


def test_adapter_uses_event_filtered_reads_for_multi_page_details() -> None:
    calls: list[tuple[str, int, str]] = []
    reports = _reports()

    def request(url: str, timeout: float) -> Mapping[str, object]:
        query = parse_qs(urlparse(url).query)
        report = query["reportName"][0]
        page = int(query["pageNumber"][0])
        source_filter = query["filter"][0]
        calls.append((report, page, source_filter))
        rows = reports[report]
        if report == BUY_REPORT and "TRADE_ID='event-1'" not in source_filter:
            return {"success": True, "result": {"count": len(rows), "pages": 2, "data": rows}}
        return {"success": True, "result": {"count": len(rows), "pages": 1, "data": rows}}

    batch = EastmoneyDragonTigerAdapter(request).fetch_dragon_tiger(TRADE_DATE)

    assert batch.normalization.events
    assert any(
        report == BUY_REPORT and "TRADE_ID='event-1'" in source_filter
        for report, _, source_filter in calls
    )
    assert not any(report == BUY_REPORT and page == 2 for report, page, _ in calls)


def test_adapter_rejects_event_filtered_details_when_declared_total_is_not_retrieved() -> None:
    reports = _reports()

    def request(url: str, timeout: float) -> Mapping[str, object]:
        query = parse_qs(urlparse(url).query)
        report = query["reportName"][0]
        source_filter = query["filter"][0]
        rows = reports[report]
        if report == BUY_REPORT and "TRADE_ID='event-1'" not in source_filter:
            return {"success": True, "result": {"count": len(rows) + 1, "pages": 2, "data": rows}}
        return {"success": True, "result": {"count": len(rows), "pages": 1, "data": rows}}

    with pytest.raises(ProviderError, match="DT_SOURCE_COUNT_MISMATCH"):
        EastmoneyDragonTigerAdapter(request).fetch_dragon_tiger(TRADE_DATE)


def test_raw_round_trip_is_deterministic_and_v3_versioned() -> None:
    batch = _adapter().fetch_dragon_tiger(TRADE_DATE)

    assert batch.schema_version == SCHEMA_VERSION == "eastmoney.dragon_tiger.v3"
    assert (
        normalize_eastmoney_dragon_tiger_raw(batch.raw_rows, batch.schema_version)
        == batch.normalization
    )


def test_historical_v1_and_v2_raw_are_replayable() -> None:
    batch = _adapter().fetch_dragon_tiger(TRADE_DATE)

    for schema in ("eastmoney.trading_billboard.v1", "eastmoney.dragon_tiger.v2"):
        assert normalize_eastmoney_dragon_tiger_raw(batch.raw_rows, schema).events


def test_adapter_keeps_bse_stock_for_domain_security_validation() -> None:
    reports = _reports()
    reports[SUMMARY_REPORT][0]["SECUCODE"] = "920000.BJ"
    for report in (BUY_REPORT, SELL_REPORT):
        for row in reports[report]:
            row["SECUCODE"] = "920000.BJ"

    event = _event(_adapter(reports))

    assert event.symbol == "BSE:920000"
    assert event.trigger_window.basis is DragonTigerWindowBasis.MARKET_SESSIONS
