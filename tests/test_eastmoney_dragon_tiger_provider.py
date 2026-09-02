from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest

from market_data_center.domain.dragon_tiger import DragonTigerPeriodType
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.eastmoney_dragon_tiger import (
    BUY_REPORT,
    SCHEMA_VERSION,
    SELL_REPORT,
    SUMMARY_REPORT,
    EastmoneyDragonTigerAdapter,
    normalize_eastmoney_dragon_tiger_raw,
)

TRADE_DATE = date(2026, 8, 20)


def _summary(reason: str = "日价格涨幅偏离值达到7%") -> dict[str, object]:
    return {
        "TRADE_ID": "event-1",
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
) -> dict[str, object]:
    return {
        "TRADE_ID": "event-1",
        "SECUCODE": "600000.SH",
        "TRADE_DATE": "2026-08-20 00:00:00",
        "OPERATEDEPT_CODE": code,
        "OPERATEDEPT_NAME": name,
        "BUY": buy,
        "SELL": sell,
        "NET": "999999",
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
        return {
            "success": True,
            "result": {"count": len(rows), "pages": 1, "data": rows},
        }

    return EastmoneyDragonTigerAdapter(request)


def test_adapter_merges_only_reliably_identified_cross_side_seats() -> None:
    batch = _adapter().fetch_dragon_tiger(TRADE_DATE)
    event = batch.records[0]

    reliable = next(trade for trade in event.seat_trades if trade.seat_source_key == "100")
    anonymous = [trade for trade in event.seat_trades if trade.seat_source_key is None]
    assert reliable.buy_amount == Decimal("80")
    assert reliable.sell_amount == Decimal("40")
    assert reliable.net_amount == Decimal("40")
    assert reliable.buy_rank == 1
    assert reliable.sell_rank == 1
    assert len(anonymous) == 2
    assert all(trade.seat_id is None and trade.is_institution for trade in anonymous)


def test_adapter_never_merges_placeholder_institutions_even_with_a_nonzero_code() -> None:
    reports = _reports()
    reports[BUY_REPORT][1]["OPERATEDEPT_CODE"] = "INST"
    reports[SELL_REPORT][1]["OPERATEDEPT_CODE"] = "INST"

    institutions = [
        trade
        for trade in _adapter(reports).fetch_dragon_tiger(TRADE_DATE).records[0].seat_trades
        if trade.is_institution
    ]

    assert len(institutions) == 2
    assert all(trade.seat_source_key is None for trade in institutions)


def test_adapter_preserves_missing_opposing_amount_instead_of_source_net() -> None:
    event = _adapter().fetch_dragon_tiger(TRADE_DATE).records[0]
    buy_institution = next(trade for trade in event.seat_trades if trade.buy_rank == 2)

    assert buy_institution.sell_amount is None
    assert buy_institution.net_amount is None


def test_adapter_classifies_three_day_without_guessing_the_start_date() -> None:
    event = (
        _adapter(_reports("连续三个交易日内涨幅偏离值累计达到20%"))
        .fetch_dragon_tiger(TRADE_DATE)
        .records[0]
    )

    assert event.period_type is DragonTigerPeriodType.THREE_DAY
    assert event.period_start_date is None
    assert event.period_end_date == TRADE_DATE


def test_adapter_rejects_an_unknown_multi_day_period() -> None:
    with pytest.raises(ProviderError, match="period is unsupported"):
        tuple(
            _adapter(_reports("最近五个交易日涨幅累计达到30%"))
            .fetch_dragon_tiger(TRADE_DATE)
            .records
        )


def test_raw_round_trip_is_deterministic_and_v2_versioned() -> None:
    batch = _adapter().fetch_dragon_tiger(TRADE_DATE)

    assert batch.schema_version == SCHEMA_VERSION
    assert normalize_eastmoney_dragon_tiger_raw(batch.raw_rows, batch.schema_version) == tuple(
        batch.records
    )


def test_historical_trading_billboard_v1_raw_is_replayable_by_new_normalizer() -> None:
    batch = _adapter().fetch_dragon_tiger(TRADE_DATE)

    assert normalize_eastmoney_dragon_tiger_raw(
        batch.raw_rows, "eastmoney.trading_billboard.v1"
    ) == tuple(batch.records)


def test_adapter_rejects_a_partial_three_report_response() -> None:
    reports = _reports()
    reports[SELL_REPORT] = []

    with pytest.raises(ProviderError, match="both buy and sell"):
        tuple(_adapter(reports).fetch_dragon_tiger(TRADE_DATE).records)


def test_adapter_keeps_bse_stock_for_domain_security_validation() -> None:
    reports = _reports()
    reports[SUMMARY_REPORT][0]["SECUCODE"] = "920000.BJ"
    for report in (BUY_REPORT, SELL_REPORT):
        for row in reports[report]:
            row["SECUCODE"] = "920000.BJ"

    assert _adapter(reports).fetch_dragon_tiger(TRADE_DATE).records[0].symbol == "BSE:920000"
