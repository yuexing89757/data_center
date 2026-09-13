from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain.dragon_tiger import (
    DragonTigerAmountPeriodBasis,
    DragonTigerWindowBasis,
)
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.tushare_dragon_tiger import (
    SCHEMA_VERSION,
    TushareDragonTigerAdapter,
    normalize_tushare_dragon_tiger_raw,
)


class FakeClient:
    def __init__(self, responses: Mapping[str, Sequence[Mapping[str, object]]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, Mapping[str, str], Sequence[str]]] = []

    def query(
        self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
    ) -> Sequence[Mapping[str, object]]:
        self.calls.append((api_name, params, fields))
        return self.responses[api_name]


def _responses() -> dict[str, list[dict[str, object]]]:
    return {
        "top_list": [
            {
                "trade_date": "20260820",
                "ts_code": "600000.SH",
                "name": "浦发银行",
                "close": "10.5",
                "pct_change": "7.2",
                "turnover_rate": "12.3",
                "amount": "1000",
                "l_sell": "40",
                "l_buy": "100",
                "l_amount": "140",
                "net_amount": "60",
                "net_rate": "6",
                "amount_rate": "14",
                "float_values": "5000",
                "reason": "日价格涨幅偏离值达到7%",
            }
        ],
        "top_inst": [
            {
                "trade_date": "20260820",
                "ts_code": "600000.SH",
                "exalter": "机构专用",
                "side": "0",
                "buy": "80",
                "buy_rate": "8",
                "sell": None,
                "sell_rate": None,
                "net_buy": "80",
                "reason": "日价格涨幅偏离值达到7%",
            },
            {
                "trade_date": "20260820",
                "ts_code": "600000.SH",
                "exalter": "机构专用",
                "side": "1",
                "buy": None,
                "buy_rate": None,
                "sell": "40",
                "sell_rate": "4",
                "net_buy": "-40",
                "reason": "日价格涨幅偏离值达到7%",
            },
        ],
    }


def test_adapter_calls_both_documented_apis_and_keeps_anonymous_rows_separate() -> None:
    client = FakeClient(_responses())
    batch = TushareDragonTigerAdapter(client).fetch_dragon_tiger(date(2026, 8, 20))
    event = batch.normalization.events[0]

    assert [call[0] for call in client.calls] == ["top_list", "top_inst"]
    assert all(call[1] == {"trade_date": "20260820"} for call in client.calls)
    assert batch.schema_version == SCHEMA_VERSION
    assert event.symbol == "SSE:600000"
    assert event.lhb_buy_amount == Decimal("100")
    assert len(event.seat_trades) == 2
    assert all(trade.seat_id is None for trade in event.seat_trades)


def test_adapter_derives_a_stable_event_identity() -> None:
    first = (
        TushareDragonTigerAdapter(FakeClient(_responses()))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .normalization.events[0]
    )
    second = (
        TushareDragonTigerAdapter(FakeClient(_responses()))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .normalization.events[0]
    )

    assert first.source_record_id == second.source_record_id


def test_adapter_skips_provider_placeholder_seat_with_both_amounts_zero() -> None:
    responses = _responses()
    responses["top_inst"].append(
        {
            "trade_date": "20260820",
            "ts_code": "600000.SH",
            "exalter": "占位营业部",
            "side": "0",
            "buy": "0",
            "buy_rate": "0",
            "sell": "0",
            "sell_rate": "0",
            "net_buy": "0",
            "reason": "日价格涨幅偏离值达到7%",
        }
    )

    event = (
        TushareDragonTigerAdapter(FakeClient(responses))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .normalization.events[0]
    )

    assert len(event.seat_trades) == 2
    assert all(trade.seat_name_raw != "占位营业部" for trade in event.seat_trades)
    result = (
        TushareDragonTigerAdapter(FakeClient(responses))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .normalization
    )
    assert result.findings[0].rule_code == "DT_ZERO_ACTIVITY_PLACEHOLDER_FILTERED"


def test_tushare_raw_round_trip_is_deterministic() -> None:
    batch = TushareDragonTigerAdapter(FakeClient(_responses())).fetch_dragon_tiger(
        date(2026, 8, 20)
    )

    assert (
        normalize_tushare_dragon_tiger_raw(batch.raw_rows, batch.schema_version)
        == batch.normalization
    )


def test_adapter_classifies_three_day_reason_without_calendar_guessing() -> None:
    responses = _responses()
    responses["top_list"][0]["reason"] = "连续三个交易日内涨幅偏离值累计达到20%"
    for row in responses["top_inst"]:
        row["reason"] = "连续三个交易日内涨幅偏离值累计达到20%"

    event = (
        TushareDragonTigerAdapter(FakeClient(responses))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .normalization.events[0]
    )

    assert event.trigger_window.basis is DragonTigerWindowBasis.MARKET_SESSIONS
    assert event.trigger_window.session_count == 3
    assert event.trigger_window.start_date is None
    assert event.amount_period.basis is DragonTigerAmountPeriodBasis.MARKET_SESSIONS


def test_adapter_keeps_unmatched_detail_in_raw_and_reports_filtered_finding() -> None:
    responses = _responses()
    responses["top_inst"][0]["reason"] = "另一个原因"

    batch = TushareDragonTigerAdapter(FakeClient(responses)).fetch_dragon_tiger(date(2026, 8, 20))

    assert any("另一个原因" in row["payload_json"] for row in batch.raw_rows)
    assert batch.normalization.findings[0].rule_code == "DT_TUSHARE_UNMATCHED_DETAIL_FILTERED"


def test_adapter_keeps_bse_rows_in_raw_but_excludes_them_from_standard_facts() -> None:
    responses = _responses()
    summary = dict(responses["top_list"][0])
    summary["ts_code"] = "920000.BJ"
    responses["top_list"].append(summary)
    for original in tuple(responses["top_inst"]):
        detail = dict(original)
        detail["ts_code"] = "920000.BJ"
        responses["top_inst"].append(detail)

    batch = TushareDragonTigerAdapter(FakeClient(responses)).fetch_dragon_tiger(date(2026, 8, 20))

    assert any("920000.BJ" in row["payload_json"] for row in batch.raw_rows)
    assert [event.symbol for event in batch.normalization.events] == ["SSE:600000"]
    assert (
        sum(
            item.filtered_count
            for item in batch.normalization.findings
            if item.rule_code == "DT_BSE_SECURITY_FILTERED"
        )
        == 3
    )


def test_adapter_filters_duplicate_summary_when_only_name_differs() -> None:
    responses = _responses()
    duplicate = dict(responses["top_list"][0])
    duplicate["name"] = "历史简称"
    responses["top_list"].append(duplicate)

    batch = TushareDragonTigerAdapter(FakeClient(responses)).fetch_dragon_tiger(date(2026, 8, 20))

    assert len(batch.raw_rows) == 4
    assert len(batch.normalization.events) == 1
    finding = next(
        item
        for item in batch.normalization.findings
        if item.rule_code == "DT_SOURCE_DUPLICATE_FILTERED"
    )
    assert finding.filtered_count == 1


def test_adapter_rejects_duplicate_summary_with_conflicting_market_fact() -> None:
    responses = _responses()
    duplicate = dict(responses["top_list"][0])
    duplicate["close"] = "10.6"
    responses["top_list"].append(duplicate)

    with pytest.raises(ProviderError, match="conflicting duplicate summary"):
        _ = (
            TushareDragonTigerAdapter(FakeClient(responses))
            .fetch_dragon_tiger(date(2026, 8, 20))
            .normalization
        )
    )


def test_adapter_rejects_an_unknown_multi_day_period() -> None:
    responses = _responses()
    responses["top_list"][0]["reason"] = "最近五个交易日涨幅累计达到30%"
    for row in responses["top_inst"]:
        row["reason"] = "最近五个交易日涨幅累计达到30%"

    with pytest.raises(ProviderError, match="DT_PERIOD_MAPPING_UNSUPPORTED"):
        tuple(
            TushareDragonTigerAdapter(FakeClient(responses))
            .fetch_dragon_tiger(date(2026, 8, 20))
            .normalization.events
        )
