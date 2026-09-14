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


@pytest.mark.parametrize(
    ("reason", "sessions", "occurrences"),
    [
        ("有价格涨跌幅限制的连续10个交易日内收盘价格涨幅偏离值累计达到100%的证券", 10, None),
        ("有价格涨跌幅限制的连续30个交易日内收盘价格涨幅偏离值累计达到200%的证券", 30, None),
        ("连续10个交易日内4次出现同正向异常波动的证券", 10, 4),
    ],
)
def test_adapter_maps_verified_long_windows_without_inventing_amount_period(
    reason: str, sessions: int, occurrences: int | None
) -> None:
    responses = _responses()
    responses["top_list"][0]["reason"] = reason
    for row in responses["top_inst"]:
        row["reason"] = reason

    event = (
        TushareDragonTigerAdapter(FakeClient(responses))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .normalization.events[0]
    )

    assert event.trigger_window.session_count == sessions
    assert event.trigger_window.occurrence_count == occurrences
    assert event.amount_period.basis is DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED
    assert event.amount_period.session_count is None


def test_adapter_joins_a_unique_source_reason_alias_for_one_symbol() -> None:
    responses = _responses()
    responses["top_list"][0]["reason"] = "日价格涨幅偏离值达到12.66%"
    for row in responses["top_inst"]:
        row["reason"] = "日涨幅偏离值达到7%的前5只证券"

    result = (
        TushareDragonTigerAdapter(FakeClient(responses))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .normalization
    )

    assert len(result.events) == 1
    assert result.events[0].reason_name_raw == "日价格涨幅偏离值达到12.66%"
    assert any(item.rule_code == "DT_TUSHARE_REASON_ALIAS_JOINED" for item in result.findings)


def test_adapter_filters_truncated_duplicate_seat_with_identical_amount_facts() -> None:
    responses = _responses()
    original = responses["top_inst"][1]
    original["exalter"] = "某证券股份有限公司上海证券"
    for index in range(4):
        row = dict(original)
        row["exalter"] = f"卖出营业部{index}"
        row["sell"] = str(50 + index)
        responses["top_inst"].append(row)
    duplicate = dict(original)
    duplicate["exalter"] = "某证券股份有限公司上海证券营业部"
    responses["top_inst"].append(duplicate)

    result = (
        TushareDragonTigerAdapter(FakeClient(responses))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .normalization
    )

    assert len([trade for trade in result.events[0].seat_trades if trade.sell_rank]) == 5
    assert any(item.rule_code == "DT_SOURCE_DUPLICATE_FILTERED" for item in result.findings)


def test_adapter_refetches_details_for_symbols_missing_from_bulk_response() -> None:
    responses = _responses()
    second = dict(responses["top_list"][0])
    second["ts_code"] = "600001.SH"
    responses["top_list"].append(second)

    class RepairingClient(FakeClient):
        def query(
            self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
        ) -> Sequence[Mapping[str, object]]:
            self.calls.append((api_name, params, fields))
            if api_name == "top_inst" and params.get("ts_code") == "600001.SH":
                repaired = []
                for row in self.responses["top_inst"]:
                    item = dict(row)
                    item["ts_code"] = "600001.SH"
                    repaired.append(item)
                return repaired
            return self.responses[api_name]

    client = RepairingClient(responses)
    batch = TushareDragonTigerAdapter(client).fetch_dragon_tiger(date(2026, 8, 20))

    assert len(batch.normalization.events) == 2
    assert any(call[1].get("ts_code") == "600001.SH" for call in client.calls)
    assert any("600001.SH" in row["payload_json"] for row in batch.raw_rows)


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
