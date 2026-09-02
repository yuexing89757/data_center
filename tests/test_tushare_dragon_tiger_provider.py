from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain.dragon_tiger import DragonTigerPeriodType
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
    event = batch.records[0]

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
        .records[0]
    )
    second = (
        TushareDragonTigerAdapter(FakeClient(_responses()))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .records[0]
    )

    assert first.source_record_id == second.source_record_id


def test_tushare_raw_round_trip_is_deterministic() -> None:
    batch = TushareDragonTigerAdapter(FakeClient(_responses())).fetch_dragon_tiger(
        date(2026, 8, 20)
    )

    assert normalize_tushare_dragon_tiger_raw(batch.raw_rows, batch.schema_version) == tuple(
        batch.records
    )


def test_adapter_classifies_three_day_reason_without_calendar_guessing() -> None:
    responses = _responses()
    responses["top_list"][0]["reason"] = "连续三个交易日内涨幅偏离值累计达到20%"
    for row in responses["top_inst"]:
        row["reason"] = "连续三个交易日内涨幅偏离值累计达到20%"

    event = (
        TushareDragonTigerAdapter(FakeClient(responses))
        .fetch_dragon_tiger(date(2026, 8, 20))
        .records[0]
    )

    assert event.period_type is DragonTigerPeriodType.THREE_DAY
    assert event.period_start_date is None


def test_adapter_rejects_detail_rows_that_cannot_join_an_event() -> None:
    responses = _responses()
    responses["top_inst"][0]["reason"] = "另一个原因"

    with pytest.raises(ProviderError, match="join"):
        tuple(
            TushareDragonTigerAdapter(FakeClient(responses))
            .fetch_dragon_tiger(date(2026, 8, 20))
            .records
        )


def test_adapter_rejects_an_unknown_multi_day_period() -> None:
    responses = _responses()
    responses["top_list"][0]["reason"] = "最近五个交易日涨幅累计达到30%"
    for row in responses["top_inst"]:
        row["reason"] = "最近五个交易日涨幅累计达到30%"

    with pytest.raises(ProviderError, match="period is unsupported"):
        tuple(
            TushareDragonTigerAdapter(FakeClient(responses))
            .fetch_dragon_tiger(date(2026, 8, 20))
            .records
        )
