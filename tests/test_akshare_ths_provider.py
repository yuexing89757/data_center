from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain import (
    BoardIndexConstituentSnapshotRecord,
    BoardIndexDailyBarRecord,
    BoardIndexRecord,
)
from market_data_center.domain.ingestion import DatasetCode
from market_data_center.providers.akshare_ths import (
    THS_BOARD_ID,
    AKShareTHSProvider,
    _parse_constituent_rows,
    _parse_daily_payload,
    _parse_page_count,
    normalize_akshare_ths_raw,
)
from market_data_center.providers.contracts import ProviderError

TODAY = date(2026, 7, 29)


class FakeClient:
    def board_index_daily_bars(
        self, board_code: str, start_date: date, end_date: date
    ) -> Sequence[Mapping[str, object]]:
        assert board_code == "883423"
        return (
            {
                "日期": TODAY,
                "开盘价": "225.229",
                "最高价": "225.772",
                "最低价": "220.542",
                "收盘价": "223.554",
                "成交量": "7365903100",
                "成交额": "147481890000.000",
            },
        )

    def board_index_constituents(self, board_code: str) -> Sequence[Mapping[str, object]]:
        assert board_code == "883423"
        return (
            {"序号": 1, "代码": "600000", "名称": "浦发银行"},
            {"序号": 2, "代码": "000001", "名称": "平安银行"},
        )


def test_explicit_board_directory_and_daily_bar_preserve_decimal_precision() -> None:
    provider = AKShareTHSProvider(FakeClient(), today=lambda: TODAY)

    directory = provider.fetch_board_indexes()
    daily = provider.fetch_board_index_daily_bars(THS_BOARD_ID, TODAY, TODAY)

    assert isinstance(directory.records[0], BoardIndexRecord)
    assert directory.records[0].name == "沪深主板昨日涨停"
    assert isinstance(daily.records[0], BoardIndexDailyBarRecord)
    assert daily.records[0].open == Decimal("225.229")
    assert daily.records[0].amount == Decimal("147481890000.000")
    assert daily.request_params["adjust"] == ""


def test_constituents_are_complete_current_snapshot_with_standard_symbols() -> None:
    provider = AKShareTHSProvider(FakeClient(), today=lambda: TODAY)

    batch = provider.fetch_board_index_constituents(THS_BOARD_ID, TODAY)

    assert isinstance(batch.records[0], BoardIndexConstituentSnapshotRecord)
    assert batch.records[0].members == ("SSE:600000", "SZSE:000001")


def test_provider_refuses_to_forge_historical_constituent_snapshot() -> None:
    provider = AKShareTHSProvider(FakeClient(), today=lambda: TODAY)

    with pytest.raises(ProviderError, match="only the current constituent snapshot"):
        provider.fetch_board_index_constituents(THS_BOARD_ID, date(2026, 7, 28))


def test_raw_replay_uses_request_identity_and_schema_version() -> None:
    rows = FakeClient().board_index_daily_bars("883423", TODAY, TODAY)
    raw_rows = [{key: str(value) for key, value in row.items()} for row in rows]

    records = normalize_akshare_ths_raw(
        DatasetCode.BOARD_INDEX_DAILY_BAR,
        "akshare_ths.board_index_daily_bar.v1",
        raw_rows,
        {"board_id": THS_BOARD_ID},
    )

    assert isinstance(records[0], BoardIndexDailyBarRecord)
    assert records[0].trade_date == TODAY


def test_ths_payload_parsers_reject_no_data_and_parse_full_pages() -> None:
    payload = 'callback({"data":"20260729,10,11,9,10.5,100,1050,,,,0"});'
    rows = _parse_daily_payload(payload, TODAY, TODAY)
    html = """
    <table><tbody><tr>
      <td>1</td><td><a>600000</a></td><td>浦发银行</td><td>10.00</td>
    </tr></tbody></table>
    <div><span class="page_info">1/3</span></div>
    """

    assert rows[0]["收盘价"] == "10.5"
    assert _parse_constituent_rows(html)[0]["代码"] == "600000"
    assert _parse_page_count(html) == 3

    with pytest.raises(ProviderError, match="payload wrapper changed"):
        _parse_daily_payload("not-json", TODAY, TODAY)
