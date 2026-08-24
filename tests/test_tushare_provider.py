from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pytest import MonkeyPatch

from market_data_center.domain import (
    DailyBarRecord,
    Exchange,
    PriceLimitStatus,
    SecurityStatus,
    ShareholderCountRecord,
    StockDailyIndicatorSnapshotRecord,
    TradeStatus,
)
from market_data_center.domain.ingestion import DatasetCode
from market_data_center.providers import ProviderError, ProviderRequestUnavailable, TushareProvider
from market_data_center.providers.tushare import normalize_tushare_raw


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, str], Sequence[str]]] = []

    def query(
        self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
    ) -> Sequence[Mapping[str, object]]:
        self.calls.append((api_name, params, fields))
        if api_name == "stock_basic":
            rows: Mapping[str, Sequence[Mapping[str, object]]] = {
                "L": (
                    {
                        "ts_code": "600000.SH",
                        "symbol": "600000",
                        "name": "浦发银行",
                        "exchange": "SSE",
                        "list_status": "L",
                        "list_date": "19991110",
                        "delist_date": None,
                    },
                ),
                "D": (
                    {
                        "ts_code": "000003.SZ",
                        "symbol": "000003",
                        "name": "测试退市股",
                        "exchange": "SZSE",
                        "list_status": "D",
                        "list_date": "19910101",
                        "delist_date": "20020101",
                    },
                ),
                "P": (),
            }
            return rows[params["list_status"]]
        if api_name == "trade_cal":
            return (
                {"exchange": "SSE", "cal_date": "20260801", "is_open": 0},
                {"exchange": "SSE", "cal_date": "20260802", "is_open": 0},
                {"exchange": "SSE", "cal_date": "20260803", "is_open": 1},
            )
        if api_name == "daily":
            return (
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260803",
                    "open": "10.1",
                    "high": "10.8",
                    "low": "10.0",
                    "close": "10.5",
                    "pre_close": "10.0",
                    "vol": "123.45",
                    "amount": "456.789",
                },
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260731",
                    "open": "9.9",
                    "high": "10.1",
                    "low": "9.8",
                    "close": "10.0",
                    "pre_close": "9.9",
                    "vol": "100",
                    "amount": "200",
                },
            )
        if api_name == "daily_basic":
            return (
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260731",
                    "close": "10.50",
                    "turnover_rate": "1.25",
                    "turnover_rate_f": "1.60",
                    "volume_ratio": "1.12",
                    "pe": "8.5",
                    "pe_ttm": "8.2",
                    "pb": "0.8",
                    "ps": "2.1",
                    "ps_ttm": "2.0",
                    "dv_ratio": "3.2",
                    "dv_ttm": "3.3",
                    "total_share": "1000.00",
                    "float_share": "800.00",
                    "free_share": "600.00",
                    "total_mv": "10500.50",
                    "circ_mv": "8400.40",
                    "limit_status": 2,
                },
            )
        if api_name == "stk_holdernumber":
            return (
                {
                    "ts_code": "920000.BJ",
                    "ann_date": "20260820",
                    "end_date": "20260630",
                    "holder_num": "12001",
                },
            )
        raise AssertionError(api_name)


def test_security_mapping_fetches_all_source_statuses() -> None:
    client = FakeClient()
    records = TushareProvider(client).fetch_securities().records

    assert [call[1]["list_status"] for call in client.calls] == ["L", "D", "P"]
    assert records[0].symbol == "SZSE:000003"
    assert records[0].status is SecurityStatus.DELISTED
    assert records[0].delisting_date == date(2002, 1, 1)
    assert records[1].exchange is Exchange.SSE
    assert records[1].status is SecurityStatus.LISTED
    assert records[1].source_code == "tushare"


def test_trading_calendar_requires_and_maps_every_natural_day() -> None:
    records = (
        TushareProvider(FakeClient())
        .fetch_trading_calendar(date(2026, 8, 1), date(2026, 8, 3))
        .records
    )

    assert [record.is_trading_day for record in records] == [False, False, True]


def test_daily_bar_is_unadjusted_sorted_and_normalizes_units() -> None:
    client = FakeClient()
    batch = TushareProvider(client).fetch_daily_bars(
        "SSE:600000", date(2026, 7, 31), date(2026, 8, 3)
    )
    records = batch.records

    assert client.calls[0][1]["ts_code"] == "600000.SH"
    assert batch.request_params["adjust"] == "none"
    assert [record.trade_date for record in records] == [date(2026, 7, 31), date(2026, 8, 3)]
    assert records[1].close == Decimal("10.5")
    assert records[1].volume == 12_345
    assert records[1].amount == Decimal("456789.000")
    assert records[1].trade_status is TradeStatus.TRADING


def test_raw_replay_preserves_daily_bar_normalization() -> None:
    batch = TushareProvider(FakeClient()).fetch_daily_bars(
        "600000.SH", date(2026, 7, 31), date(2026, 8, 3)
    )

    records = normalize_tushare_raw(
        DatasetCode.DAILY_BAR,
        batch.schema_version,
        batch.raw_rows,
        batch.request_params,
    )

    assert isinstance(records[1], DailyBarRecord)
    assert records[1].volume == 12_345
    assert records[1].amount == Decimal("456789.000")


def test_daily_indicator_normalizes_units_status_and_raw_replay() -> None:
    provider = TushareProvider(FakeClient())
    batch = provider.fetch_stock_daily_indicators(
        "SSE:600000", date(2026, 7, 31), date(2026, 7, 31)
    )
    record = batch.records[0]

    assert record.total_shares == 10_000_000
    assert record.free_float_shares == 6_000_000
    assert record.total_market_value == Decimal("105005000.00")
    assert record.turnover_rate_pct == Decimal("1.25")
    assert record.price_limit_status is PriceLimitStatus.LIMIT_UP

    replayed = normalize_tushare_raw(
        DatasetCode.STOCK_DAILY_INDICATOR,
        batch.schema_version,
        batch.raw_rows,
        batch.request_params,
    )
    assert isinstance(replayed[0], StockDailyIndicatorSnapshotRecord)
    assert replayed[0] == record


def test_daily_indicator_snapshot_queries_one_complete_trade_date() -> None:
    client = FakeClient()

    batch = TushareProvider(client).fetch_stock_daily_indicator_snapshot(date(2026, 7, 31))

    assert client.calls[0][0] == "daily_basic"
    assert client.calls[0][1] == {"trade_date": "20260731"}
    assert batch.request_params == {"trade_date": "20260731"}
    assert batch.records[0].symbol == "SSE:600000"


@pytest.mark.parametrize("row_count", [0, 6_000])
def test_daily_indicator_snapshot_rejects_empty_or_limit_sized_response(
    row_count: int,
) -> None:
    class SnapshotSizeClient(FakeClient):
        def query(
            self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
        ) -> Sequence[Mapping[str, object]]:
            rows = super().query(api_name, params=params, fields=fields)
            if api_name != "daily_basic":
                return rows
            return tuple(rows[0] for _ in range(row_count))

    batch = TushareProvider(SnapshotSizeClient()).fetch_stock_daily_indicator_snapshot(
        date(2026, 7, 31)
    )

    with pytest.raises(ProviderError, match=r"empty market snapshot|response limit"):
        _ = batch.records


def test_default_requires_token(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ProviderError, match="TUSHARE_TOKEN is required"):
        TushareProvider.default()


def test_default_loads_token_from_dotenv(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("TUSHARE_TOKEN=test-token\n", encoding="utf-8")

    assert isinstance(TushareProvider.default(), TushareProvider)


def test_source_symbol_maps_all_supported_exchanges() -> None:
    provider = TushareProvider(FakeClient())

    assert provider.source_symbol("SSE:600000") == "600000.SH"
    assert provider.source_symbol("SZSE:000001") == "000001.SZ"
    assert provider.source_symbol("BSE:920000") == "920000.BJ"


def test_unaccepted_capabilities_are_explicitly_unavailable() -> None:
    provider = TushareProvider(FakeClient())

    with pytest.raises(ProviderRequestUnavailable):
        provider.fetch_capital("600000.SH")
    with pytest.raises(ProviderRequestUnavailable):
        provider.fetch_classification_catalog("industry", date(2026, 8, 2))


class IncompleteCalendarClient(FakeClient):
    def query(
        self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
    ) -> Sequence[Mapping[str, object]]:
        rows = super().query(api_name, params=params, fields=fields)
        return rows[:-1] if api_name == "trade_cal" else rows


def test_calendar_gap_is_not_silently_treated_as_closed() -> None:
    batch = TushareProvider(IncompleteCalendarClient()).fetch_trading_calendar(
        date(2026, 8, 1), date(2026, 8, 3)
    )

    with pytest.raises(ProviderError, match="first missing date"):
        _ = batch.records


def test_shareholder_count_maps_bse_request_and_raw_replay() -> None:
    client = FakeClient()
    provider = TushareProvider(client)

    batch = provider.fetch_shareholder_counts("BSE:920000", date(2026, 8, 1), date(2026, 8, 24))

    assert client.calls[0] == (
        "stk_holdernumber",
        {"ts_code": "920000.BJ", "start_date": "20260801", "end_date": "20260824"},
        ("ts_code", "ann_date", "end_date", "holder_num"),
    )
    assert batch.request_params == {
        "source_symbol": "920000.BJ",
        "start_date": "20260801",
        "end_date": "20260824",
    }
    assert batch.schema_version == "tushare.shareholder_count.v1"
    assert batch.records[0].shareholder_count == 12001
    assert batch.records[0].symbol == "BSE:920000"

    replayed = normalize_tushare_raw(
        DatasetCode.SHAREHOLDER_COUNT,
        batch.schema_version,
        batch.raw_rows,
        batch.request_params,
    )
    assert isinstance(replayed[0], ShareholderCountRecord)
    assert replayed == batch.records


def test_shareholder_count_all_market_request_can_succeed_empty() -> None:
    class EmptyClient(FakeClient):
        def query(
            self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
        ) -> Sequence[Mapping[str, object]]:
            self.calls.append((api_name, params, fields))
            if api_name == "stk_holdernumber":
                return ()
            return super().query(api_name, params=params, fields=fields)

    client = EmptyClient()
    batch = TushareProvider(client).fetch_shareholder_counts(
        None, date(2026, 8, 1), date(2026, 8, 24)
    )

    assert client.calls[0][1] == {"start_date": "20260801", "end_date": "20260824"}
    assert batch.request_params["source_symbol"] is None
    assert batch.records == ()


@pytest.mark.parametrize("holder_num", [None, "", "1.5", "0"])
def test_shareholder_count_rejects_invalid_integer_counts(holder_num: object) -> None:
    class InvalidCountClient(FakeClient):
        def query(
            self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
        ) -> Sequence[Mapping[str, object]]:
            if api_name == "stk_holdernumber":
                return (
                    {
                        "ts_code": "600000.SH",
                        "ann_date": "20260820",
                        "end_date": "20260630",
                        "holder_num": holder_num,
                    },
                )
            return super().query(api_name, params=params, fields=fields)

    batch = TushareProvider(InvalidCountClient()).fetch_shareholder_counts(
        "SSE:600000", date(2026, 8, 1), date(2026, 8, 24)
    )

    with pytest.raises(ProviderError, match=r"holder_num|positive"):
        _ = batch.records


def test_shareholder_count_rejects_missing_count_field() -> None:
    class MissingCountClient(FakeClient):
        def query(
            self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
        ) -> Sequence[Mapping[str, object]]:
            if api_name == "stk_holdernumber":
                return (
                    {
                        "ts_code": "600000.SH",
                        "ann_date": "20260820",
                        "end_date": "20260630",
                    },
                )
            return super().query(api_name, params=params, fields=fields)

    with pytest.raises(ProviderError, match="missing fields: holder_num"):
        TushareProvider(MissingCountClient()).fetch_shareholder_counts(
            "SSE:600000", date(2026, 8, 1), date(2026, 8, 24)
        )


def test_shareholder_count_rejects_reversed_dates_before_request() -> None:
    client = FakeClient()

    with pytest.raises(ValueError, match="end_date"):
        TushareProvider(client).fetch_shareholder_counts(None, date(2026, 8, 24), date(2026, 8, 1))

    assert client.calls == []


def test_only_shareholder_count_requests_are_paced() -> None:
    sleeps: list[float] = []
    provider = TushareProvider(
        FakeClient(),
        shareholder_count_request_interval_seconds=1 / 3,
        sleeper=sleeps.append,
    )

    provider.fetch_shareholder_counts(None, date(2026, 8, 1), date(2026, 8, 24))
    provider.fetch_shareholder_counts(None, date(2026, 8, 1), date(2026, 8, 24))

    assert sleeps == [pytest.approx(1 / 3)]
