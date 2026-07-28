from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal

from market_data_center.domain import Exchange, SecurityStatus, TradeStatus
from market_data_center.providers import AKShareProvider
from market_data_center.providers.akshare import TabularResult


class FakeFrame:
    def __init__(self, rows: Sequence[Mapping[str, object]]) -> None:
        self._rows = rows
        self.columns = tuple(rows[0]) if rows else ()

    def to_dict(self, orient: str) -> object:
        assert orient == "records"
        return list(self._rows)


class FakeClient:
    def __init__(self) -> None:
        self.daily_arguments: dict[str, str] = {}

    def stock_info_a_code_name(self) -> TabularResult:
        return FakeFrame(({"code": "600000", "name": "浦发银行"},))

    def tool_trade_date_hist_sina(self) -> TabularResult:
        return FakeFrame(
            (
                {"trade_date": date(1990, 12, 19)},
                {"trade_date": date(2026, 7, 24)},
                {"trade_date": date(2026, 7, 27)},
            )
        )

    def stock_zh_a_hist(
        self, *, symbol: str, period: str, start_date: str, end_date: str, adjust: str
    ) -> TabularResult:
        self.daily_arguments = {
            "symbol": symbol,
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "adjust": adjust,
        }
        return FakeFrame(
            (
                {
                    "日期": date(2026, 7, 24),
                    "股票代码": "600000",
                    "开盘": "10.00",
                    "收盘": "10.50",
                    "最高": "11.00",
                    "最低": "9.50",
                    "成交量": "100",
                    "成交额": "1050.00",
                    "涨跌额": "0.60",
                },
            )
        )


def test_security_mapping_keeps_unknown_lifecycle_fields_explicit() -> None:
    record = AKShareProvider(FakeClient()).fetch_securities().records[0]

    assert record.symbol == "SSE:600000"
    assert record.exchange is Exchange.SSE
    assert record.status is SecurityStatus.UNKNOWN
    assert record.ipo_date is None
    assert record.source_code == "akshare"


def test_trading_calendar_expands_to_natural_days() -> None:
    batch = AKShareProvider(FakeClient()).fetch_trading_calendar(
        date(2026, 7, 24), date(2026, 7, 26)
    )

    assert [record.is_trading_day for record in batch.records] == [True, False, False]


def test_daily_bar_requests_unadjusted_data_and_maps_decimal_values() -> None:
    client = FakeClient()
    record = (
        AKShareProvider(client)
        .fetch_daily_bars("600000", date(2026, 7, 24), date(2026, 7, 24))
        .records[0]
    )

    assert client.daily_arguments["adjust"] == ""
    assert client.daily_arguments["period"] == "daily"
    assert record.close == Decimal("10.50")
    assert record.previous_close == Decimal("9.90")
    assert record.trade_status is TradeStatus.TRADING
    assert record.is_st is None


def test_standard_symbol_maps_to_akshare_source_symbol() -> None:
    assert AKShareProvider(FakeClient()).source_symbol("SSE:600000") == "600000"
