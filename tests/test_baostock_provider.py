from collections.abc import Sequence
from datetime import date

import pytest

from market_data_center.domain import Exchange, SecurityStatus, SecurityType, TradeStatus
from market_data_center.providers import BaoStockProvider


class FakeResponse:
    error_code = "0"
    error_msg = "success"


class FakeResult(FakeResponse):
    def __init__(self, fields: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
        self.fields = fields
        self._rows = iter(rows)
        self._current: Sequence[str] = ()

    def next(self) -> bool:
        try:
            self._current = next(self._rows)
        except StopIteration:
            return False
        return True

    def get_row_data(self) -> Sequence[str]:
        return self._current


class FakeClient:
    def __init__(self, security_type: str = "1") -> None:
        self.daily_bar_arguments: dict[str, str] = {}
        self.security_type = security_type

    def login(self) -> FakeResponse:
        return FakeResponse()

    def logout(self) -> FakeResponse:
        return FakeResponse()

    def query_stock_basic(self, code: str = "", code_name: str = "") -> FakeResult:
        return FakeResult(
            ("code", "code_name", "ipoDate", "outDate", "type", "status"),
            (("sh.600000", "浦发银行", "1999-11-10", "", self.security_type, "1"),),
        )

    def query_trade_dates(self, start_date: str, end_date: str) -> FakeResult:
        return FakeResult(
            ("calendar_date", "is_trading_day"),
            (("2026-07-24", "1"), ("2026-07-25", "0"), ("2026-07-26", "0")),
        )

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> FakeResult:
        self.daily_bar_arguments = {
            "code": code,
            "fields": fields,
            "frequency": frequency,
            "adjustflag": adjustflag,
        }
        return FakeResult(
            tuple(fields.split(",")),
            (
                (
                    "2026-07-24",
                    "sh.600000",
                    "10.00",
                    "11.00",
                    "9.50",
                    "10.50",
                    "9.90",
                    "100",
                    "1050.00",
                    "1",
                    "0",
                ),
            ),
        )


def test_security_mapping_consumes_baostock_fields() -> None:
    provider = BaoStockProvider(FakeClient())

    record = provider.fetch_securities().records[0]

    assert record.symbol == "SSE:600000"
    assert record.exchange is Exchange.SSE
    assert record.status is SecurityStatus.LISTED
    assert not hasattr(record, "ipoDate")


@pytest.mark.parametrize(
    ("source_type", "expected"),
    [
        ("1", SecurityType.STOCK),
        ("2", SecurityType.INDEX),
        ("3", SecurityType.OTHER),
        ("4", SecurityType.CONVERTIBLE_BOND),
        ("5", SecurityType.ETF),
        ("", SecurityType.UNKNOWN),
    ],
)
def test_security_type_mapping(source_type: str, expected: SecurityType) -> None:
    record = BaoStockProvider(FakeClient(source_type)).fetch_securities().records[0]

    assert record.security_type is expected


def test_trading_calendar_contains_every_natural_day() -> None:
    provider = BaoStockProvider(FakeClient())

    batch = provider.fetch_trading_calendar(date(2026, 7, 24), date(2026, 7, 26))

    assert len(batch.records) == 3
    assert [record.is_trading_day for record in batch.records] == [True, False, False]


def test_daily_bar_explicitly_requests_unadjusted_prices() -> None:
    client = FakeClient()
    provider = BaoStockProvider(client)

    record = provider.fetch_daily_bars("sh.600000", date(2026, 7, 24), date(2026, 7, 24)).records[0]

    assert client.daily_bar_arguments["adjustflag"] == "3"
    assert client.daily_bar_arguments["frequency"] == "d"
    assert record.trade_status is TradeStatus.TRADING
    assert not hasattr(record, "adjustflag")


def test_standard_symbol_maps_to_baostock_source_symbol() -> None:
    assert BaoStockProvider(FakeClient()).source_symbol("SSE:600000") == "sh.600000"
