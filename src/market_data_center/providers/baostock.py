"""BaoStock adapter that contains every source-specific field name."""

from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from types import TracebackType
from typing import Protocol, Self, cast

from market_data_center.domain.records import (
    DailyBarRecord,
    Exchange,
    Market,
    SecurityRecord,
    SecurityStatus,
    SecurityType,
    TradeStatus,
    TradingDayRecord,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError, RawRow

DAILY_BAR_FIELDS = (
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "tradestatus",
    "isST",
)


class BaoStockResult(Protocol):
    error_code: str
    error_msg: str
    fields: Sequence[str]

    def next(self) -> bool: ...

    def get_row_data(self) -> Sequence[str]: ...


class BaoStockResponse(Protocol):
    error_code: str
    error_msg: str


class BaoStockClient(Protocol):
    def login(self) -> BaoStockResponse: ...

    def logout(self) -> BaoStockResponse: ...

    def query_stock_basic(self, code: str = "", code_name: str = "") -> BaoStockResult: ...

    def query_trade_dates(self, start_date: str, end_date: str) -> BaoStockResult: ...

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> BaoStockResult: ...


class BaoStockProvider:
    source_code = "baostock"

    def __init__(self, client: BaoStockClient) -> None:
        self._client = client

    @classmethod
    def default(cls) -> Self:
        import baostock  # type: ignore[import-untyped]

        return cls(cast(BaoStockClient, baostock))

    def __enter__(self) -> Self:
        response = self._client.login()
        _ensure_success(response, "login")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        response = self._client.logout()
        if exc_type is None:
            _ensure_success(response, "logout")

    def fetch_securities(self) -> ProviderBatch[SecurityRecord]:
        raw_rows = _read_result(self._client.query_stock_basic(), "query_stock_basic")
        records = [_map_security(row) for row in raw_rows]
        return ProviderBatch(
            records=records,
            raw_rows=raw_rows,
            request_params={},
            schema_version="baostock.security.v1",
        )

    def fetch_trading_calendar(
        self, start_date: date, end_date: date
    ) -> ProviderBatch[TradingDayRecord]:
        _ensure_date_range(start_date, end_date)
        result = self._client.query_trade_dates(
            start_date=start_date.isoformat(), end_date=end_date.isoformat()
        )
        raw_rows = _read_result(result, "query_trade_dates")
        by_date = {date.fromisoformat(row["calendar_date"]): row for row in raw_rows}
        records: list[TradingDayRecord] = []
        current = start_date
        while current <= end_date:
            row = by_date.get(current)
            records.append(
                TradingDayRecord(
                    market=Market.CN_A_SHARE,
                    trade_date=current,
                    is_trading_day=row is not None and row["is_trading_day"] == "1",
                    source_code=self.source_code,
                )
            )
            current += timedelta(days=1)
        return ProviderBatch(
            records=records,
            raw_rows=raw_rows,
            request_params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            schema_version="baostock.trading_calendar.v1",
        )

    def fetch_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> ProviderBatch[DailyBarRecord]:
        _ensure_date_range(start_date, end_date)
        result = self._client.query_history_k_data_plus(
            code=source_symbol,
            fields=",".join(DAILY_BAR_FIELDS),
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            frequency="d",
            adjustflag="3",
        )
        raw_rows = _read_result(result, "query_history_k_data_plus")
        return ProviderBatch(
            records=[_map_daily_bar(row) for row in raw_rows],
            raw_rows=raw_rows,
            request_params={
                "source_symbol": source_symbol,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "frequency": "d",
                "adjustflag": "3",
            },
            schema_version="baostock.daily_bar.v1",
        )


def _read_result(result: BaoStockResult, operation: str) -> list[RawRow]:
    _ensure_success(result, operation)
    fields = tuple(result.fields)
    rows: list[RawRow] = []
    while result.next():
        values = tuple(result.get_row_data())
        if len(values) != len(fields):
            raise ProviderError(f"{operation} returned a row with an unexpected field count")
        rows.append(dict(zip(fields, values, strict=True)))
    _ensure_success(result, operation)
    return rows


def _ensure_success(response: BaoStockResponse, operation: str) -> None:
    if response.error_code != "0":
        raise ProviderError(f"BaoStock {operation} failed: {response.error_msg}")


def _ensure_date_range(start_date: date, end_date: date) -> None:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")


def _map_security(row: Mapping[str, str]) -> SecurityRecord:
    exchange, code, symbol = _normalize_symbol(row["code"])
    return SecurityRecord(
        symbol=symbol,
        code=code,
        exchange=exchange,
        name=row["code_name"].strip(),
        security_type=SecurityType.STOCK if row.get("type") == "1" else SecurityType.UNKNOWN,
        status=_security_status(row.get("status", "")),
        ipo_date=_optional_date(row.get("ipoDate")),
        delisting_date=_optional_date(row.get("outDate")),
        source_code="baostock",
    )


def _map_daily_bar(row: Mapping[str, str]) -> DailyBarRecord:
    _, _, symbol = _normalize_symbol(row["code"])
    return DailyBarRecord(
        symbol=symbol,
        trade_date=date.fromisoformat(row["date"]),
        market=Market.CN_A_SHARE,
        open=_optional_decimal(row.get("open")),
        high=_optional_decimal(row.get("high")),
        low=_optional_decimal(row.get("low")),
        close=_optional_decimal(row.get("close")),
        previous_close=_optional_decimal(row.get("preclose")),
        volume=_optional_int(row.get("volume")),
        amount=_optional_decimal(row.get("amount")),
        trade_status=_trade_status(row.get("tradestatus", "")),
        is_st=_optional_bool(row.get("isST")),
        source_code="baostock",
    )


def _normalize_symbol(source_symbol: str) -> tuple[Exchange, str, str]:
    try:
        source_exchange, code = source_symbol.lower().split(".", maxsplit=1)
        exchange = {"sh": Exchange.SSE, "sz": Exchange.SZSE, "bj": Exchange.BSE}[source_exchange]
    except (KeyError, ValueError) as error:
        raise ProviderError(f"unsupported BaoStock symbol: {source_symbol}") from error
    return exchange, code, f"{exchange.value}:{code}"


def _security_status(value: str) -> SecurityStatus:
    return {"1": SecurityStatus.LISTED, "0": SecurityStatus.DELISTED}.get(
        value, SecurityStatus.UNKNOWN
    )


def _trade_status(value: str) -> TradeStatus:
    return {"1": TradeStatus.TRADING, "0": TradeStatus.SUSPENDED}.get(value, TradeStatus.UNKNOWN)


def _optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _optional_decimal(value: str | None) -> Decimal | None:
    if value is None or not value.strip():
        return None
    try:
        return Decimal(value)
    except InvalidOperation as error:
        raise ProviderError(f"invalid decimal value: {value}") from error


def _optional_int(value: str | None) -> int | None:
    return int(value) if value is not None and value.strip() else None


def _optional_bool(value: str | None) -> bool | None:
    if value == "1":
        return True
    if value == "0":
        return False
    return None
