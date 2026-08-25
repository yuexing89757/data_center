"""Tushare Pro adapter; source fields and units stop at this boundary."""

import json
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from functools import partial
from time import sleep
from types import TracebackType
from typing import Protocol, Self
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pydantic import ValidationError

from market_data_center.domain.classification import ClassificationRecord
from market_data_center.domain.convertible_bond import (
    ConvertibleBondBasicRecord,
    ConvertibleBondDailyBarRecord,
    ConvertibleBondRecord,
)
from market_data_center.domain.deducted_profit import (
    DeductedProfitRecord,
    deducted_profit_revision_key,
)
from market_data_center.domain.ingestion import DatasetCode
from market_data_center.domain.records import (
    CapitalRecord,
    DailyBarRecord,
    Exchange,
    Market,
    SecurityRecord,
    SecurityStatus,
    SecurityType,
    TradeStatus,
    TradingDayRecord,
)
from market_data_center.domain.shareholder_count import (
    ShareholderCountRecord,
    shareholder_count_revision_key,
)
from market_data_center.domain.stock_daily_indicator import (
    PriceLimitStatus,
    StockDailyIndicatorSnapshotRecord,
)
from market_data_center.providers.contracts import (
    ProviderBatch,
    ProviderError,
    ProviderRecord,
    ProviderRequestUnavailable,
    RawRow,
)
from market_data_center.settings import TushareSettings

SECURITY_FIELDS = (
    "ts_code",
    "symbol",
    "name",
    "exchange",
    "list_status",
    "list_date",
    "delist_date",
)
CALENDAR_FIELDS = ("exchange", "cal_date", "is_open")
DAILY_BAR_FIELDS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "vol",
    "amount",
)
STOCK_DAILY_INDICATOR_FIELDS = (
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
    "limit_status",
)
STOCK_DAILY_INDICATOR_RESPONSE_LIMIT = 6_000
DISCLOSURE_FIELDS = ("ts_code", "ann_date", "end_date", "actual_date", "modify_date")
DEDUCTED_PROFIT_FIELDS = (
    "ts_code",
    "ann_date",
    "end_date",
    "profit_dedt",
    "q_dtprofit",
    "update_flag",
)
SHAREHOLDER_COUNT_FIELDS = ("ts_code", "ann_date", "end_date", "holder_num")
SHAREHOLDER_COUNT_RESPONSE_LIMIT = 3_000
CB_BASIC_FIELDS = (
    "ts_code",
    "bond_id",
    "bond_short_name",
    "bond_full_name",
    "list_date",
    "delist_date",
    "maturity_date",
    "par",
    "issue_size",
    "value_date",
    "maturity",
    "convert_price_initial",
    "convert_price",
    "stock_ts_code",
    "redeem_clause",
    "sell_back_clause",
)
CB_DAILY_FIELDS = (
    "ts_code",
    "trade_date",
    "pre_close",
    "open",
    "high",
    "low",
    "close",
    "pct_chg",
    "vol",
    "amount",
    "convert_value",
    "convert_pct",
    "convert_price",
    "remain_size",
)


class TushareClient(Protocol):
    def query(
        self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
    ) -> Sequence[Mapping[str, object]]: ...


class TushareHttpClient:
    """Small client for Tushare's documented JSON API."""

    def __init__(
        self,
        token: str,
        *,
        endpoint: str = "https://api.tushare.pro",
        timeout_seconds: float = 30,
    ) -> None:
        if not token.strip():
            raise ValueError("Tushare token must not be blank")
        self._token = token.strip()
        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds

    def query(
        self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
    ) -> Sequence[Mapping[str, object]]:
        payload = json.dumps(
            {
                "api_name": api_name,
                "token": self._token,
                "params": dict(params),
                "fields": ",".join(fields),
            }
        ).encode("utf-8")
        request = Request(
            self._endpoint,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "market-data-center/0.1"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                document = json.loads(response.read(), parse_float=Decimal)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
            raise ProviderError(f"Tushare {api_name} request failed") from error
        if not isinstance(document, Mapping):
            raise ProviderError(f"Tushare {api_name} returned an invalid response")
        code = document.get("code")
        if code != 0:
            message = str(document.get("msg") or "unknown provider error").replace(
                self._token, "<redacted>"
            )
            raise ProviderError(f"Tushare {api_name} rejected the request: {message}")
        data = document.get("data")
        if not isinstance(data, Mapping):
            raise ProviderError(f"Tushare {api_name} response is missing data")
        response_fields = data.get("fields")
        items = data.get("items")
        if not isinstance(response_fields, list) or not all(
            isinstance(field, str) for field in response_fields
        ):
            raise ProviderError(f"Tushare {api_name} response has invalid fields")
        if not isinstance(items, list):
            raise ProviderError(f"Tushare {api_name} response has invalid items")
        rows: list[Mapping[str, object]] = []
        for item in items:
            if not isinstance(item, list) or len(item) != len(response_fields):
                raise ProviderError(f"Tushare {api_name} returned a malformed row")
            rows.append(dict(zip(response_fields, item, strict=True)))
        return rows


class TushareProvider(AbstractContextManager["TushareProvider"]):
    source_code = "tushare"

    def __init__(
        self,
        client: TushareClient,
        *,
        shareholder_count_request_interval_seconds: float = 0,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        if shareholder_count_request_interval_seconds < 0:
            raise ValueError("shareholder-count request interval must not be negative")
        self._client = client
        self._shareholder_count_request_interval_seconds = (
            shareholder_count_request_interval_seconds
        )
        self._shareholder_count_request_started = False
        self._sleeper = sleeper

    @classmethod
    def default(cls) -> Self:
        try:
            settings = TushareSettings()  # type: ignore[call-arg]
            token = settings.tushare_token.get_secret_value().strip()
        except ValidationError as error:
            raise ProviderError("TUSHARE_TOKEN is required for the Tushare provider") from error
        if not token:
            raise ProviderError("TUSHARE_TOKEN is required for the Tushare provider")
        return cls(
            TushareHttpClient(token),
            shareholder_count_request_interval_seconds=(
                60 / settings.tushare_shareholder_count_max_calls_per_minute
            ),
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def source_symbol(self, symbol: str) -> str:
        return _source_symbol(symbol)

    def fetch_securities(self) -> ProviderBatch[SecurityRecord]:
        rows: list[RawRow] = []
        for status in ("L", "D", "P"):
            result = _provider_call(
                "stock_basic", partial(_query_stock_basic, self._client, status)
            )
            rows.extend(_rows(result, SECURITY_FIELDS, "stock_basic"))
        unique_rows = {row["ts_code"]: row for row in rows}
        ordered = [unique_rows[key] for key in sorted(unique_rows)]
        return ProviderBatch(
            raw_rows=ordered,
            request_params={"list_statuses": ["L", "D", "P"]},
            schema_version="tushare.security.v1",
            record_factory=lambda: [_map_security(row) for row in ordered],
        )

    def fetch_trading_calendar(
        self, start_date: date, end_date: date
    ) -> ProviderBatch[TradingDayRecord]:
        _ensure_date_range(start_date, end_date)
        result: Sequence[Mapping[str, object]] = _provider_call(
            "trade_cal",
            lambda: self._client.query(
                "trade_cal",
                params={
                    "exchange": "SSE",
                    "start_date": start_date.strftime("%Y%m%d"),
                    "end_date": end_date.strftime("%Y%m%d"),
                },
                fields=CALENDAR_FIELDS,
            ),
        )
        rows = _rows(result, CALENDAR_FIELDS, "trade_cal")
        return ProviderBatch(
            raw_rows=rows,
            request_params={
                "exchange": "SSE",
                "start_date": start_date.strftime("%Y%m%d"),
                "end_date": end_date.strftime("%Y%m%d"),
            },
            schema_version="tushare.trading_calendar.v1",
            record_factory=lambda: _calendar_records(rows, start_date, end_date),
        )

    def fetch_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> ProviderBatch[DailyBarRecord]:
        _ensure_date_range(start_date, end_date)
        ts_code = _source_symbol(source_symbol)
        result: Sequence[Mapping[str, object]] = _provider_call(
            "daily",
            lambda: self._client.query(
                "daily",
                params={
                    "ts_code": ts_code,
                    "start_date": start_date.strftime("%Y%m%d"),
                    "end_date": end_date.strftime("%Y%m%d"),
                },
                fields=DAILY_BAR_FIELDS,
            ),
        )
        rows = _rows(result, DAILY_BAR_FIELDS, "daily")
        rows.sort(key=lambda row: row["trade_date"])
        return ProviderBatch(
            raw_rows=rows,
            request_params={
                "source_symbol": ts_code,
                "start_date": start_date.strftime("%Y%m%d"),
                "end_date": end_date.strftime("%Y%m%d"),
                "adjust": "none",
            },
            schema_version="tushare.daily_bar.v1",
            record_factory=lambda: [_map_daily_bar(row) for row in rows],
        )

    def fetch_capital(self, source_symbol: str) -> ProviderBatch[CapitalRecord]:
        raise ProviderRequestUnavailable("Tushare Capital is not accepted in ADR-0013")

    def fetch_stock_daily_indicators(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> ProviderBatch[StockDailyIndicatorSnapshotRecord]:
        _ensure_date_range(start_date, end_date)
        ts_code = _source_symbol(source_symbol)
        result: Sequence[Mapping[str, object]] = _provider_call(
            "daily_basic",
            lambda: self._client.query(
                "daily_basic",
                params={
                    "ts_code": ts_code,
                    "start_date": start_date.strftime("%Y%m%d"),
                    "end_date": end_date.strftime("%Y%m%d"),
                },
                fields=STOCK_DAILY_INDICATOR_FIELDS,
            ),
        )
        rows = _rows(result, STOCK_DAILY_INDICATOR_FIELDS, "daily_basic")
        rows.sort(key=lambda row: row["trade_date"])
        return ProviderBatch(
            raw_rows=rows,
            request_params={
                "source_symbol": ts_code,
                "start_date": start_date.strftime("%Y%m%d"),
                "end_date": end_date.strftime("%Y%m%d"),
            },
            schema_version="tushare.stock_daily_indicator.v1",
            record_factory=lambda: [_map_stock_daily_indicator(row) for row in rows],
        )

    def fetch_stock_daily_indicator_snapshot(
        self, trade_date: date
    ) -> ProviderBatch[StockDailyIndicatorSnapshotRecord]:
        result: Sequence[Mapping[str, object]] = _provider_call(
            "daily_basic",
            lambda: self._client.query(
                "daily_basic",
                params={"trade_date": trade_date.strftime("%Y%m%d")},
                fields=STOCK_DAILY_INDICATOR_FIELDS,
            ),
        )
        rows = _rows(result, STOCK_DAILY_INDICATOR_FIELDS, "daily_basic")
        rows.sort(key=lambda row: row["ts_code"])
        return ProviderBatch(
            raw_rows=rows,
            request_params={"trade_date": trade_date.strftime("%Y%m%d")},
            schema_version="tushare.stock_daily_indicator.v1",
            record_factory=lambda: _complete_stock_daily_indicator_snapshot(rows),
        )

    def fetch_deducted_profit_updates(
        self, as_of_date: date
    ) -> ProviderBatch[DeductedProfitRecord]:
        date_key = as_of_date.strftime("%Y%m%d")
        disclosure_rows: list[RawRow] = []
        for period in _recent_report_periods(as_of_date):
            result = _provider_call(
                "disclosure_date",
                partial(
                    self._client.query,
                    "disclosure_date",
                    params={"end_date": period.strftime("%Y%m%d")},
                    fields=DISCLOSURE_FIELDS,
                ),
            )
            disclosure_rows.extend(_rows(result, DISCLOSURE_FIELDS, "disclosure_date"))
        affected = {
            (row["ts_code"], row["end_date"]): row
            for row in disclosure_rows
            if row.get("actual_date") == date_key or date_key in row.get("modify_date", "")
        }
        raw_rows = [{"row_type": "disclosure", **row} for row in disclosure_rows]
        joined: list[tuple[RawRow, RawRow]] = []
        for (ts_code, period_key), disclosure in sorted(affected.items()):
            result = _provider_call(
                "fina_indicator",
                partial(
                    self._client.query,
                    "fina_indicator",
                    params={"ts_code": ts_code, "period": period_key},
                    fields=DEDUCTED_PROFIT_FIELDS,
                ),
            )
            indicator_rows = _rows(result, DEDUCTED_PROFIT_FIELDS, "fina_indicator")
            raw_rows.extend({"row_type": "fina_indicator", **row} for row in indicator_rows)
            joined.extend((row, disclosure) for row in indicator_rows)
        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={
                "as_of_date": date_key,
                "report_period_count": len(_recent_report_periods(as_of_date)),
                "affected_count": len(affected),
            },
            schema_version="tushare.deducted_profit.v1",
            record_factory=lambda: [
                _map_deducted_profit(row, disclosure) for row, disclosure in joined
            ],
        )

    def fetch_shareholder_counts(
        self, source_symbol: str | None, start_date: date, end_date: date
    ) -> ProviderBatch[ShareholderCountRecord]:
        _ensure_date_range(start_date, end_date)
        ts_code = _source_symbol(source_symbol) if source_symbol is not None else None
        params = {
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
        }
        if ts_code is not None:
            params = {"ts_code": ts_code, **params}
        if self._shareholder_count_request_started:
            self._sleeper(self._shareholder_count_request_interval_seconds)
        self._shareholder_count_request_started = True
        result: Sequence[Mapping[str, object]] = _provider_call(
            "stk_holdernumber",
            lambda: self._client.query(
                "stk_holdernumber",
                params=params,
                fields=SHAREHOLDER_COUNT_FIELDS,
            ),
        )
        rows = _rows(result, SHAREHOLDER_COUNT_FIELDS, "stk_holdernumber")
        rows.sort(
            key=lambda row: (
                row["ts_code"],
                row["end_date"],
                row["ann_date"],
                row["holder_num"],
            )
        )
        return ProviderBatch(
            raw_rows=rows,
            request_params={
                "source_symbol": ts_code,
                "start_date": params["start_date"],
                "end_date": params["end_date"],
            },
            schema_version="tushare.shareholder_count.v1",
            record_factory=lambda: _shareholder_count_records(rows),
        )

    def fetch_convertible_bonds(self) -> ProviderBatch[ConvertibleBondRecord]:
        result = _provider_call(
            "cb_basic",
            partial(self._client.query, "cb_basic", params={}, fields=CB_BASIC_FIELDS),
        )
        rows = _rows(result, CB_BASIC_FIELDS, "cb_basic")
        return ProviderBatch(
            raw_rows=rows,
            request_params={},
            schema_version="tushare.convertible_bond.v1",
            record_factory=lambda: [_map_cb_basic(row) for row in rows],
        )

    def fetch_convertible_bond_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> ProviderBatch[ConvertibleBondRecord]:
        ts_code = self.source_symbol(source_symbol)
        result = _provider_call(
            "cb_daily",
            partial(
                self._client.query,
                "cb_daily",
                params={
                    "ts_code": ts_code,
                    "start_date": start_date.strftime("%Y%m%d"),
                    "end_date": end_date.strftime("%Y%m%d"),
                },
                fields=CB_DAILY_FIELDS,
            ),
        )
        rows = _rows(result, CB_DAILY_FIELDS, "cb_daily")
        rows.sort(key=lambda row: row["trade_date"])
        return ProviderBatch(
            raw_rows=rows,
            request_params={"ts_code": ts_code},
            schema_version="tushare.convertible_bond_daily_bar.v1",
            record_factory=lambda: [_map_cb_daily_bar(row) for row in rows],
        )

    def fetch_classification_catalog(
        self, classification_type: str, snapshot_date: date
    ) -> ProviderBatch[ClassificationRecord]:
        raise ProviderRequestUnavailable("Tushare classifications are not accepted in ADR-0013")

    def fetch_classification_members(
        self, classification_type: str, classification_code: str, snapshot_date: date
    ) -> ProviderBatch[ClassificationRecord]:
        raise ProviderRequestUnavailable("Tushare classifications are not accepted in ADR-0013")


def normalize_tushare_raw(
    dataset_code: DatasetCode,
    schema_version: str,
    raw_rows: Sequence[Mapping[str, str]],
    request_params: Mapping[str, object],
) -> tuple[ProviderRecord, ...]:
    expected = {
        DatasetCode.SECURITY: "tushare.security.v1",
        DatasetCode.TRADING_CALENDAR: "tushare.trading_calendar.v1",
        DatasetCode.DAILY_BAR: "tushare.daily_bar.v1",
        DatasetCode.STOCK_DAILY_INDICATOR: "tushare.stock_daily_indicator.v1",
        DatasetCode.DEDUCTED_PROFIT: "tushare.deducted_profit.v1",
        DatasetCode.SHAREHOLDER_COUNT: "tushare.shareholder_count.v1",
        DatasetCode.CONVERTIBLE_BOND: "tushare.convertible_bond.v1",
        DatasetCode.CONVERTIBLE_BOND_DAILY_BAR: "tushare.convertible_bond_daily_bar.v1",
    }.get(dataset_code)
    if expected is None or schema_version != expected:
        raise ProviderError(f"unsupported Tushare Raw schema: {schema_version}")
    if dataset_code is DatasetCode.SECURITY:
        return tuple(_map_security(row) for row in raw_rows)
    if dataset_code is DatasetCode.TRADING_CALENDAR:
        start_date = _request_date(request_params, "start_date")
        end_date = _request_date(request_params, "end_date")
        return tuple(_calendar_records(raw_rows, start_date, end_date))
    if dataset_code is DatasetCode.STOCK_DAILY_INDICATOR:
        return tuple(
            _map_stock_daily_indicator(row)
            for row in sorted(raw_rows, key=lambda row: row["trade_date"])
        )
    if dataset_code is DatasetCode.DEDUCTED_PROFIT:
        disclosures = {
            (row["ts_code"], row["end_date"]): row
            for row in raw_rows
            if row.get("row_type") == "disclosure"
        }
        return tuple(
            _map_deducted_profit(row, disclosures[(row["ts_code"], row["end_date"])])
            for row in raw_rows
            if row.get("row_type") == "fina_indicator"
            and (row["ts_code"], row["end_date"]) in disclosures
        )
    if dataset_code is DatasetCode.SHAREHOLDER_COUNT:
        return tuple(
            _shareholder_count_records(
                sorted(
                    raw_rows,
                    key=lambda row: (
                        row["ts_code"],
                        row["end_date"],
                        row["ann_date"],
                        row.get("holder_num", ""),
                    ),
                )
            )
        )
    if dataset_code is DatasetCode.CONVERTIBLE_BOND:
        return tuple(_map_cb_basic(row) for row in raw_rows)
    if dataset_code is DatasetCode.CONVERTIBLE_BOND_DAILY_BAR:
        return tuple(
            _map_cb_daily_bar(row) for row in sorted(raw_rows, key=lambda row: row["trade_date"])
        )
    return tuple(_map_daily_bar(row) for row in sorted(raw_rows, key=lambda row: row["trade_date"]))


def _rows(
    objects: Sequence[Mapping[str, object]], required: Sequence[str], operation: str
) -> list[RawRow]:
    rows: list[RawRow] = []
    for item in objects:
        missing = set(required).difference(item)
        if missing:
            raise ProviderError(f"Tushare {operation} missing fields: {', '.join(sorted(missing))}")
        rows.append({str(key): _raw_value(value) for key, value in item.items()})
    return rows


def _query_stock_basic(client: TushareClient, status: str) -> Sequence[Mapping[str, object]]:
    return client.query("stock_basic", params={"list_status": status}, fields=SECURITY_FIELDS)


def _provider_call[ResultT](operation: str, call: Callable[[], ResultT]) -> ResultT:
    try:
        return call()
    except ProviderError:
        raise
    except Exception as error:
        raise ProviderError(f"Tushare {operation} request failed") from error


def _raw_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return str(value)


def _map_security(row: Mapping[str, str]) -> SecurityRecord:
    exchange, code, symbol = _normalize_symbol(row["ts_code"])
    status = {
        "L": SecurityStatus.LISTED,
        "D": SecurityStatus.DELISTED,
        "P": SecurityStatus.UNKNOWN,
    }.get(row["list_status"].strip().upper(), SecurityStatus.UNKNOWN)
    return SecurityRecord(
        symbol=symbol,
        code=code,
        exchange=exchange,
        name=row["name"].strip(),
        security_type=SecurityType.STOCK,
        status=status,
        ipo_date=_optional_date(row.get("list_date")),
        delisting_date=_optional_date(row.get("delist_date")),
        source_code="tushare",
    )


def _calendar_records(
    rows: Sequence[Mapping[str, str]], start_date: date, end_date: date
) -> list[TradingDayRecord]:
    by_date = {_parse_date(row["cal_date"]): row for row in rows}
    expected_dates: list[date] = []
    current = start_date
    while current <= end_date:
        expected_dates.append(current)
        current += timedelta(days=1)
    missing = [day for day in expected_dates if day not in by_date]
    if missing:
        raise ProviderError(
            f"Tushare trade_cal does not cover requested range; first missing date: {missing[0]}"
        )
    return [
        TradingDayRecord(
            market=Market.CN_A_SHARE,
            trade_date=day,
            is_trading_day=by_date[day]["is_open"] == "1",
            source_code="tushare",
        )
        for day in expected_dates
    ]


def _map_daily_bar(row: Mapping[str, str]) -> DailyBarRecord:
    _, _, symbol = _normalize_symbol(row["ts_code"])
    volume_lots = _decimal(row.get("vol"))
    volume_shares = volume_lots * 100 if volume_lots is not None else None
    if volume_shares is not None and volume_shares != volume_shares.to_integral_value():
        raise ProviderError(
            f"Tushare volume cannot be normalized to whole shares: {row.get('vol')}"
        )
    amount_thousand = _decimal(row.get("amount"))
    return DailyBarRecord(
        symbol=symbol,
        trade_date=_parse_date(row["trade_date"]),
        market=Market.CN_A_SHARE,
        open=_decimal(row.get("open")),
        high=_decimal(row.get("high")),
        low=_decimal(row.get("low")),
        close=_decimal(row.get("close")),
        previous_close=_decimal(row.get("pre_close")),
        volume=int(volume_shares) if volume_shares is not None else None,
        amount=amount_thousand * 1000 if amount_thousand is not None else None,
        trade_status=TradeStatus.TRADING,
        is_st=None,
        source_code="tushare",
    )


def _map_cb_basic(row: Mapping[str, str]) -> ConvertibleBondBasicRecord:
    _, _, symbol = _normalize_symbol(row["ts_code"])
    underlying_symbol: str
    stock_code = row.get("stock_ts_code", "").strip()
    if stock_code:
        _, _, underlying_symbol = _normalize_symbol(stock_code)
    else:
        raise ProviderError(f"cb_basic row for {symbol} is missing stock_ts_code")
    list_date = _optional_date(row.get("list_date"))
    delist_date = _optional_date(row.get("delist_date"))
    if delist_date is not None:
        lifecycle = "delisted"
    elif list_date is not None:
        lifecycle = "listed"
    else:
        lifecycle = "pending_list"
    return ConvertibleBondBasicRecord(
        symbol=symbol,
        bond_code=symbol.split(":")[1],
        bond_short_name=row["bond_short_name"].strip(),
        bond_full_name=row["bond_full_name"].strip(),
        underlying_symbol=underlying_symbol,
        exchange=symbol.split(":")[0],
        par_value=_decimal(row.get("par")) or Decimal("100"),
        issue_size=_decimal(row.get("issue_size")),
        issue_date=None,
        value_date=_optional_date(row.get("value_date")),
        maturity_years=_optional_int(row.get("maturity")),
        maturity_date=_optional_date(row.get("maturity_date")),
        convert_price_initial=_decimal(row.get("convert_price_initial")),
        convert_price=_decimal(row.get("convert_price")),
        convert_start_date=None,
        convert_end_date=None,
        coupon_rate=None,
        redeem_clause=row.get("redeem_clause"),
        sell_back_clause=row.get("sell_back_clause"),
        lifecycle_status=lifecycle,
        source_code="tushare",
    )


def _map_cb_daily_bar(row: Mapping[str, str]) -> ConvertibleBondDailyBarRecord:
    _, _, symbol = _normalize_symbol(row["ts_code"])
    # Tushare cb_daily.vol is in lots (1 lot = 10 bonds); normalize to 张.
    volume_lots = _decimal(row.get("vol"))
    volume = int(volume_lots * 10) if volume_lots is not None else None
    # Tushare amount is in thousands of CNY.
    amount_thousand = _decimal(row.get("amount"))
    amount = amount_thousand * 1000 if amount_thousand is not None else None
    return ConvertibleBondDailyBarRecord(
        symbol=symbol,
        trade_date=_parse_date(row["trade_date"]),
        market=Market.CN_A_SHARE,
        open=_decimal(row.get("open")),
        high=_decimal(row.get("high")),
        low=_decimal(row.get("low")),
        close=_decimal(row.get("close")),
        previous_close=_decimal(row.get("pre_close")),
        volume=volume,
        amount=amount,
        pct_chg=_decimal(row.get("pct_chg")),
        convert_value=_decimal(row.get("convert_value")),
        convert_premium_pct=_decimal(row.get("convert_pct")),
        convert_price=_decimal(row.get("convert_price")),
        remain_size=_decimal(row.get("remain_size")),
        trade_status="trading",
        source_code="tushare",
    )


def _map_stock_daily_indicator(
    row: Mapping[str, str],
) -> StockDailyIndicatorSnapshotRecord:
    _, _, symbol = _normalize_symbol(row["ts_code"])
    return StockDailyIndicatorSnapshotRecord(
        symbol=symbol,
        trade_date=_parse_date(row["trade_date"]),
        market=Market.CN_A_SHARE,
        close=_decimal(row.get("close")),
        turnover_rate_pct=_decimal(row.get("turnover_rate")),
        free_float_turnover_rate_pct=_decimal(row.get("turnover_rate_f")),
        volume_ratio=_decimal(row.get("volume_ratio")),
        pe=_decimal(row.get("pe")),
        pe_ttm=_decimal(row.get("pe_ttm")),
        pb=_decimal(row.get("pb")),
        ps=_decimal(row.get("ps")),
        ps_ttm=_decimal(row.get("ps_ttm")),
        dividend_yield_pct=_decimal(row.get("dv_ratio")),
        dividend_yield_ttm_pct=_decimal(row.get("dv_ttm")),
        total_shares=_scaled_integer(row.get("total_share"), "total_share"),
        circulating_shares=_scaled_integer(row.get("float_share"), "float_share"),
        free_float_shares=_scaled_integer(row.get("free_share"), "free_share"),
        total_market_value=_scaled_decimal(row.get("total_mv")),
        circulating_market_value=_scaled_decimal(row.get("circ_mv")),
        price_limit_status=_price_limit_status(row.get("limit_status")),
        source_code="tushare",
    )


def _complete_stock_daily_indicator_snapshot(
    rows: Sequence[RawRow],
) -> list[StockDailyIndicatorSnapshotRecord]:
    if not rows:
        raise ProviderError("Tushare daily_basic returned an empty market snapshot")
    if len(rows) >= STOCK_DAILY_INDICATOR_RESPONSE_LIMIT:
        raise ProviderError(
            "Tushare daily_basic market snapshot may be truncated at the response limit"
        )
    return [_map_stock_daily_indicator(row) for row in rows]


def _recent_report_periods(as_of_date: date) -> tuple[date, ...]:
    candidates = [
        date(year, month, day)
        for year in range(as_of_date.year - 2, as_of_date.year + 1)
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31))
        if date(year, month, day) <= as_of_date
    ]
    return tuple(sorted(candidates, reverse=True)[:5])


def _map_deducted_profit(
    row: RawRow,
    disclosure: RawRow,
) -> DeductedProfitRecord:
    _, _, symbol = _normalize_symbol(row["ts_code"])
    report_period = _parse_date(row["end_date"])
    announcement_date = _parse_date(row["ann_date"])
    actual_date = _optional_date(disclosure.get("actual_date"))
    cumulative = _decimal(row.get("profit_dedt"))
    quarterly = _decimal(row.get("q_dtprofit"))
    update_flag = row.get("update_flag") or None
    revision_key = deducted_profit_revision_key(
        symbol=symbol,
        report_period=report_period,
        announcement_date=announcement_date,
        actual_announcement_date=actual_date,
        cumulative_deducted_profit=cumulative,
        quarterly_deducted_profit=quarterly,
        update_flag=update_flag,
    )
    return DeductedProfitRecord(
        symbol=symbol,
        report_period=report_period,
        announcement_date=announcement_date,
        actual_announcement_date=actual_date,
        cumulative_deducted_profit=cumulative,
        quarterly_deducted_profit=quarterly,
        update_flag=update_flag,
        revision_key=revision_key,
        source_code="tushare",
    )


def _map_shareholder_count(row: Mapping[str, str]) -> ShareholderCountRecord:
    _, _, symbol = _normalize_symbol(row["ts_code"])
    statistics_date = _parse_date(row["end_date"])
    announcement_date = _parse_date(row["ann_date"])
    shareholder_count = _strict_positive_integer(row.get("holder_num"), "holder_num")
    return ShareholderCountRecord(
        symbol=symbol,
        statistics_date=statistics_date,
        announcement_date=announcement_date,
        shareholder_count=shareholder_count,
        revision_key=shareholder_count_revision_key(
            symbol=symbol,
            statistics_date=statistics_date,
            announcement_date=announcement_date,
            shareholder_count=shareholder_count,
        ),
        source_code="tushare",
    )


def _shareholder_count_records(
    rows: Sequence[Mapping[str, str]],
) -> list[ShareholderCountRecord]:
    records: list[ShareholderCountRecord] = []
    for row in rows:
        if "holder_num" not in row:
            raise ProviderError("Tushare stk_holdernumber missing fields: holder_num")
        if not row["holder_num"].strip():
            continue
        records.append(_map_shareholder_count(row))
    return records


def _source_symbol(value: str) -> str:
    exchange, code, _ = _normalize_symbol(value)
    suffix = {Exchange.SSE: "SH", Exchange.SZSE: "SZ", Exchange.BSE: "BJ"}[exchange]
    return f"{code}.{suffix}"


def _normalize_symbol(value: str) -> tuple[Exchange, str, str]:
    candidate = value.strip().upper()
    if ":" in candidate:
        prefix, code = candidate.split(":", maxsplit=1)
        exchange = {"SSE": Exchange.SSE, "SZSE": Exchange.SZSE, "BSE": Exchange.BSE}.get(prefix)
    elif "." in candidate:
        code, suffix = candidate.split(".", maxsplit=1)
        exchange = {"SH": Exchange.SSE, "SZ": Exchange.SZSE, "BJ": Exchange.BSE}.get(suffix)
    else:
        code = candidate
        exchange = _infer_exchange(code)
    if exchange is None or len(code) != 6 or not code.isdigit():
        raise ProviderError(f"unsupported Tushare symbol: {value}")
    return exchange, code, f"{exchange.value}:{code}"


def _infer_exchange(code: str) -> Exchange | None:
    if code.startswith(("4", "8", "92")):
        return Exchange.BSE
    if code.startswith(("5", "6", "9")):
        return Exchange.SSE
    if code.startswith(("0", "1", "2", "3")):
        return Exchange.SZSE
    return None


def _parse_date(value: str) -> date:
    text = value.strip()
    try:
        if len(text) == 8 and text.isdigit():
            return date(int(text[:4]), int(text[4:6]), int(text[6:]))
        return date.fromisoformat(text)
    except ValueError as error:
        raise ProviderError(f"invalid Tushare date: {value}") from error


def _optional_date(value: str | None) -> date | None:
    if value is None or not value.strip():
        return None
    return _parse_date(value)


def _optional_int(value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    try:
        return int(float(value))
    except ValueError as error:
        raise ProviderError(f"invalid Tushare integer: {value}") from error


def _strict_positive_integer(value: str | None, field_name: str) -> int:
    if value is None or not value.strip() or not value.strip().isdigit():
        raise ProviderError(f"invalid Tushare integer: {field_name}")
    result = int(value)
    if result <= 0:
        raise ProviderError(f"Tushare {field_name} must be positive")
    return result


def _decimal(value: str | None) -> Decimal | None:
    if value is None or not value.strip():
        return None
    try:
        return Decimal(value)
    except InvalidOperation as error:
        raise ProviderError(f"invalid Tushare decimal: {value}") from error


def _scaled_decimal(value: str | None) -> Decimal | None:
    number = _decimal(value)
    return number * Decimal(10_000) if number is not None else None


def _scaled_integer(value: str | None, field_name: str) -> int | None:
    scaled = _scaled_decimal(value)
    if scaled is None:
        return None
    if scaled != scaled.to_integral_value():
        raise ProviderError(f"Tushare {field_name} cannot be normalized to whole shares")
    return int(scaled)


def _price_limit_status(value: str | None) -> PriceLimitStatus:
    text = "" if value is None else value.strip()
    return {
        "0": PriceLimitStatus.FLAT,
        "1": PriceLimitStatus.RISE,
        "2": PriceLimitStatus.LIMIT_UP,
        "3": PriceLimitStatus.ONE_PRICE_LIMIT_UP,
        "4": PriceLimitStatus.FALL,
        "5": PriceLimitStatus.LIMIT_DOWN,
        "6": PriceLimitStatus.ONE_PRICE_LIMIT_DOWN,
    }.get(text, PriceLimitStatus.UNKNOWN)


def _request_date(request_params: Mapping[str, object], name: str) -> date:
    value = request_params.get(name)
    if not isinstance(value, str):
        raise ProviderError(f"Tushare replay request is missing {name}")
    return _parse_date(value)


def _ensure_date_range(start_date: date, end_date: date) -> None:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
