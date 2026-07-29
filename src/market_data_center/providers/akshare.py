"""AKShare adapter; source-specific names are contained in this module."""

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from types import TracebackType
from typing import Protocol, Self, cast

from market_data_center.domain.ingestion import DatasetCode
from market_data_center.domain.records import (
    CapitalRecord,
    CorporateActionStatus,
    DailyBarRecord,
    DistributionRecord,
    Exchange,
    Market,
    RightsIssueRecord,
    SecurityRecord,
    SecurityStatus,
    SecurityType,
    ShareCapitalRecord,
    TradeStatus,
    TradingDayRecord,
)
from market_data_center.providers.contracts import (
    ProviderBatch,
    ProviderError,
    ProviderRecord,
    RawRow,
)


class TabularResult(Protocol):
    @property
    def columns(self) -> Sequence[object]: ...

    def to_dict(self, orient: str) -> object: ...


class AKShareClient(Protocol):
    def stock_info_a_code_name(self) -> TabularResult: ...

    def tool_trade_date_hist_sina(self) -> TabularResult: ...

    def stock_zh_a_hist(
        self, *, symbol: str, period: str, start_date: str, end_date: str, adjust: str
    ) -> TabularResult: ...

    def stock_zh_a_gbjg_em(self, *, symbol: str) -> TabularResult: ...

    def stock_fhps_detail_em(self, *, symbol: str) -> TabularResult: ...

    def stock_history_dividend_detail(self, *, symbol: str, indicator: str) -> TabularResult: ...


class AKShareProvider(AbstractContextManager["AKShareProvider"]):
    source_code = "akshare"

    def __init__(self, client: AKShareClient) -> None:
        self._client = client

    @classmethod
    def default(cls) -> Self:
        import akshare  # type: ignore[import-untyped]

        return cls(cast(AKShareClient, akshare))

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
        return _source_code(symbol)

    def fetch_securities(self) -> ProviderBatch[SecurityRecord]:
        result = _provider_call("stock list", self._client.stock_info_a_code_name)
        rows = _rows(result, ("code", "name"), "stock list")
        return ProviderBatch(
            raw_rows=rows,
            request_params={},
            schema_version="akshare.security.v1",
            record_factory=lambda: [_map_security(row) for row in rows],
        )

    def fetch_trading_calendar(
        self, start_date: date, end_date: date
    ) -> ProviderBatch[TradingDayRecord]:
        _ensure_date_range(start_date, end_date)
        result = _provider_call("trading calendar", self._client.tool_trade_date_hist_sina)
        rows = _rows(result, ("trade_date",), "trading calendar")
        return ProviderBatch(
            raw_rows=rows,
            request_params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            schema_version="akshare.trading_calendar.v1",
            record_factory=lambda: _calendar_records(rows, start_date, end_date, self.source_code),
        )

    def fetch_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> ProviderBatch[DailyBarRecord]:
        _ensure_date_range(start_date, end_date)
        code = _source_code(source_symbol)
        result = _provider_call(
            "daily bars",
            lambda: self._client.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
                adjust="",
            ),
        )
        rows = _rows(
            result,
            ("日期", "股票代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额"),
            "daily bars",
        )
        return ProviderBatch(
            raw_rows=rows,
            request_params={
                "source_symbol": code,
                "start_date": start_date.strftime("%Y%m%d"),
                "end_date": end_date.strftime("%Y%m%d"),
                "period": "daily",
                "adjust": "",
            },
            schema_version="akshare.daily_bar.v1",
            record_factory=lambda: [_map_daily_bar(row) for row in rows],
        )

    def fetch_capital(self, source_symbol: str) -> ProviderBatch[CapitalRecord]:
        code = _source_code(source_symbol)
        exchange = _normalize_symbol(code)[0]
        suffix = {Exchange.SSE: "SH", Exchange.SZSE: "SZ", Exchange.BSE: "BJ"}[exchange]
        source_code = f"{code}.{suffix}"
        capital_frame = _provider_call(
            "share capital", lambda: self._client.stock_zh_a_gbjg_em(symbol=source_code)
        )
        distribution_frame = _provider_call(
            "distribution plans", lambda: self._client.stock_fhps_detail_em(symbol=code)
        )
        rights_frame = _provider_call(
            "rights issues",
            lambda: self._client.stock_history_dividend_detail(symbol=code, indicator="配股"),
        )
        capital_rows = _tagged_rows(
            capital_frame,
            ("变更日期", "总股本", "流通受限股份", "已流通股份", "已上市流通A股", "变动原因"),
            "share capital",
            "share_capital",
        )
        distribution_rows = _tagged_rows(
            distribution_frame,
            (
                "报告期",
                "送转股份-送股比例",
                "送转股份-转股比例",
                "现金分红-现金分红比例",
                "预案公告日",
                "股权登记日",
                "除权除息日",
                "方案进度",
                "最新公告日期",
            ),
            "distribution plans",
            "distribution",
            allow_empty=True,
        )
        rights_rows = _tagged_rows(
            rights_frame,
            (
                "公告日期",
                "配股方案",
                "配股价格",
                "基准股本",
                "除权日",
                "股权登记日",
                "缴款起始日",
                "缴款终止日",
                "配股上市日",
                "募集资金合计",
            ),
            "rights issues",
            "rights_issue",
            allow_empty=True,
        )
        rows = [*capital_rows, *distribution_rows, *rights_rows]
        return ProviderBatch(
            raw_rows=rows,
            request_params={"source_symbol": code},
            schema_version="akshare.capital.v1",
            record_factory=lambda: list(_map_capital_rows(rows, code)),
        )


def normalize_akshare_raw(
    dataset_code: DatasetCode,
    schema_version: str,
    raw_rows: Sequence[Mapping[str, str]],
    request_params: Mapping[str, object],
) -> tuple[ProviderRecord, ...]:
    expected_schema = {
        DatasetCode.SECURITY: "akshare.security.v1",
        DatasetCode.TRADING_CALENDAR: "akshare.trading_calendar.v1",
        DatasetCode.DAILY_BAR: "akshare.daily_bar.v1",
        DatasetCode.CAPITAL: "akshare.capital.v1",
    }[dataset_code]
    if schema_version != expected_schema:
        raise ProviderError(f"unsupported AKShare Raw schema: {schema_version}")
    if dataset_code is DatasetCode.SECURITY:
        return tuple(_map_security(row) for row in raw_rows)
    if dataset_code is DatasetCode.TRADING_CALENDAR:
        start_date = _replay_request_date(request_params, "start_date")
        end_date = _replay_request_date(request_params, "end_date")
        return tuple(_calendar_records(raw_rows, start_date, end_date, "akshare"))
    if dataset_code is DatasetCode.CAPITAL:
        source_symbol = request_params.get("source_symbol")
        if not isinstance(source_symbol, str):
            raise ProviderError("AKShare replay request is missing source_symbol")
        return tuple(_map_capital_rows(raw_rows, _source_code(source_symbol)))
    return tuple(_map_daily_bar(row) for row in raw_rows)


def _tagged_rows(
    result: TabularResult,
    required: Sequence[str],
    operation: str,
    record_type: str,
    *,
    allow_empty: bool = False,
) -> list[RawRow]:
    if allow_empty and len(result.columns) == 0:
        return []
    rows = _rows(result, required, operation)
    return [{"_capital_record_type": record_type, **row} for row in rows]


def _map_capital_rows(rows: Sequence[Mapping[str, str]], code: str) -> tuple[CapitalRecord, ...]:
    _, _, symbol = _normalize_symbol(code)
    records: list[CapitalRecord] = []
    for row in rows:
        record_type = row.get("_capital_record_type")
        if record_type == "share_capital":
            records.append(_map_share_capital(row, symbol))
        elif record_type == "distribution":
            allocation = _map_distribution(row, symbol)
            if allocation is not None:
                records.append(allocation)
        elif record_type == "rights_issue":
            records.append(_map_rights_issue(row, symbol))
        else:
            raise ProviderError(f"unsupported AKShare Capital record type: {record_type}")
    return tuple(records)


def _map_share_capital(row: Mapping[str, str], symbol: str) -> ShareCapitalRecord:
    return ShareCapitalRecord(
        symbol=symbol,
        effective_date=_parse_date(row["变更日期"]),
        total_shares=_required_integer(row.get("总股本"), "总股本"),
        restricted_shares=_integer(row.get("流通受限股份")),
        circulating_shares=_integer(row.get("已流通股份")),
        listed_a_shares=_integer(row.get("已上市流通A股")),
        change_reason=_optional_text(row.get("变动原因")),
        source_code="akshare",
    )


def _map_distribution(row: Mapping[str, str], symbol: str) -> DistributionRecord | None:
    cash = _per_share(row.get("现金分红-现金分红比例"))
    bonus = _per_share(row.get("送转股份-送股比例"))
    transfer = _per_share(row.get("送转股份-转股比例"))
    if not any(value is not None and value > 0 for value in (cash, bonus, transfer)):
        return None
    return DistributionRecord(
        symbol=symbol,
        report_period=_parse_date(row["报告期"]),
        announcement_date=_optional_date(row.get("最新公告日期"))
        or _optional_date(row.get("预案公告日")),
        record_date=_optional_date(row.get("股权登记日")),
        ex_date=_optional_date(row.get("除权除息日")),
        cash_dividend_per_share=cash,
        bonus_share_ratio=bonus,
        transfer_share_ratio=transfer,
        status=_corporate_action_status(row.get("方案进度")),
        source_code="akshare",
    )


def _map_rights_issue(row: Mapping[str, str], symbol: str) -> RightsIssueRecord:
    record_date = _optional_date(row.get("股权登记日"))
    if record_date is None:
        raise ProviderError("AKShare rights issue is missing 股权登记日")
    return RightsIssueRecord(
        symbol=symbol,
        record_date=record_date,
        announcement_date=_optional_date(row.get("公告日期"), reject_placeholder=True),
        ex_date=_optional_date(row.get("除权日"), reject_placeholder=True),
        payment_start_date=_optional_date(row.get("缴款起始日"), reject_placeholder=True),
        payment_end_date=_optional_date(row.get("缴款终止日"), reject_placeholder=True),
        listing_date=_optional_date(row.get("配股上市日"), reject_placeholder=True),
        rights_ratio=_required_decimal(row.get("配股方案"), "配股方案") / Decimal(10),
        rights_price=_required_decimal(row.get("配股价格"), "配股价格"),
        base_shares=_integer(row.get("基准股本")),
        proceeds=_decimal(row.get("募集资金合计")),
        source_code="akshare",
    )


def _per_share(value: str | None) -> Decimal | None:
    number = _decimal(value)
    return number / Decimal(10) if number is not None else None


def _required_decimal(value: str | None, field: str) -> Decimal:
    number = _decimal(value)
    if number is None:
        raise ProviderError(f"AKShare Capital field is missing: {field}")
    return number


def _required_integer(value: str | None, field: str) -> int:
    number = _integer(value)
    if number is None:
        raise ProviderError(f"AKShare Capital field is missing: {field}")
    return number


def _optional_text(value: str | None) -> str | None:
    if value is None or not value.strip() or value.strip().lower() in {"nan", "nat", "--"}:
        return None
    return value.strip()


def _optional_date(value: str | None, *, reject_placeholder: bool = False) -> date | None:
    text = _optional_text(value)
    if text is None:
        return None
    parsed = _parse_date(text)
    if reject_placeholder and parsed.year <= 1900:
        return None
    return parsed


def _corporate_action_status(value: str | None) -> CorporateActionStatus:
    text = _optional_text(value)
    if text is None:
        return CorporateActionStatus.UNKNOWN
    if "取消" in text or "终止" in text:
        return CorporateActionStatus.CANCELLED
    if "实施" in text or "完成" in text:
        return CorporateActionStatus.IMPLEMENTED
    if "股东大会" in text or "批准" in text:
        return CorporateActionStatus.APPROVED
    if "预案" in text or "董事会" in text:
        return CorporateActionStatus.PLANNED
    return CorporateActionStatus.UNKNOWN


def _replay_request_date(request_params: Mapping[str, object], name: str) -> date:
    value = request_params.get(name)
    if not isinstance(value, str):
        raise ProviderError(f"AKShare replay request is missing {name}")
    candidate = value.strip()
    try:
        if len(candidate) == 8 and candidate.isdigit():
            return date(int(candidate[:4]), int(candidate[4:6]), int(candidate[6:]))
        return date.fromisoformat(candidate)
    except ValueError as error:
        raise ProviderError(f"AKShare replay request has invalid {name}") from error


def _calendar_records(
    rows: Sequence[Mapping[str, str]],
    start_date: date,
    end_date: date,
    source_code: str,
) -> list[TradingDayRecord]:
    trading_dates = {_parse_date(row["trade_date"]) for row in rows}
    if not trading_dates or start_date < min(trading_dates) or end_date > max(trading_dates):
        raise ProviderError("AKShare trading calendar does not cover the requested date range")
    records: list[TradingDayRecord] = []
    current = start_date
    while current <= end_date:
        records.append(
            TradingDayRecord(
                market=Market.CN_A_SHARE,
                trade_date=current,
                is_trading_day=current in trading_dates,
                source_code=source_code,
            )
        )
        current += timedelta(days=1)
    return records


def _rows(result: TabularResult, required: Sequence[str], operation: str) -> list[RawRow]:
    columns = {str(column) for column in result.columns}
    missing = set(required).difference(columns)
    if missing:
        raise ProviderError(f"AKShare {operation} missing fields: {', '.join(sorted(missing))}")
    objects = cast(list[Mapping[object, object]], result.to_dict(orient="records"))
    return [{str(key): _raw_value(value) for key, value in row.items()} for row in objects]


def _provider_call[ResultT](operation: str, call: Callable[[], ResultT]) -> ResultT:
    try:
        return call()
    except ProviderError:
        raise
    except Exception as error:
        raise ProviderError(f"AKShare {operation} request failed") from error


def _raw_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (date, datetime)):
        return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
    return str(value)


def _map_security(row: Mapping[str, str]) -> SecurityRecord:
    exchange, code, symbol = _normalize_symbol(row["code"])
    return SecurityRecord(
        symbol=symbol,
        code=code,
        exchange=exchange,
        name=row["name"].strip(),
        security_type=SecurityType.STOCK,
        status=SecurityStatus.UNKNOWN,
        ipo_date=None,
        delisting_date=None,
        source_code="akshare",
    )


def _map_daily_bar(row: Mapping[str, str]) -> DailyBarRecord:
    _, _, symbol = _normalize_symbol(row["股票代码"])
    close = _decimal(row.get("收盘"))
    change = _decimal(row.get("涨跌额"))
    previous_close = close - change if close is not None and change is not None else None
    return DailyBarRecord(
        symbol=symbol,
        trade_date=_parse_date(row["日期"]),
        market=Market.CN_A_SHARE,
        open=_decimal(row.get("开盘")),
        high=_decimal(row.get("最高")),
        low=_decimal(row.get("最低")),
        close=close,
        previous_close=previous_close,
        volume=_integer(row.get("成交量")),
        amount=_decimal(row.get("成交额")),
        trade_status=TradeStatus.TRADING,
        is_st=None,
        source_code="akshare",
    )


def _source_code(value: str) -> str:
    candidate = value.strip().lower()
    if "." in candidate:
        candidate = candidate.split(".", maxsplit=1)[1]
    if ":" in candidate:
        candidate = candidate.split(":", maxsplit=1)[1]
    _normalize_symbol(candidate)
    return candidate


def _normalize_symbol(value: str) -> tuple[Exchange, str, str]:
    code = value.strip().lower()
    if "." in code:
        code = code.split(".", maxsplit=1)[1]
    if ":" in code:
        code = code.split(":", maxsplit=1)[1]
    if not code.isdigit() or len(code) != 6:
        raise ProviderError(f"unsupported AKShare symbol: {value}")
    if code.startswith(("4", "8", "92")):
        exchange = Exchange.BSE
    elif code.startswith(("5", "6", "9")):
        exchange = Exchange.SSE
    elif code.startswith(("0", "1", "2", "3")):
        exchange = Exchange.SZSE
    else:
        raise ProviderError(f"unsupported AKShare symbol: {value}")
    return exchange, code, f"{exchange.value}:{code}"


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value[:10])
    except ValueError as error:
        raise ProviderError(f"invalid AKShare date: {value}") from error


def _decimal(value: str | None) -> Decimal | None:
    if value is None or not value.strip() or value.lower() == "nan":
        return None
    try:
        return Decimal(value)
    except InvalidOperation as error:
        raise ProviderError(f"invalid AKShare decimal: {value}") from error


def _integer(value: str | None) -> int | None:
    number = _decimal(value)
    if number is None:
        return None
    if number != number.to_integral_value():
        raise ProviderError(f"invalid AKShare integer: {value}")
    return int(number)


def _ensure_date_range(start_date: date, end_date: date) -> None:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
