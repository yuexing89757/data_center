"""EastMoney Adapter for provider-neutral DragonTiger facts."""

import json
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from time import sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from market_data_center.domain.dragon_tiger import (
    DragonTigerEventDraft,
    DragonTigerPeriodType,
    DragonTigerReason,
    DragonTigerReasonType,
    SeatTradeRecord,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError, RawRow

SUMMARY_REPORT = "RPT_DAILYBILLBOARD_DETAILS"
BUY_REPORT = "RPT_BILLBOARD_DAILYDETAILSBUY"
SELL_REPORT = "RPT_BILLBOARD_DAILYDETAILSSELL"
SCHEMA_VERSION = "eastmoney.dragon_tiger.v2"
LEGACY_SCHEMA_VERSION = "eastmoney.trading_billboard.v1"
BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
PAGE_SIZE = 500
MAX_PAGES = 100
MAX_RESPONSE_BYTES = 16 * 1024 * 1024

type SourceRow = Mapping[str, object]
type RequestJson = Callable[[str, float], Mapping[str, Any]]


class EastmoneyDragonTigerAdapter:
    source_code = "eastmoney"

    def __init__(
        self,
        request_json: RequestJson = lambda url, timeout: _request_json(url, timeout),
        *,
        timeout: float = 8.0,
        max_attempts: int = 2,
    ) -> None:
        if timeout <= 0 or max_attempts < 1:
            raise ValueError("EastMoney DragonTiger request bounds must be positive")
        self._request_json = request_json
        self._timeout = timeout
        self._max_attempts = max_attempts

    def fetch_dragon_tiger(self, trade_date: date) -> ProviderBatch[DragonTigerEventDraft]:
        raw_rows: list[RawRow] = []
        counts: dict[str, int] = {}
        for report, kind in (
            (SUMMARY_REPORT, "summary"),
            (BUY_REPORT, "buy_seat"),
            (SELL_REPORT, "sell_seat"),
        ):
            rows, count = self._fetch_report(report, trade_date)
            counts[report] = count
            raw_rows.extend(_raw_row(kind, row.page, row.index, row.payload) for row in rows)
        frozen_rows = tuple(raw_rows)
        return ProviderBatch(
            raw_rows=frozen_rows,
            request_params={
                "trade_date": trade_date.isoformat(),
                "reports": [SUMMARY_REPORT, BUY_REPORT, SELL_REPORT],
                "source_counts": counts,
            },
            schema_version=SCHEMA_VERSION,
            record_factory=lambda: normalize_eastmoney_dragon_tiger_raw(
                frozen_rows, SCHEMA_VERSION
            ),
        )

    def _fetch_report(self, report: str, trade_date: date) -> tuple[tuple["PagedRow", ...], int]:
        rows: list[PagedRow] = []
        expected_count: int | None = None
        expected_pages: int | None = None
        fingerprints: set[str] = set()
        page = 1
        while True:
            payload = self._request_page(report, trade_date, page)
            result = payload.get("result")
            if payload.get("success") is not True or not isinstance(result, Mapping):
                raise ProviderError("EastMoney DragonTiger response is unavailable")
            count = _bounded_int(result.get("count"), "count", minimum=0)
            pages = _bounded_int(result.get("pages"), "pages", minimum=0)
            data = result.get("data")
            if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
                raise ProviderError("EastMoney DragonTiger response data is unavailable")
            if pages > MAX_PAGES:
                raise ProviderError("EastMoney DragonTiger page bound exceeded")
            if expected_count is None:
                expected_count, expected_pages = count, pages
            elif count != expected_count:
                raise ProviderError("EastMoney DragonTiger response count changed")
            elif pages != expected_pages:
                raise ProviderError("EastMoney DragonTiger response page count changed")
            for index, item in enumerate(data):
                if not isinstance(item, Mapping):
                    raise ProviderError("EastMoney DragonTiger row is not an object")
                fingerprint = sha256(_canonical_json(item).encode("utf-8")).hexdigest()
                if fingerprint in fingerprints:
                    raise ProviderError("EastMoney DragonTiger duplicate page content")
                fingerprints.add(fingerprint)
                rows.append(PagedRow(page, index, item))
            if pages == 0 or page >= pages:
                break
            page += 1
        if expected_count is None or len(rows) != expected_count:
            raise ProviderError("EastMoney DragonTiger response row count mismatch")
        return tuple(rows), expected_count

    def _request_page(self, report: str, trade_date: date, page_number: int) -> Mapping[str, Any]:
        query = urlencode(
            {
                "reportName": report,
                "columns": "ALL",
                "filter": f"(TRADE_DATE='{trade_date.isoformat()}')",
                "pageNumber": page_number,
                "pageSize": PAGE_SIZE,
                "sortColumns": "TRADE_ID",
                "sortTypes": "1",
                "source": "WEB",
                "client": "WEB",
            }
        )
        for attempt in range(1, self._max_attempts + 1):
            try:
                return self._request_json(f"{BASE_URL}?{query}", self._timeout)
            except Exception as error:
                if attempt == self._max_attempts:
                    raise ProviderError("EastMoney DragonTiger request failed") from error
                sleep(0.05)
        raise AssertionError("bounded request loop must return or raise")


class PagedRow:
    __slots__ = ("index", "page", "payload")

    def __init__(self, page: int, index: int, payload: SourceRow) -> None:
        self.page = page
        self.index = index
        self.payload = payload


def normalize_eastmoney_dragon_tiger_raw(
    rows: Sequence[RawRow], schema_version: str
) -> tuple[DragonTigerEventDraft, ...]:
    if schema_version not in {SCHEMA_VERSION, LEGACY_SCHEMA_VERSION}:
        raise ProviderError("unsupported EastMoney DragonTiger Raw schema")
    grouped: dict[str, list[SourceRow]] = {
        "summary": [],
        "buy_seat": [],
        "sell_seat": [],
    }
    for raw in rows:
        kind = raw.get("record_kind")
        if kind not in grouped:
            raise ProviderError("EastMoney DragonTiger Raw record kind is invalid")
        try:
            payload = json.loads(raw["payload_json"], parse_float=Decimal)
        except (KeyError, json.JSONDecodeError, InvalidOperation) as error:
            raise ProviderError("EastMoney DragonTiger Raw payload is invalid") from error
        if not isinstance(payload, Mapping):
            raise ProviderError("EastMoney DragonTiger Raw payload is not an object")
        grouped[kind].append(payload)
    return _normalize(grouped["summary"], grouped["buy_seat"], grouped["sell_seat"])


def _normalize(
    summaries: Sequence[SourceRow],
    buy_rows: Sequence[SourceRow],
    sell_rows: Sequence[SourceRow],
) -> tuple[DragonTigerEventDraft, ...]:
    if not summaries:
        raise ProviderError("EastMoney DragonTiger contains no stock summaries")
    by_side = {
        "buy": _group_details(buy_rows),
        "sell": _group_details(sell_rows),
    }
    events: list[DragonTigerEventDraft] = []
    seen_events: set[str] = set()
    for summary in summaries:
        event_id = _required_text(summary, "TRADE_ID")
        if event_id in seen_events:
            raise ProviderError("EastMoney DragonTiger has duplicate event identity")
        seen_events.add(event_id)
        symbol = _stock_symbol(summary)
        if symbol is None:
            continue
        trade_date = _source_date(summary)
        buy = by_side["buy"].get(event_id, ())
        sell = by_side["sell"].get(event_id, ())
        if not buy or not sell:
            raise ProviderError("EastMoney DragonTiger requires both buy and sell disclosures")
        ordered_buy = tuple(sorted(buy, key=lambda row: _seat_sort_key(row, "buy")))
        ordered_sell = tuple(sorted(sell, key=lambda row: _seat_sort_key(row, "sell")))
        if len(ordered_buy) > 5 or len(ordered_sell) > 5:
            raise ProviderError("EastMoney DragonTiger allows at most five seats per side")
        trades = _merge_seat_rows(event_id, symbol, trade_date, ordered_buy, ordered_sell)
        reason_name = _required_text(summary, "EXPLANATION")
        period = _period_type(reason_name)
        reason_type = _reason_type(reason_name)
        source_reason_code = _required_text(summary, "CHANGE_TYPE")
        reason = DragonTigerReason(
            reason_code=_reason_code(reason_type, period, reason_name),
            reason_name=reason_name,
            reason_type=reason_type,
            period_type=period,
            source_code="eastmoney",
            source_reason_code=source_reason_code,
            source_reason_name=reason_name,
        )
        events.append(
            DragonTigerEventDraft(
                source_record_id=event_id,
                symbol=symbol,
                trade_date=trade_date,
                period_type=period,
                period_start_date=trade_date if period is DragonTigerPeriodType.DAY else None,
                period_end_date=trade_date,
                reason=reason,
                reason_name_raw=reason_name,
                close_price=_decimal(summary, "CLOSE_PRICE"),
                change_pct=_decimal(summary, "CHANGE_RATE"),
                turnover_amount=_decimal(summary, "ACCUM_AMOUNT"),
                turnover_rate=_decimal(summary, "TURNOVERRATE"),
                amplitude=_decimal(summary, "AMPLITUDE"),
                lhb_buy_amount=_decimal(summary, "BILLBOARD_BUY_AMT"),
                lhb_sell_amount=_decimal(summary, "BILLBOARD_SELL_AMT"),
                seat_trades=trades,
                source_code="eastmoney",
            )
        )
    if not events:
        raise ProviderError("EastMoney DragonTiger contains no accepted stock summaries")
    return tuple(events)


def _merge_seat_rows(
    event_id: str,
    symbol: str,
    trade_date: date,
    buy_rows: tuple[SourceRow, ...],
    sell_rows: tuple[SourceRow, ...],
) -> tuple[SeatTradeRecord, ...]:
    names_by_code: dict[str, set[str]] = {}
    for row in (*buy_rows, *sell_rows):
        name = _required_text(row, "OPERATEDEPT_NAME")
        code = _optional_text(row.get("OPERATEDEPT_CODE"))
        if code not in {None, "0"} and not _is_placeholder_seat_name(name):
            names_by_code.setdefault(code, set()).add(name)
    ambiguous_codes = {code for code, names in names_by_code.items() if len(names) > 1}

    builders: dict[str, dict[str, object]] = {}
    order: list[str] = []
    for side, rows in (("buy", buy_rows), ("sell", sell_rows)):
        for rank, row in enumerate(rows, start=1):
            if _source_date(row) != trade_date or _stock_symbol(row) != symbol:
                raise ProviderError("EastMoney DragonTiger seat parent identity mismatch")
            name = _required_text(row, "OPERATEDEPT_NAME")
            code = _optional_text(row.get("OPERATEDEPT_CODE"))
            reliable_code = (
                code
                if code not in {None, "0"}
                and code not in ambiguous_codes
                and not _is_placeholder_seat_name(name)
                else None
            )
            fingerprint = sha256(_canonical_json(row).encode("utf-8")).hexdigest()[:16]
            key = (
                f"seat:{reliable_code}"
                if reliable_code is not None
                else f"anonymous:{side}:{rank}:{fingerprint}"
            )
            if key not in builders:
                builders[key] = {
                    "name": name,
                    "code": reliable_code,
                    "buy": None,
                    "sell": None,
                    "buy_rank": None,
                    "sell_rank": None,
                }
                order.append(key)
            builder = builders[key]
            if builder["name"] != name:
                raise ProviderError("EastMoney DragonTiger seat code has conflicting names")
            builder["buy"] = _coalesce_amount(builder["buy"], _decimal(row, "BUY"))
            builder["sell"] = _coalesce_amount(builder["sell"], _decimal(row, "SELL"))
            builder[f"{side}_rank"] = rank
    trades: list[SeatTradeRecord] = []
    for key in order:
        value = builders[key]
        name = str(value["name"])
        raw_code = value["code"]
        seat_source_key = str(raw_code) if raw_code is not None else None
        source_id = (
            f"{event_id}:seat:{seat_source_key}"
            if seat_source_key is not None
            else f"{event_id}:{key}"
        )
        trades.append(
            SeatTradeRecord(
                source_record_id=source_id,
                source_event_id=event_id,
                symbol=symbol,
                trade_date=trade_date,
                seat_id=None,
                seat_source_key=seat_source_key,
                seat_name_raw=name,
                buy_amount=value["buy"] if isinstance(value["buy"], Decimal) else None,
                sell_amount=value["sell"] if isinstance(value["sell"], Decimal) else None,
                buy_rank=value["buy_rank"] if isinstance(value["buy_rank"], int) else None,
                sell_rank=value["sell_rank"] if isinstance(value["sell_rank"], int) else None,
                is_institution=name == "机构专用",
                is_northbound=name in {"沪股通专用", "深股通专用", "北向资金专用"},
                source_code="eastmoney",
            )
        )
    return tuple(trades)


def _coalesce_amount(current: object, incoming: Decimal | None) -> Decimal | None:
    if current is None:
        return incoming
    if incoming is None:
        return current if isinstance(current, Decimal) else None
    if not isinstance(current, Decimal) or current != incoming:
        raise ProviderError("EastMoney DragonTiger seat amount conflicts across sides")
    return current


def _period_type(reason: str) -> DragonTigerPeriodType:
    if "连续三个交易日" in reason:
        return DragonTigerPeriodType.THREE_DAY
    if "交易日" in reason or re.search(
        r"(?:最近|连续).{0,16}日|[二两三四五六七八九十2-9]+个?日内", reason
    ):
        raise ProviderError("EastMoney DragonTiger period is unsupported")
    return DragonTigerPeriodType.DAY


def _is_placeholder_seat_name(name: str) -> bool:
    return name in {"机构专用", "沪股通专用", "深股通专用", "北向资金专用"}


def _reason_type(reason: str) -> DragonTigerReasonType:
    if "换手率" in reason:
        return DragonTigerReasonType.TURNOVER
    if "振幅" in reason:
        return DragonTigerReasonType.AMPLITUDE
    if "ST" in reason.upper():
        return DragonTigerReasonType.ST
    if "连续涨停" in reason:
        return DragonTigerReasonType.CONTINUOUS_LIMIT
    if "偏离值" in reason:
        return DragonTigerReasonType.PRICE_DEVIATION
    return DragonTigerReasonType.OTHER


def _reason_code(
    reason_type: DragonTigerReasonType,
    period: DragonTigerPeriodType,
    reason_name: str,
) -> str:
    digest = sha256(reason_name.strip().encode("utf-8")).hexdigest()[:12].upper()
    return f"{reason_type.value}_{period.value}_{digest}"


def _group_details(rows: Sequence[SourceRow]) -> dict[str, tuple[SourceRow, ...]]:
    grouped: dict[str, list[SourceRow]] = {}
    for row in rows:
        grouped.setdefault(_required_text(row, "TRADE_ID"), []).append(row)
    return {key: tuple(value) for key, value in grouped.items()}


def _seat_sort_key(row: SourceRow, side: str) -> tuple[object, ...]:
    amount = _decimal(row, "BUY" if side == "buy" else "SELL")
    return (
        amount is None,
        -(amount or Decimal(0)),
        _optional_text(row.get("OPERATEDEPT_CODE")) or "",
        _required_text(row, "OPERATEDEPT_NAME"),
        sha256(_canonical_json(row).encode("utf-8")).hexdigest(),
    )


def _stock_symbol(row: SourceRow) -> str | None:
    secucode = _required_text(row, "SECUCODE").upper()
    code, separator, suffix = secucode.partition(".")
    if separator != "." or len(code) != 6 or not code.isdigit():
        raise ProviderError("EastMoney DragonTiger security identifier is invalid")
    if suffix == "SH" and code.startswith("6"):
        return f"SSE:{code}"
    if suffix == "SZ" and code.startswith(("0", "3")):
        return f"SZSE:{code}"
    if suffix == "BJ" and code.startswith(("4", "8", "9")):
        return f"BSE:{code}"
    return None


def _source_date(row: SourceRow) -> date:
    value = row.get("TRADE_DATE")
    if not isinstance(value, str) or len(value) < 10:
        raise ProviderError("EastMoney DragonTiger required TRADE_DATE is missing")
    try:
        return date.fromisoformat(value[:10])
    except ValueError as error:
        raise ProviderError("EastMoney DragonTiger response date is invalid") from error


def _required_text(row: SourceRow, field: str) -> str:
    value = _optional_text(row.get(field))
    if value is None:
        raise ProviderError(f"EastMoney DragonTiger required {field} is missing")
    return value


def _optional_text(value: object) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (str, int, Decimal)):
        text = str(value).strip()
        return text or None
    return None


def _decimal(row: SourceRow, field: str) -> Decimal | None:
    value = row.get(field)
    if value is None or value == "":
        return None
    if isinstance(value, (bool, float)):
        raise ProviderError(f"EastMoney DragonTiger {field} decimal is invalid")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ProviderError(f"EastMoney DragonTiger {field} decimal is invalid") from error
    if not parsed.is_finite():
        raise ProviderError(f"EastMoney DragonTiger {field} decimal is invalid")
    return parsed


def _raw_row(kind: str, page: int, index: int, row: SourceRow) -> RawRow:
    return {
        "record_kind": kind,
        "source_page": str(page),
        "source_index": str(index),
        "payload_json": _canonical_json(row),
    }


def _bounded_int(value: object, field: str, *, minimum: int) -> int:
    if isinstance(value, bool):
        raise ProviderError(f"EastMoney DragonTiger {field} is invalid")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as error:
        raise ProviderError(f"EastMoney DragonTiger {field} is invalid") from error
    if parsed < minimum or str(parsed) != str(value):
        raise ProviderError(f"EastMoney DragonTiger {field} is invalid")
    return parsed


def _canonical_json(value: object) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        raise ProviderError("EastMoney response contains binary floating-point data")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ProviderError("EastMoney response object key is invalid")
        return (
            "{"
            + ",".join(
                f"{_canonical_json(key)}:{_canonical_json(value[key])}" for key in sorted(value)
            )
            + "}"
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return "[" + ",".join(_canonical_json(item) for item in value) + "]"
    raise ProviderError("EastMoney response contains an unsupported JSON value")


def _request_json(url: str, timeout: float) -> Mapping[str, Any]:
    request = Request(
        url,
        headers={
            "User-Agent": "MarketDataCenter/0.2",
            "Referer": "https://data.eastmoney.com/stock/tradedetail.html",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                raise ProviderError("EastMoney returned a non-success response")
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except (HTTPError, URLError) as error:
        raise ProviderError("EastMoney DragonTiger request failed") from error
    if len(body) > MAX_RESPONSE_BYTES:
        raise ProviderError("EastMoney DragonTiger response exceeds the byte bound")
    try:
        value = json.loads(body, parse_float=Decimal)
    except (json.JSONDecodeError, UnicodeDecodeError, InvalidOperation) as error:
        raise ProviderError("EastMoney DragonTiger response is invalid JSON") from error
    if not isinstance(value, Mapping):
        raise ProviderError("EastMoney DragonTiger response is not an object")
    return value
