"""Bounded Eastmoney adapter for daily A-share trading billboard facts."""

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from market_data_center.domain.trading_billboard import (
    TradingBillboardRecord,
    TradingBillboardSeatRecord,
    TradingBillboardSide,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError, RawRow

ENDPOINT = "https://datacenter-web.eastmoney.com/api/data/v1/get"
SCHEMA_VERSION = "eastmoney.trading_billboard.v1"
SUMMARY_REPORT = "RPT_DAILYBILLBOARD_DETAILS"
BUY_REPORT = "RPT_BILLBOARD_DAILYDETAILSBUY"
SELL_REPORT = "RPT_BILLBOARD_DAILYDETAILSSELL"
PAGE_SIZE = 500
MAX_PAGES = 20
MAX_RESPONSE_BYTES = 2_000_000
DEFAULT_TIMEOUT_SECONDS = 8.0
MAX_ATTEMPTS = 2

type SourceRow = Mapping[str, object]
type PagedRow = tuple[int, int, SourceRow]


class EastmoneyTradingBillboardProvider:
    source_code = "eastmoney"

    def __init__(
        self,
        request_json: Callable[[str, float], Mapping[str, Any]] | None = None,
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int = MAX_ATTEMPTS,
    ) -> None:
        if not 1 <= max_attempts <= MAX_ATTEMPTS:
            raise ValueError("Eastmoney max_attempts is outside the code-owned bound")
        if not 1 <= timeout_seconds <= DEFAULT_TIMEOUT_SECONDS:
            raise ValueError("Eastmoney timeout is outside the code-owned bound")
        self._request_json = request_json or _request_json
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts

    def fetch_trading_billboard(self, trade_date: date) -> ProviderBatch[TradingBillboardRecord]:
        report_rows: dict[str, tuple[PagedRow, ...]] = {}
        report_metadata: dict[str, dict[str, int]] = {}
        for report in (SUMMARY_REPORT, BUY_REPORT, SELL_REPORT):
            rows, pages = self._fetch_report(report, trade_date)
            report_rows[report] = rows
            report_metadata[report] = {"rows": len(rows), "pages": pages}

        raw_rows = tuple(
            _raw_row(kind, page, index, row)
            for report, kind in (
                (SUMMARY_REPORT, "summary"),
                (BUY_REPORT, "buy_seat"),
                (SELL_REPORT, "sell_seat"),
            )
            for page, index, row in report_rows[report]
        )
        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={
                "trade_date": trade_date.isoformat(),
                "reports": report_metadata,
            },
            schema_version=SCHEMA_VERSION,
            record_factory=lambda: normalize_eastmoney_trading_billboard_raw(
                raw_rows, SCHEMA_VERSION
            ),
        )

    def _fetch_report(self, report: str, trade_date: date) -> tuple[tuple[PagedRow, ...], int]:
        expected_count: int | None = None
        expected_pages: int | None = None
        page_signatures: set[str] = set()
        collected: list[PagedRow] = []
        page = 1
        while expected_pages is None or page <= expected_pages:
            payload = self._request_page(report, trade_date, page)
            if payload.get("success") is not True:
                raise ProviderError("Eastmoney trading billboard response is unavailable")
            result = payload.get("result")
            if not isinstance(result, Mapping):
                raise ProviderError("Eastmoney trading billboard response is unavailable")
            count = _bounded_int(result.get("count"), "count", minimum=0)
            pages = _bounded_int(result.get("pages"), "pages", minimum=1)
            if pages > MAX_PAGES:
                raise ProviderError("Eastmoney trading billboard page bound exceeded")
            if expected_count is None:
                expected_count, expected_pages = count, pages
            elif count != expected_count:
                raise ProviderError(
                    "Eastmoney trading billboard result count changed between pages"
                )
            elif pages != expected_pages:
                raise ProviderError("Eastmoney trading billboard page count changed between pages")

            data = result.get("data")
            if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
                raise ProviderError("Eastmoney trading billboard result data is unavailable")
            signature = sha256(_canonical_json(data).encode("utf-8")).hexdigest()
            if signature in page_signatures:
                raise ProviderError("Eastmoney trading billboard returned a duplicate page")
            page_signatures.add(signature)
            for index, value in enumerate(data):
                if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
                    raise ProviderError("Eastmoney trading billboard row is not an object")
                row = dict(value)
                if _source_date(row) != trade_date:
                    raise ProviderError("Eastmoney trading billboard response date mismatch")
                collected.append((page, index, row))
            page += 1

        if expected_count is None or expected_pages is None:  # pragma: no cover
            raise ProviderError("Eastmoney trading billboard pagination is unavailable")
        if len(collected) != expected_count:
            raise ProviderError("Eastmoney trading billboard result count is incomplete")
        return tuple(collected), expected_pages

    def _request_page(self, report: str, trade_date: date, page_number: int) -> Mapping[str, Any]:
        params = {
            "reportName": report,
            "columns": "ALL",
            "filter": f"(TRADE_DATE='{trade_date.isoformat()}')",
            "pageNumber": page_number,
            "pageSize": PAGE_SIZE,
            "sortColumns": "TRADE_ID",
            "sortTypes": "1",
        }
        url = f"{ENDPOINT}?{urlencode(params)}"
        for attempt in range(self._max_attempts):
            try:
                return self._request_json(url, self._timeout_seconds)
            except (OSError, ValueError, ProviderError) as error:
                if attempt + 1 == self._max_attempts:
                    raise ProviderError("Eastmoney trading billboard request failed") from error
        raise AssertionError("unreachable")


def normalize_eastmoney_trading_billboard_raw(
    rows: Sequence[RawRow], schema_version: str
) -> tuple[TradingBillboardRecord, ...]:
    if schema_version != SCHEMA_VERSION:
        raise ProviderError("unsupported Eastmoney trading billboard Raw schema")
    grouped: dict[str, list[SourceRow]] = {
        "summary": [],
        "buy_seat": [],
        "sell_seat": [],
    }
    for raw in rows:
        kind = raw.get("record_kind")
        if kind not in grouped:
            raise ProviderError("unknown Eastmoney trading billboard Raw record kind")
        _bounded_int(raw.get("source_page"), "source_page", minimum=1)
        _bounded_int(raw.get("source_index"), "source_index", minimum=0)
        payload_json = raw.get("payload_json")
        if not isinstance(payload_json, str):
            raise ProviderError("Eastmoney trading billboard Raw payload is missing")
        try:
            payload = json.loads(payload_json, parse_float=Decimal)
        except (json.JSONDecodeError, InvalidOperation) as error:
            raise ProviderError("Eastmoney trading billboard Raw payload is invalid") from error
        if not isinstance(payload, Mapping):
            raise ProviderError("Eastmoney trading billboard Raw payload is not an object")
        grouped[kind].append(payload)
    return _normalize(grouped["summary"], grouped["buy_seat"], grouped["sell_seat"])


def _normalize(
    summaries: Sequence[SourceRow],
    buy_rows: Sequence[SourceRow],
    sell_rows: Sequence[SourceRow],
) -> tuple[TradingBillboardRecord, ...]:
    accepted: list[tuple[SourceRow, str]] = []
    source_ids: set[str] = set()
    semantic_keys: set[tuple[str, date, str]] = set()
    common_date: date | None = None
    for row in summaries:
        row_date = _source_date(row)
        if common_date is None:
            common_date = row_date
        elif row_date != common_date:
            raise ProviderError("Eastmoney trading billboard response date mismatch")
        event_id = _required_text(row, "TRADE_ID")
        if event_id in source_ids:
            raise ProviderError("Eastmoney trading billboard has conflicting summary keys")
        source_ids.add(event_id)
        symbol = _stock_symbol(row)
        if symbol is None:
            continue
        reason_code = _required_text(row, "CHANGE_TYPE")
        semantic_key = (symbol, row_date, reason_code)
        if semantic_key in semantic_keys:
            raise ProviderError("Eastmoney trading billboard has conflicting summary keys")
        semantic_keys.add(semantic_key)
        accepted.append((row, symbol))
    if not accepted or common_date is None:
        raise ProviderError("Eastmoney trading billboard contains no accepted stock summaries")

    details = {
        TradingBillboardSide.BUY: _group_details(buy_rows),
        TradingBillboardSide.SELL: _group_details(sell_rows),
    }
    records: list[TradingBillboardRecord] = []
    for summary, symbol in accepted:
        event_id = _required_text(summary, "TRADE_ID")
        trade_date = _source_date(summary)
        seat_groups: dict[TradingBillboardSide, tuple[TradingBillboardSeatRecord, ...]] = {}
        for side in (TradingBillboardSide.BUY, TradingBillboardSide.SELL):
            source_seats = details[side].get(event_id, ())
            if not 1 <= len(source_seats) <= 5:
                raise ProviderError(
                    "Eastmoney trading billboard requires one to five seats per side"
                )
            ordered = sorted(source_seats, key=lambda row: _seat_sort_key(row, side))
            seat_groups[side] = tuple(
                _normalize_seat(row, event_id, symbol, trade_date, side, rank)
                for rank, row in enumerate(ordered, start=1)
            )
        records.append(
            TradingBillboardRecord(
                symbol=symbol,
                trade_date=trade_date,
                source_event_id=event_id,
                reason_code=_required_text(summary, "CHANGE_TYPE"),
                reason_text=_required_text(summary, "EXPLANATION"),
                close_price=_decimal(summary, "CLOSE_PRICE"),
                change_rate_pct=_decimal(summary, "CHANGE_RATE"),
                turnover_rate_pct=_decimal(summary, "TURNOVERRATE"),
                market_amount=_decimal(summary, "ACCUM_AMOUNT"),
                buy_amount=_required_decimal(summary, "BILLBOARD_BUY_AMT"),
                sell_amount=_required_decimal(summary, "BILLBOARD_SELL_AMT"),
                net_amount=_required_decimal(summary, "BILLBOARD_NET_AMT"),
                deal_amount=_required_decimal(summary, "BILLBOARD_DEAL_AMT"),
                deal_to_market_pct=_decimal(summary, "DEAL_AMOUNT_RATIO"),
                net_to_market_pct=_decimal(summary, "DEAL_NET_RATIO"),
                free_float_market_value=_decimal(summary, "FREE_MARKET_CAP"),
                buy_seats=seat_groups[TradingBillboardSide.BUY],
                sell_seats=seat_groups[TradingBillboardSide.SELL],
            )
        )
    return tuple(records)


def _group_details(rows: Sequence[SourceRow]) -> dict[str, tuple[SourceRow, ...]]:
    grouped: dict[str, list[SourceRow]] = {}
    for row in rows:
        grouped.setdefault(_required_text(row, "TRADE_ID"), []).append(row)
    return {key: tuple(values) for key, values in grouped.items()}


def _normalize_seat(
    row: SourceRow,
    event_id: str,
    symbol: str,
    trade_date: date,
    side: TradingBillboardSide,
    rank: int,
) -> TradingBillboardSeatRecord:
    if _source_date(row) != trade_date or _stock_symbol(row) != symbol:
        raise ProviderError("Eastmoney trading billboard seat parent identity mismatch")
    code = _optional_text(row.get("OPERATEDEPT_CODE"))
    if code == "0":
        code = None
    return TradingBillboardSeatRecord(
        source_event_id=event_id,
        symbol=symbol,
        trade_date=trade_date,
        side=side,
        rank=rank,
        seat_code=code,
        seat_name=_required_text(row, "OPERATEDEPT_NAME"),
        buy_amount=_decimal(row, "BUY"),
        sell_amount=_decimal(row, "SELL"),
        net_amount=_decimal(row, "NET"),
        buy_to_market_pct=_decimal(row, "TOTAL_BUYRIO"),
        sell_to_market_pct=_decimal(row, "TOTAL_SELLRIO"),
    )


def _seat_sort_key(row: SourceRow, side: TradingBillboardSide) -> tuple[object, ...]:
    amount = _decimal(row, "BUY" if side is TradingBillboardSide.BUY else "SELL")
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
        raise ProviderError("Eastmoney trading billboard has an invalid security identifier")
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
        raise ProviderError("Eastmoney trading billboard required TRADE_DATE is missing")
    try:
        return date.fromisoformat(value[:10])
    except ValueError as error:
        raise ProviderError("Eastmoney trading billboard response date is invalid") from error


def _required_text(row: SourceRow, field: str) -> str:
    value = _optional_text(row.get(field))
    if value is None:
        raise ProviderError(f"Eastmoney trading billboard required {field} is missing")
    return value


def _optional_text(value: object) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (str, int, Decimal)):
        text = str(value).strip()
        return text or None
    return None


def _required_decimal(row: SourceRow, field: str) -> Decimal:
    value = _decimal(row, field)
    if value is None:
        raise ProviderError(f"Eastmoney trading billboard required {field} is missing")
    return value


def _decimal(row: SourceRow, field: str) -> Decimal | None:
    value = row.get(field)
    if value is None or value == "":
        return None
    if isinstance(value, (bool, float)):
        raise ProviderError(f"Eastmoney trading billboard {field} decimal is invalid")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ProviderError(f"Eastmoney trading billboard {field} decimal is invalid") from error
    if not parsed.is_finite():
        raise ProviderError(f"Eastmoney trading billboard {field} decimal is invalid")
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
        raise ProviderError(f"Eastmoney trading billboard {field} is invalid")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as error:
        raise ProviderError(f"Eastmoney trading billboard {field} is invalid") from error
    if parsed < minimum or str(parsed) != str(value):
        raise ProviderError(f"Eastmoney trading billboard {field} is invalid")
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
        raise ProviderError("Eastmoney response contains a binary floating-point value")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ProviderError("Eastmoney response object contains a non-string key")
        return (
            "{"
            + ",".join(
                f"{_canonical_json(key)}:{_canonical_json(value[key])}" for key in sorted(value)
            )
            + "}"
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return "[" + ",".join(_canonical_json(item) for item in value) + "]"
    raise ProviderError("Eastmoney response contains an unsupported JSON value")


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
                raise ProviderError("Eastmoney returned a non-success response")
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except (HTTPError, URLError) as error:
        raise ProviderError("Eastmoney trading billboard request failed") from error
    if len(body) > MAX_RESPONSE_BYTES:
        raise ProviderError("Eastmoney trading billboard response exceeds the byte bound")
    try:
        value = json.loads(body, parse_float=Decimal)
    except (json.JSONDecodeError, UnicodeDecodeError, InvalidOperation) as error:
        raise ProviderError("Eastmoney trading billboard response is invalid JSON") from error
    if not isinstance(value, Mapping):
        raise ProviderError("Eastmoney trading billboard response is not an object")
    return value
