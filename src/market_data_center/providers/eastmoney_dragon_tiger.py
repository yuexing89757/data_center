"""Bounded EastMoney transport for immutable DragonTiger Raw."""

import json
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from decimal import Decimal
from time import sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from market_data_center.providers.contracts import (
    DragonTigerProviderBatch,
    ProviderError,
    RawRow,
)
from market_data_center.providers.eastmoney_dragon_tiger_normalizer import (
    SCHEMA_VERSION,
    normalize_eastmoney_dragon_tiger_raw,
)

SUMMARY_REPORT = "RPT_DAILYBILLBOARD_DETAILS"
BUY_REPORT = "RPT_BILLBOARD_DAILYDETAILSBUY"
SELL_REPORT = "RPT_BILLBOARD_DAILYDETAILSSELL"
BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
PAGE_SIZE = 500
MAX_PAGES = 100
MAX_EVENTS = 500
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

    def fetch_dragon_tiger(self, trade_date: date) -> DragonTigerProviderBatch:
        summary_rows, summary_count = self._fetch_paginated(SUMMARY_REPORT, trade_date)
        event_ids = tuple(_event_id(row.payload) for row in summary_rows)
        if len(event_ids) > MAX_EVENTS:
            raise ProviderError("EastMoney DragonTiger event bound exceeded")
        raw_rows = [_raw_row("summary", row.page, row.index, row.payload) for row in summary_rows]
        counts: dict[str, int] = {SUMMARY_REPORT: summary_count}
        for report, kind in ((BUY_REPORT, "buy_seat"), (SELL_REPORT, "sell_seat")):
            rows, declared_count = self._fetch_details(report, trade_date, event_ids)
            counts[report] = declared_count
            raw_rows.extend(_raw_row(kind, row.page, row.index, row.payload) for row in rows)
        frozen_rows = tuple(raw_rows)
        return DragonTigerProviderBatch(
            raw_rows=frozen_rows,
            request_params={
                "trade_date": trade_date.isoformat(),
                "reports": [SUMMARY_REPORT, BUY_REPORT, SELL_REPORT],
                "source_counts": counts,
            },
            schema_version=SCHEMA_VERSION,
            normalization_factory=lambda: normalize_eastmoney_dragon_tiger_raw(
                frozen_rows, SCHEMA_VERSION
            ),
        )

    def _fetch_details(
        self, report: str, trade_date: date, event_ids: Sequence[str]
    ) -> tuple[tuple["PagedRow", ...], int]:
        first_payload = self._request_page(report, trade_date, 1)
        first_rows, count, pages = _page_rows(first_payload, 1)
        if pages <= 1:
            if len(first_rows) != count:
                raise ProviderError("DT_SOURCE_COUNT_MISMATCH")
            return first_rows, count
        probe_rows = list(first_rows)
        event_rows: list[PagedRow] = []
        retrieved_count = 0
        for event_id in event_ids:
            payload = self._request_page(report, trade_date, 1, event_id=event_id)
            rows, event_count, event_pages = _page_rows(payload, 1)
            if event_pages > 1 or event_count != len(rows) or event_count > 5:
                raise ProviderError("DT_SOURCE_COUNT_MISMATCH")
            event_rows.extend(rows)
            retrieved_count += event_count
        if retrieved_count != count:
            raise ProviderError("DT_SOURCE_COUNT_MISMATCH")
        return tuple((*probe_rows, *event_rows)), count

    def _fetch_paginated(self, report: str, trade_date: date) -> tuple[tuple["PagedRow", ...], int]:
        rows: list[PagedRow] = []
        expected_count: int | None = None
        expected_pages: int | None = None
        page = 1
        while True:
            payload = self._request_page(report, trade_date, page)
            page_rows, count, pages = _page_rows(payload, page)
            if expected_count is None:
                expected_count, expected_pages = count, pages
            elif count != expected_count or pages != expected_pages:
                raise ProviderError("DT_SOURCE_COUNT_MISMATCH")
            rows.extend(page_rows)
            if pages == 0 or page >= pages:
                break
            page += 1
        if expected_count is None or len(rows) != expected_count:
            raise ProviderError("DT_SOURCE_COUNT_MISMATCH")
        return tuple(rows), expected_count

    def _request_page(
        self,
        report: str,
        trade_date: date,
        page_number: int,
        *,
        event_id: str | None = None,
    ) -> Mapping[str, Any]:
        source_filter = f"(TRADE_DATE='{trade_date.isoformat()}')"
        if event_id is not None:
            if re.fullmatch(r"[A-Za-z0-9_-]+", event_id) is None:
                raise ProviderError("EastMoney DragonTiger event identity is invalid")
            source_filter += f"(TRADE_ID='{event_id}')"
        query = urlencode(
            {
                "reportName": report,
                "columns": "ALL",
                "filter": source_filter,
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


def _page_rows(payload: Mapping[str, Any], page: int) -> tuple[tuple[PagedRow, ...], int, int]:
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
    rows: list[PagedRow] = []
    for index, item in enumerate(data):
        if not isinstance(item, Mapping):
            raise ProviderError("EastMoney DragonTiger row is not an object")
        rows.append(PagedRow(page, index, item))
    return tuple(rows), count, pages


def _event_id(row: SourceRow) -> str:
    value = row.get("TRADE_ID")
    if not isinstance(value, (str, int)) or not str(value).strip():
        raise ProviderError("EastMoney DragonTiger event identity is invalid")
    return str(value).strip()


def _raw_row(kind: str, page: int, index: int, row: SourceRow) -> RawRow:
    return {
        "raw_schema_version": SCHEMA_VERSION,
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
            "Accept": "application/json",
            "User-Agent": "market-data-center/0.2",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read(MAX_RESPONSE_BYTES + 1)
    except (HTTPError, URLError, TimeoutError, OSError) as error:
        raise ProviderError("EastMoney DragonTiger request failed") from error
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ProviderError("EastMoney DragonTiger response exceeds the byte bound")
    try:
        payload = json.loads(raw, parse_float=Decimal)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ProviderError("EastMoney DragonTiger response is invalid JSON") from error
    if not isinstance(payload, Mapping):
        raise ProviderError("EastMoney DragonTiger response is not an object")
    return payload
