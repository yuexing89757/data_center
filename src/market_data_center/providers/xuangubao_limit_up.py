"""One bounded, anonymous observation of Xuangubao's latest limit-up pool."""

import json
import re
import socket
import ssl
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from http.client import HTTPException, HTTPSConnection
from threading import Timer
from typing import Any
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request
from zoneinfo import ZoneInfo

from market_data_center.providers.kaipanla_boards import _connect_http, _remaining

SOURCE_URL = "https://flash-api.xuangubao.cn/api/pool/detail?pool_name=limit_up"
MAX_RESPONSE_BYTES = 2_000_000
MAX_ITEMS = 5000
SHANGHAI = ZoneInfo("Asia/Shanghai")
EVENTS = (
    "first_limit_up",
    "last_limit_up",
    "first_break_limit_up",
    "last_break_limit_up",
    "first_limit_down",
    "last_limit_down",
    "first_break_limit_down",
    "last_break_limit_down",
    "yesterday_first_limit_up",
    "yesterday_last_limit_up",
)
COUNTS = {
    "limit_up_days": "limit_up_days",
    "limit_down_days": "limit_down_days",
    "yesterday_limit_up_days": "yesterday_limit_up_days",
    "yesterday_limit_down_days": "yesterday_limit_down_days",
    "break_limit_up_count": "break_limit_up_times",
    "break_limit_down_count": "break_limit_down_times",
    "yesterday_break_limit_up_count": "yesterday_break_limit_up_times",
    "multi_board_days": "m_days_n_boards_days",
    "multi_board_count": "m_days_n_boards_boards",
}
# Named source observations, explicitly not certified standard quantities or indicators.
UNVERIFIED = {
    "buy_lock_ratio": "buy_lock_volume_ratio",
    "sell_lock_ratio": "sell_lock_volume_ratio",
    "stock_type_code": "stock_type",
    "new_stock_flag": "is_new_stock",
    "momentum_value": "mtm",
    "volume_bias_ratio": "volume_bias_ratio",
    "total_capital_value": "total_capital",
    "non_restricted_capital_value": "non_restricted_capital",
    "nearly_new_accumulated_change": "nearly_new_acc_pcp",
    "nearly_new_break_days": "nearly_new_break_days",
    "new_stock_accumulated_change": "new_stock_acc_pcp",
    "new_stock_break_flag": "new_stock_break_limit_up",
    "new_stock_limit_up_days": "new_stock_limit_up_days",
    "new_stock_pre_break_price": "new_stock_limit_up_price_before_broken",
}


class XuangubaoLimitUpUpstream(RuntimeError):
    """Unavailable, malformed or inconsistent source observation."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate source key")
        result[key] = value
    return result


def _request_bytes(request: Request, timeout: float) -> bytes:
    if request.full_url != SOURCE_URL or request.method != "GET":
        raise ValueError("unexpected source endpoint")
    return _request_pool_bytes(request, timeout)


def _request_pool_bytes(request: Request, timeout: float) -> bytes:
    """Shared fixed-host transport for the three pools, with one total deadline."""
    url = urlsplit(request.full_url)
    params = parse_qs(url.query, keep_blank_values=True, strict_parsing=True)
    if (
        request.get_method() != "GET"
        or url.scheme != "https"
        or url.netloc != "flash-api.xuangubao.cn"
        or url.path != "/api/pool/detail"
        or url.fragment
        or set(params) - {"pool_name", "date"}
        or params.get("pool_name")
        not in (["limit_up"], ["limit_up_broken"], ["yesterday_limit_up"])
    ):
        raise ValueError("unexpected source endpoint")
    if "date" in params:
        values = params["date"]
        if len(values) != 1 or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", values[0]):
            raise ValueError("invalid source date")
        date.fromisoformat(values[0])
    deadline = time.monotonic() + timeout
    host = "flash-api.xuangubao.cn"
    context = ssl.create_default_context()
    # Reuse the existing bounded DNS/single-address connector; no proxies or discovery.
    with _connect_http(host, deadline) as raw:
        connection = context.wrap_socket(raw, server_hostname=host, do_handshake_on_connect=False)
        try:
            connection.settimeout(_remaining(deadline))
            connection.do_handshake()
            http = HTTPSConnection(host, timeout=_remaining(deadline), context=context)
            http.sock = connection

            def abort() -> None:
                with suppress(OSError):
                    connection.shutdown(socket.SHUT_RDWR)

            timer = Timer(_remaining(deadline), abort)
            timer.daemon = True
            timer.start()
            try:
                http.request(
                    "GET",
                    url.path + "?" + url.query,
                    headers=dict(request.header_items()),
                )
                with http.getresponse() as response:
                    body = response.read(MAX_RESPONSE_BYTES + 1)
                    _remaining(deadline)
                    if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
                        raise ValueError("invalid HTTP response")
                    return body
            finally:
                timer.cancel()
                timer.join()
                http.close()
        finally:
            connection.close()


def _number(value: object, *, nonnegative: bool = False) -> Decimal | None:
    if value is None:
        return None
    if type(value) not in (int, str, Decimal):
        raise ValueError("invalid source number")
    result = Decimal(str(value))
    if not result.is_finite() or (nonnegative and result < 0):
        raise ValueError("invalid source number")
    return result


def _integer(value: object) -> int | None:
    result = _number(value, nonnegative=True)
    if result is None:
        return None
    if result != result.to_integral_value() or result > 2**63 - 1:
        raise ValueError("invalid source count")
    return int(result)


def _text(value: object, *, required: bool = False) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or len(value) > 20_000 or (required and not value.strip()):
        raise ValueError("invalid source text")
    return value


def _symbol(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{6}\.(SS|SZ|BJ)", value):
        raise ValueError("invalid source symbol")
    code, suffix = value.split(".")
    if not (
        (suffix == "SS" and code.startswith("6"))
        or (suffix == "SZ" and code.startswith(("0", "3")))
        or (suffix == "BJ" and code.startswith(("4", "8", "9")))
    ):
        raise ValueError("symbol exchange mismatch")
    return f"{ {'SS': 'SSE', 'SZ': 'SZSE', 'BJ': 'BSE'}[suffix] }:{code}"


def _timestamp(value: object, observed: datetime) -> datetime | None:
    seconds = _integer(value)
    if not seconds:
        return None
    stamp = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)
    if stamp > observed:
        raise ValueError("future source timestamp")
    return stamp.astimezone(SHANGHAI)


def _reason(value: object, source_symbol: object) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or value.get("symbol") not in (None, source_symbol):
        raise ValueError("invalid source reason")
    plates = value.get("related_plates")
    normalized = None
    if plates is not None:
        if not isinstance(plates, list) or len(plates) > 100:
            raise ValueError("invalid source plates")
        normalized = []
        for plate in plates:
            if not isinstance(plate, dict):
                raise ValueError("invalid source plate")
            identity = plate.get("plate_id")
            if identity is not None and (type(identity) not in (str, int) or not str(identity)):
                raise ValueError("invalid source plate identity")
            normalized.append(
                {
                    "source_id": None if identity is None else str(identity),
                    "name": _text(plate.get("plate_name")),
                    "reason": _text(plate.get("plate_reason")),
                }
            )
    return {
        "symbol": _symbol(value["symbol"]) if value.get("symbol") else None,
        "text": _text(value.get("stock_reason")),
        "related_plates": normalized,
    }


def _item(row: object, observed: datetime) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ValueError("invalid source item")
    result: dict[str, Any] = {
        "symbol": _symbol(row.get("symbol")),
        "source_symbol": row["symbol"],
        "name": _text(row.get("stock_chi_name"), required=True),
        "last_price": _number(row.get("price"), nonnegative=True),
        "issue_price": _number(row.get("issue_price"), nonnegative=True),
        "change_ratio": _number(row.get("change_percent")),
        "turnover_ratio": _number(row.get("turnover_ratio"), nonnegative=True),
        "source_reason": _reason(row.get("surge_reason"), row["symbol"]),
        "source_counts": {key: _integer(row.get(raw)) for key, raw in COUNTS.items()},
        "source_statistics": {
            key: _number(
                int(row[raw])
                if type(row.get(raw)) is bool and key in ("new_stock_flag", "new_stock_break_flag")
                else row.get(raw)
            )
            for key, raw in UNVERIFIED.items()
        },
    }
    listed = _timestamp(row.get("listed_date"), observed)
    result["listing_date"] = listed.date() if listed else None
    for key in EVENTS:
        result[key + "_at"] = _timestamp(row.get(key), observed)
    for suffix in ("limit_up", "break_limit_up", "limit_down", "break_limit_down"):
        first, last = result[f"first_{suffix}_at"], result[f"last_{suffix}_at"]
        if first and last and first > last:
            raise ValueError("reversed event times")
    timeline = row.get("limit_timeline")
    result["timeline"] = None
    if timeline is not None:
        if not isinstance(timeline, dict) or not isinstance(timeline.get("items"), list):
            raise ValueError("invalid source timeline")
        if len(timeline["items"]) > 2000:
            raise ValueError("timeline exceeds bound")
        result["timeline"] = []
        for event in timeline["items"]:
            if not isinstance(event, dict):
                raise ValueError("invalid timeline item")
            result["timeline"].append(
                {
                    "at": _timestamp(event.get("timestamp"), observed),
                    "source_status": _integer(event.get("status")),
                }
            )
    return result


class XuangubaoLimitUpProvider:
    def __init__(
        self,
        *,
        timeout_seconds: float = 8,
        request_bytes: Callable[[Request, float], bytes] = _request_bytes,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 0 < timeout_seconds <= 10:
            raise ValueError("timeout must be between 0 and 10 seconds")
        self._timeout, self._request, self._clock = timeout_seconds, request_bytes, clock

    def fetch(self) -> dict[str, Any]:
        request = Request(
            SOURCE_URL,
            method="GET",
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://finance.sina.com.cn/",
                "Accept": "application/json",
            },
        )
        try:
            body = self._request(request, self._timeout)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError("source exceeds size bound")
            data = json.loads(
                body.decode("utf-8-sig"),
                parse_float=Decimal,
                object_pairs_hook=_unique_object,
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite")),
            )
            if (
                not isinstance(data, dict)
                or type(data.get("code")) is not int
                or data["code"] != 20000
            ):
                raise ValueError("source failed")
            rows = data.get("data")
            if not isinstance(rows, list) or len(rows) > MAX_ITEMS:
                raise ValueError("invalid source list")
            observed = self._clock()
            if observed.tzinfo is None or observed.utcoffset() is None:
                raise ValueError("naive observation timestamp")
            items = [_item(row, observed) for row in rows]
            if len({item["symbol"] for item in items}) != len(items):
                raise ValueError("duplicate source securities")
            dates = set()
            missing_date = False
            for item in items:
                row_dates = {
                    item[key].date()
                    for key in ("first_limit_up_at", "last_limit_up_at")
                    if item[key] is not None
                }
                dates.update(row_dates)
                missing_date |= not row_dates
            if len(dates) > 1:
                raise ValueError("mixed source dates")
            source_date = next(iter(dates)) if dates and not missing_date else None
        except (
            OSError,
            HTTPException,
            ValueError,
            TypeError,
            KeyError,
            ArithmeticError,
            RecursionError,
        ) as exc:
            raise XuangubaoLimitUpUpstream("Xuangubao limit-up source failed") from exc
        return {
            "source_code": "xuangubao",
            "source_name": "选股宝",
            "source_url": SOURCE_URL,
            "source_layout": "xuangubao.limit_up.v1",
            "persisted": False,
            "observed_at": observed,
            "source_date": source_date,
            "date_status": "verified_from_events" if source_date else "unknown",
            "returned_count": len(items),
            "items": items,
        }
