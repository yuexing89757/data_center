"""Bounded anonymous board reads, with named fields and exact source dates."""

import json
import re
import socket
import ssl
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, date, datetime
from decimal import Decimal
from http.client import HTTPException, HTTPSConnection
from threading import BoundedSemaphore, Event, Thread, Timer
from typing import cast
from urllib.parse import urlencode, urlsplit
from urllib.request import Request
from zoneinfo import ZoneInfo

from market_data_center.domain.kaipanla_boards import (
    ForecastStatus,
    KaipanlaBoardItem,
    KaipanlaBoardMember,
    KaipanlaBoardMembersPage,
    KaipanlaBoardRankingPage,
    KaipanlaProfitForecast,
)
from market_data_center.providers.kaipanla_boards_socket import request_members as _request_members

MAX_RESPONSE_BYTES = 2_000_000
MAX_MEMBER_ROWS = 6000
MAX_MEMBER_BYTES = 8 * 1024 * 1024
LEADER_RANKS = {f"龙{label}": rank for rank, label in enumerate("一二三四五六七八九十", 1)}
_DNS_SLOTS = BoundedSemaphore(16)
HQ = "https://apphq.longhuvip.com/w1/api/index.php"
HIS = "https://apphis.longhuvip.com/w1/api/index.php"
SHANGHAI = ZoneInfo("Asia/Shanghai")
UA = "Dalvik/2.1.0 (Linux; U; Android 6.0.1; MuMu Build/V417IR)"


class KaipanlaBoardsUpstream(RuntimeError):
    """Unsuccessful, malformed or date-mismatched board response."""


class KaipanlaBoardsInvalid(ValueError):
    """Invalid bounded request or unsupported date."""


def _remaining(deadline: float) -> float:
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError("HTTP board deadline exceeded")
    return value


def _connect_http(host: str, deadline: float) -> socket.socket:
    _remaining(deadline)
    # System DNS cannot be cancelled; bound abandoned resolvers as well as caller wait.
    if not _DNS_SLOTS.acquire(blocking=False):
        raise OSError("board DNS capacity exceeded")
    ready = Event()
    addresses: list[
        tuple[int, int, int, str, tuple[str, int] | tuple[str, int, int, int] | tuple[int, bytes]]
    ] = []
    errors: list[OSError] = []

    def resolve() -> None:
        try:
            addresses.extend(socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM))
        except OSError as exc:
            errors.append(exc)
        finally:
            _DNS_SLOTS.release()
            ready.set()

    resolver = Thread(target=resolve, daemon=True)
    try:
        resolver.start()
    except RuntimeError:
        _DNS_SLOTS.release()
        raise OSError("could not start board resolver") from None
    if not ready.wait(_remaining(deadline)):
        raise TimeoutError("board DNS deadline exceeded")
    if errors or not addresses:
        raise OSError("board DNS failed")
    family, kind, protocol, _, address = addresses[0]
    raw = socket.socket(family, kind, protocol)
    try:
        raw.settimeout(_remaining(deadline))
        raw.connect(address)
        _remaining(deadline)
        return raw
    except BaseException:
        raw.close()
        raise


def _request_bytes(request: Request, timeout: float) -> bytes:
    deadline = time.monotonic() + timeout

    def remaining() -> float:
        return _remaining(deadline)

    url = urlsplit(request.full_url)
    if url.scheme != "https" or url.hostname not in {urlsplit(HQ).hostname, urlsplit(HIS).hostname}:
        raise ValueError("unexpected board endpoint")
    host = cast(str, url.hostname)
    context = ssl.create_default_context()
    with _connect_http(host, deadline) as raw:
        connection = context.wrap_socket(raw, server_hostname=host, do_handshake_on_connect=False)
        try:
            connection.settimeout(remaining())
            connection.do_handshake()
            http = HTTPSConnection(host, timeout=remaining(), context=context)
            http.sock = connection

            def abort() -> None:
                # Interrupt slow response headers as well as a slow body.
                with suppress(OSError):
                    connection.shutdown(socket.SHUT_RDWR)

            timer = Timer(remaining(), abort)
            timer.daemon = True
            timer.start()
            try:
                connection.settimeout(remaining())
                http.request(
                    "GET", url.path + "?" + url.query, headers=dict(request.header_items())
                )
                with http.getresponse() as response:
                    body = response.read(MAX_RESPONSE_BYTES + 1)
                    remaining()
                    if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
                        raise KaipanlaBoardsUpstream("invalid source response")
                    return body
            finally:
                timer.cancel()
                timer.join()
                http.close()
        finally:
            connection.close()


class KaipanlaBoardsProvider:
    def __init__(
        self,
        *,
        timeout_seconds: float = 8,
        request_bytes: Callable[[Request, float], bytes] = _request_bytes,
        request_members: Callable[[str, int, int, float], dict[str, object]] = _request_members,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 0 < timeout_seconds <= 10:
            raise ValueError("timeout must be between 0 and 10 seconds")
        self._timeout = timeout_seconds
        self._request = request_bytes
        self._request_members = request_members
        self._clock = clock

    def ranking(
        self, *, trade_date: date | None = None, offset: int = 0, limit: int = 60
    ) -> KaipanlaBoardRankingPage:
        _, actual, rows = self._fetch(trade_date, offset, limit)
        try:
            items = tuple(_board(row) for row in rows)
            if len({item.board_code for item in items}) != len(items):
                raise ValueError("duplicate board code")
        except (ValueError, TypeError, ArithmeticError) as exc:
            raise KaipanlaBoardsUpstream("invalid board ranking") from exc
        return KaipanlaBoardRankingPage(
            query_mode="history" if trade_date else "latest",
            requested_date=trade_date,
            trade_date=actual,
            observed_at=self._clock(),
            offset=offset,
            limit=limit,
            returned_count=len(items),
            next_offset=_next(offset, limit, len(items)),
            items=items,
        )

    def members(
        self, board_code: str, *, trade_date: date, offset: int = 0, limit: int = 60
    ) -> KaipanlaBoardMembersPage:
        if not re.fullmatch(r"[0-9]{6}", board_code):
            raise KaipanlaBoardsInvalid("invalid board code")
        today = self._clock().astimezone(SHANGHAI).date()
        if not 0 <= offset <= 10000 or not 1 <= limit <= 60 or trade_date > today:
            raise KaipanlaBoardsInvalid("invalid member request")
        deadline = time.monotonic() + self._timeout
        collected: list[KaipanlaBoardMember] = []
        seen: set[str] = set()
        years: tuple[int, ...] | None = None
        source_total: object = None
        response_bytes = 0
        try:
            for source_offset in range(0, MAX_MEMBER_ROWS + 1, 60):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("member collection deadline exceeded")
                data, actual, rows = self._fetch(
                    trade_date,
                    source_offset,
                    60,
                    board_code,
                    timeout_seconds=remaining,
                    today=today,
                )
                page_bytes = data.get("_response_bytes", 0)
                if not isinstance(page_bytes, int) or page_bytes < 0:
                    raise ValueError("invalid source byte count")
                response_bytes += page_bytes
                if response_bytes > MAX_MEMBER_BYTES or time.monotonic() >= deadline:
                    raise ValueError("member collection resource limit")
                page_years = _forecast_years(data.get("ZB"))
                if years is None:
                    years = page_years
                    source_total = data.get("_source_total")
                if page_years != years or data.get("_source_total") != source_total:
                    raise ValueError("inconsistent member pages")
                for row in rows:
                    item = _member(row, years)
                    if item.code in seen:
                        raise ValueError("duplicate stock code")
                    seen.add(item.code)
                    collected.append(item)
                if len(collected) > MAX_MEMBER_ROWS:
                    raise ValueError("too many members")
                if len(rows) < 60:
                    if source_total is not None and source_total != len(collected):
                        raise ValueError("incomplete member collection")
                    break
            else:
                raise ValueError("unterminated member collection")
            # Stable passes avoid Decimal unary minus rounding at context precision.
            collected.sort(key=lambda item: item.code)
            collected.sort(
                key=lambda item: (item.change_pct is not None, item.change_pct or Decimal(0)),
                reverse=True,
            )
            collected.sort(key=lambda item: LEADER_RANKS.get(item.leader_label or "", 11))
            items = tuple(collected[offset : offset + limit])
            if time.monotonic() >= deadline:
                raise TimeoutError("member collection deadline exceeded")
        except (OSError, ValueError, TypeError, ArithmeticError) as exc:
            raise KaipanlaBoardsUpstream("invalid board members") from exc
        return KaipanlaBoardMembersPage(
            query_mode="history",
            requested_date=trade_date,
            trade_date=actual,
            observed_at=self._clock(),
            board_code=board_code,
            forecast_years=years or (),
            offset=offset,
            limit=limit,
            returned_count=len(items),
            next_offset=offset + len(items) if offset + len(items) < len(collected) else None,
            items=items,
        )

    def _fetch(
        self,
        trade_date: date | None,
        offset: int,
        limit: int,
        board_code: str | None = None,
        *,
        timeout_seconds: float | None = None,
        today: date | None = None,
    ) -> tuple[dict[str, object], date, list[object]]:
        today = today or self._clock().astimezone(SHANGHAI).date()
        timeout = self._timeout if timeout_seconds is None else timeout_seconds
        if not 0 <= offset <= 10000 or not 1 <= limit <= 60:
            raise KaipanlaBoardsInvalid("invalid page bounds")
        if trade_date is not None and trade_date > today:
            raise KaipanlaBoardsInvalid("future dates are unsupported")
        if board_code is not None and trade_date == today:
            try:
                data = self._request_members(board_code, offset, limit, timeout)
                return _validate_page(data, trade_date, today, limit)
            except (OSError, ValueError, TypeError, ArithmeticError, RecursionError) as exc:
                raise KaipanlaBoardsUpstream("Kaipanla board request failed") from exc
        historical = trade_date is not None and trade_date < today
        params = dict(
            c="ZhiShuRanking",
            a="ZhiShuStockList_W8" if board_code else "RealRankingInfo",
            Order="1",
            Type="1",
            st=str(limit),
            Index=str(offset),
            PhoneOSNew="1",
            VerSion="5",
            apiv="w41" if board_code else ("w26" if historical else "w21"),
        )
        if historical and trade_date is not None:
            params["Date"] = trade_date.isoformat()
        if board_code:
            params.update(
                PlateID=board_code, IsZZ="0", IsKZZType="0", filterType="0", TSZB="0", TSZB_Type="0"
            )
        else:
            params["ZSType"] = "7"
        request = Request(
            (HIS if historical else HQ) + "?" + urlencode(params),
            headers={"User-Agent": UA, "Accept": "application/json"},
            method="GET",
        )
        try:
            body = self._request(request, timeout)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError("source response too large")
            data = json.loads(
                body.decode("utf-8-sig"), parse_float=Decimal, parse_constant=_invalid_constant
            )
            validated, actual, rows = _validate_page(data, trade_date, today, limit)
            validated["_response_bytes"] = len(body)
            return validated, actual, rows
        except (
            OSError,
            HTTPException,
            ValueError,
            TypeError,
            ArithmeticError,
            RecursionError,
        ) as exc:
            raise KaipanlaBoardsUpstream("Kaipanla board request failed") from exc


def _validate_page(
    data: object, trade_date: date | None, today: date, limit: int
) -> tuple[dict[str, object], date, list[object]]:
    if (
        not isinstance(data, dict)
        or type(data.get("errcode")) not in (str, int)
        or str(data["errcode"]) != "0"
    ):
        raise ValueError("unsuccessful source response")
    day = data.get("Day")
    if isinstance(day, list) and len(day) == 1:
        day = day[0]
    if not isinstance(day, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", day):
        raise ValueError("missing or invalid source date")
    actual = date.fromisoformat(day)
    if actual > today or (trade_date is not None and actual != trade_date):
        raise ValueError("mismatched source date")
    rows = data.get("list")
    if not isinstance(rows, list) or len(rows) > limit:
        raise ValueError("missing source list or ignored page limit")
    return data, actual, rows


def _next(offset: int, limit: int, count: int) -> int | None:
    return offset + count if count == limit and offset + count <= 10000 else None


def _invalid_constant(value: str) -> None:
    raise ValueError("non-finite source number")


def _number(value: object, *, nonnegative: bool = False) -> Decimal | None:
    if value in (None, "", "--"):
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("invalid source number")
    result = Decimal(value)
    if not result.is_finite() or (nonnegative and result < 0):
        raise ValueError("invalid source number")
    return result


def _text(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("invalid source text")
    return value


def _row(value: object, minimum: int) -> list[object]:
    if not isinstance(value, list) or not minimum <= len(value) <= 100:
        raise ValueError("invalid source row")
    if not isinstance(value[0], str) or not re.fullmatch(r"[0-9]{6}", value[0]):
        raise ValueError("invalid source code")
    return value


def _board(value: object) -> KaipanlaBoardItem:
    row = _row(value, 13)
    return KaipanlaBoardItem(
        board_code=cast(str, row[0]),
        name=_text(row[1]),
        strength=_number(row[2]),
        change_pct=_number(row[3]),
        change_speed_pct=_number(row[4]),
        amount_cny=_number(row[5], nonnegative=True),
        main_net_amount_cny=_number(row[6]),
        volume_ratio=_number(row[9], nonnegative=True),
        large_order_net_amount_cny=_number(row[12]),
    )


def _forecast_years(value: object) -> tuple[int, ...]:
    if value is None:
        return ()
    if (
        not isinstance(value, list)
        or len(value) > 100
        or any(not isinstance(v, str) for v in value)
    ):
        raise ValueError("invalid forecast labels")
    years = tuple(int(m[1]) for v in value if (m := re.fullmatch(r"([0-9]{4})年预测净利润", v)))
    if len(years) != 3:
        return ()
    if len(set(years)) != 3 or years != tuple(sorted(years)) or years[0] < 1900:
        raise ValueError("invalid forecast years")
    return years


def _member(value: object, years: tuple[int, ...]) -> KaipanlaBoardMember:
    row = _row(value, 25)
    code = cast(str, row[0])
    if code.startswith("6"):
        market = "SSE"
    elif code.startswith(("0", "3")):
        market = "SZSE"
    elif code.startswith(("4", "8", "9")):
        market = "BSE"
    else:
        raise ValueError("unsupported stock code")

    def number(index: int, *, nonnegative: bool = False) -> Decimal | None:
        return _number(row[index] if index < len(row) else None, nonnegative=nonnegative)

    count = number(40, nonnegative=True)
    if count is not None and count != count.to_integral_value():
        raise ValueError("non-integer leading count")
    profits = tuple(number(i) for i in (44, 45, 46))
    status: ForecastStatus = "years_unverified"
    if years:
        status = "not_provided" if all(v is None or v == 0 for v in profits) else "available"
    return KaipanlaBoardMember(
        symbol=f"{market}:{code}",
        code=code,
        name=_text(row[1]),
        concept=_text(row[4]),
        last_price=number(5, nonnegative=True),
        change_pct=number(6),
        amount_cny=number(7, nonnegative=True),
        actual_float_market_value_cny=number(10, nonnegative=True),
        main_buy_amount_cny=number(11, nonnegative=True),
        main_sell_amount_cny=number(12),
        main_net_amount_cny=number(13),
        buy_share_pct=number(17, nonnegative=True),
        sell_share_pct=number(18, nonnegative=True),
        volume_ratio=number(21, nonnegative=True),
        height_label=_text(row[23]),
        leader_label=_text(row[24]),
        close_limit_bid_amount_cny=number(28, nonnegative=True),
        max_limit_bid_amount_cny=number(29, nonnegative=True),
        total_market_value_cny=number(37, nonnegative=True),
        float_market_value_cny=number(38, nonnegative=True),
        leading_count=int(count) if count is not None else None,
        large_order_net_amount_cny=number(50),
        forecast_status=status,
        profit_forecasts=tuple(
            KaipanlaProfitForecast(year, profit)
            for year, profit in zip(years, profits, strict=False)
        ),
    )
