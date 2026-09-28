"""Bounded historical Kaipanla stock-list adapter; no current-day or write path."""

import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from market_data_center.domain.kaipanla_market_history import (
    AuctionHistoryItem,
    BrokenLimitUpHistoryItem,
    HistoryListKind,
    LimitDownHistoryItem,
    LimitUpHistoryItem,
    MarketHistoryPage,
)

HISTORY_URL = "https://apphis.kaipanla.com/w1/api/index.php"
SHANGHAI = ZoneInfo("Asia/Shanghai")
MAX_RESPONSE_BYTES = 2_000_000
KIND_PARAMS: dict[HistoryListKind, tuple[str, str]] = {
    "auction": ("8", "18"),
    "limit_up": ("1", "6"),
    "limit_down": ("3", "6"),
    "broken_limit_up": ("2", "4"),
}


class KaipanlaMarketHistoryInvalid(ValueError):
    """Invalid bounded request or unsupported date."""


class KaipanlaMarketHistoryUpstream(RuntimeError):
    """Unusable or date-mismatched source response."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _request_bytes(request: Request, timeout: float) -> bytes:
    with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("invalid source response")
        return cast(bytes, body)


class KaipanlaMarketHistoryProvider:
    def __init__(
        self,
        *,
        timeout_seconds: float = 8,
        request_bytes: Callable[[Request, float], bytes] = _request_bytes,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 0 < timeout_seconds <= 10:
            raise ValueError("timeout must be between 0 and 10 seconds")
        self._timeout = timeout_seconds
        self._request = request_bytes
        self._clock = clock

    def fetch(
        self,
        *,
        kind: HistoryListKind,
        trade_date: date,
        offset: int = 0,
        limit: int = 30,
    ) -> MarketHistoryPage:
        if (
            kind not in KIND_PARAMS
            or not isinstance(trade_date, date)
            or trade_date >= self._clock().astimezone(SHANGHAI).date()
            or not isinstance(offset, int)
            or isinstance(offset, bool)
            or not 0 <= offset <= 10000
            or not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 30
        ):
            raise KaipanlaMarketHistoryInvalid("invalid history request")
        pid_type, sort_type = KIND_PARAMS[kind]
        request = Request(
            HISTORY_URL,
            data=urlencode(
                {
                    "c": "HisHomeDingPan",
                    "a": "HisDaBanList",
                    "PidType": pid_type,
                    "Type": sort_type,
                    "Order": "1",
                    "Day": trade_date.isoformat(),
                    "Index": str(offset),
                    "st": str(limit),
                    "Is_st": "1",
                    "Filter": "0",
                    "FilterMotherboard": "0",
                    "FilterGem": "0",
                    "FilterTIB": "0",
                    "apiv": "w48",
                    "PhoneOSNew": "1",
                    "VerSion": "6.3.20.0",
                }
            ).encode("ascii"),
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            body = self._request(request, self._timeout)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError("source response too large")
            data = json.loads(
                body.decode("utf-8-sig"), parse_float=Decimal, parse_constant=_invalid_constant
            )
            if (
                not isinstance(data, dict)
                or type(data.get("errcode")) not in (str, int)
                or str(data["errcode"]) != "0"
                or not isinstance(data.get("day"), str)
                or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", data["day"])
                or date.fromisoformat(data["day"]) != trade_date
            ):
                raise ValueError("unsuccessful or date-mismatched source response")
            rows = data.get("list")
            if not isinstance(rows, list) or len(rows) > limit:
                raise ValueError("missing source list or ignored page limit")
            items = tuple(_normalize_row(kind, row, trade_date) for row in rows)
            if len({item.code for item in items}) != len(items):
                raise ValueError("duplicate source code")
        except (
            OSError,
            ValueError,
            TypeError,
            ArithmeticError,
            OverflowError,
            RecursionError,
        ) as exc:
            raise KaipanlaMarketHistoryUpstream("Kaipanla history request failed") from exc
        next_offset = (
            offset + len(items) if len(items) == limit and offset + limit <= 10000 else None
        )
        return MarketHistoryPage(
            list_type=kind,
            requested_date=trade_date,
            trade_date=trade_date,
            observed_at=self._clock(),
            offset=offset,
            limit=limit,
            returned_count=len(items),
            next_offset=next_offset,
            items=items,
        )


def _invalid_constant(_: str) -> None:
    raise ValueError("non-finite source number")


def _text(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("invalid source text")
    return value


def _number(value: object, *, nonnegative: bool = False) -> Decimal | None:
    if value in (None, "", "--"):
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("invalid source number")
    result = Decimal(value)
    if not result.is_finite() or (nonnegative and result < 0):
        raise ValueError("invalid source number")
    return result


def _event(value: object, trade_date: date) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("invalid source timestamp")
    result = datetime.fromtimestamp(value, SHANGHAI)
    if result.date() != trade_date:
        raise ValueError("source event outside requested date")
    return result


def _normalize_row(
    kind: HistoryListKind, row: object, trade_date: date
) -> AuctionHistoryItem | LimitUpHistoryItem | LimitDownHistoryItem | BrokenLimitUpHistoryItem:
    if not isinstance(row, list) or not 23 <= len(row) <= 100:
        raise ValueError("invalid history row")
    code = row[0]
    if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
        raise ValueError("invalid source code or placeholder row")
    if code.startswith("6"):
        market = "SSE"
    elif code.startswith(("0", "3")):
        market = "SZSE"
    elif code.startswith(("4", "8", "9")):
        market = "BSE"
    else:
        raise ValueError("unsupported source code")
    symbol = f"{market}:{code}"
    name = _text(row[1])
    if kind == "auction":
        return AuctionHistoryItem(
            symbol=symbol,
            code=code,
            name=name,
            board_name=_text(row[11]),
            limit_up_bid_amount_cny=_number(row[18], nonnegative=True),
            auction_change_pct=_number(row[19]),
        )
    if kind == "limit_up":
        return LimitUpHistoryItem(
            symbol=symbol,
            code=code,
            name=name,
            limit_up_at=_event(row[6], trade_date),
            status=_text(row[9]),
            reason=_text(row[16]),
        )
    if kind == "limit_down":
        return LimitDownHistoryItem(
            symbol=symbol,
            code=code,
            name=name,
            limit_down_at=_event(row[6], trade_date),
            sealed_amount_cny=_number(row[8], nonnegative=True),
            board_name=_text(row[11]),
        )
    return BrokenLimitUpHistoryItem(
        symbol=symbol,
        code=code,
        name=name,
        change_pct=_number(row[4]),
        limit_up_at=_event(row[6], trade_date),
        opened_at=_event(row[7], trade_date),
    )
