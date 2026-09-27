"""One bounded request for the Kaipanla auction pool; no persistence or fallback."""

import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from market_data_center.domain.kaipanla_auction import KaipanlaAuctionItem, KaipanlaAuctionPage

MAX_RESPONSE_BYTES = 2_000_000
HQ = "https://apphwshhq.kaipanla.com/w1/api/index.php"
HIS = "https://apphis.kaipanla.com/w1/api/index.php"
SHANGHAI = ZoneInfo("Asia/Shanghai")


class KaipanlaAuctionUpstream(RuntimeError):
    """Unusable, unsuccessful or date-mismatched upstream auction response."""


class KaipanlaAuctionInvalid(ValueError):
    """Invalid bounded request or a requested future date."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _request_bytes(request: Request, timeout: float) -> bytes:
    with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
            raise KaipanlaAuctionUpstream("invalid source response")
        return cast(bytes, body)


class KaipanlaAuctionProvider:
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
        trade_date: date | None = None,
        offset: int = 0,
        limit: int = 30,
        exclude_st: bool = True,
    ) -> KaipanlaAuctionPage:
        today = self._clock().astimezone(SHANGHAI).date()
        if not 0 <= offset <= 10000 or not 1 <= limit <= 30:
            raise KaipanlaAuctionInvalid("invalid page bounds")
        if trade_date is not None and trade_date > today:
            raise KaipanlaAuctionInvalid("future dates are unsupported")
        params = {
            "c": "HisHomeDingPan" if trade_date else "HomeDingPan",
            "a": "HisDaBanList" if trade_date else "DaBanList",
            "PidType": "8",
            "Type": "18",
            "Order": "1",
            "Index": str(offset),
            "st": str(limit),
            "Is_st": "1" if exclude_st else "0",
            "Filter": "0",
            "FilterMotherboard": "0",
            "FilterGem": "0",
            "FilterTIB": "0",
            "apiv": "w48",
            "PhoneOSNew": "1",
            "VerSion": "6.3.20.0",
        }
        if trade_date is not None:
            params["Day"] = trade_date.isoformat()
        request = Request(
            HIS if trade_date else HQ,
            data=urlencode(params).encode("ascii"),
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
            ):
                raise ValueError("unsuccessful source response")
            source_day = data.get("day")
            if not isinstance(source_day, str) or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}", source_day
            ):
                raise ValueError("missing or invalid source date")
            returned_date = date.fromisoformat(source_day)
            if returned_date > today or (trade_date is not None and returned_date != trade_date):
                raise ValueError("mismatched source date")
            rows = data.get("list")
            if not isinstance(rows, list) or len(rows) > limit:
                raise ValueError("missing source list or ignored page limit")
            items = tuple(_normalize_row(row) for row in rows)
            if len({item.code for item in items}) != len(items):
                raise ValueError("duplicate source code")
        except (OSError, ValueError, TypeError, ArithmeticError, RecursionError) as exc:
            raise KaipanlaAuctionUpstream("Kaipanla auction request failed") from exc
        next_offset = (
            offset + len(items) if len(items) == limit and offset + limit <= 10000 else None
        )
        return KaipanlaAuctionPage(
            query_mode="history" if trade_date else "current",
            requested_date=trade_date,
            trade_date=returned_date,
            observed_at=self._clock(),
            exclude_st=exclude_st,
            offset=offset,
            limit=limit,
            returned_count=len(items),
            next_offset=next_offset,
            items=items,
        )


def _invalid_constant(value: str) -> None:
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


def _normalize_row(row: object) -> KaipanlaAuctionItem:
    if not isinstance(row, list) or not 23 <= len(row) <= 100:
        raise ValueError("invalid auction row")
    code = row[0]
    if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
        # The HQ endpoint can return an all-'kaipanla.com' placeholder row with errcode=0.
        raise ValueError("invalid source code or placeholder row")
    if code.startswith("6"):
        market = "SSE"
    elif code.startswith(("0", "3")):
        market = "SZSE"
    elif code.startswith(("4", "8", "9")):
        market = "BSE"
    else:
        raise ValueError("unsupported source code")
    # Verified against PatternDynamicQuota and the APP, not MorningBiddingDynamicQuota.
    return KaipanlaAuctionItem(
        symbol=f"{market}:{code}",
        code=code,
        name=_text(row[1]),
        board_name=_text(row[11]),
        change_pct=_number(row[4]),
        actual_float_market_value_cny=_number(row[15], nonnegative=True),
        limit_up_bid_amount_cny=_number(row[18], nonnegative=True),
        auction_change_pct=_number(row[19]),
        auction_net_amount_cny=_number(row[20]),
        auction_turnover_rate_pct=_number(row[21], nonnegative=True),
        auction_amount_cny=_number(row[22], nonnegative=True),
    )
