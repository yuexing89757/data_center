"""One bounded Kaipanla money-effect page; no persistence or detail reads."""

import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import pairwise
from typing import cast
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from market_data_center.domain.kaipanla_money_effect import MoneyEffectItem, MoneyEffectPage

MAX_RESPONSE_BYTES = 2_000_000
HISTORY_URL = "https://apphis.kaipanla.com/w1/api/index.php"


class KaipanlaMoneyEffectUpstream(RuntimeError):
    """Upstream did not return a valid successful money-effect page."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _request_bytes(request: Request, timeout: float) -> bytes:
    with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
            raise KaipanlaMoneyEffectUpstream("invalid source response")
        return cast(bytes, body)


class KaipanlaMoneyEffectProvider:
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

    def fetch(self, *, offset: int = 0, limit: int = 20) -> MoneyEffectPage:
        if not 0 <= offset <= 10000 or not 1 <= limit <= 100:
            raise ValueError("invalid page bounds")
        params = {
            "c": "Emotion",
            "a": "GetMoneyDate",
            "index": str(offset),
            "st": str(limit),
        }
        request = Request(
            HISTORY_URL,
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
            rows = data.get("ZY")
            if not isinstance(rows, list) or len(rows) > limit:
                raise ValueError("missing source list or ignored page limit")
            items = tuple(_normalize_row(row) for row in rows)
            dates = [item.trade_date for item in items]
            if len(set(dates)) != len(dates) or any(
                current <= following for current, following in pairwise(dates)
            ):
                raise ValueError("source dates are duplicate or not descending")
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            ArithmeticError,
            RecursionError,
        ) as exc:
            raise KaipanlaMoneyEffectUpstream("Kaipanla money-effect request failed") from exc
        next_offset = (
            offset + len(items) if len(items) == limit and offset + limit <= 10000 else None
        )
        return MoneyEffectPage(
            observed_at=self._clock(),
            offset=offset,
            limit=limit,
            returned_count=len(items),
            next_offset=next_offset,
            items=items,
        )


def _invalid_constant(value: str) -> None:
    raise ValueError("non-finite source number")


def _decimal(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("invalid source number")
    number = Decimal(value)
    if not number.is_finite():
        raise ValueError("invalid source number")
    return number


def _normalize_row(row: object) -> MoneyEffectItem:
    if not isinstance(row, dict):
        raise ValueError("invalid money-effect row")
    day = row["Day"]
    if not isinstance(day, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        raise ValueError("invalid source date")
    success_rate = _decimal(row["CGL"])
    profit_rate = _decimal(row["YLL"])
    if not Decimal("0") <= success_rate <= Decimal("100"):
        raise ValueError("invalid board success rate")
    return MoneyEffectItem(
        trade_date=date.fromisoformat(day),
        board_success_rate_pct=success_rate,
        board_profit_rate_pct=profit_rate,
    )
