"""Bounded historical market-emotion chart reads, without credentials or persistence."""

import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import pairwise
from typing import cast
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from market_data_center.domain.kaipanla_emotion_chart import EmotionChartItem, EmotionChartPage

MAX_RESPONSE_BYTES = 2_000_000
HISTORY_URL = "https://apphis.kaipanla.com/w1/api/index.php"


class KaipanlaEmotionChartUpstream(RuntimeError):
    """The source did not return a valid market-emotion chart page."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _request_bytes(request: Request, timeout: float) -> bytes:
    with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
            raise KaipanlaEmotionChartUpstream("invalid source response")
        return cast(bytes, body)


class KaipanlaEmotionChartProvider:
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

    def fetch(self, *, offset: int = 0, limit: int = 100) -> EmotionChartPage:
        if not 0 <= offset <= 10000 or not 1 <= limit <= 100:
            raise ValueError("invalid page bounds")
        request = Request(
            HISTORY_URL,
            data=urlencode(
                {"c": "HisHomeDingPan", "a": "ChangeStatistics", "Index": offset, "st": limit}
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
            data = json.loads(body.decode("utf-8-sig"), parse_float=Decimal)
            if (
                not isinstance(data, dict)
                or type(data.get("errcode")) not in (str, int)
                or str(data["errcode"]) != "0"
            ):
                raise ValueError("unsuccessful source response")
            rows = data.get("info")
            if not isinstance(rows, list) or len(rows) > limit:
                raise ValueError("missing or oversized source list")
            items = tuple(_normalize_row(row) for row in rows)
            if any(a.trade_date <= b.trade_date for a, b in pairwise(items)):
                raise ValueError("duplicate or unordered source dates")
        except (OSError, ValueError, TypeError, KeyError, ArithmeticError, RecursionError) as exc:
            raise KaipanlaEmotionChartUpstream("Kaipanla emotion-chart request failed") from exc
        return EmotionChartPage(
            observed_at=self._clock(),
            offset=offset,
            limit=limit,
            returned_count=len(items),
            next_offset=offset + limit if len(items) == limit and offset + limit <= 10000 else None,
            items=items,
        )


def _integer(value: object) -> int:
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        return int(value)
    raise ValueError("invalid source count")


def _normalize_row(row: object) -> EmotionChartItem:
    if not isinstance(row, dict):
        raise ValueError("invalid chart row")
    day = row["Day"]
    if not isinstance(day, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", day):
        raise ValueError("invalid source date")
    emotion = _integer(row["strong"])
    if emotion > 100:
        raise ValueError("invalid emotion index")
    return EmotionChartItem(
        trade_date=date.fromisoformat(day),
        emotion_index=emotion,
        large_drawdown_count=_integer(row["df_num"]),
        limit_up_count=_integer(row["ztjs"]),
        consecutive_limit_up_height=_integer(row["lbgd"]),
    )
