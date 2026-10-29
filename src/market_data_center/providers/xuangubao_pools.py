"""Bounded Xuangubao three-pool reads; no persistence or date fallback."""

import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from http.client import HTTPException
from urllib.parse import urlencode
from urllib.request import Request

from market_data_center.domain.auction_indicative import SHANGHAI
from market_data_center.domain.xuangubao_pools import PoolType, XuangubaoPoolItem, XuangubaoPoolPage
from market_data_center.providers.xuangubao_limit_up import (
    MAX_RESPONSE_BYTES,
    _reason,
    _request_pool_bytes,
    _symbol,
    _text,
    _timestamp,
    _unique_object,
)
from market_data_center.providers.xuangubao_limit_up import (
    _number as _decimal,
)

SOURCE_URL = "https://flash-api.xuangubao.cn/api/pool/detail"
MAX_ITEMS = 2000
SOURCE_POOLS = {
    "limit_up": "limit_up",
    "broken_limit_up": "limit_up_broken",
    "previous_limit_up": "yesterday_limit_up",
}


class XuangubaoPoolsInvalid(ValueError):
    """Unsupported pool or future date."""


class XuangubaoPoolsUpstream(RuntimeError):
    """Unavailable or invalid source response."""


class XuangubaoPoolsProvider:
    def __init__(
        self,
        *,
        timeout_seconds: float = 8,
        request_bytes: Callable[[Request, float], bytes] = _request_pool_bytes,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 0 < timeout_seconds <= 10:
            raise ValueError("timeout must be between 0 and 10 seconds")
        self._timeout = timeout_seconds
        self._request = request_bytes
        self._clock = clock

    def fetch(self, *, pool_type: PoolType, trade_date: date | None = None) -> XuangubaoPoolPage:
        observed = self._clock()
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise XuangubaoPoolsInvalid("naive observation clock")
        today = observed.astimezone(SHANGHAI).date()
        if pool_type not in SOURCE_POOLS or (trade_date is not None and trade_date > today):
            raise XuangubaoPoolsInvalid("invalid pool or date")
        params = {"pool_name": SOURCE_POOLS[pool_type]}
        if trade_date is not None and trade_date < today:
            params["date"] = trade_date.isoformat()
        request = Request(
            SOURCE_URL + "?" + urlencode(params),
            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
        )
        try:
            body = self._request(request, self._timeout)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError("oversized source response")
            observed = self._clock()
            payload = json.loads(
                body.decode("utf-8-sig"),
                parse_float=Decimal,
                object_pairs_hook=_unique_object,
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite")),
            )
            if (
                not isinstance(payload, dict)
                or type(payload.get("code")) is not int
                or payload["code"] != 20000
            ):
                raise ValueError("unsuccessful source response")
            rows = payload.get("data")
            if not isinstance(rows, list) or len(rows) > MAX_ITEMS:
                raise ValueError("missing or oversized source list")
            items = tuple(_normalize(row, observed) for row in rows)
            if len({item.symbol for item in items}) != len(items):
                raise ValueError("duplicate source symbol")
            # Yesterday's events cannot establish the date of today's quote.
            event_dates = {
                stamp.astimezone(SHANGHAI).date()
                for item in items
                for stamp in (
                    item.first_limit_up_at,
                    item.last_limit_up_at,
                    item.last_broken_limit_up_at,
                )
                if stamp is not None
            }
            if len(event_dates) > 1:
                raise ValueError("conflicting source event dates")
            source_day = next(iter(event_dates), None)
            if source_day is not None and (
                source_day > today or (trade_date is not None and source_day != trade_date)
            ):
                raise ValueError("source event date mismatch")
        except (
            OSError,
            HTTPException,
            ValueError,
            TypeError,
            KeyError,
            ArithmeticError,
            RecursionError,
        ) as exc:
            raise XuangubaoPoolsUpstream("Xuangubao stock-pool request failed") from exc
        return XuangubaoPoolPage(
            pool_type=pool_type,
            requested_date=trade_date,
            source_event_date=source_day,
            date_verification="event_dates_consistent" if source_day else "unverified",
            observed_at=observed,
            returned_count=len(items),
            items=items,
        )


def _boards(value: object) -> int | None:
    number = _decimal(value)
    if number is None or number == -1:
        return None
    if number < 0 or number != number.to_integral_value() or number > 10000:
        raise ValueError("invalid consecutive board count")
    return int(number)


def _normalize(row: object, observed: datetime) -> XuangubaoPoolItem:
    if not isinstance(row, dict):
        raise ValueError("invalid source row")
    symbol = _symbol(row.get("symbol"))
    change = _decimal(row.get("change_percent"))
    reason = _reason(row.get("surge_reason"), row.get("symbol"))
    plates = reason.get("related_plates") if reason else None
    topics = tuple(dict.fromkeys(plate["name"] for plate in plates or [] if plate["name"]))
    item = XuangubaoPoolItem(
        symbol=symbol,
        code=symbol.split(":")[1],
        name=_text(row.get("stock_chi_name")),
        change_pct=change * Decimal(100) if change is not None else None,
        consecutive_limit_up_days=_boards(row.get("limit_up_days")),
        previous_consecutive_limit_up_days=_boards(row.get("yesterday_limit_up_days")),
        first_limit_up_at=_timestamp(row.get("first_limit_up"), observed),
        last_limit_up_at=_timestamp(row.get("last_limit_up"), observed),
        last_broken_limit_up_at=_timestamp(row.get("last_break_limit_up"), observed),
        previous_last_limit_up_at=_timestamp(row.get("yesterday_last_limit_up"), observed),
        topics=topics,
    )
    if (
        item.first_limit_up_at is not None
        and item.last_limit_up_at is not None
        and item.first_limit_up_at > item.last_limit_up_at
    ):
        raise ValueError("reversed limit-up event times")
    return item
