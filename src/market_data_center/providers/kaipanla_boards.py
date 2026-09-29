"""Bounded anonymous HTTP board reads, with named fields and exact source dates."""

import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from market_data_center.domain.kaipanla_boards import (
    ForecastStatus,
    KaipanlaBoardItem,
    KaipanlaBoardMember,
    KaipanlaBoardMembersPage,
    KaipanlaBoardRankingPage,
    KaipanlaProfitForecast,
)

MAX_RESPONSE_BYTES = 2_000_000
HQ = "https://apphq.longhuvip.com/w1/api/index.php"
HIS = "https://apphis.longhuvip.com/w1/api/index.php"
SHANGHAI = ZoneInfo("Asia/Shanghai")
UA = "Dalvik/2.1.0 (Linux; U; Android 6.0.1; MuMu Build/V417IR)"


class KaipanlaBoardsUpstream(RuntimeError):
    """Unsuccessful, malformed or date-mismatched board response."""


class KaipanlaBoardsInvalid(ValueError):
    """Invalid bounded request or unsupported date."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _request_bytes(request: Request, timeout: float) -> bytes:
    with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
            raise KaipanlaBoardsUpstream("invalid source response")
        return cast(bytes, body)


class KaipanlaBoardsProvider:
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
        if trade_date >= self._clock().astimezone(SHANGHAI).date():
            raise KaipanlaBoardsInvalid("historical members require a past date")
        data, actual, rows = self._fetch(trade_date, offset, limit, board_code)
        try:
            years = _forecast_years(data.get("ZB"))
            items = tuple(_member(row, years) for row in rows)
            if len({item.code for item in items}) != len(items):
                raise ValueError("duplicate stock code")
        except (ValueError, TypeError, ArithmeticError) as exc:
            raise KaipanlaBoardsUpstream("invalid board members") from exc
        return KaipanlaBoardMembersPage(
            query_mode="history",
            requested_date=trade_date,
            trade_date=actual,
            observed_at=self._clock(),
            board_code=board_code,
            forecast_years=years,
            offset=offset,
            limit=limit,
            returned_count=len(items),
            next_offset=_next(offset, limit, len(items)),
            items=items,
        )

    def _fetch(
        self, trade_date: date | None, offset: int, limit: int, board_code: str | None = None
    ) -> tuple[dict[str, object], date, list[object]]:
        today = self._clock().astimezone(SHANGHAI).date()
        if not 0 <= offset <= 10000 or not 1 <= limit <= 60:
            raise KaipanlaBoardsInvalid("invalid page bounds")
        if trade_date is not None and trade_date > today:
            raise KaipanlaBoardsInvalid("future dates are unsupported")
        params = dict(
            c="ZhiShuRanking",
            a="ZhiShuStockList_W8" if board_code else "RealRankingInfo",
            Order="1",
            Type="1",
            st=str(limit),
            Index=str(offset),
            PhoneOSNew="1",
            VerSion="5",
            apiv="w41" if board_code else ("w26" if trade_date else "w21"),
        )
        if trade_date is not None:
            params["Date"] = trade_date.isoformat()
        if board_code:
            params.update(
                PlateID=board_code, IsZZ="0", IsKZZType="0", filterType="0", TSZB="0", TSZB_Type="0"
            )
        else:
            params["ZSType"] = "7"
        request = Request(
            (HIS if trade_date else HQ) + "?" + urlencode(params),
            headers={"User-Agent": UA, "Accept": "application/json"},
            method="GET",
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
        except (OSError, ValueError, TypeError, ArithmeticError, RecursionError) as exc:
            raise KaipanlaBoardsUpstream("Kaipanla board request failed") from exc


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
