"""Fixed, anonymous, bounded read-only Kaipanla alert requests (APP 6.3.20.0)."""

import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, cast
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from market_data_center.domain.kaipanla_alerts import AlertGroup, AlertItem, AlertSnapshot

MAX_RESPONSE_BYTES = 2_000_000
HQ = "https://apphwshhq.kaipanla.com/w1/api/index.php"
HIS = "https://apphis.kaipanla.com/w1/api/index.php"
COMMON = {"apiv": "w48", "PhoneOSNew": "1", "VerSion": "6.3.20.0", "Red": "0"}
Endpoint = Literal[
    "severe_current",
    "severe_history",
    "severe_history_all",
    "hot_current",
    "hot_history",
    "monitor_current",
    "monitor_history_initial",
    "monitor_history_more",
    "inquiry_initial",
    "inquiry_history",
    "quote_refresh",
    "legacy_index",
    "legacy_history",
    "legacy_many",
]
AlertFilter = Literal["all", "triggered", "suspended"]
_ENDPOINTS: dict[str, tuple[str, str, str, str]] = {
    "severe_current": (HQ, "GET", "GetPianLiZhi_W46", "severe"),
    "severe_history": (HIS, "GET", "GetYDTPZFPL_W46", "severe"),
    "severe_history_all": (HIS, "GET", "GetYDTPZFPL_W46_HisAll", "severe"),
    "hot_current": (HQ, "GET", "GetPianLiZhi_Hot", "hot"),
    "hot_history": (HIS, "GET", "GetPianLiZhi_Hot_His", "hot"),
    "monitor_current": (HQ, "POST", "GetYDTP_ZDJK_Today", "monitor"),
    "monitor_history_initial": (HQ, "POST", "GetYDTP_ZDJK_His", "monitor"),
    "monitor_history_more": (HIS, "POST", "GetYDTP_ZDJK_His", "monitor"),
    "inquiry_initial": (HQ, "POST", "GetYDTP_WXHJ_His", "inquiry"),
    "inquiry_history": (HIS, "POST", "GetYDTP_WXHJ_His", "inquiry"),
    "quote_refresh": (HQ, "POST", "RefreshStockList_price", "quote"),
    "legacy_index": (HQ, "POST", "GetPianLiZhi_Index", "legacy"),
    "legacy_history": (HIS, "POST", "GetPianLiZhi_Index_W32", "legacy"),
    "legacy_many": (HQ, "POST", "GetPianLiZhi_Many", "multiple"),
}
_DATED = {
    "severe_history",
    "severe_history_all",
    "hot_history",
    "legacy_history",
    "monitor_history_more",
}
_PAGED = {"severe_history_all", "inquiry_history"}


class KaipanlaAlertsUpstream(RuntimeError):
    """Source did not return a valid successful alert response."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _request_bytes(request: Request, timeout: float) -> bytes:
    # Do not forward host proxy credentials or follow redirects to other hosts.
    with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
            raise KaipanlaAlertsUpstream("invalid source response")
        return cast(bytes, body)


class KaipanlaAlertsProvider:
    def __init__(
        self,
        *,
        timeout_seconds: float = 8,
        request_bytes: Callable[[Request, float], bytes] = _request_bytes,
    ) -> None:
        if not 0 < timeout_seconds <= 10:
            raise ValueError("timeout must be between 0 and 10 seconds")
        self._timeout = timeout_seconds
        self._request = request_bytes

    def fetch(
        self,
        endpoint: Endpoint,
        *,
        trade_date: date | None = None,
        offset: int = 0,
        limit: int = 20,
        filter: AlertFilter = "all",
        codes: tuple[str, ...] = (),
        triggered_only: bool = False,
    ) -> AlertSnapshot:
        if endpoint not in _ENDPOINTS or not 0 <= offset <= 10000 or not 1 <= limit <= 20:
            raise ValueError("invalid alert request")
        if filter not in {"all", "triggered", "suspended"}:
            raise ValueError("invalid filter")
        if endpoint in _DATED and trade_date is None:
            raise ValueError("date is required")
        if endpoint == "quote_refresh" and (
            not 1 <= len(codes) <= 100 or any(not re.fullmatch(r"[0-9]{6}", c) for c in codes)
        ):
            raise ValueError("expected 1 to 100 stock codes")
        url, method, action, layout = _ENDPOINTS[endpoint]
        params = {**COMMON, "c": "StockBidYiDong", "a": action}
        if endpoint in _DATED:
            assert trade_date is not None
            params["Day"] = trade_date.isoformat()
        if endpoint in _PAGED:
            params.update(Index=str(offset), st=str(limit))
        if endpoint == "severe_history_all":
            params["IsZT"] = "1" if filter == "triggered" else "0"
            if filter == "suspended":
                params["Status"] = "1"
        if endpoint == "legacy_index":
            params["ZDJK_Type"] = "1"
        if endpoint == "legacy_history":
            params["IsZT"] = "1" if triggered_only else "0"
        if endpoint == "quote_refresh":
            params.update(c="UserSelectStock", StockIDList=",".join(dict.fromkeys(codes)))
        encoded = urlencode(params)
        request = Request(
            url + ("?" + encoded if method == "GET" else ""),
            data=encoded.encode("ascii") if method == "POST" else None,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method=method,
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
                or type(data.get("errcode")) not in {str, int}
                or str(data["errcode"]) != "0"
            ):
                raise ValueError("source response is not successful")
            return _normalize(data, endpoint, layout, trade_date, offset, limit, codes)
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            IndexError,
            ArithmeticError,
            RecursionError,
        ) as exc:
            raise KaipanlaAlertsUpstream("Kaipanla alert request failed") from exc


def _invalid_constant(value: str) -> None:
    raise ValueError("non-finite source number")


def _date(value: object) -> date | None:
    if value in (None, "", "0000-00-00"):
        return None
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("invalid source date")
    return date.fromisoformat(value)


def _decimal(value: object) -> Decimal | None:
    if value in (None, "", "--"):
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("invalid source number")
    number = Decimal(value)
    if not number.is_finite():
        raise ValueError("non-finite source number")
    return number


def _item(row: object, layout: str) -> AlertItem:
    if isinstance(row, dict):
        row = [row["StockID"], row["StockName"]]
    if not isinstance(row, list) or len(row) < 2 or len(row) > 100:
        raise ValueError("invalid source row")
    for cell in row:
        if cell is not None and (
            type(cell) not in (str, int, Decimal)
            or (isinstance(cell, Decimal) and not cell.is_finite())
        ):
            raise ValueError("unsupported source cell")
    code = row[0]
    if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
        raise ValueError("invalid source code")
    if code.startswith("6"):
        market = "SSE"
    elif code.startswith(("0", "3")):
        market = "SZSE"
    elif code.startswith(("4", "8", "9")):
        market = "BSE"
    else:
        raise ValueError("unsupported source market")
    name = None if layout == "quote" else row[1]
    if name is not None and not isinstance(name, str):
        raise ValueError("invalid source name")
    price = change = None
    start = end = announcement = None
    document = None
    if layout == "quote":
        price, change = _decimal(row[1]), _decimal(row[2])
        if price is not None and price < 0:
            raise ValueError("negative source price")
    if layout == "monitor":
        start, end = _date(row[2]), _date(row[3])
        if start is not None and end is not None and start > end:
            raise ValueError("reversed monitor dates")
    if layout == "inquiry":
        announcement, document = _date(row[2]), row[3]
        if document in (None, ""):
            document = None
        elif not isinstance(document, str) or urlsplit(document).scheme not in ("https", "http"):
            raise ValueError("invalid document URL")
    return AlertItem(
        symbol=f"{market}:{code}",
        code=code,
        name=name,
        source_values=tuple(row),
        last_price=price,
        change_pct=change,
        start_date=start,
        end_date=end,
        announcement_date=announcement,
        document_url=document,
    )


def _normalize(
    data: dict[str, object],
    endpoint: str,
    layout: str,
    requested: date | None,
    offset: int,
    limit: int,
    codes: tuple[str, ...],
) -> AlertSnapshot:
    fields = [("List", "items", layout)]
    if endpoint in {"severe_current", "severe_history"}:
        fields = [("List_Today", "today", layout), ("List_Tormorow", "tomorrow", layout)]
    elif endpoint == "severe_history_all":
        fields = [("List_His", "history", layout)]
    elif endpoint == "legacy_index":
        fields += [("ZDJKList", "monitor", "stock"), ("WXHJList", "inquiries", "stock")]
    groups = []
    for field, key, row_layout in fields:
        rows = data.get(field)
        if not isinstance(rows, list) or len(rows) > 5000:
            raise ValueError("invalid or missing source list")
        items = tuple(_item(row, row_layout) for row in rows)
        if endpoint == "quote_refresh":
            returned = [item.code for item in items]
            if len(set(returned)) != len(returned) or not set(returned) <= set(codes):
                raise ValueError("unexpected quote identifiers")
        groups.append(AlertGroup(key=key, items=items))
    source_day = _date(data.get("Day"))
    source_time = _decimal(data.get("Time"))
    timestamp = None
    if source_time is not None:
        if source_time < 0 or source_time != source_time.to_integral_value():
            raise ValueError("invalid source timestamp")
        timestamp = datetime.fromtimestamp(int(source_time), UTC)
    total = data.get("List_His_Total")
    if total is not None and (type(total) is not int or total < 0):
        raise ValueError("invalid source total")
    count = len(groups[0].items)
    paged = endpoint in _PAGED
    if paged and count > limit:
        raise ValueError("source ignored page bound")
    next_offset = None
    if (
        paged
        and count
        and offset + count <= 10000
        and (
            (isinstance(total, int) and offset + count < total)
            or (total is None and count == limit)
        )
    ):
        next_offset = offset + count
    next_date = None
    if endpoint in {"monitor_history_initial", "monitor_history_more"} and count:
        next_date = groups[0].items[-1].start_date
        if requested is not None and next_date is not None and next_date >= requested:
            next_date = None
    return AlertSnapshot(
        source_layout=f"kaipanla.alerts.{layout}.v1",
        observed_at=datetime.now(UTC),
        source_timestamp=timestamp,
        requested_date=requested,
        trade_date=source_day,
        date_matches_request=(source_day == requested)
        if requested is not None and source_day is not None
        else None,
        groups=tuple(groups),
        offset=offset if paged else None,
        limit=limit if paged else None,
        total=total if isinstance(total, int) else None,
        next_offset=next_offset,
        next_date=next_date,
        missing_codes=tuple(
            code for code in dict.fromkeys(codes) if code not in {i.code for i in groups[0].items}
        ),
    )
