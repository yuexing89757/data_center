"""One credential-free request for the newest published topic-date group."""

import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import pairwise
from typing import cast
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from market_data_center.domain.kaipanla_hot_talks import HotTalkItem, HotTalkPage

SOURCE_URL = "https://applhb.kaipanla.com/w1/api/index.php"
MAX_RESPONSE_BYTES = 2_000_000
MAX_TOPICS_PER_DAY = 100


class KaipanlaHotTalksUpstream(RuntimeError):
    """The source did not provide a trustworthy newest topic-date group."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _request_bytes(request: Request, timeout: float) -> bytes:
    with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if response.status != 200 or len(body) > MAX_RESPONSE_BYTES:
            raise KaipanlaHotTalksUpstream("invalid source response")
        return cast(bytes, body)


def _count(value: object) -> int:
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,20}", value):
        return int(value)
    raise ValueError("invalid source integer")


def _item(row: object, rank: int) -> HotTalkItem:
    if not isinstance(row, dict):
        raise ValueError("invalid topic row")
    topic_id = row["ID"]
    if type(topic_id) is int and topic_id > 0:
        topic_id = str(topic_id)
    if not isinstance(topic_id, str) or not re.fullmatch(r"[0-9]{1,64}", topic_id):
        raise ValueError("invalid topic identity")
    title = row["Title"]
    if not isinstance(title, str) or not title.strip() or len(title) > 1000:
        raise ValueError("invalid topic title")
    vote = _count(row["IsVote"])
    if vote not in (0, 1):
        raise ValueError("invalid source vote flag")
    published = row["Time"]
    published_at = None
    if published is not None:
        seconds = _count(published)
        if seconds <= 0:
            raise ValueError("invalid publication timestamp")
        published_at = datetime.fromtimestamp(seconds, UTC)
    reads = row["HotVal"]
    return HotTalkItem(
        topic_id=topic_id,
        rank=rank,
        title=title.strip(),
        heat_value=None if reads is None else _count(reads),
        published_at=published_at,
        has_vote=bool(vote),
    )


class KaipanlaHotTalksProvider:
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

    def fetch(self) -> HotTalkPage:
        request = Request(
            SOURCE_URL,
            data=urlencode({"c": "Topic", "a": "InfoList", "Index": 0, "st": 20}).encode("ascii"),
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
            groups = data["List"]
            if not isinstance(groups, list) or len(groups) > 21:
                raise ValueError("invalid topic-date groups")
            observed_at = self._clock()
            dates = []
            for group in groups:
                if not isinstance(group, dict):
                    raise ValueError("invalid date group")
                day = group["Day"]
                if not isinstance(day, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", day):
                    raise ValueError("invalid topic date")
                dates.append(date.fromisoformat(day))
                if not isinstance(group["List"], list) or len(group["List"]) > MAX_TOPICS_PER_DAY:
                    raise ValueError("invalid topic list")
            if any(a <= b for a, b in pairwise(dates)):
                raise ValueError("duplicate or unordered topic dates")
            if dates and dates[0] > observed_at.astimezone(ZoneInfo("Asia/Shanghai")).date():
                raise ValueError("future topic date")
            items = (
                tuple(_item(row, rank) for rank, row in enumerate(groups[0]["List"], 1))
                if groups
                else ()
            )
            if len({item.topic_id for item in items}) != len(items):
                raise ValueError("duplicate topics")
        except (OSError, ValueError, TypeError, KeyError, ArithmeticError, RecursionError) as exc:
            raise KaipanlaHotTalksUpstream("Kaipanla hot-talks request failed") from exc
        return HotTalkPage(
            observed_at=observed_at,
            topic_date=dates[0] if dates else None,
            returned_count=len(items),
            items=items,
        )
