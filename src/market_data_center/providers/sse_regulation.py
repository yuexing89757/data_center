"""Bounded adapter for SSE official abnormal-volatility disclosures."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from market_data_center.domain.records import Exchange
from market_data_center.domain.regulation import (
    RegulationDirection,
    RegulationEventRecord,
    RegulationEventType,
    RegulationRuleLevel,
    RegulationSegment,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError, RawRow
from market_data_center.providers.official_regulation_common import validate_official_url

SSE_ALLOWED_HOSTS = frozenset({"www.sse.com.cn", "query.sse.com.cn"})
SSE_LIST_ROOT = "https://query.sse.com.cn/commonSoaQuery.do"
SSE_DOCUMENT_ROOT = "https://www.sse.com.cn/disclosure/diclosure/public/"
MAX_PAGES = 20
MAX_DOCUMENTS = 500
CONNECT_TIMEOUT_SECONDS = 5
READ_TIMEOUT_SECONDS = 15

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_JSONP = re.compile(rb"^[^(]*\((.*)\)\s*;?\s*$", re.DOTALL)
_MAINBOARD_CODE = re.compile(r"^60[0-5][0-9]{3}$")


@dataclass(frozen=True, slots=True)
class SSEResponse:
    url: str
    content_type: str
    body: bytes


type SSEFetch = Callable[[str, dict[str, str]], SSEResponse]


@dataclass(frozen=True, slots=True)
class _Reason:
    title: str
    level: RegulationRuleLevel
    direction: RegulationDirection | None
    rule_codes: tuple[str, ...]


_REASONS = {
    "1": _Reason(
        "涨幅偏离累计达20%",
        RegulationRuleLevel.ABNORMAL,
        RegulationDirection.UP,
        ("SSE_MAIN_ABNORMAL_3D_DEV_UP",),
    ),
    "2": _Reason(
        "跌幅偏离累计达20%",
        RegulationRuleLevel.ABNORMAL,
        RegulationDirection.DOWN,
        ("SSE_MAIN_ABNORMAL_3D_DEV_DOWN",),
    ),
    "31": _Reason("异常波动停牌(A股)", RegulationRuleLevel.ABNORMAL, None, ()),
    "Z3": _Reason(
        "10日内4次出现同正向异常波动",
        RegulationRuleLevel.SERIOUS_ABNORMAL,
        RegulationDirection.UP,
        ("SSE_MAIN_SERIOUS_10D_4_UP",),
    ),
    "Z4": _Reason(
        "10日内4次出现同负向异常波动",
        RegulationRuleLevel.SERIOUS_ABNORMAL,
        RegulationDirection.DOWN,
        ("SSE_MAIN_SERIOUS_10D_4_DOWN",),
    ),
    "Z5": _Reason(
        "10日涨幅偏离累计达100%",
        RegulationRuleLevel.SERIOUS_ABNORMAL,
        RegulationDirection.UP,
        ("SSE_MAIN_SERIOUS_10D_DEV_100_UP",),
    ),
    "Z6": _Reason(
        "10日跌幅偏离累计达50%",
        RegulationRuleLevel.SERIOUS_ABNORMAL,
        RegulationDirection.DOWN,
        ("SSE_MAIN_SERIOUS_10D_DEV_50_DOWN",),
    ),
    "Z7": _Reason(
        "30日涨幅偏离累计达200%",
        RegulationRuleLevel.SERIOUS_ABNORMAL,
        RegulationDirection.UP,
        ("SSE_MAIN_SERIOUS_30D_DEV_200_UP",),
    ),
    "Z8": _Reason(
        "30日跌幅偏离累计达70%",
        RegulationRuleLevel.SERIOUS_ABNORMAL,
        RegulationDirection.DOWN,
        ("SSE_MAIN_SERIOUS_30D_DEV_70_DOWN",),
    ),
}


class SSEOfficialRegulationEventProvider:
    """Read SSE mainboard official trading disclosures without source fallback."""

    source_code = "sse_official"

    def __init__(
        self,
        *,
        fetch: SSEFetch | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._fetch = fetch or _fetch
        self._clock = clock or (lambda: datetime.now(tz=_SHANGHAI))

    def fetch_events(
        self, observed_from: datetime, observed_to: datetime
    ) -> ProviderBatch[RegulationEventRecord]:
        _validate_bounds(observed_from, observed_to)
        observed_at = self._clock()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ProviderError("provider clock must return a timezone-aware datetime")

        start_date = observed_from.astimezone(_SHANGHAI).date()
        end_date = observed_to.astimezone(_SHANGHAI).date()
        rows = self._fetch_rows(start_date, end_date)
        raw_rows = tuple(_raw_row(row) for row in rows)

        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={
                "observed_from": observed_from.isoformat(),
                "observed_to": observed_to.isoformat(),
                "trade_date_start": start_date.isoformat(),
                "trade_date_end": end_date.isoformat(),
            },
            schema_version="sse.regulation_event.v1",
            record_factory=lambda: _normalize_rows(
                rows, start_date=start_date, end_date=end_date, observed_at=observed_at
            ),
        )

    def _fetch_rows(self, start_date: date, end_date: date) -> tuple[Mapping[str, object], ...]:
        unique: dict[str, Mapping[str, object]] = {}
        page_no = 1
        page_count = 1
        while page_no <= page_count:
            params = _request_params(start_date, end_date, page_no)
            try:
                response = self._fetch(SSE_LIST_ROOT, params)
            except ProviderError:
                raise
            except (HTTPError, URLError, TimeoutError, OSError) as error:
                raise ProviderError("SSE official request failed") from error
            validate_official_url(response.url, SSE_ALLOWED_HOSTS)
            payload = _parse_payload(response.body)
            page_help = payload.get("pageHelp")
            if not isinstance(page_help, Mapping):
                raise ProviderError("SSE official response is missing pagination")
            page_count = _positive_int(page_help.get("pageCount"), "page count")
            if page_count > MAX_PAGES:
                raise ProviderError("SSE official pagination exceeds bound")
            data = page_help.get("data", payload.get("result"))
            if not isinstance(data, list):
                raise ProviderError("SSE official response rows are invalid")
            for item in data:
                if not isinstance(item, Mapping):
                    raise ProviderError("SSE official response row is invalid")
                canonical = _canonical_json(item)
                unique.setdefault(canonical, dict(item))
                if len(unique) > MAX_DOCUMENTS:
                    raise ProviderError("SSE official document count exceeds bound")
            page_no += 1
        return tuple(unique.values())


def _fetch(url: str, params: dict[str, str]) -> SSEResponse:
    validate_official_url(url, SSE_ALLOWED_HOSTS)
    request = Request(
        f"{url}?{urlencode(params)}",
        headers={
            "Accept": "application/javascript, application/json",
            "Referer": "https://www.sse.com.cn/",
            "User-Agent": "market-data-center/1.0",
        },
    )
    timeout = CONNECT_TIMEOUT_SECONDS + READ_TIMEOUT_SECONDS
    with urlopen(request, timeout=timeout) as response:
        return SSEResponse(
            url=response.geturl(),
            content_type=response.headers.get_content_type(),
            body=response.read(),
        )


def _request_params(start_date: date, end_date: date, page_no: int) -> dict[str, str]:
    return {
        "jsonCallBack": "callback",
        "isPagination": "true",
        "pageHelp.pageSize": "100",
        "pageHelp.pageNo": str(page_no),
        "pageHelp.beginPage": str(page_no),
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": str(page_no),
        "sqlId": "JYGKXX_ZL",
        "token": "QUERY",
        "tradeDateStart": start_date.isoformat(),
        "tradeDateEnd": end_date.isoformat(),
        "branchName": "",
        "secCode": "",
        "refType": "",
        "bsType": "",
    }


def _parse_payload(body: bytes) -> Mapping[str, object]:
    match = _JSONP.fullmatch(body.strip())
    encoded = match.group(1) if match is not None else body
    try:
        value = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderError("SSE official response is not valid JSONP") from error
    if not isinstance(value, Mapping):
        raise ProviderError("SSE official response root is invalid")
    return value


def _normalize_rows(
    rows: tuple[Mapping[str, object], ...],
    *,
    start_date: date,
    end_date: date,
    observed_at: datetime,
) -> tuple[RegulationEventRecord, ...]:
    events: list[RegulationEventRecord] = []
    for row in rows:
        event = _normalize_row(
            row, start_date=start_date, end_date=end_date, observed_at=observed_at
        )
        if event is not None:
            events.append(event)
    return tuple(events)


def _normalize_row(
    row: Mapping[str, object],
    *,
    start_date: date,
    end_date: date,
    observed_at: datetime,
) -> RegulationEventRecord | None:
    code = _text(row.get("secCode"))
    reason_code = _text(row.get("refType"))
    reason = _REASONS.get(reason_code)
    if reason is None or _MAINBOARD_CODE.fullmatch(code) is None:
        return None
    try:
        trade_date = _compact_date(row.get("tradeDate"))
        period_start = _compact_date(row.get("abnormalStart"))
        period_end = _compact_date(row.get("abnormalEnd"))
    except ValueError:
        return None
    if not start_date <= trade_date <= end_date or period_end != trade_date:
        return None
    published_at = datetime.combine(trade_date, time.min, tzinfo=_SHANGHAI)
    normalized_observed_at = observed_at.astimezone(_SHANGHAI)
    if normalized_observed_at < published_at:
        return None
    source_event_id = f"{trade_date:%Y%m%d}:{code}:{reason_code}"
    name = _text(row.get("secAbbr")) or code
    source_url = (
        f"{SSE_DOCUMENT_ROOT}dailydata/detail.shtml?secCode={code}"
        f"&refType={reason_code}&tradeDate={trade_date:%Y%m%d}"
    )
    canonical = _canonical_json(row)
    event_type = (
        RegulationEventType.ABNORMAL_VOLATILITY
        if reason.level is RegulationRuleLevel.ABNORMAL
        else RegulationEventType.SERIOUS_ABNORMAL_VOLATILITY
    )
    return RegulationEventRecord(
        symbol=f"SSE:{code}",
        exchange=Exchange.SSE,
        segment=RegulationSegment.SSE_MAIN,
        event_type=event_type,
        event_level=reason.level,
        direction=reason.direction,
        period_start_date=period_start,
        period_end_date=period_end,
        published_at=published_at,
        effective_reset_date=None,
        source_event_id=source_event_id,
        source_title=f"{name} {reason.title}",
        source_url=source_url,
        source_content_hash=hashlib.sha256(canonical.encode()).hexdigest(),
        source_code="sse_official",
        explicit_rule_codes=reason.rule_codes,
        observed_at=normalized_observed_at,
    )


def _raw_row(row: Mapping[str, object]) -> RawRow:
    return {
        "source": "sse_official",
        "payload": _canonical_json(row),
    }


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _compact_date(value: object) -> date:
    return datetime.strptime(_text(value), "%Y%m%d").date()


def _text(value: object) -> str:
    return "" if value is None else str(value).strip()


def _positive_int(value: object, field_name: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as error:
        raise ProviderError(f"SSE official {field_name} is invalid") from error
    if parsed < 1:
        raise ProviderError(f"SSE official {field_name} is invalid")
    return parsed


def _validate_bounds(observed_from: datetime, observed_to: datetime) -> None:
    bounds = (observed_from, observed_to)
    if any(value.tzinfo is None or value.utcoffset() is None for value in bounds):
        raise ValueError("observation bounds must be timezone-aware")
    if observed_to < observed_from:
        raise ValueError("observation end must not precede start")
