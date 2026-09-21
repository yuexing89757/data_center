"""Bounded adapter for SZSE official abnormal-volatility disclosures."""

from __future__ import annotations

import hashlib
import html
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
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

SZSE_ALLOWED_HOSTS = frozenset({"www.szse.cn", "disc.static.szse.cn"})
SZSE_LIST_ROOTS = (
    "https://www.szse.cn/disclosure/deal/public/",
    "https://www.szse.cn/disclosure/deal/inquiry/",
)
SZSE_REPORT_URL = "https://www.szse.cn/api/report/ShowReport/data"
MAX_PAGES = 20
MAX_DOCUMENTS = 500
CONNECT_TIMEOUT_SECONDS = 5
READ_TIMEOUT_SECONDS = 15

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAINBOARD_CODE = re.compile(r"^(?:000|001|002|003)[0-9]{3}$")
_GEM_CODE = re.compile(r"^(?:300|301)[0-9]{3}$")
_DETAIL_PARAMS = re.compile(r"a-param=['\"]([^'\"]+)['\"]", re.IGNORECASE)
_PERIOD = re.compile(r"(?P<start>\d{4}-\d{2}-\d{2})\s*至\s*(?P<end>\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True, slots=True)
class SZSEResponse:
    url: str
    content_type: str
    body: bytes


type SZSEFetch = Callable[[str, dict[str, str]], SZSEResponse]


class SZSEOfficialRegulationEventProvider:
    """Read SZSE mainboard/GEM official trading disclosures without fallback."""

    source_code = "szse_official"

    def __init__(
        self,
        *,
        fetch: SZSEFetch | None = None,
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
        evidence = self._fetch_evidence(start_date, end_date)
        raw_rows = tuple(_raw_row(listing, detail) for listing, detail in evidence)
        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={
                "observed_from": observed_from.isoformat(),
                "observed_to": observed_to.isoformat(),
                "trade_date_start": start_date.isoformat(),
                "trade_date_end": end_date.isoformat(),
            },
            schema_version="szse.regulation_event.v1",
            record_factory=lambda: _normalize_evidence(
                evidence,
                start_date=start_date,
                end_date=end_date,
                observed_at=observed_at.astimezone(_SHANGHAI),
            ),
        )

    def _fetch_evidence(
        self, start_date: date, end_date: date
    ) -> tuple[tuple[Mapping[str, object], Mapping[str, object]], ...]:
        listings = self._fetch_listing_rows(start_date, end_date)
        evidence: list[tuple[Mapping[str, object], Mapping[str, object]]] = []
        for listing in listings:
            detail_params = _detail_params(listing)
            response = self._request(detail_params)
            detail = _detail_record(_parse_report(response.body))
            evidence.append((listing, detail))
            if len(evidence) > MAX_DOCUMENTS:
                raise ProviderError("SZSE official document count exceeds bound")
        return tuple(evidence)

    def _fetch_listing_rows(
        self, start_date: date, end_date: date
    ) -> tuple[Mapping[str, object], ...]:
        unique: dict[str, Mapping[str, object]] = {}
        page_no = 1
        page_count = 1
        while page_no <= page_count:
            response = self._request(_listing_params(start_date, end_date, page_no))
            report = _single_report(_parse_report(response.body))
            metadata = report.get("metadata")
            if not isinstance(metadata, Mapping):
                raise ProviderError("SZSE official response is missing metadata")
            page_count = _positive_int(metadata.get("pagecount"), "page count")
            if page_count > MAX_PAGES:
                raise ProviderError("SZSE official pagination exceeds bound")
            data = report.get("data")
            if not isinstance(data, list):
                raise ProviderError("SZSE official response rows are invalid")
            for item in data:
                if not isinstance(item, Mapping):
                    raise ProviderError("SZSE official response row is invalid")
                canonical = _canonical_json(item)
                unique.setdefault(canonical, dict(item))
            page_no += 1
        return tuple(unique.values())

    def _request(self, params: dict[str, str]) -> SZSEResponse:
        try:
            response = self._fetch(SZSE_REPORT_URL, params)
        except ProviderError:
            raise
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            raise ProviderError("SZSE official request failed") from error
        validate_official_url(response.url, SZSE_ALLOWED_HOSTS)
        return response


def _fetch(url: str, params: dict[str, str]) -> SZSEResponse:
    validate_official_url(url, SZSE_ALLOWED_HOSTS)
    request = Request(
        f"{url}?{urlencode(params)}",
        headers={
            "Accept": "application/json",
            "Referer": SZSE_LIST_ROOTS[0],
            "User-Agent": "market-data-center/1.0",
        },
    )
    timeout = CONNECT_TIMEOUT_SECONDS + READ_TIMEOUT_SECONDS
    with urlopen(request, timeout=timeout) as response:
        return SZSEResponse(
            url=response.geturl(),
            content_type=response.headers.get_content_type(),
            body=response.read(),
        )


def _listing_params(start_date: date, end_date: date, page_no: int) -> dict[str, str]:
    return {
        "SHOWTYPE": "JSON",
        "CATALOGID": "1842_xxpl_after",
        "PAGENO": str(page_no),
        "tab1PAGESIZE": "100",
        "txtStart": start_date.isoformat(),
        "txtEnd": end_date.isoformat(),
    }


def _detail_params(listing: Mapping[str, object]) -> dict[str, str]:
    markup = _clean_text(listing.get("bz"))
    match = _DETAIL_PARAMS.search(markup)
    if match is None:
        raise ProviderError("SZSE official detail link is missing")
    query = urlsplit(html.unescape(match.group(1))).query
    values = parse_qs(query, keep_blank_values=True)
    required = ("DQRQ", "ZQDM", "ZBDM")
    if any(not values.get(name) for name in required):
        raise ProviderError("SZSE official detail link is invalid")
    return {
        "SHOWTYPE": "JSON",
        "CATALOGID": "1842_detal",
        "TABKEY": "tab1,tab2",
        **{name: values[name][0] for name in required},
    }


def _parse_report(body: bytes) -> list[object]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderError("SZSE official response is not valid JSON") from error
    if not isinstance(value, list) or not value:
        raise ProviderError("SZSE official response root is invalid")
    return value


def _single_report(reports: list[object]) -> Mapping[str, object]:
    report = reports[0]
    if not isinstance(report, Mapping):
        raise ProviderError("SZSE official report is invalid")
    return report


def _detail_record(reports: list[object]) -> Mapping[str, object]:
    for report in reports:
        if not isinstance(report, Mapping):
            continue
        metadata = report.get("metadata")
        if not isinstance(metadata, Mapping) or metadata.get("tabkey") != "tab1":
            continue
        rows = report.get("data")
        if isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], Mapping):
            return dict(rows[0])
    raise ProviderError("SZSE official detail evidence is invalid")


def _normalize_evidence(
    evidence: tuple[tuple[Mapping[str, object], Mapping[str, object]], ...],
    *,
    start_date: date,
    end_date: date,
    observed_at: datetime,
) -> tuple[RegulationEventRecord, ...]:
    events: list[RegulationEventRecord] = []
    for listing, detail in evidence:
        event = _normalize_event(
            listing,
            detail,
            start_date=start_date,
            end_date=end_date,
            observed_at=observed_at,
        )
        if event is not None:
            events.append(event)
    return tuple(events)


def _normalize_event(
    listing: Mapping[str, object],
    detail: Mapping[str, object],
    *,
    start_date: date,
    end_date: date,
    observed_at: datetime,
) -> RegulationEventRecord | None:
    code = _clean_text(listing.get("zqdm"))
    segment = _segment(code)
    if segment is None:
        return None
    try:
        published_date = date.fromisoformat(_clean_text(detail.get("dqrq")))
        period_match = _PERIOD.fullmatch(_clean_text(detail.get("ycqj")))
        if period_match is None:
            return None
        period_start = date.fromisoformat(period_match.group("start"))
        period_end = date.fromisoformat(period_match.group("end"))
    except ValueError:
        return None
    if (
        not start_date <= published_date <= end_date
        or period_end != published_date
        or period_start > period_end
    ):
        return None
    if _clean_text(detail.get("zqjc")).find(code) < 0:
        return None
    reason = _clean_text(detail.get("plyy"))
    classification = _classify_reason(reason, segment)
    if classification is None:
        return None
    level, direction, rule_codes = classification
    published_at = datetime.combine(published_date, time.min, tzinfo=_SHANGHAI)
    if observed_at < published_at:
        return None
    event_type = (
        RegulationEventType.ABNORMAL_VOLATILITY
        if level is RegulationRuleLevel.ABNORMAL
        else RegulationEventType.SERIOUS_ABNORMAL_VOLATILITY
    )
    detail_params = _detail_params(listing)
    source_event_id = ":".join((published_date.strftime("%Y%m%d"), code, detail_params["ZBDM"]))
    source_url = f"{SZSE_REPORT_URL}?{urlencode(detail_params)}"
    canonical = _canonical_json(detail)
    name = re.sub(r"\s*\([^)]*\)\s*$", "", _clean_text(detail.get("zqjc"))) or code
    return RegulationEventRecord(
        symbol=f"SZSE:{code}",
        exchange=Exchange.SZSE,
        segment=segment,
        event_type=event_type,
        event_level=level,
        direction=direction,
        period_start_date=period_start,
        period_end_date=period_end,
        published_at=published_at,
        effective_reset_date=None,
        source_event_id=source_event_id,
        source_title=f"{name} {reason}",
        source_url=source_url,
        source_content_hash=hashlib.sha256(canonical.encode()).hexdigest(),
        source_code="szse_official",
        explicit_rule_codes=rule_codes,
        observed_at=observed_at,
    )


def _classify_reason(
    reason: str, segment: RegulationSegment
) -> tuple[RegulationRuleLevel, RegulationDirection | None, tuple[str, ...]] | None:
    prefix = "SZSE_MAIN" if segment is RegulationSegment.SZSE_MAIN else "GEM"
    serious_rules: list[tuple[str, RegulationDirection]] = []
    count = "4" if segment is RegulationSegment.SZSE_MAIN else "3"
    if re.search(rf"10个交易日内{count}次出现同正向异常波动", reason):
        serious_rules.append((f"{prefix}_SERIOUS_10D_COUNT_UP", RegulationDirection.UP))
    if re.search(rf"10个交易日内{count}次出现同负向异常波动", reason):
        serious_rules.append((f"{prefix}_SERIOUS_10D_COUNT_DOWN", RegulationDirection.DOWN))
    serious_patterns = (
        (r"10个交易日内[^;\uFF1B]*涨幅偏离值累计达到", "10D_DEV_UP", RegulationDirection.UP),
        (r"10个交易日内[^;\uFF1B]*跌幅偏离值累计达到", "10D_DEV_DOWN", RegulationDirection.DOWN),
        (r"30个交易日内[^;\uFF1B]*涨幅偏离值累计达到", "30D_DEV_UP", RegulationDirection.UP),
        (r"30个交易日内[^;\uFF1B]*跌幅偏离值累计达到", "30D_DEV_DOWN", RegulationDirection.DOWN),
    )
    for pattern, suffix, direction in serious_patterns:
        if re.search(pattern, reason):
            serious_rules.append((f"{prefix}_SERIOUS_{suffix}", direction))
    if serious_rules:
        directions = {direction for _, direction in serious_rules}
        resolved_direction: RegulationDirection | None = (
            directions.pop() if len(directions) == 1 else None
        )
        return (
            RegulationRuleLevel.SERIOUS_ABNORMAL,
            resolved_direction,
            tuple(rule for rule, _ in serious_rules),
        )
    if re.search(r"异常期间价格涨幅偏离值累计达到", reason):
        return (
            RegulationRuleLevel.ABNORMAL,
            RegulationDirection.UP,
            (f"{prefix}_ABNORMAL_3D_DEV_UP",),
        )
    if re.search(r"异常期间价格跌幅偏离值累计达到", reason):
        return (
            RegulationRuleLevel.ABNORMAL,
            RegulationDirection.DOWN,
            (f"{prefix}_ABNORMAL_3D_DEV_DOWN",),
        )
    if "异常期间" in reason and "换手率" in reason:
        return (
            RegulationRuleLevel.ABNORMAL,
            None,
            (f"{prefix}_ABNORMAL_TURNOVER",),
        )
    if "交易所认定属于异常波动" in reason:
        return RegulationRuleLevel.ABNORMAL, None, ()
    return None


def _segment(code: str) -> RegulationSegment | None:
    if _MAINBOARD_CODE.fullmatch(code):
        return RegulationSegment.SZSE_MAIN
    if _GEM_CODE.fullmatch(code):
        return RegulationSegment.GEM
    return None


def _raw_row(listing: Mapping[str, object], detail: Mapping[str, object]) -> RawRow:
    return {
        "source": "szse_official",
        "listing": _canonical_json(listing),
        "detail": _canonical_json(detail),
    }


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", html.unescape("" if value is None else str(value))).strip()


def _positive_int(value: object, field_name: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as error:
        raise ProviderError(f"SZSE official {field_name} is invalid") from error
    if parsed < 1:
        raise ProviderError(f"SZSE official {field_name} is invalid")
    return parsed


def _validate_bounds(observed_from: datetime, observed_to: datetime) -> None:
    bounds = (observed_from, observed_to)
    if any(value.tzinfo is None or value.utcoffset() is None for value in bounds):
        raise ValueError("observation bounds must be timezone-aware")
    if observed_to < observed_from:
        raise ValueError("observation end must not precede start")
