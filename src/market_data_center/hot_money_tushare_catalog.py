"""Build a reviewed hot-money catalog from Tushare transaction facts."""

import json
import re
from calendar import monthrange
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from hashlib import sha256
from uuid import UUID

from market_data_center.domain.hot_money import (
    HotMoneyActor,
    HotMoneyReviewStatus,
    HotMoneySeatMapping,
)
from market_data_center.hot_money_catalog_service import HotMoneyCatalog
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.tushare import TushareClient

HM_LIST_FIELDS = ("name", "desc", "orgs")
HM_DETAIL_FIELDS = (
    "trade_date",
    "ts_code",
    "ts_name",
    "buy_amount",
    "sell_amount",
    "net_amount",
    "hm_name",
    "hm_orgs",
)
SOURCE_PAGE_SIZE = 2_000
MAX_SOURCE_PAGES_PER_MONTH = 50
MAX_SOURCE_ATTEMPTS = 3
AMOUNT_PRECISION = Decimal("1E+2")


@dataclass(frozen=True, slots=True)
class TushareHotMoneyRosterEntry:
    name: str
    description: str
    organizations: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TushareHotMoneyDetail:
    trade_date: date
    symbol: str
    buy_amount: Decimal | None
    sell_amount: Decimal | None
    actor_name: str
    organization_name: str


@dataclass(frozen=True, slots=True)
class StableSeatTradeFact:
    trade_date: date
    symbol: str
    buy_amount: Decimal | None
    sell_amount: Decimal | None
    seat_id: UUID


@dataclass(frozen=True, slots=True)
class HotMoneyCatalogBuildResult:
    catalog: HotMoneyCatalog
    matched_mapping_count: int
    unmatched_detail_count: int
    ambiguous_detail_count: int
    conflicting_seat_count: int
    conflicting_organization_count: int


class TushareHotMoneyCatalogSource:
    def __init__(self, client: TushareClient) -> None:
        self._client = client

    def fetch_roster(self) -> tuple[TushareHotMoneyRosterEntry, ...]:
        rows = self._query("hm_list", params={}, fields=HM_LIST_FIELDS)
        return tuple(_roster_entry(row) for row in rows)

    def fetch_details(self, start_date: date, end_date: date) -> tuple[TushareHotMoneyDetail, ...]:
        if end_date < start_date:
            raise ValueError("hot-money detail date range is invalid")
        details: list[TushareHotMoneyDetail] = []
        for window_start, window_end in _month_windows(start_date, end_date):
            for page in range(MAX_SOURCE_PAGES_PER_MONTH):
                rows = self._query(
                    "hm_detail",
                    params={
                        "start_date": window_start.strftime("%Y%m%d"),
                        "end_date": window_end.strftime("%Y%m%d"),
                        "limit": str(SOURCE_PAGE_SIZE),
                        "offset": str(page * SOURCE_PAGE_SIZE),
                    },
                    fields=HM_DETAIL_FIELDS,
                )
                details.extend(_detail(row) for row in rows)
                if len(rows) < SOURCE_PAGE_SIZE:
                    break
            else:
                raise ProviderError("Tushare hm_detail exceeded the monthly pagination bound")
        if any(item.trade_date < start_date or item.trade_date > end_date for item in details):
            raise ProviderError("Tushare hm_detail returned an out-of-range date")
        return tuple(details)

    def _query(
        self,
        api_name: str,
        *,
        params: Mapping[str, str],
        fields: Sequence[str],
    ) -> Sequence[Mapping[str, object]]:
        for attempt in range(MAX_SOURCE_ATTEMPTS):
            try:
                return self._client.query(api_name, params=params, fields=fields)
            except ProviderError:
                if attempt == MAX_SOURCE_ATTEMPTS - 1:
                    raise
        raise AssertionError("unreachable")


def build_reviewed_hot_money_catalog(
    roster: Sequence[TushareHotMoneyRosterEntry],
    details: Sequence[TushareHotMoneyDetail],
    seat_facts: Sequence[StableSeatTradeFact],
    *,
    catalog_version: str,
    reviewed_at: datetime,
) -> HotMoneyCatalogBuildResult:
    if reviewed_at.tzinfo is None:
        raise ValueError("hot-money catalog review timestamp must be timezone-aware")
    names = [item.name for item in roster]
    if len(set(names)) != len(names):
        raise ValueError("Tushare hot-money roster contains duplicate names")
    actors = tuple(
        HotMoneyActor(_actor_code(item.name), item.name, ())
        for item in sorted(roster, key=lambda x: x.name)
    )
    actor_codes = {actor.canonical_name: actor.actor_code for actor in actors}
    approved_relations = {
        (item.name, organization) for item in roster for organization in item.organizations
    }
    facts_by_key: dict[tuple[date, str, Decimal | None, Decimal | None], set[UUID]] = defaultdict(
        set
    )
    for fact in seat_facts:
        facts_by_key[_fact_key(fact)].add(fact.seat_id)

    unmatched = 0
    ambiguous = 0
    relation_matches: dict[tuple[str, str], list[tuple[UUID, date]]] = defaultdict(list)
    for detail in details:
        relation = (detail.actor_name, detail.organization_name)
        if relation not in approved_relations:
            unmatched += 1
            continue
        candidates = facts_by_key[_fact_key(detail)]
        if not candidates:
            unmatched += 1
        elif len(candidates) > 1:
            ambiguous += 1
        else:
            relation_matches[relation].append((next(iter(candidates)), detail.trade_date))

    actor_seat_evidence: dict[tuple[str, UUID], list[tuple[str, date]]] = defaultdict(list)
    for (actor_name, organization), observations in relation_matches.items():
        seats = {seat_id for seat_id, _ in observations}
        if len(seats) != 1:
            ambiguous += len(observations)
            continue
        seat_id = next(iter(seats))
        actor_seat_evidence[(actor_name, seat_id)].extend(
            (organization, trade_date) for _, trade_date in observations
        )

    actors_by_seat: dict[UUID, set[str]] = defaultdict(set)
    for actor_name, seat_id in actor_seat_evidence:
        actors_by_seat[seat_id].add(actor_name)
    conflicting_seats = {seat_id for seat_id, values in actors_by_seat.items() if len(values) > 1}
    seats_by_organization: dict[str, set[UUID]] = defaultdict(set)
    for (_, seat_id), evidence in actor_seat_evidence.items():
        for organization, _ in evidence:
            seats_by_organization[organization].add(seat_id)
    conflicting_organizations = {
        organization for organization, seats in seats_by_organization.items() if len(seats) > 1
    }
    mappings = tuple(
        _mapping(
            actor_codes[actor_name],
            seat_id,
            evidence,
            catalog_version=catalog_version,
            reviewed_at=reviewed_at,
        )
        for (actor_name, seat_id), evidence in sorted(
            actor_seat_evidence.items(), key=lambda item: (item[0][0], str(item[0][1]))
        )
        if seat_id not in conflicting_seats
        and not any(organization in conflicting_organizations for organization, _ in evidence)
    )
    catalog = HotMoneyCatalog(catalog_version, reviewed_at, actors, mappings)
    return HotMoneyCatalogBuildResult(
        catalog,
        len(mappings),
        unmatched,
        ambiguous,
        len(conflicting_seats),
        len(conflicting_organizations),
    )


def _roster_entry(row: Mapping[str, object]) -> TushareHotMoneyRosterEntry:
    name = _text(row, "name")
    description = str(row.get("desc") or "").strip()
    raw_organizations = row.get("orgs")
    organizations: object
    if isinstance(raw_organizations, str) and raw_organizations.strip().startswith("["):
        try:
            organizations = json.loads(raw_organizations)
        except json.JSONDecodeError as error:
            raise ProviderError("Tushare hm_list organizations are invalid") from error
    elif isinstance(raw_organizations, str):
        organizations = re.split(r"[\u3001\uff0c,]", raw_organizations)
    else:
        organizations = None
    if not isinstance(organizations, list) or not all(
        isinstance(item, str) and item.strip() for item in organizations
    ):
        raise ProviderError("Tushare hm_list organizations are invalid")
    return TushareHotMoneyRosterEntry(
        name,
        description,
        tuple(dict.fromkeys(item.strip() for item in organizations)),
    )


def _detail(row: Mapping[str, object]) -> TushareHotMoneyDetail:
    trade_date = _compact_date(_text(row, "trade_date"))
    return TushareHotMoneyDetail(
        trade_date,
        _standard_symbol(_text(row, "ts_code")),
        _decimal(row.get("buy_amount")),
        _decimal(row.get("sell_amount")),
        _text(row, "hm_name"),
        _text(row, "hm_orgs"),
    )


def _mapping(
    actor_code: str,
    seat_id: UUID,
    evidence: Sequence[tuple[str, date]],
    *,
    catalog_version: str,
    reviewed_at: datetime,
) -> HotMoneySeatMapping:
    dates = [trade_date for _, trade_date in evidence]
    organizations = sorted({organization for organization, _ in evidence})
    return HotMoneySeatMapping(
        actor_code=actor_code,
        seat_id=seat_id,
        source_alias_name=organizations[0],
        valid_from=min(dates),
        valid_to=None,
        evidence_note=(
            "Tushare hm_list/hm_detail exact transaction-fact match after "
            "half-up normalization to CNY 100 precision; "
            f"observations={len(evidence)}; first={min(dates)}; last={max(dates)}; "
            f"source_aliases={len(organizations)}"
        ),
        review_status=HotMoneyReviewStatus.APPROVED,
        reviewed_at=reviewed_at,
        catalog_version=catalog_version,
    )


def _actor_code(name: str) -> str:
    return f"HM_{sha256(name.encode('utf-8')).hexdigest()[:12].upper()}"


def _month_windows(start_date: date, end_date: date) -> tuple[tuple[date, date], ...]:
    windows: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        month_end = date(current.year, current.month, monthrange(current.year, current.month)[1])
        window_end = min(month_end, end_date)
        windows.append((current, window_end))
        current = date(
            window_end.year + (window_end.month == 12),
            1 if window_end.month == 12 else window_end.month + 1,
            1,
        )
    return tuple(windows)


def _fact_key(
    value: TushareHotMoneyDetail | StableSeatTradeFact,
) -> tuple[date, str, Decimal | None, Decimal | None]:
    return (
        value.trade_date,
        value.symbol,
        _normalized_amount(value.buy_amount),
        _normalized_amount(value.sell_amount),
    )


def _normalized_amount(value: Decimal | None) -> Decimal | None:
    return None if value is None else value.quantize(AMOUNT_PRECISION, rounding=ROUND_HALF_UP)


def _standard_symbol(value: str) -> str:
    candidate = value.strip().upper()
    if "." not in candidate:
        raise ProviderError("Tushare hm_detail symbol is invalid")
    code, suffix = candidate.split(".", maxsplit=1)
    exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}.get(suffix)
    if exchange is None or len(code) != 6 or not code.isdigit():
        raise ProviderError("Tushare hm_detail symbol is invalid")
    return f"{exchange}:{code}"


def _compact_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as error:
        raise ProviderError("Tushare hot-money date is invalid") from error


def _decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation as error:
        raise ProviderError("Tushare hot-money amount is invalid") from error


def _text(row: Mapping[str, object], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ProviderError(f"Tushare hot-money row missing {field}")
    return value.strip()
