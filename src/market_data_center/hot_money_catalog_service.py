"""Load, validate and atomically publish the reviewed hot-money catalog."""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Protocol, cast
from uuid import UUID

from market_data_center.domain.hot_money import (
    HotMoneyActor,
    HotMoneyReviewStatus,
    HotMoneySeatMapping,
)


@dataclass(frozen=True, slots=True)
class HotMoneyCatalog:
    catalog_version: str
    reviewed_at: datetime | None
    actors: tuple[HotMoneyActor, ...]
    mappings: tuple[HotMoneySeatMapping, ...]


@dataclass(frozen=True, slots=True)
class CatalogSyncSummary:
    catalog_version: str
    actor_count: int
    mapping_count: int
    dry_run: bool


class HotMoneyCatalogPersistence(Protocol):
    def replace_catalog(self, catalog: HotMoneyCatalog) -> None: ...


class HotMoneyCatalogService:
    def __init__(self, persistence: HotMoneyCatalogPersistence) -> None:
        self._persistence = persistence

    def sync(self, catalog: HotMoneyCatalog, *, dry_run: bool) -> CatalogSyncSummary:
        if not dry_run:
            self._persistence.replace_catalog(catalog)
        return CatalogSyncSummary(
            catalog.catalog_version, len(catalog.actors), len(catalog.mappings), dry_run
        )


def load_hot_money_catalog(path: Path) -> HotMoneyCatalog:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, Mapping):
        raise ValueError("hot-money catalog must be a JSON object")
    version = _text(document, "catalog_version")
    reviewed_at = _optional_datetime(document.get("reviewed_at"))
    actor_rows = _object_rows(document, "actors")
    mapping_rows = _object_rows(document, "mappings")
    actors = tuple(
        HotMoneyActor(
            actor_code=_text(row, "actor_code"),
            canonical_name=_text(row, "canonical_name"),
            aliases=tuple(_string_list(row, "aliases")),
            is_active=bool(row.get("is_active", True)),
        )
        for row in actor_rows
    )
    actor_codes = [actor.actor_code for actor in actors]
    if len(set(actor_codes)) != len(actor_codes):
        raise ValueError("hot-money catalog contains duplicate actor codes")
    mappings = tuple(
        _mapping(row, version=version, catalog_reviewed_at=reviewed_at) for row in mapping_rows
    )
    if any(mapping.actor_code not in actor_codes for mapping in mappings):
        raise ValueError("hot-money mapping references an unknown actor")
    _reject_overlapping_approved_mappings(mappings)
    return HotMoneyCatalog(version, reviewed_at, actors, mappings)


def _mapping(
    row: Mapping[str, object], *, version: str, catalog_reviewed_at: datetime | None
) -> HotMoneySeatMapping:
    status = HotMoneyReviewStatus(_text(row, "review_status"))
    reviewed_at = None if status is HotMoneyReviewStatus.PENDING else catalog_reviewed_at
    return HotMoneySeatMapping(
        actor_code=_text(row, "actor_code"),
        seat_id=UUID(_text(row, "seat_id")),
        source_alias_name=_text(row, "source_alias_name"),
        valid_from=_optional_date(row.get("valid_from")),
        valid_to=_optional_date(row.get("valid_to")),
        evidence_note=_text(row, "evidence_note"),
        review_status=status,
        reviewed_at=reviewed_at,
        catalog_version=version,
    )


def _reject_overlapping_approved_mappings(
    mappings: Sequence[HotMoneySeatMapping],
) -> None:
    approved = [item for item in mappings if item.review_status is HotMoneyReviewStatus.APPROVED]
    for index, left in enumerate(approved):
        for right in approved[index + 1 :]:
            if left.seat_id == right.seat_id and _ranges_overlap(left, right):
                raise ValueError("approved hot-money seat mappings overlap")


def _ranges_overlap(left: HotMoneySeatMapping, right: HotMoneySeatMapping) -> bool:
    left_starts_before_right_ends = right.valid_to is None or (
        left.valid_from is None or left.valid_from <= right.valid_to
    )
    right_starts_before_left_ends = left.valid_to is None or (
        right.valid_from is None or right.valid_from <= left.valid_to
    )
    return left_starts_before_right_ends and right_starts_before_left_ends


def _object_rows(document: Mapping[str, object], key: str) -> tuple[Mapping[str, object], ...]:
    value = document.get(key)
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"hot-money catalog {key} must be an object array")
    return tuple(cast(Mapping[str, object], item) for item in value)


def _string_list(document: Mapping[str, object], key: str) -> tuple[str, ...]:
    value = document.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"hot-money catalog {key} must be a string array")
    return tuple(cast(str, item) for item in value)


def _text(document: Mapping[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"hot-money catalog {key} must be nonblank text")
    return value.strip()


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("hot-money catalog date must be text or null")
    return date.fromisoformat(value)


def _optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("hot-money catalog reviewed_at must be text or null")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("hot-money catalog reviewed_at must be timezone-aware")
    return parsed
