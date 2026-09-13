"""Reviewed hot-money actors and effective-dated stable-seat mappings."""

import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from uuid import UUID

_ACTOR_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
_GENERIC_SEAT_ALIASES = frozenset({"机构专用", "沪股通专用", "深股通专用", "北向资金专用"})


class HotMoneyReviewStatus(StrEnum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    PENDING = "PENDING"


@dataclass(frozen=True, slots=True)
class HotMoneyActor:
    actor_code: str
    canonical_name: str
    aliases: tuple[str, ...]
    is_active: bool = True

    def __post_init__(self) -> None:
        if not _ACTOR_CODE_PATTERN.fullmatch(self.actor_code):
            raise ValueError("hot-money actor code is invalid")
        if not self.canonical_name.strip():
            raise ValueError("hot-money canonical name must not be blank")
        normalized = tuple(alias.strip() for alias in self.aliases)
        if any(not alias for alias in normalized) or len(set(normalized)) != len(normalized):
            raise ValueError("hot-money aliases must be nonblank and unique")
        object.__setattr__(self, "canonical_name", self.canonical_name.strip())
        object.__setattr__(self, "aliases", normalized)


@dataclass(frozen=True, slots=True)
class HotMoneySeatMapping:
    actor_code: str
    seat_id: UUID
    source_alias_name: str
    valid_from: date | None
    valid_to: date | None
    evidence_note: str
    review_status: HotMoneyReviewStatus
    reviewed_at: datetime | None
    catalog_version: str

    def __post_init__(self) -> None:
        if not _ACTOR_CODE_PATTERN.fullmatch(self.actor_code):
            raise ValueError("hot-money mapping actor code is invalid")
        alias = self.source_alias_name.strip()
        if not alias:
            raise ValueError("hot-money source alias must not be blank")
        if alias in _GENERIC_SEAT_ALIASES:
            raise ValueError("generic seat alias cannot map to hot money")
        if (
            self.valid_from is not None
            and self.valid_to is not None
            and self.valid_from > self.valid_to
        ):
            raise ValueError("hot-money mapping valid range is invalid")
        if not self.evidence_note.strip():
            raise ValueError("hot-money mapping evidence must not be blank")
        if not self.catalog_version.strip():
            raise ValueError("hot-money catalog version must not be blank")
        reviewed = self.review_status in {
            HotMoneyReviewStatus.APPROVED,
            HotMoneyReviewStatus.REJECTED,
        }
        if reviewed != (self.reviewed_at is not None):
            raise ValueError("hot-money review status and review timestamp disagree")
        if self.reviewed_at is not None and self.reviewed_at.tzinfo is None:
            raise ValueError("hot-money review timestamp must be timezone-aware")
        object.__setattr__(self, "source_alias_name", alias)
        object.__setattr__(self, "evidence_note", self.evidence_note.strip())
        object.__setattr__(self, "catalog_version", self.catalog_version.strip())
