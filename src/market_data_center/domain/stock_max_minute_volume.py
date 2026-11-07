"""Source quantities and deterministic daily peak; no persistence lineage in records."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID

ALGORITHM_VERSION = "stock_max_minute_volume.v1"


@dataclass(frozen=True, slots=True)
class MinuteVolumeBar:
    timestamp: datetime
    volume_shares: int


@dataclass(frozen=True, slots=True)
class MinuteVolumeSource:
    symbol: str
    trade_date: date
    bars: tuple[MinuteVolumeBar, ...]
    raw_rows: tuple[Mapping[str, str], ...]
    metadata: Mapping[str, object]
    quality_reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MinuteVolumePeak:
    symbol: str
    trade_date: date
    max_minute_volume_shares: int | None
    peak_at: datetime | None
    expected_count: int
    observed_count: int
    quality_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MinuteVolumeBuildSummary:
    status: str
    trade_date: date
    calculation_id: UUID | None
    version: int | None
    candidate_count: int
    complete_count: int
