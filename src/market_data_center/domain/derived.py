"""Provider-neutral inputs and outputs for deterministic derived calculations."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Self
from uuid import UUID

from market_data_center.domain.classification import ClassificationType
from market_data_center.domain.records import (
    DailyBarRecord,
    DistributionRecord,
    RightsIssueRecord,
    ShareCapitalRecord,
)


class AdjustmentType(StrEnum):
    FORWARD = "forward"
    BACKWARD = "backward"


class CalculationMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"


class CalculationStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ClassificationMembershipSnapshot:
    namespace: str
    classification_type: ClassificationType
    classification_code: str
    snapshot_date: date
    members: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.namespace.strip() or not self.classification_code.strip():
            raise ValueError("classification identity must not be blank")
        if len(self.members) != len(set(self.members)):
            raise ValueError("classification members must be unique")


@dataclass(frozen=True, slots=True)
class DerivedCalculationInput:
    daily_bars: tuple[DailyBarRecord, ...]
    distributions: tuple[DistributionRecord, ...]
    rights_issues: tuple[RightsIssueRecord, ...]
    share_capital: tuple[ShareCapitalRecord, ...]
    memberships: tuple[ClassificationMembershipSnapshot, ...]


@dataclass(frozen=True, slots=True)
class AdjustedDailyBarRecord:
    symbol: str
    trade_date: date
    adjustment_type: AdjustmentType
    adjustment_factor: Decimal
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    previous_close: Decimal | None

    def __post_init__(self) -> None:
        if self.adjustment_factor <= 0:
            raise ValueError("adjustment_factor must be positive")


@dataclass(frozen=True, slots=True)
class DailyMetricRecord:
    symbol: str
    trade_date: date
    total_return_1d: Decimal | None
    moving_average_5: Decimal | None
    moving_average_10: Decimal | None
    moving_average_20: Decimal | None


@dataclass(frozen=True, slots=True)
class MarketCapitalizationRecord:
    symbol: str
    trade_date: date
    total_market_cap: Decimal
    circulating_market_cap: Decimal | None

    def __post_init__(self) -> None:
        if self.total_market_cap < 0:
            raise ValueError("total_market_cap must not be negative")
        if self.circulating_market_cap is not None and self.circulating_market_cap < 0:
            raise ValueError("circulating_market_cap must not be negative")


@dataclass(frozen=True, slots=True)
class ClassificationDailyMetricRecord:
    namespace: str
    classification_type: ClassificationType
    classification_code: str
    membership_snapshot_date: date
    trade_date: date
    member_count: int
    priced_member_count: int
    advancing_count: int
    declining_count: int
    unchanged_count: int
    total_volume: int
    total_amount: Decimal
    equal_weight_return: Decimal | None
    total_market_cap: Decimal | None
    market_cap_member_count: int

    def __post_init__(self) -> None:
        counts = (
            self.member_count,
            self.priced_member_count,
            self.advancing_count,
            self.declining_count,
            self.unchanged_count,
            self.total_volume,
            self.market_cap_member_count,
        )
        if any(value < 0 for value in counts):
            raise ValueError("classification metric counts must not be negative")
        if self.priced_member_count > self.member_count:
            raise ValueError("priced_member_count must not exceed member_count")
        if self.advancing_count + self.declining_count + self.unchanged_count != (
            self.priced_member_count
        ):
            raise ValueError("direction counts must equal priced_member_count")


@dataclass(frozen=True, slots=True)
class DerivedCalculationOutput:
    adjusted_daily_bars: tuple[AdjustedDailyBarRecord, ...]
    daily_metrics: tuple[DailyMetricRecord, ...]
    market_capitalizations: tuple[MarketCapitalizationRecord, ...]
    classification_metrics: tuple[ClassificationDailyMetricRecord, ...]


@dataclass(frozen=True, slots=True)
class CalculationRun:
    calculation_id: UUID
    calculation_code: str
    algorithm_version: str
    mode: CalculationMode
    start_date: date
    end_date: date
    status: CalculationStatus
    input_watermark: dict[str, str | None]
    input_hash: str
    requested_at: datetime
    calculated_at: datetime | None = None
    finished_at: datetime | None = None
    output_rows: int = 0
    error_summary: str | None = None

    def __post_init__(self) -> None:
        if not self.calculation_code.strip() or not self.algorithm_version.strip():
            raise ValueError("calculation code and algorithm version must not be blank")
        if self.end_date < self.start_date:
            raise ValueError("end_date must not precede start_date")
        if len(self.input_hash) != 64 or any(c not in "0123456789abcdef" for c in self.input_hash):
            raise ValueError("input_hash must be a lowercase SHA-256 digest")
        if self.output_rows < 0:
            raise ValueError("output_rows must not be negative")

    def succeeded(self, *, finished_at: datetime, output_rows: int) -> Self:
        return type(self)(
            calculation_id=self.calculation_id,
            calculation_code=self.calculation_code,
            algorithm_version=self.algorithm_version,
            mode=self.mode,
            start_date=self.start_date,
            end_date=self.end_date,
            status=CalculationStatus.SUCCEEDED,
            input_watermark=self.input_watermark,
            input_hash=self.input_hash,
            requested_at=self.requested_at,
            calculated_at=finished_at,
            finished_at=finished_at,
            output_rows=output_rows,
        )

    def failed(self, *, finished_at: datetime, error_summary: str) -> Self:
        return type(self)(
            calculation_id=self.calculation_id,
            calculation_code=self.calculation_code,
            algorithm_version=self.algorithm_version,
            mode=self.mode,
            start_date=self.start_date,
            end_date=self.end_date,
            status=CalculationStatus.FAILED,
            input_watermark=self.input_watermark,
            input_hash=self.input_hash,
            requested_at=self.requested_at,
            finished_at=finished_at,
            error_summary=error_summary,
        )
