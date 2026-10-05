"""Typed hand-offs for top10 float-holder request preparation and publication."""

from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from market_data_center.domain import (
    DatasetCode,
    IngestionEnvelope,
    IngestionRun,
    QualityResult,
    QualitySeverity,
    QualityStatus,
    RawManifest,
    Top10FloatHolderRecord,
)


@dataclass(frozen=True, slots=True)
class PreparedTop10FloatHolderBatch:
    """One Tushare request captured and validated before aggregate publication."""

    run: IngestionRun
    manifest: RawManifest | None
    records: tuple[IngestionEnvelope[Top10FloatHolderRecord], ...]
    quality_results: tuple[QualityResult, ...] = ()


@dataclass(frozen=True, slots=True)
class Top10FloatHolderSyncSummary:
    """Outcome of one weekly or manual announcement-window synchronization."""

    request_count: int
    fetched_rows: int
    accepted_rows: int
    superseded_request_count: int
    rejected_rows: int = 0
    announcement_days: int = 0

    def __post_init__(self) -> None:
        counts = (
            self.request_count,
            self.fetched_rows,
            self.accepted_rows,
            self.superseded_request_count,
            self.rejected_rows,
        )
        if min(counts) < 0:
            raise ValueError("top10 float-holder synchronization counts cannot be negative")
        if self.accepted_rows + self.rejected_rows > self.fetched_rows:
            raise ValueError("accounted top10 float-holder rows cannot exceed fetched rows")
        if self.superseded_request_count > self.request_count:
            raise ValueError("superseded requests cannot exceed total requests")


def top10_float_holder_split_quality_result(
    *, quality_result_id: UUID, ingestion_id: UUID, natural_key: dict[str, object]
) -> QualityResult:
    return QualityResult(
        quality_result_id=quality_result_id,
        ingestion_id=ingestion_id,
        dataset_code=DatasetCode.TOP10_FLOAT_HOLDER,
        rule_code="top10_float_holder.response_split",
        severity=QualitySeverity.INFO,
        status=QualityStatus.PASSED,
        message=(
            "Tushare response reached its row limit and was superseded "
            "by per-report-period split requests"
        ),
        natural_key=natural_key,
    )


def top10_float_holder_rejection_quality_results(
    rejected: tuple[tuple[Top10FloatHolderRecord, str], ...],
    *,
    ingestion_id: UUID,
    uuid_factory: Callable[[], UUID],
) -> tuple[QualityResult, ...]:
    return tuple(
        QualityResult(
            quality_result_id=uuid_factory(),
            ingestion_id=ingestion_id,
            dataset_code=DatasetCode.TOP10_FLOAT_HOLDER,
            rule_code="top10_float_holder.invalid_record",
            severity=QualitySeverity.ERROR,
            status=QualityStatus.FAILED,
            message=reason,
            natural_key={
                "symbol": record.symbol,
                "end_date": record.end_date.isoformat(),
                "announcement_date": record.announcement_date.isoformat(),
                "holder_rank": record.holder_rank,
            },
        )
        for record, reason in rejected
    )
