"""Typed hand-offs for shareholder-count request preparation and publication."""

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
    ShareholderCountRecord,
)


@dataclass(frozen=True, slots=True)
class PreparedShareholderCountBatch:
    """One Tushare request captured and validated before aggregate publication."""

    run: IngestionRun
    manifest: RawManifest | None
    records: tuple[IngestionEnvelope[ShareholderCountRecord], ...]
    quality_results: tuple[QualityResult, ...] = ()


@dataclass(frozen=True, slots=True)
class ShareholderCountSyncSummary:
    """Outcome of one controlled daily or backfill synchronization."""

    request_count: int
    fetched_rows: int
    accepted_rows: int
    superseded_request_count: int
    rejected_rows: int = 0

    def __post_init__(self) -> None:
        counts = (
            self.request_count,
            self.fetched_rows,
            self.accepted_rows,
            self.superseded_request_count,
            self.rejected_rows,
        )
        if min(counts) < 0:
            raise ValueError("shareholder-count synchronization counts cannot be negative")
        if self.accepted_rows + self.rejected_rows > self.fetched_rows:
            raise ValueError("accounted shareholder-count rows cannot exceed fetched rows")
        if self.superseded_request_count > self.request_count:
            raise ValueError("superseded requests cannot exceed total requests")


def shareholder_count_missing_source_quality_result(
    *, quality_result_id: UUID, ingestion_id: UUID, rejected_rows: int
) -> QualityResult:
    if rejected_rows <= 0:
        raise ValueError("missing shareholder-count source rows must be positive")
    return QualityResult(
        quality_result_id=quality_result_id,
        ingestion_id=ingestion_id,
        dataset_code=DatasetCode.SHAREHOLDER_COUNT,
        rule_code="shareholder_count.missing_source_value",
        severity=QualitySeverity.ERROR,
        status=QualityStatus.FAILED,
        message="Source rows with missing shareholder count were omitted from Core",
        details={"rejected_rows": rejected_rows},
    )
