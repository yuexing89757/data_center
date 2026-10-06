"""Typed hand-offs for board money-flow request preparation and publication."""

from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from market_data_center.domain import (
    BoardMoneyflowRecord,
    DatasetCode,
    IngestionEnvelope,
    IngestionRun,
    QualityResult,
    QualitySeverity,
    QualityStatus,
    RawManifest,
)


@dataclass(frozen=True, slots=True)
class PreparedBoardMoneyflowBatch:
    """One Tushare request captured and validated before aggregate publication."""

    run: IngestionRun
    manifest: RawManifest | None
    records: tuple[IngestionEnvelope[BoardMoneyflowRecord], ...]
    quality_results: tuple[QualityResult, ...] = ()


@dataclass(frozen=True, slots=True)
class BoardMoneyflowSyncSummary:
    """Outcome of one daily or backfill money-flow synchronization."""

    request_count: int
    fetched_rows: int
    accepted_rows: int
    rejected_rows: int = 0
    trade_days: int = 0

    def __post_init__(self) -> None:
        counts = (self.request_count, self.fetched_rows, self.accepted_rows, self.rejected_rows)
        if min(counts) < 0:
            raise ValueError("board money-flow synchronization counts cannot be negative")
        if self.accepted_rows + self.rejected_rows > self.fetched_rows:
            raise ValueError("accounted board money-flow rows cannot exceed fetched rows")


def board_moneyflow_rejection_quality_results(
    rejected: tuple[tuple[BoardMoneyflowRecord, str], ...],
    *,
    ingestion_id: UUID,
    uuid_factory: Callable[[], UUID],
) -> tuple[QualityResult, ...]:
    return tuple(
        QualityResult(
            quality_result_id=uuid_factory(),
            ingestion_id=ingestion_id,
            dataset_code=DatasetCode.BOARD_MONEYFLOW,
            rule_code="board_moneyflow.invalid_record",
            severity=QualitySeverity.ERROR,
            status=QualityStatus.FAILED,
            message=reason,
            natural_key={
                "board_code": record.board_code,
                "trade_date": record.trade_date.isoformat(),
            },
        )
        for record, reason in rejected
    )
