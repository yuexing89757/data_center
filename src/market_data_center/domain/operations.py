"""Operational workflow and job-execution records."""

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from uuid import UUID


class ExecutionStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PARTIAL = "partial"


class TriggerSource(StrEnum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    RECOVERY = "recovery"


class WorkflowCode(StrEnum):
    DAILY_MARKET = "daily_market"
    STOCK_DAILY_INDICATOR = "stock_daily_indicator"
    STALE_RUN_RECOVERY = "stale_run_recovery"
    DEDUCTED_PROFIT = "deducted_profit"
    STOCK_POOL = "stock_pool"


@dataclass(frozen=True, slots=True)
class WorkflowRun:
    workflow_run_id: UUID
    workflow_code: WorkflowCode
    scheduled_for: datetime
    trigger_source: TriggerSource
    attempt: int
    status: ExecutionStatus
    started_at: datetime
    finished_at: datetime | None = None
    accepted_rows: int = 0
    rejected_rows: int = 0
    error_summary: str | None = None

    def finish(
        self,
        status: ExecutionStatus,
        finished_at: datetime,
        *,
        accepted_rows: int = 0,
        rejected_rows: int = 0,
        error_summary: str | None = None,
    ) -> "WorkflowRun":
        if self.status is not ExecutionStatus.RUNNING:
            raise ValueError("only running workflows can finish")
        if status is ExecutionStatus.RUNNING:
            raise ValueError("workflow finish status must be terminal")
        if finished_at < self.started_at:
            raise ValueError("workflow finish cannot precede start")
        return replace(
            self,
            status=status,
            finished_at=finished_at,
            accepted_rows=accepted_rows,
            rejected_rows=rejected_rows,
            error_summary=error_summary,
        )


@dataclass(frozen=True, slots=True)
class JobExecution:
    job_execution_id: UUID
    workflow_run_id: UUID
    job_code: str
    sequence_no: int
    attempt: int
    status: ExecutionStatus
    started_at: datetime
    finished_at: datetime | None = None
    fetched_rows: int = 0
    accepted_rows: int = 0
    rejected_rows: int = 0
    error_summary: str | None = None

    def finish(
        self,
        status: ExecutionStatus,
        finished_at: datetime,
        *,
        fetched_rows: int = 0,
        accepted_rows: int = 0,
        rejected_rows: int = 0,
        error_summary: str | None = None,
    ) -> "JobExecution":
        if self.status is not ExecutionStatus.RUNNING:
            raise ValueError("only running jobs can finish")
        if status is ExecutionStatus.RUNNING:
            raise ValueError("job finish status must be terminal")
        if min(fetched_rows, accepted_rows, rejected_rows) < 0:
            raise ValueError("job row counts cannot be negative")
        if accepted_rows + rejected_rows > fetched_rows:
            raise ValueError("accepted and rejected rows cannot exceed fetched rows")
        if finished_at < self.started_at:
            raise ValueError("job finish cannot precede start")
        return replace(
            self,
            status=status,
            finished_at=finished_at,
            fetched_rows=fetched_rows,
            accepted_rows=accepted_rows,
            rejected_rows=rejected_rows,
            error_summary=error_summary,
        )
