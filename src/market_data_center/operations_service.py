"""Application service for durable workflow and step execution records."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import TypeVar

from market_data_center.domain.ingestion import IngestionRun, IngestionStatus
from market_data_center.domain.operations import (
    ExecutionStatus,
    TriggerSource,
    WorkflowCode,
    WorkflowRun,
)
from market_data_center.domain.stock_pool import StockPoolBuildSummary
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence

T = TypeVar("T")


class RecordedStepFailed(RuntimeError):
    """A completed ingestion result reported a terminal failure."""


class WorkflowExecution:
    def __init__(
        self,
        persistence: PostgreSQLOperationsPersistence,
        run: WorkflowRun,
    ) -> None:
        self._persistence = persistence
        self.run = run
        self._accepted_rows = 0
        self._rejected_rows = 0
        self._partial = False

    def step(self, job_code: str, sequence_no: int, operation: Callable[[], T]) -> T:
        job = self._persistence.start_job(self.run.workflow_run_id, job_code, sequence_no)
        try:
            result = operation()
        except BaseException as error:
            self._persistence.finish_job(
                job.finish(
                    ExecutionStatus.FAILED,
                    datetime.now(UTC),
                    error_summary=safe_error_summary(error),
                )
            )
            raise
        fetched, accepted, rejected, status = _result_statistics(result)
        self._accepted_rows += accepted
        self._rejected_rows += rejected
        self._partial = self._partial or status is ExecutionStatus.PARTIAL
        self._persistence.finish_job(
            job.finish(
                status,
                datetime.now(UTC),
                fetched_rows=fetched,
                accepted_rows=accepted,
                rejected_rows=rejected,
            )
        )
        if status is ExecutionStatus.FAILED:
            raise RecordedStepFailed(f"recorded step failed: {job_code}")
        return result

    def succeed(self) -> None:
        status = ExecutionStatus.PARTIAL if self._partial else ExecutionStatus.SUCCEEDED
        self.run = self.run.finish(
            status,
            datetime.now(UTC),
            accepted_rows=self._accepted_rows,
            rejected_rows=self._rejected_rows,
        )
        self._persistence.finish_workflow(self.run)

    def fail(self, error: BaseException) -> None:
        self.run = self.run.finish(
            ExecutionStatus.FAILED,
            datetime.now(UTC),
            accepted_rows=self._accepted_rows,
            rejected_rows=self._rejected_rows,
            error_summary=safe_error_summary(error),
        )
        self._persistence.finish_workflow(self.run)


class WorkflowExecutionService:
    def __init__(self, persistence: PostgreSQLOperationsPersistence) -> None:
        self._persistence = persistence

    def start(
        self,
        workflow_code: WorkflowCode,
        scheduled_for: datetime,
        trigger_source: TriggerSource,
    ) -> WorkflowExecution:
        return WorkflowExecution(
            self._persistence,
            self._persistence.start_workflow(workflow_code, scheduled_for, trigger_source),
        )


def safe_error_summary(error: BaseException) -> str:
    """Return a bounded category without exception text, parameters, paths, or secrets."""
    return type(error).__name__[:200]


def _result_statistics(result: object) -> tuple[int, int, int, ExecutionStatus]:
    if isinstance(result, StockPoolBuildSummary):
        return (
            result.candidate_count,
            result.member_count,
            result.rejected_count,
            ExecutionStatus.PARTIAL if result.rejected_count else ExecutionStatus.SUCCEEDED,
        )
    if not isinstance(result, IngestionRun):
        return 0, 0, 0, ExecutionStatus.SUCCEEDED
    status = {
        IngestionStatus.SUCCEEDED: ExecutionStatus.SUCCEEDED,
        IngestionStatus.PARTIAL: ExecutionStatus.PARTIAL,
        IngestionStatus.FAILED: ExecutionStatus.FAILED,
        IngestionStatus.PENDING: ExecutionStatus.FAILED,
        IngestionStatus.RUNNING: ExecutionStatus.FAILED,
    }[result.status]
    return result.fetched_rows, result.accepted_rows, result.rejected_rows, status
