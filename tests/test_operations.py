from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

import pytest

from market_data_center.domain.operations import (
    ExecutionStatus,
    JobExecution,
    TriggerSource,
    WorkflowCode,
    WorkflowRun,
)
from market_data_center.operations_service import WorkflowExecutionService, safe_error_summary
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
from market_data_center.scheduling_catalog import WORKFLOW_DEFINITIONS, job_definitions
from market_data_center.settings import SchedulerSettings

NOW = datetime(2026, 8, 2, 10, tzinfo=UTC)


class MemoryOperationsPersistence:
    def __init__(self) -> None:
        self.finished_jobs: list[JobExecution] = []
        self.finished_workflows: list[WorkflowRun] = []

    def start_workflow(
        self, workflow_code: WorkflowCode, scheduled_for: datetime, trigger_source: TriggerSource
    ) -> WorkflowRun:
        return WorkflowRun(
            uuid4(),
            workflow_code,
            scheduled_for,
            trigger_source,
            1,
            ExecutionStatus.RUNNING,
            NOW,
        )

    def start_job(self, workflow_run_id: object, job_code: str, sequence_no: int) -> JobExecution:
        return JobExecution(
            uuid4(), workflow_run_id, job_code, sequence_no, 1, ExecutionStatus.RUNNING, NOW
        )  # type: ignore[arg-type]

    def finish_job(self, job: JobExecution) -> None:
        self.finished_jobs.append(job)

    def finish_workflow(self, run: WorkflowRun) -> None:
        self.finished_workflows.append(run)


def test_workflow_and_job_allow_only_running_to_terminal_transitions() -> None:
    run = WorkflowRun(
        uuid4(),
        WorkflowCode.DAILY_MARKET,
        NOW,
        TriggerSource.SCHEDULED,
        1,
        ExecutionStatus.RUNNING,
        NOW,
    )
    completed = run.finish(ExecutionStatus.SUCCEEDED, NOW + timedelta(seconds=2))
    job = JobExecution(uuid4(), run.workflow_run_id, "security", 1, 1, ExecutionStatus.RUNNING, NOW)
    partial = job.finish(
        ExecutionStatus.PARTIAL,
        NOW + timedelta(seconds=1),
        fetched_rows=10,
        accepted_rows=9,
        rejected_rows=1,
    )

    assert completed.status is ExecutionStatus.SUCCEEDED
    assert partial.rejected_rows == 1
    with pytest.raises(ValueError, match="only running workflows"):
        completed.finish(ExecutionStatus.FAILED, NOW + timedelta(seconds=3))
    with pytest.raises(ValueError, match="cannot exceed"):
        job.finish(
            ExecutionStatus.FAILED,
            NOW + timedelta(seconds=1),
            fetched_rows=1,
            accepted_rows=1,
            rejected_rows=1,
        )


def test_error_summary_is_category_only() -> None:
    error = RuntimeError("token=secret database=/private/path")

    assert safe_error_summary(error) == "RuntimeError"


def test_job_catalog_is_stable_and_references_defined_workflows() -> None:
    workflows = {definition.code: definition for definition in WORKFLOW_DEFINITIONS}
    jobs = job_definitions(SchedulerSettings())

    assert len({job.code for job in jobs}) == len(jobs)
    assert all(job.workflow_code in workflows for job in jobs)
    assert workflows["daily_market"].step_codes == ("security", "trading_calendar", "daily_bar")
    assert workflows["stock_pool"].step_codes == ("build_stock_pools",)
    assert all(job.timezone == "Asia/Shanghai" for job in jobs)


def test_execution_service_records_failed_step_and_redacted_workflow_error() -> None:
    persistence = MemoryOperationsPersistence()
    execution = WorkflowExecutionService(cast(PostgreSQLOperationsPersistence, persistence)).start(
        WorkflowCode.DAILY_MARKET, NOW, TriggerSource.SCHEDULED
    )

    with pytest.raises(RuntimeError, match="sensitive message") as captured:
        execution.step(
            "security", 1, lambda: (_ for _ in ()).throw(RuntimeError("sensitive message"))
        )
    execution.fail(captured.value)

    assert persistence.finished_jobs[0].status is ExecutionStatus.FAILED
    assert persistence.finished_jobs[0].error_summary == "RuntimeError"
    assert persistence.finished_workflows[0].error_summary == "RuntimeError"
