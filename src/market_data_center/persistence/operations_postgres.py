"""PostgreSQL repository for the operations bounded context."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import Engine, text

from market_data_center.domain.operations import (
    ExecutionStatus,
    JobExecution,
    TriggerSource,
    WorkflowCode,
    WorkflowRun,
)


class PostgreSQLOperationsPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def start_workflow(
        self,
        workflow_code: WorkflowCode,
        scheduled_for: datetime,
        trigger_source: TriggerSource,
    ) -> WorkflowRun:
        now = datetime.now(UTC)
        with self._engine.begin() as connection:
            connection.execute(
                text("select pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"operations:{workflow_code.value}:{scheduled_for.isoformat()}"},
            )
            attempt = connection.execute(
                text("""
select coalesce(max(attempt), 0) + 1 from operations.workflow_run
where workflow_code = :workflow_code and scheduled_for = :scheduled_for
"""),
                {"workflow_code": workflow_code.value, "scheduled_for": scheduled_for},
            ).scalar_one()
            run = WorkflowRun(
                workflow_run_id=uuid4(),
                workflow_code=workflow_code,
                scheduled_for=scheduled_for,
                trigger_source=trigger_source,
                attempt=attempt,
                status=ExecutionStatus.RUNNING,
                started_at=now,
            )
            connection.execute(
                text("""
insert into operations.workflow_run (
 workflow_run_id, workflow_code, scheduled_for, trigger_source, attempt, status, started_at
) values (
 :workflow_run_id, :workflow_code, :scheduled_for, :trigger_source, :attempt, :status, :started_at
)
"""),
                _workflow_parameters(run),
            )
        return run

    def start_job(self, workflow_run_id: UUID, job_code: str, sequence_no: int) -> JobExecution:
        now = datetime.now(UTC)
        with self._engine.begin() as connection:
            attempt = connection.execute(
                text("""
select coalesce(max(attempt), 0) + 1 from operations.job_execution
where workflow_run_id = :workflow_run_id and job_code = :job_code
"""),
                {"workflow_run_id": workflow_run_id, "job_code": job_code},
            ).scalar_one()
            job = JobExecution(
                job_execution_id=uuid4(),
                workflow_run_id=workflow_run_id,
                job_code=job_code,
                sequence_no=sequence_no,
                attempt=attempt,
                status=ExecutionStatus.RUNNING,
                started_at=now,
            )
            connection.execute(
                text("""
insert into operations.job_execution (
 job_execution_id, workflow_run_id, job_code, sequence_no, attempt, status, started_at
) values (
 :job_execution_id, :workflow_run_id, :job_code, :sequence_no, :attempt, :status, :started_at
)
"""),
                _job_parameters(job),
            )
        return job

    def finish_workflow(self, run: WorkflowRun) -> None:
        with self._engine.begin() as connection:
            result = connection.execute(
                text("""
update operations.workflow_run set status=:status, finished_at=:finished_at,
 accepted_rows=:accepted_rows, rejected_rows=:rejected_rows, error_summary=:error_summary
where workflow_run_id=:workflow_run_id and status='running'
"""),
                _workflow_parameters(run),
            )
        if result.rowcount != 1:
            raise RuntimeError("workflow is no longer running")

    def finish_job(self, job: JobExecution) -> None:
        with self._engine.begin() as connection:
            result = connection.execute(
                text("""
update operations.job_execution set status=:status, finished_at=:finished_at,
 fetched_rows=:fetched_rows, accepted_rows=:accepted_rows, rejected_rows=:rejected_rows,
 error_summary=:error_summary
where job_execution_id=:job_execution_id and status='running'
"""),
                _job_parameters(job),
            )
        if result.rowcount != 1:
            raise RuntimeError("job is no longer running")

    def recover_stale(self, stale_before: datetime) -> int:
        with self._engine.begin() as connection:
            connection.execute(
                text("""
update operations.job_execution set status='failed', finished_at=greatest(now(), started_at),
 error_summary='worker_interrupted_or_timed_out'
where status='running' and started_at < :stale_before
"""),
                {"stale_before": stale_before},
            )
            result = connection.execute(
                text("""
update operations.workflow_run set status='failed', finished_at=greatest(now(), started_at),
 error_summary='worker_interrupted_or_timed_out'
where status='running' and started_at < :stale_before
"""),
                {"stale_before": stale_before},
            )
        return result.rowcount

    def recent_workflows(self, limit: int = 10) -> tuple[WorkflowRun, ...]:
        if not 1 <= limit <= 50:
            raise ValueError("workflow history limit must be between 1 and 50")
        with self._engine.connect() as connection:
            rows = (
                connection.execute(
                    text("""
select workflow_run_id, workflow_code, scheduled_for, trigger_source, attempt, status,
 started_at, finished_at, accepted_rows, rejected_rows, error_summary
from operations.workflow_run order by started_at desc limit :limit
"""),
                    {"limit": limit},
                )
                .mappings()
                .all()
            )
        return tuple(
            WorkflowRun(
                workflow_run_id=row["workflow_run_id"],
                workflow_code=WorkflowCode(row["workflow_code"]),
                scheduled_for=row["scheduled_for"],
                trigger_source=TriggerSource(row["trigger_source"]),
                attempt=row["attempt"],
                status=ExecutionStatus(row["status"]),
                started_at=row["started_at"],
                finished_at=row["finished_at"],
                accepted_rows=row["accepted_rows"],
                rejected_rows=row["rejected_rows"],
                error_summary=row["error_summary"],
            )
            for row in rows
        )


def _workflow_parameters(run: WorkflowRun) -> dict[str, object]:
    return {
        field: getattr(run, field)
        for field in (
            "workflow_run_id",
            "workflow_code",
            "scheduled_for",
            "trigger_source",
            "attempt",
            "status",
            "started_at",
            "finished_at",
            "accepted_rows",
            "rejected_rows",
            "error_summary",
        )
    }


def _job_parameters(job: JobExecution) -> dict[str, object]:
    return {
        field: getattr(job, field)
        for field in (
            "job_execution_id",
            "workflow_run_id",
            "job_code",
            "sequence_no",
            "attempt",
            "status",
            "started_at",
            "finished_at",
            "fetched_rows",
            "accepted_rows",
            "rejected_rows",
            "error_summary",
        )
    }
