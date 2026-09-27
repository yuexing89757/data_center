"""Shared manual/scheduled cleanup boundary; preview has no write side effects."""

import logging
from datetime import UTC, datetime
from threading import Event
from time import monotonic

from sqlalchemy import Engine

from market_data_center.data_cleanup_service import DataCleanupFailure, DataCleanupService
from market_data_center.operations_service import WorkflowExecution
from market_data_center.persistence.postgres import PostgreSQLPersistence
from market_data_center.persistence.quality_archive_postgres import QualityArchivePersistence
from market_data_center.quality_cleanup_service import (
    CleanupBudget,
    QualityCleanupFailure,
    QualityCleanupResult,
    QualityCleanupService,
    maintenance_seconds,
    quality_cutoff,
)
from market_data_center.settings import WorkerSettings

logger = logging.getLogger(__name__)


def run_cleanup_workflow(
    engine: Engine,
    settings: WorkerSettings,
    execution: WorkflowExecution,
    *,
    now: datetime,
    scheduled: bool,
) -> QualityCleanupResult:
    from zoneinfo import ZoneInfo

    reference_date = now.astimezone(ZoneInfo("Asia/Shanghai")).date()
    repo = QualityArchivePersistence(engine)
    service = QualityCleanupService(repo, settings.raw_data_root)
    run_id = execution.run.workflow_run_id
    duration = maintenance_seconds(now, scheduled=scheduled)
    budget = CleanupBudget(monotonic() + duration)
    repo.start_report(
        run_id, reference_date, now, quality_cutoff(reference_date), service.disk_report()
    )
    report: dict[str, object] = {}
    status = "failed"
    try:
        if not duration:
            result = execution.step(
                "archive_auction_quality",
                2,
                lambda: QualityCleanupResult(errors=("outside_maintenance_window",)),
            )
        else:
            lost = Event()
            budget.cancelled = lost.is_set
            persistence = PostgreSQLPersistence(engine)
            with persistence.task_lock("maintenance:data_cleanup", on_lock_lost=lost.set):
                budget.checkpoint()
                snapshot = execution.step(
                    "cleanup_call_auction_market_series_snapshots",
                    1,
                    lambda: DataCleanupService(persistence).run(
                        reference_date, checkpoint=budget.checkpoint
                    ),
                )
                report["snapshots"] = {
                    "deleted_rows": snapshot.deleted_rows,
                    "history_deleted_rows": snapshot.history_deleted_rows,
                }
                result = execution.step(
                    "archive_auction_quality",
                    2,
                    lambda: service.run(reference_date, run_id, budget),
                )
        report["quality"] = result.report()
        report["remaining"] = repo.preview(quality_cutoff(reference_date))
        status = "partial" if result.partial else "succeeded"
        return result
    except BaseException as error:
        if isinstance(error, QualityCleanupFailure):
            report["quality"] = error.result.report()
        if isinstance(error, DataCleanupFailure):
            report["snapshots"] = {
                "deleted_rows": error.result.deleted_rows,
                "history_deleted_rows": error.result.history_deleted_rows,
            }
        report["error_code"] = type(error).__name__
        raise
    finally:
        # Never replace the primary error with a report failure. Successful work
        # still fails outward if its durable final report could not be written.
        try:
            disk = service.disk_report()
            repo.finish_report(run_id, datetime.now(UTC), status, report, disk)
            logger.info(
                "data_cleanup_report",
                extra={"cleanup_status": status, "disk": disk, "cleanup_steps": report},
            )
        except Exception:
            if status != "failed":
                raise
            logger.error("cleanup_report_finalization_failed")
