"""Shared manual/scheduled cleanup boundary; preview has no write side effects."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Event
from time import monotonic

from sqlalchemy import Engine

from market_data_center.data_cleanup_service import DataCleanupFailure, DataCleanupService
from market_data_center.history_cleanup_service import HistoryCleanupResult, HistoryCleanupService
from market_data_center.operations_service import WorkflowExecution
from market_data_center.persistence.history_retention_postgres import HistoryRetentionPersistence
from market_data_center.persistence.postgres import PostgreSQLPersistence
from market_data_center.persistence.quality_archive_postgres import QualityArchivePersistence
from market_data_center.persistence.raw_compression_postgres import RawCompressionPersistence
from market_data_center.persistence.raw_retention_postgres import RawRetentionPersistence
from market_data_center.quality_cleanup_service import (
    CleanupBudget,
    QualityCleanupFailure,
    QualityCleanupResult,
    QualityCleanupService,
    maintenance_seconds,
    quality_cutoff,
)
from market_data_center.raw_cleanup_service import (
    RawCleanupFailure,
    RawCleanupResult,
    RawCleanupService,
    raw_cutoff,
)
from market_data_center.raw_retention_service import RawRetentionResult, RawRetentionService
from market_data_center.settings import WorkerSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CleanupWorkflowResult:
    quality: QualityCleanupResult
    raw: RawCleanupResult | None = None
    history: HistoryCleanupResult | None = None
    raw_retention: RawRetentionResult | None = None

    @property
    def errors(self) -> tuple[str, ...]:
        return (
            self.quality.errors
            + (self.raw.errors if self.raw else ())
            + (self.history.errors if self.history else ())
            + (self.raw_retention.errors if self.raw_retention else ())
        )

    @property
    def partial(self) -> bool:
        return bool(self.errors)

    def report(self) -> dict[str, object]:
        return {
            "quality": self.quality.report(),
            "raw": self.raw.report() if self.raw else {"skipped": True},
            "history": self.history.report() if self.history else {"skipped": True},
            "raw_retention": self.raw_retention.report()
            if self.raw_retention
            else {"skipped": True},
            "partial": self.partial,
        }


def run_cleanup_workflow(
    engine: Engine,
    settings: WorkerSettings,
    execution: WorkflowExecution,
    *,
    now: datetime,
    scheduled: bool,
) -> CleanupWorkflowResult:
    from zoneinfo import ZoneInfo

    reference_date = now.astimezone(ZoneInfo("Asia/Shanghai")).date()
    repo = QualityArchivePersistence(engine)
    service = QualityCleanupService(repo, settings.raw_data_root)
    run_id = execution.run.workflow_run_id
    duration = maintenance_seconds(now, scheduled=scheduled)
    budget = CleanupBudget(monotonic() + duration)
    repo.start_report(
        run_id,
        reference_date,
        now,
        quality_cutoff(reference_date),
        service.disk_report(),
        raw_before=raw_cutoff(reference_date),
    )
    report: dict[str, object] = {}
    status = "failed"
    raw_result = None
    history_result = None
    retention_result = None
    try:
        if not duration:
            result = execution.step(
                "archive_auction_quality",
                3,
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
                raw_result = execution.step(
                    "compress_raw",
                    2,
                    lambda: RawCleanupService(
                        RawCompressionPersistence(engine), settings.raw_data_root
                    ).run(
                        reference_date,
                        run_id,
                        CleanupBudget(
                            min(budget.deadline - 900, monotonic() + 600), cancelled=lost.is_set
                        ),
                    ),
                )
                report["raw"] = raw_result.report()
                result = execution.step(
                    "archive_auction_quality",
                    3,
                    lambda: service.run(
                        reference_date,
                        run_id,
                        CleanupBudget(
                            min(budget.deadline - 900, monotonic() + 300), cancelled=lost.is_set
                        ),
                    ),
                )
                report["quality"] = result.report()
                history_result = execution.step(
                    "archive_history_results",
                    4,
                    lambda: HistoryCleanupService(
                        HistoryRetentionPersistence(engine), settings.raw_data_root
                    ).run(
                        reference_date,
                        run_id,
                        CleanupBudget(
                            min(budget.deadline - 300, monotonic() + 600), cancelled=lost.is_set
                        ),
                    ),
                )
                report["history"] = history_result.report()
                retention_result = execution.step(
                    "purge_expired_raw",
                    5,
                    lambda: RawRetentionService(
                        RawRetentionPersistence(engine),
                        settings.raw_data_root,
                        additional_roots=(settings.raw_retention_api_root,)
                        if settings.raw_retention_api_root
                        else (),
                    ).run(
                        reference_date,
                        CleanupBudget(
                            min(budget.deadline, monotonic() + 300), cancelled=lost.is_set
                        ),
                    ),
                )
                report["raw_retention"] = retention_result.report()
        report["quality"] = result.report()
        combined = CleanupWorkflowResult(result, raw_result, history_result, retention_result)
        # Reporting must not restart a global scan after work or budget expiry.
        report["remaining"] = {"counts_complete": False, "has_more": combined.partial}
        status = "partial" if combined.partial else "succeeded"
        return combined
    except BaseException as error:
        if isinstance(error, QualityCleanupFailure):
            report["quality"] = error.result.report()
        if isinstance(error, RawCleanupFailure):
            report["raw"] = error.result.report()
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
