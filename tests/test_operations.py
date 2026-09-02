from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

import pytest

from market_data_center.call_auction_market_series_service import (
    CallAuctionMarketSeriesSummary,
)
from market_data_center.call_auction_market_service import CallAuctionMarketCollectionSummary
from market_data_center.daily_bar_batch import DailyBarBulkSummary
from market_data_center.domain.close_price_new_highs import ClosePriceNewHighBuildSummary
from market_data_center.domain.operations import (
    ExecutionStatus,
    JobExecution,
    TriggerSource,
    WorkflowCode,
    WorkflowRun,
)
from market_data_center.operations_service import WorkflowExecutionService, safe_error_summary
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
from market_data_center.providers.pytdx_pool import (
    PytdxEndpointPool,
    PytdxPoolRefreshResult,
)
from market_data_center.scheduler import execute_pytdx_pool_refresh
from market_data_center.scheduling_catalog import WORKFLOW_DEFINITIONS, job_definitions
from market_data_center.settings import PytdxPoolSettings, SchedulerSettings
from market_data_center.shareholder_count_batch import ShareholderCountSyncSummary

NOW = datetime(2026, 8, 2, 10, tzinfo=UTC)


def test_dragon_tiger_daily_is_a_distinct_workflow_identity() -> None:
    assert WorkflowCode("dragon_tiger_daily") is WorkflowCode.DRAGON_TIGER_DAILY


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
    jobs = job_definitions(SchedulerSettings(_env_file=None))

    assert len({job.code for job in jobs}) == len(jobs)
    assert all(job.workflow_code in workflows for job in jobs)
    assert workflows["daily_market"].step_codes == ("security", "trading_calendar", "daily_bar")
    assert workflows["stock_pool"].step_codes == ("build_stock_pools",)
    assert workflows["call_auction_market_snapshot"].step_codes == (
        "collect_call_auction_market_snapshot",
    )
    assert workflows["call_auction_market_series"].step_codes == (
        "collect_call_auction_market_series",
    )
    assert workflows["stale_run_recovery"].step_codes[-1] == (
        "recover_call_auction_market_series_sessions"
    )
    assert workflows["call_auction_snapshot"].step_codes == ("finalize_call_auction_snapshot",)
    assert workflows["close_price_new_highs_120d"].step_codes == (
        "build_close_price_new_highs_120d_snapshot",
    )
    assert workflows["board_index_daily_bar"].step_codes == ("collect_board_index_daily_bars",)
    assert workflows["shareholder_count_daily"].step_codes == ("shareholder_count_daily",)
    assert workflows["shareholder_count_backfill"].step_codes == ("shareholder_count_backfill",)
    assert workflows["dragon_tiger_daily"].step_codes == ("collect_dragon_tiger",)
    assert all(job.timezone == "Asia/Shanghai" for job in jobs)
    assert {workflow.value for workflow in WorkflowCode} == set(workflows)


def test_job_catalog_owns_all_fixed_schedules() -> None:
    jobs = {job.code: job for job in job_definitions(SchedulerSettings(_env_file=None))}

    assert "opening-auction-limit-up-quotes" not in jobs
    assert WorkflowCode("auction_collection") is WorkflowCode.AUCTION_COLLECTION
    assert (
        jobs["call-auction-market-series"].hour,
        jobs["call-auction-market-series"].minute,
    ) == (9, 15)
    assert jobs["call-auction-market-series"].cadence_seconds is None
    assert (jobs["daily-run"].hour, jobs["daily-run"].minute) == (20, 0)
    assert (
        jobs["stock-daily-indicators-daily"].hour,
        jobs["stock-daily-indicators-daily"].minute,
    ) == (20, 30)
    assert (
        jobs["mainboard-price-limit-stock-pools-daily"].hour,
        jobs["mainboard-price-limit-stock-pools-daily"].minute,
    ) == (21, 0)
    assert (
        jobs["eod-quote-snapshot-daily"].hour,
        jobs["eod-quote-snapshot-daily"].minute,
    ) == (21, 10)
    assert (
        jobs["close-price-new-highs-120d-daily"].hour,
        jobs["close-price-new-highs-120d-daily"].minute,
    ) == (21, 30)
    assert jobs["close-price-new-highs-120d-daily"].enabled is True
    assert jobs["board-index-883423-daily-bar"].hour == "15-17"
    assert jobs["board-index-883423-daily-bar"].minute == 30
    assert jobs["board-index-883423-daily-bar"].enabled is True
    assert (
        jobs["dragon-tiger-daily"].hour,
        jobs["dragon-tiger-daily"].minute,
    ) == (20, 30)
    assert jobs["dragon-tiger-daily"].enabled is False
    assert (
        jobs["call-auction-market-snapshot-daily"].hour,
        jobs["call-auction-market-snapshot-daily"].minute,
        jobs["call-auction-market-snapshot-daily"].second,
    ) == (9, 25, 30)
    assert jobs["call-auction-market-snapshot-daily"].enabled is True
    assert "call-auction-snapshot-daily" not in jobs
    assert (
        jobs["deducted-profit-daily"].hour,
        jobs["deducted-profit-daily"].minute,
    ) == (20, 0)
    assert jobs["recover-stale-ingestion-runs"].interval_hours == 1
    assert jobs["pytdx-pool-refresh"].interval_hours == 1
    assert all(job.timezone == "Asia/Shanghai" for job in jobs.values())
    assert all(job.timeout_seconds == 21_600 for job in jobs.values())
    shareholder_count = jobs["shareholder-count-daily"]
    assert shareholder_count.workflow_code == "shareholder_count_daily"
    assert shareholder_count.day_of_week is None
    assert (shareholder_count.hour, shareholder_count.minute) == (21, 0)
    assert shareholder_count.timezone == "Asia/Shanghai"
    assert shareholder_count.enabled is False


def test_catalog_registers_hourly_pytdx_pool_refresh() -> None:
    jobs = {job.code: job for job in job_definitions(SchedulerSettings(_env_file=None))}

    refresh = jobs["pytdx-pool-refresh"]
    assert refresh.workflow_code == "pytdx_pool_refresh"
    assert refresh.trigger_type == "interval"
    assert refresh.interval_hours == 1
    assert refresh.enabled is True


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


@pytest.mark.parametrize(
    ("used_last_good", "expected_status"),
    [
        (False, ExecutionStatus.SUCCEEDED),
        (True, ExecutionStatus.PARTIAL),
    ],
)
def test_execution_service_records_pool_refresh_statistics(
    used_last_good: bool, expected_status: ExecutionStatus
) -> None:
    persistence = MemoryOperationsPersistence()
    execution = WorkflowExecutionService(cast(PostgreSQLOperationsPersistence, persistence)).start(
        WorkflowCode.DAILY_MARKET, NOW, TriggerSource.SCHEDULED
    )
    result = PytdxPoolRefreshResult(
        candidate_count=4,
        usable_node_count=3,
        rejected_node_count=1,
        published=not used_last_good,
        used_last_good=used_last_good,
        pool=PytdxEndpointPool(NOW, ()),
    )

    execution.step("refresh_pytdx_pool", 1, lambda: result)
    execution.succeed()

    job = persistence.finished_jobs[0]
    workflow = persistence.finished_workflows[0]
    assert (job.fetched_rows, job.accepted_rows, job.rejected_rows) == (4, 3, 1)
    assert job.status is expected_status
    assert workflow.status is expected_status


def test_execution_service_records_call_auction_market_statistics() -> None:
    persistence = MemoryOperationsPersistence()
    execution = WorkflowExecutionService(cast(PostgreSQLOperationsPersistence, persistence)).start(
        WorkflowCode.CALL_AUCTION_MARKET_SNAPSHOT, NOW, TriggerSource.SCHEDULED
    )
    summary = CallAuctionMarketCollectionSummary(
        status="partial",
        attempts=2,
        expected_rows=5_200,
        accepted_rows=5_199,
        rejected_rows=1,
        ingestion_id=uuid4(),
    )

    execution.step("collect_call_auction_market_snapshot", 1, lambda: summary)
    execution.succeed()

    job = persistence.finished_jobs[0]
    workflow = persistence.finished_workflows[0]
    assert (job.fetched_rows, job.accepted_rows, job.rejected_rows) == (5_200, 5_199, 1)
    assert job.status is ExecutionStatus.PARTIAL
    assert workflow.status is ExecutionStatus.PARTIAL


def test_execution_service_records_shareholder_count_statistics() -> None:
    persistence = MemoryOperationsPersistence()
    execution = WorkflowExecutionService(cast(PostgreSQLOperationsPersistence, persistence)).start(
        WorkflowCode.SHAREHOLDER_COUNT_DAILY, NOW, TriggerSource.SCHEDULED
    )
    summary = ShareholderCountSyncSummary(
        request_count=3,
        fetched_rows=3_002,
        accepted_rows=2,
        rejected_rows=1,
        superseded_request_count=1,
    )

    execution.step("shareholder_count_daily", 1, lambda: summary)
    execution.succeed()

    job = persistence.finished_jobs[0]
    assert (job.fetched_rows, job.accepted_rows, job.rejected_rows) == (3_002, 2, 1)
    assert job.status is ExecutionStatus.PARTIAL


def test_execution_service_records_call_auction_market_series_statistics() -> None:
    persistence = MemoryOperationsPersistence()
    execution = WorkflowExecutionService(cast(PostgreSQLOperationsPersistence, persistence)).start(
        WorkflowCode.CALL_AUCTION_MARKET_SERIES, NOW, TriggerSource.SCHEDULED
    )
    summary = CallAuctionMarketSeriesSummary(
        status="partial",
        expected_rows=166_656,
        accepted_rows=166_650,
        rejected_rows=6,
        session_id=uuid4(),
    )

    execution.step("collect_call_auction_market_series", 1, lambda: summary)
    execution.succeed()

    job = persistence.finished_jobs[0]
    workflow = persistence.finished_workflows[0]
    assert (job.fetched_rows, job.accepted_rows, job.rejected_rows) == (
        166_656,
        166_650,
        6,
    )
    assert job.status is ExecutionStatus.PARTIAL
    assert workflow.status is ExecutionStatus.PARTIAL


def test_execution_service_keeps_daily_bar_gaps_as_partial() -> None:
    persistence = MemoryOperationsPersistence()
    execution = WorkflowExecutionService(cast(PostgreSQLOperationsPersistence, persistence)).start(
        WorkflowCode.DAILY_MARKET, NOW, TriggerSource.SCHEDULED
    )
    summary = DailyBarBulkSummary(
        expected_symbols=5_208,
        accepted_symbols=5_204,
        failed_symbols=3,
        unavailable_symbols=1,
    )

    execution.step("daily_bar", 3, lambda: summary)
    execution.succeed()

    job = persistence.finished_jobs[0]
    workflow = persistence.finished_workflows[0]
    assert (job.fetched_rows, job.accepted_rows, job.rejected_rows) == (5_208, 5_204, 4)
    assert job.status is ExecutionStatus.PARTIAL
    assert workflow.status is ExecutionStatus.PARTIAL


def test_execution_service_records_integer_finalization_rows() -> None:
    persistence = MemoryOperationsPersistence()
    execution = WorkflowExecutionService(cast(PostgreSQLOperationsPersistence, persistence)).start(
        WorkflowCode.CALL_AUCTION_SNAPSHOT, NOW, TriggerSource.SCHEDULED
    )

    execution.step("finalize_call_auction_snapshot", 1, lambda: 2)
    execution.succeed()

    job = persistence.finished_jobs[0]
    workflow = persistence.finished_workflows[0]
    assert (job.fetched_rows, job.accepted_rows, job.rejected_rows) == (2, 2, 0)
    assert job.status is ExecutionStatus.SUCCEEDED
    assert workflow.accepted_rows == 2


def test_execution_service_records_close_price_new_high_snapshot_statistics() -> None:
    persistence = MemoryOperationsPersistence()
    execution = WorkflowExecutionService(cast(PostgreSQLOperationsPersistence, persistence)).start(
        WorkflowCode.CLOSE_PRICE_NEW_HIGHS_120D, NOW, TriggerSource.SCHEDULED
    )
    summary = ClosePriceNewHighBuildSummary(
        status="succeeded",
        calculation_id=uuid4(),
        snapshot_id=uuid4(),
        trade_date=NOW.date(),
        candidate_count=5_200,
        member_count=88,
        omitted_count=12,
    )

    execution.step("build_close_price_new_highs_120d_snapshot", 1, lambda: summary)
    execution.succeed()

    job = persistence.finished_jobs[0]
    assert (job.fetched_rows, job.accepted_rows, job.rejected_rows) == (5_200, 88, 12)
    assert job.status is ExecutionStatus.PARTIAL


def test_execute_pytdx_pool_refresh_records_the_controlled_workflow(tmp_path) -> None:
    persistence = MemoryOperationsPersistence()
    pool_settings = PytdxPoolSettings(pytdx_pool_path=tmp_path / "pytdx_pool.json", _env_file=None)
    expected = PytdxPoolRefreshResult(
        candidate_count=3,
        usable_node_count=3,
        rejected_node_count=0,
        published=True,
        used_last_good=False,
        pool=PytdxEndpointPool(NOW, ()),
    )
    refreshed_paths = []

    def refresh(path):
        refreshed_paths.append(path)
        return expected

    actual = execute_pytdx_pool_refresh(
        cast(PostgreSQLOperationsPersistence, persistence),
        pool_settings,
        TriggerSource.RECOVERY,
        clock=lambda: NOW,
        refresh=refresh,
    )

    assert actual is expected
    assert refreshed_paths == [pool_settings.pytdx_pool_path]
    assert persistence.finished_jobs[0].job_code == "refresh_pytdx_pool"
    assert persistence.finished_workflows[0].workflow_code is WorkflowCode.PYTDX_POOL_REFRESH
    assert persistence.finished_workflows[0].trigger_source is TriggerSource.RECOVERY
