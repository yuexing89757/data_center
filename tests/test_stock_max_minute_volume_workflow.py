from datetime import UTC, date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from market_data_center import cli
from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
)
from market_data_center.domain.operations import (
    ExecutionStatus,
    JobExecution,
    TriggerSource,
    WorkflowCode,
    WorkflowRun,
)
from market_data_center.domain.stock_max_minute_volume import MinuteVolumeBuildSummary
from market_data_center.operations_service import WorkflowExecutionService
from market_data_center.scheduling_catalog import WORKFLOW_DEFINITIONS

DAY = date(2026, 11, 3)
NOW = datetime(2026, 11, 3, 12, tzinfo=UTC)


class Operations:
    def __init__(self):
        self.jobs = []
        self.runs = []

    def start_workflow(self, code, scheduled, trigger):
        return WorkflowRun(uuid4(), code, scheduled, trigger, 1, ExecutionStatus.RUNNING, NOW)

    def start_job(self, workflow_id, code, sequence):
        return JobExecution(uuid4(), workflow_id, code, sequence, 1, ExecutionStatus.RUNNING, NOW)

    def finish_job(self, job):
        self.jobs.append(job)

    def finish_workflow(self, run):
        self.runs.append(run)


@pytest.mark.parametrize(
    "minute_fail,snapshot_fail",
    [(False, False), (True, False), (False, True), (True, True), ("zero", True), ("zero", False)],
)
def test_daily_business_steps_are_independent_and_workflow_status_is_truthful(
    monkeypatch, minute_fail, snapshot_fail
):
    order = []
    persistence = SimpleNamespace(latest_trading_date=lambda *args: DAY, _engine=object())
    persistence.delete_stock_daily_indicators_before = lambda *args: order.append("retention") or 0

    def operation(args, *rest):
        calendar = args.dataset == "trading-calendar"
        order.append("calendar" if calendar else "tushare")
        if not calendar and snapshot_fail:
            raise RuntimeError("source failed")
        return IngestionRun(
            uuid4(),
            ProviderCode.TUSHARE,
            DatasetCode.TRADING_CALENDAR if calendar else DatasetCode.STOCK_DAILY_INDICATOR,
            IngestionStatus.SUCCEEDED,
            NOW,
            NOW,
            NOW,
            fetched_rows=1,
            accepted_rows=1,
        )

    def minute(*args, **kwargs):
        order.append("minute")
        if minute_fail is True:
            raise RuntimeError("minute failed")
        if minute_fail == "zero":
            return MinuteVolumeBuildSummary("partial", DAY, uuid4(), 1, 10, 0)
        return MinuteVolumeBuildSummary("ready", DAY, uuid4(), 1, 1, 1)

    monkeypatch.setattr(cli, "_execute_operation", operation)
    monkeypatch.setattr(cli, "_build_stock_max_minute_volume", minute, raising=False)
    ops = Operations()
    execution = WorkflowExecutionService(ops).start(
        WorkflowCode.STOCK_DAILY_INDICATOR, NOW, TriggerSource.MANUAL
    )
    args = cli._parser().parse_args(
        ["--provider", "tushare", "stock-daily-indicators-daily", "--as-of-date", DAY.isoformat()]
    )
    if minute_fail and snapshot_fail:
        with pytest.raises(RuntimeError) as caught:
            cli.run_stock_daily_indicator_workflow(args, persistence, object(), execution=execution)
        execution.fail(caught.value)
    else:
        cli.run_stock_daily_indicator_workflow(args, persistence, object(), execution=execution)
        execution.succeed()
    assert order == ["calendar", "minute", "tushare"] + ([] if snapshot_fail else ["retention"])
    assert ops.runs[0].status is (
        ExecutionStatus.FAILED
        if minute_fail and snapshot_fail
        else ExecutionStatus.PARTIAL
        if minute_fail or snapshot_fail
        else ExecutionStatus.SUCCEEDED
    )
    assert [job.sequence_no for job in ops.jobs] == list(range(1, len(ops.jobs) + 1))


def test_caught_step_failure_cannot_be_marked_success():
    ops = Operations()
    execution = WorkflowExecutionService(ops).start(
        WorkflowCode.STOCK_DAILY_INDICATOR, NOW, TriggerSource.MANUAL
    )
    with pytest.raises(RuntimeError):
        execution.step("stock_max_minute_volume", 1, lambda: (_ for _ in ()).throw(RuntimeError()))
    execution.succeed()
    assert ops.runs[0].status is ExecutionStatus.FAILED


def test_daily_catalog_reuses_existing_workflow_and_manual_input_is_exact():
    workflow = next(w for w in WORKFLOW_DEFINITIONS if w.code == "stock_daily_indicator")
    assert workflow.step_codes == (
        "trading_calendar",
        "stock_max_minute_volume",
        "stock_daily_indicator",
        "retention",
    )
    args = cli._parser().parse_args(
        ["stock-max-minute-volume-build", "--trade-date", "2026-11-03", "--symbols", "SSE:600327"]
    )
    assert args.trade_date == DAY
    assert args.symbols == ["SSE:600327"]
    with pytest.raises(SystemExit):
        cli._parser().parse_args(["stock-max-minute-volume-build"])
