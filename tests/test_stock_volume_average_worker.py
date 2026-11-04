from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from pydantic import SecretStr

from market_data_center import cli, scheduler
from market_data_center.domain.operations import ExecutionStatus, TriggerSource, WorkflowCode
from market_data_center.domain.stock_volume_average import VolumeAverageBuildSummary
from market_data_center.operations_service import _result_statistics
from market_data_center.scheduling_catalog import WORKFLOW_DEFINITIONS, job_definitions
from market_data_center.settings import SchedulerSettings

JOB_ID = "stock-volume-average-5d-daily"


def test_catalog_registers_fixed_shanghai_2145_and_can_be_disabled(tmp_path):
    settings = SchedulerSettings(_env_file=None, scheduler_store_path=tmp_path / "jobs.sqlite")
    job = next(j for j in job_definitions(settings) if j.code == JOB_ID)
    assert (job.hour, job.minute, job.day_of_week, job.timezone) == (
        21,
        45,
        "mon-fri",
        "Asia/Shanghai",
    )
    assert job.enabled
    workflow = next(w for w in WORKFLOW_DEFINITIONS if w.code == "stock_volume_average_5d")
    assert workflow.step_codes == ("build_stock_volume_average_5d",)
    built = scheduler.build_scheduler(settings)
    assert built.get_job(JOB_ID).func == scheduler.run_stock_volume_average_5d_job
    disabled = scheduler.build_scheduler(
        settings.model_copy(update={"stock_volume_average_5d_enabled": False})
    )
    assert disabled.get_job(JOB_ID) is None


def test_statistics_preserve_partial_and_zero_complete_snapshot():
    result = VolumeAverageBuildSummary("succeeded", date(2026, 11, 4), uuid4(), 1, 10, 0)
    assert _result_statistics(result) == (10, 0, 10, ExecutionStatus.PARTIAL)


@pytest.mark.parametrize("fails", [False, True])
def test_scheduled_job_uses_planned_shanghai_date_records_failure_and_disposes(monkeypatch, fails):
    engine = MagicMock()
    execution = MagicMock()
    monkeypatch.setattr(
        scheduler,
        "WorkerSettings",
        lambda: SimpleNamespace(database_url=SecretStr("postgresql://synthetic/test")),
    )
    monkeypatch.setattr(scheduler, "create_engine", lambda *a, **k: engine)
    execution_service = MagicMock()
    execution_service.start.return_value = execution
    monkeypatch.setattr(scheduler, "WorkflowExecutionService", lambda *a: execution_service)
    builder = MagicMock()
    monkeypatch.setattr(scheduler, "StockVolumeAverageService", lambda *a: builder)
    planned = datetime(2026, 1, 5, 13, 45, tzinfo=UTC)
    monkeypatch.setattr(scheduler, "_scheduled_job_fire_time", lambda *a: planned)
    execution.step.side_effect = lambda code, sequence, operation: operation()
    if fails:
        builder.build.side_effect = RuntimeError("synthetic")
        with pytest.raises(RuntimeError):
            scheduler.run_stock_volume_average_5d_job()
        execution.fail.assert_called_once()
        execution.succeed.assert_not_called()
    else:
        scheduler.run_stock_volume_average_5d_job()
        execution.succeed.assert_called_once()
    builder.build.assert_called_once_with(date(2026, 1, 5))
    execution_service.start.assert_called_once_with(
        WorkflowCode.STOCK_VOLUME_AVERAGE_5D, planned, TriggerSource.SCHEDULED
    )
    engine.dispose.assert_called_once()


def test_cli_requires_an_explicit_iso_trade_date():
    args = cli._parser().parse_args(["stock-volume-average-5d-build", "--trade-date", "2026-11-04"])
    assert args.trade_date == date(2026, 11, 4)
    with pytest.raises(SystemExit):
        cli._parser().parse_args(["stock-volume-average-5d-build"])
    with pytest.raises(SystemExit):
        cli._parser().parse_args(["stock-volume-average-5d-build", "--trade-date", "2026-02-30"])


def test_cli_manual_build_uses_same_service_and_disposes_engine(monkeypatch, capsys):
    engine = MagicMock()
    execution = MagicMock()
    runner = MagicMock()
    runner.start.return_value = execution
    builder = MagicMock()
    target = date(2026, 11, 4)
    builder.build.return_value = VolumeAverageBuildSummary("skipped", target, None, None, 0, 0)
    monkeypatch.setattr(
        cli,
        "WorkerSettings",
        lambda: SimpleNamespace(database_url=SecretStr("postgresql://synthetic/test")),
    )
    monkeypatch.setattr(cli, "create_engine", lambda *a, **k: engine)
    monkeypatch.setattr(cli, "WorkflowExecutionService", lambda *a: runner)
    monkeypatch.setattr(cli, "StockVolumeAverageService", lambda *a: builder)
    execution.step.side_effect = lambda code, sequence, operation: operation()
    cli._run_stock_volume_average_command(target)
    builder.build.assert_called_once_with(target)
    assert runner.start.call_args.args[0] is WorkflowCode.STOCK_VOLUME_AVERAGE_5D
    assert runner.start.call_args.args[2] is TriggerSource.MANUAL
    execution.succeed.assert_called_once()
    engine.dispose.assert_called_once()
    assert '"trade_date": "2026-11-04"' in capsys.readouterr().out
