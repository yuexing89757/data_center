from argparse import Namespace
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import market_data_center.cli as cli
from market_data_center.domain import IngestionStatus
from market_data_center.scheduling_catalog import job_definitions
from market_data_center.settings import SchedulerSettings


def test_technical_factor_job_has_bounded_evening_readiness_retries() -> None:
    jobs = {job.code: job for job in job_definitions(SchedulerSettings(_env_file=None))}
    job = jobs["stock-technical-factor-daily"]
    assert (job.day_of_week, job.hour, job.minute, job.timezone) == (
        "mon-fri",
        "21-23",
        0,
        "Asia/Shanghai",
    )
    assert job.enabled
    assert jobs["stock-daily-indicators-daily"].minute == 30
    catchup = jobs["stock-technical-factor-catchup"]
    assert (catchup.day_of_week, catchup.hour, catchup.minute, catchup.timezone) == (
        "mon-sun",
        4,
        0,
        "Asia/Shanghai",
    )
    assert catchup.workflow_code == job.workflow_code


@pytest.mark.parametrize("is_open", [True, False])
def test_factor_workflow_checks_exact_date_before_collecting(monkeypatch, is_open) -> None:
    target = date(2026, 10, 9)
    persistence = Mock()
    persistence.latest_trading_date.return_value = target if is_open else None
    calendar = SimpleNamespace(status=IngestionStatus.SUCCEEDED)
    sync_calendar = Mock(return_value=calendar)
    monkeypatch.setattr(cli, "_execute_operation", sync_calendar)
    collect = Mock(return_value=SimpleNamespace(status=IngestionStatus.SUCCEEDED, accepted_rows=2))
    monkeypatch.setattr(cli, "collect_stock_technical_factors", collect, raising=False)
    result = cli.run_stock_technical_factor_workflow(
        Namespace(provider="tushare", as_of_date="2026-10-09"), persistence, Mock()
    )
    calendar_args = sync_calendar.call_args.args[0]
    assert calendar_args.start_date == calendar_args.end_date == "2026-10-09"
    persistence.latest_trading_date.assert_called_once_with(target, target)
    if is_open:
        assert result.accepted_rows == 2
        assert collect.call_args.args[0] == target
    else:
        assert result is None
        collect.assert_not_called()


def test_factor_workflow_fails_when_calendar_sync_failed(monkeypatch) -> None:
    monkeypatch.setattr(
        cli, "_execute_operation", Mock(return_value=SimpleNamespace(status=IngestionStatus.FAILED))
    )
    collect = Mock()
    monkeypatch.setattr(cli, "collect_stock_technical_factors", collect, raising=False)
    with pytest.raises(RuntimeError, match="calendar"):
        cli.run_stock_technical_factor_workflow(
            Namespace(provider="tushare", as_of_date="2026-10-09"), Mock(), Mock()
        )
    collect.assert_not_called()


def test_factor_cli_accepts_explicit_single_day() -> None:
    args = cli._parser().parse_args(
        ["--provider", "tushare", "stock-technical-factor-daily", "--as-of-date", "2026-10-09"]
    )
    assert args.dataset == "stock-technical-factor-daily"
    assert args.as_of_date == "2026-10-09"


def test_scheduler_registers_factor_runner(tmp_path) -> None:
    from market_data_center.scheduler import build_scheduler

    scheduler = build_scheduler(
        SchedulerSettings(scheduler_store_path=tmp_path / "jobs.sqlite", _env_file=None)
    )
    job = scheduler.get_job("stock-technical-factor-daily")
    assert job is not None
    assert job.func.__name__ == "run_stock_technical_factor_job"
    assert job.max_instances == 1
    assert job.coalesce
    assert job.misfire_grace_time == 21_600
    catchup = scheduler.get_job("stock-technical-factor-catchup")
    assert catchup is not None
    assert catchup.func.__name__ == "run_stock_technical_factor_catchup_job"
    assert catchup.max_instances == 1 and catchup.coalesce
    assert str(catchup.trigger) == "cron[day_of_week='mon-sun', hour='4', minute='0']"
    # Friday evening UTC is already Saturday's next-day catch-up in Shanghai.
    from datetime import UTC, datetime

    fire = catchup.trigger.get_next_fire_time(None, datetime(2026, 10, 16, 19, 59, tzinfo=UTC))
    assert fire.isoformat() == "2026-10-17T04:00:00+08:00"
    assert fire.hour < 5


def test_catchup_pins_previous_shanghai_date_not_current_day(monkeypatch):
    from datetime import datetime

    from market_data_center import scheduler

    run = Mock()
    monkeypatch.setattr(scheduler, "_run_stock_technical_factor_job", run, raising=False)
    monkeypatch.setattr(
        scheduler,
        "datetime",
        SimpleNamespace(now=lambda zone: datetime(2026, 10, 17, 4, 0, tzinfo=zone)),
    )
    scheduler.run_stock_technical_factor_catchup_job()
    run.assert_called_once_with("stock-technical-factor-catchup", "2026-10-16")


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("day", [17, 18])
def test_catchup_records_weekend_fire_time_and_disposes_resources(
    monkeypatch, tmp_path, failed, day
):
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    from market_data_center import scheduler

    engine, execution, service = MagicMock(), MagicMock(), MagicMock()
    service.start.return_value = execution
    monkeypatch.setattr(
        scheduler,
        "WorkerSettings",
        lambda: SimpleNamespace(
            database_url=SimpleNamespace(get_secret_value=lambda: "unused"), raw_data_root=tmp_path
        ),
    )
    monkeypatch.setattr(scheduler, "sqlalchemy_url", lambda value: value)
    monkeypatch.setattr(scheduler, "create_engine", Mock(return_value=engine))
    monkeypatch.setattr(scheduler, "WorkflowExecutionService", Mock(return_value=service))

    class Weekend(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, day, 4, 0, tzinfo=tz)

    monkeypatch.setattr(scheduler, "datetime", Weekend)
    workflow = Mock(side_effect=RuntimeError("not ready") if failed else None)
    monkeypatch.setattr(scheduler, "run_stock_technical_factor_workflow", workflow)
    if failed:
        with pytest.raises(RuntimeError, match="not ready"):
            scheduler.run_stock_technical_factor_catchup_job()
        execution.fail.assert_called_once()
        execution.succeed.assert_not_called()
    else:
        scheduler.run_stock_technical_factor_catchup_job()
        execution.succeed.assert_called_once()
    assert service.start.call_args.args[1] == datetime(2026, 10, day - 1, 20, 0, tzinfo=UTC)
    args = workflow.call_args.args[0]
    assert args.provider == "tushare" and args.as_of_date == f"2026-10-{day - 1}"
    engine.dispose.assert_called_once()
