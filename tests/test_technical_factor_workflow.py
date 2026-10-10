from argparse import Namespace
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import market_data_center.cli as cli
from market_data_center.domain import IngestionStatus
from market_data_center.scheduling_catalog import job_definitions
from market_data_center.settings import SchedulerSettings


def test_technical_factor_job_is_independent_and_runs_at_21_shanghai() -> None:
    jobs = {job.code: job for job in job_definitions(SchedulerSettings(_env_file=None))}
    job = jobs["stock-technical-factor-daily"]
    assert (job.day_of_week, job.hour, job.minute, job.timezone) == (
        "mon-fri",
        21,
        0,
        "Asia/Shanghai",
    )
    assert job.enabled
    assert jobs["stock-daily-indicators-daily"].minute == 30


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
