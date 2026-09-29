from datetime import date
from pathlib import Path

import pytest

from market_data_center.cli import _parser, _validate_regulation_calculation_date
from market_data_center.domain.operations import WorkflowCode
from market_data_center.scheduling_catalog import (
    REGULATION_DAILY_CALCULATION_JOB_ID,
    job_definition,
    workflow_definition,
)
from market_data_center.settings import SchedulerSettings


def test_regulation_daily_schedule_is_opt_in_and_fixed_at_2230(tmp_path: Path) -> None:
    disabled = SchedulerSettings(scheduler_store_path=tmp_path / "disabled.sqlite")
    enabled = SchedulerSettings(
        scheduler_store_path=tmp_path / "enabled.sqlite",
        regulation_daily_enabled=True,
    )

    assert disabled.regulation_daily_enabled is False
    disabled_job = job_definition(REGULATION_DAILY_CALCULATION_JOB_ID, disabled)
    enabled_job = job_definition(REGULATION_DAILY_CALCULATION_JOB_ID, enabled)
    assert disabled_job.enabled is False
    assert enabled_job.enabled is True
    assert enabled_job.day_of_week == "mon-fri"
    assert enabled_job.hour == 22
    assert enabled_job.minute == 30
    assert enabled_job.timezone == "Asia/Shanghai"


def test_regulation_workflow_collects_benchmarks_before_calculation() -> None:
    definition = workflow_definition(WorkflowCode.REGULATION_DAILY_CALCULATION.value)

    assert definition.step_codes == (
        "collect_regulation_benchmarks",
        "calculate_regulation_warnings",
        "calculate_regulation_monitor",
    )


def test_regulation_cli_requires_one_supported_nonfuture_trade_date() -> None:
    args = _parser().parse_args(["regulation-calculate", "--trade-date", "2026-09-02"])
    assert args.trade_date == "2026-09-02"
    assert _validate_regulation_calculation_date(args, today=date(2026, 9, 2)) == date(2026, 9, 2)

    old = _parser().parse_args(["regulation-calculate", "--trade-date", "2026-07-05"])
    with pytest.raises(ValueError, match="2026-07-06"):
        _validate_regulation_calculation_date(old, today=date(2026, 9, 2))

    future = _parser().parse_args(["regulation-calculate", "--trade-date", "2026-09-03"])
    with pytest.raises(ValueError, match="future"):
        _validate_regulation_calculation_date(future, today=date(2026, 9, 2))


def test_regulation_worker_migration_adds_only_the_workflow_catalog_value() -> None:
    migration = (
        Path("supabase/migrations/20260902000200_add_regulation_daily_workflow.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "'regulation_daily_calculation'" in migration
    assert "security_type_check" not in migration
    assert "cron" not in migration


@pytest.mark.parametrize("trading", [True, False])
def test_worker_executes_monitor_after_existing_calculation(monkeypatch, trading):
    from contextlib import nullcontext
    from datetime import datetime
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    from pydantic import SecretStr

    from market_data_center import scheduler

    actions, steps = [], []
    engine = MagicMock()
    execution = MagicMock()

    def step(code, position, action):
        steps.append(code)
        return action()

    execution.step.side_effect = step
    monkeypatch.setattr(
        scheduler,
        "WorkerSettings",
        lambda: SimpleNamespace(
            database_url=SecretStr("postgresql://test@localhost/test"), raw_data_root=Path("unused")
        ),
    )
    monkeypatch.setattr(scheduler, "create_engine", lambda *a, **k: engine)
    monkeypatch.setattr(
        scheduler,
        "_scheduled_job_fire_time",
        lambda *a: datetime(2026, 9, 29, 22, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    monkeypatch.setattr(
        scheduler,
        "PostgreSQLPersistence",
        lambda _: SimpleNamespace(is_trading_day=lambda _: trading),
    )
    monkeypatch.setattr(scheduler, "PostgreSQLOperationsPersistence", lambda _: object())
    monkeypatch.setattr(
        scheduler, "WorkflowExecutionService", lambda _: SimpleNamespace(start=lambda *a: execution)
    )
    monkeypatch.setattr(scheduler, "create_provider", lambda _: nullcontext(object()))
    monkeypatch.setattr(scheduler, "IngestionPipeline", lambda **k: object())
    monkeypatch.setattr(scheduler, "LocalRawStore", lambda _: object())
    monkeypatch.setattr(scheduler, "PostgreSQLRegulationPersistence", lambda _: object())
    monkeypatch.setattr(
        scheduler,
        "RegulationBenchmarkService",
        lambda *a: SimpleNamespace(collect=lambda _: actions.append("benchmarks")),
    )
    monkeypatch.setattr(
        scheduler,
        "RegulationService",
        lambda *a, **k: SimpleNamespace(calculate=lambda _: actions.append("legacy")),
    )
    monkeypatch.setattr(
        scheduler,
        "RegulationMonitorService",
        lambda *a, **k: SimpleNamespace(calculate=lambda _: actions.append("monitor")),
    )
    scheduler.run_regulation_daily_calculation_job()
    assert steps == [
        "collect_regulation_benchmarks",
        "calculate_regulation_warnings",
        "calculate_regulation_monitor",
    ]
    assert actions == (["benchmarks", "legacy", "monitor"] if trading else [])
    execution.succeed.assert_called_once()
    engine.dispose.assert_called_once()
