from datetime import UTC, date, datetime
from pathlib import Path
from sqlite3 import connect
from typing import cast

from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.scheduler import (
    build_scheduler,
    check_scheduler_health,
)
from market_data_center.scheduling_catalog import (
    DAILY_RUN_JOB_ID,
    DEDUCTED_PROFIT_JOB_ID,
    STALE_RUN_RECOVERY_JOB_ID,
    STOCK_DAILY_INDICATOR_JOB_ID,
    STOCK_POOL_JOB_ID,
)
from market_data_center.settings import SchedulerSettings


def test_scheduler_registers_persistent_single_instance_market_job(tmp_path: Path) -> None:
    store_path = tmp_path / "scheduler" / "jobs.sqlite"
    scheduler = build_scheduler(SchedulerSettings(scheduler_store_path=store_path))

    daily_run = scheduler.get_job(DAILY_RUN_JOB_ID)
    assert daily_run is not None
    assert str(daily_run.trigger) == "cron[day_of_week='mon-fri', hour='18', minute='30']"
    assert daily_run.max_instances == 1
    job = scheduler.get_job(STOCK_DAILY_INDICATOR_JOB_ID)

    assert job is not None
    assert str(job.trigger) == "cron[day_of_week='mon-fri', hour='19', minute='0']"
    assert job.coalesce
    assert job.max_instances == 1
    assert job.misfire_grace_time == 21_600
    recovery = scheduler.get_job(STALE_RUN_RECOVERY_JOB_ID)
    assert recovery is not None
    assert str(recovery.trigger) == "interval[1:00:00]"
    assert recovery.max_instances == 1
    deducted_profit = scheduler.get_job(DEDUCTED_PROFIT_JOB_ID)
    assert deducted_profit is not None
    assert str(deducted_profit.trigger) == "cron[hour='20', minute='0']"
    stock_pool = scheduler.get_job(STOCK_POOL_JOB_ID)
    assert stock_pool is not None
    assert str(stock_pool.trigger) == "cron[day_of_week='mon-fri', hour='19', minute='30']"
    assert store_path.parent.is_dir()


class HealthPersistence:
    def __init__(self, *, stale: bool = False) -> None:
        self.stale = stale

    def stale_ingestion_run_ids(self, stale_before: datetime) -> list[str]:
        del stale_before
        return ["stale-run"] if self.stale else []

    def latest_stock_daily_indicator_snapshot(self) -> tuple[date, int]:
        return date(2026, 7, 31), 5_522


def _create_job_store(path: Path, job_ids: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with connect(path) as connection:
        connection.execute(
            "create table apscheduler_jobs (id varchar primary key, next_run_time float)"
        )
        connection.executemany(
            "insert into apscheduler_jobs (id, next_run_time) values (?, ?)",
            [(job_id, 1_785_666_600.0) for job_id in job_ids],
        )


def test_scheduler_health_requires_jobs_fresh_snapshot_and_no_stale_runs(tmp_path: Path) -> None:
    store_path = tmp_path / "jobs.sqlite"
    _create_job_store(
        store_path,
        (
            DAILY_RUN_JOB_ID,
            DEDUCTED_PROFIT_JOB_ID,
            STALE_RUN_RECOVERY_JOB_ID,
            STOCK_DAILY_INDICATOR_JOB_ID,
            STOCK_POOL_JOB_ID,
        ),
    )
    settings = SchedulerSettings(scheduler_store_path=store_path)

    healthy = check_scheduler_health(
        settings,
        cast(PostgreSQLPersistence, HealthPersistence()),
        now=datetime(2026, 8, 2, tzinfo=UTC),
    )
    stale = check_scheduler_health(
        settings,
        cast(PostgreSQLPersistence, HealthPersistence(stale=True)),
        now=datetime(2026, 8, 2, tzinfo=UTC),
    )

    assert healthy.healthy
    assert healthy.latest_snapshot_rows == 5_522
    assert not stale.healthy
    assert stale.stale_run_count == 1
