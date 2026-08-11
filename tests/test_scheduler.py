from datetime import UTC, date, datetime
from pathlib import Path
from sqlite3 import connect
from typing import cast

import pytest

from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx_pool import (
    PytdxEndpointPool,
    PytdxPoolRefreshResult,
)
from market_data_center.scheduler import (
    build_scheduler,
    check_scheduler_health,
    prepare_locked_worker,
)
from market_data_center.scheduling_catalog import (
    AUCTION_COLLECTION_JOB_ID,
    CALL_AUCTION_SNAPSHOT_JOB_ID,
    DAILY_RUN_JOB_ID,
    DEDUCTED_PROFIT_JOB_ID,
    EOD_QUOTE_SNAPSHOT_JOB_ID,
    PYTDX_POOL_REFRESH_JOB_ID,
    STALE_RUN_RECOVERY_JOB_ID,
    STOCK_DAILY_INDICATOR_JOB_ID,
    STOCK_POOL_JOB_ID,
)
from market_data_center.settings import PytdxPoolSettings, SchedulerSettings


def test_scheduler_registers_persistent_single_instance_market_job(tmp_path: Path) -> None:
    store_path = tmp_path / "scheduler" / "jobs.sqlite"
    settings = SchedulerSettings(
        scheduler_store_path=store_path,
        auction_collection_enabled=False,
        _env_file=None,
    )
    scheduler = build_scheduler(settings)

    daily_run = scheduler.get_job(DAILY_RUN_JOB_ID)
    assert daily_run is not None
    assert str(daily_run.trigger) == "cron[day_of_week='mon-fri', hour='20', minute='0']"
    assert daily_run.max_instances == 1
    job = scheduler.get_job(STOCK_DAILY_INDICATOR_JOB_ID)

    assert job is not None
    assert str(job.trigger) == "cron[day_of_week='mon-fri', hour='20', minute='30']"
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
    assert str(stock_pool.trigger) == "cron[day_of_week='mon-fri', hour='21', minute='0']"
    assert store_path.parent.is_dir()
    assert scheduler.get_job(AUCTION_COLLECTION_JOB_ID) is None


def test_scheduler_registers_twelve_hour_pytdx_pool_refresh(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(scheduler_store_path=tmp_path / "refresh.sqlite", _env_file=None),
        PytdxPoolSettings(pytdx_pool_refresh_hours=12, _env_file=None),
    )

    refresh = scheduler.get_job(PYTDX_POOL_REFRESH_JOB_ID)
    assert refresh is not None
    assert str(refresh.trigger) == "interval[12:00:00]"
    assert refresh.max_instances == 1
    assert refresh.coalesce


class FakeScheduler:
    running = False


class FakeAdminServer:
    def shutdown(self) -> None:
        return None

    def server_close(self) -> None:
        return None


def test_locked_worker_refreshes_before_scheduler_and_admin(tmp_path: Path) -> None:
    events: list[str] = []
    pool_settings = PytdxPoolSettings(pytdx_pool_path=tmp_path / "pytdx_pool.json", _env_file=None)
    last_good = PytdxPoolRefreshResult(
        candidate_count=3,
        usable_node_count=0,
        rejected_node_count=3,
        published=False,
        used_last_good=True,
        pool=PytdxEndpointPool(datetime(2026, 8, 11, tzinfo=UTC), ()),
    )

    def refresh(_trigger_source):
        events.append("pool-refreshed")
        return last_good

    def scheduler_factory(_settings, _pool_settings):
        events.append("scheduler-built")
        return FakeScheduler()

    def admin_factory(_settings, _persistence, _operations):
        events.append("admin-started")
        return FakeAdminServer()

    scheduler, admin = prepare_locked_worker(
        SchedulerSettings(_env_file=None),
        pool_settings,
        cast(PostgreSQLPersistence, object()),
        cast(PostgreSQLOperationsPersistence, object()),
        startup_refresh=refresh,
        scheduler_factory=scheduler_factory,
        admin_factory=admin_factory,
    )

    assert isinstance(scheduler, FakeScheduler)
    assert isinstance(admin, FakeAdminServer)
    assert events == ["pool-refreshed", "scheduler-built", "admin-started"]


def test_locked_worker_does_not_build_runtime_when_refresh_fails(tmp_path: Path) -> None:
    events: list[str] = []

    def fail_refresh(_trigger_source):
        events.append("pool-failed")
        raise ProviderError("no usable pytdx endpoint pool")

    def scheduler_factory(_settings, _pool_settings):
        events.append("scheduler-built")
        return FakeScheduler()

    def admin_factory(_settings, _persistence, _operations):
        events.append("admin-started")
        return FakeAdminServer()

    with pytest.raises(ProviderError, match="no usable"):
        prepare_locked_worker(
            SchedulerSettings(_env_file=None),
            PytdxPoolSettings(pytdx_pool_path=tmp_path / "missing.json", _env_file=None),
            cast(PostgreSQLPersistence, object()),
            cast(PostgreSQLOperationsPersistence, object()),
            startup_refresh=fail_refresh,
            scheduler_factory=scheduler_factory,
            admin_factory=admin_factory,
        )

    assert events == ["pool-failed"]


def test_scheduler_registers_one_auction_session_job_only_when_enabled(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "auction.sqlite",
            auction_collection_enabled=True,
            _env_file=None,
        )
    )

    auction = scheduler.get_job(AUCTION_COLLECTION_JOB_ID)
    assert auction is not None
    assert str(auction.trigger) == "cron[day_of_week='mon-fri', hour='9', minute='15']"
    assert auction.max_instances == 1


def test_call_auction_snapshot_job_registered_when_enabled(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "call_auction.sqlite",
            call_auction_snapshot_enabled=True,
            _env_file=None,
        )
    )

    job = scheduler.get_job(CALL_AUCTION_SNAPSHOT_JOB_ID)
    assert job is not None
    assert str(job.trigger) == "cron[day_of_week='mon-fri', hour='18', minute='0']"
    assert job.max_instances == 1
    assert job.coalesce


def test_eod_quote_snapshot_job_registered_after_stock_pool_when_enabled(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "eod.sqlite",
            eod_quote_snapshot_enabled=True,
            _env_file=None,
        )
    )

    job = scheduler.get_job(EOD_QUOTE_SNAPSHOT_JOB_ID)
    assert job is not None
    assert str(job.trigger) == "cron[day_of_week='mon-fri', hour='21', minute='10']"
    assert job.max_instances == 1
    assert job.coalesce


def test_call_auction_snapshot_job_absent_when_disabled(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "no_call_auction.sqlite",
            call_auction_snapshot_enabled=False,
            _env_file=None,
        )
    )

    assert scheduler.get_job(CALL_AUCTION_SNAPSHOT_JOB_ID) is None


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
            PYTDX_POOL_REFRESH_JOB_ID,
            STALE_RUN_RECOVERY_JOB_ID,
            STOCK_DAILY_INDICATOR_JOB_ID,
            STOCK_POOL_JOB_ID,
        ),
    )
    settings = SchedulerSettings(
        scheduler_store_path=store_path,
        auction_collection_enabled=False,
        call_auction_snapshot_enabled=False,
        _env_file=None,
    )

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
