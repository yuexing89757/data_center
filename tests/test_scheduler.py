from datetime import UTC, date, datetime
from pathlib import Path
from sqlite3 import connect
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pytest

import market_data_center.scheduler as scheduler_module
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
    CALL_AUCTION_MARKET_SERIES_JOB_ID,
    CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID,
    CLOSE_PRICE_NEW_HIGHS_120D_JOB_ID,
    DAILY_RUN_JOB_ID,
    DEDUCTED_PROFIT_JOB_ID,
    EOD_QUOTE_SNAPSHOT_JOB_ID,
    PYTDX_POOL_REFRESH_JOB_ID,
    STALE_RUN_RECOVERY_JOB_ID,
    STOCK_DAILY_INDICATOR_JOB_ID,
    STOCK_POOL_JOB_ID,
)
from market_data_center.settings import SchedulerSettings


def test_scheduler_registers_persistent_single_instance_market_job(tmp_path: Path) -> None:
    store_path = tmp_path / "scheduler" / "jobs.sqlite"
    settings = SchedulerSettings(
        scheduler_store_path=store_path,
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
    closing_highs = scheduler.get_job(CLOSE_PRICE_NEW_HIGHS_120D_JOB_ID)
    assert closing_highs is not None
    assert str(closing_highs.trigger) == ("cron[day_of_week='mon-fri', hour='21', minute='30']")
    assert store_path.parent.is_dir()
    assert scheduler.get_job("opening-auction-limit-up-quotes") is None


def test_scheduler_registers_twelve_hour_pytdx_pool_refresh(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(scheduler_store_path=tmp_path / "refresh.sqlite", _env_file=None),
    )

    refresh = scheduler.get_job(PYTDX_POOL_REFRESH_JOB_ID)
    assert refresh is not None
    assert str(refresh.trigger) == "interval[12:00:00]"
    assert refresh.max_instances == 1
    assert refresh.coalesce


def test_legacy_time_environment_cannot_change_registered_jobs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CALL_AUCTION_MARKET_SNAPSHOT_HOUR", "0")
    monkeypatch.setenv("CALL_AUCTION_MARKET_SNAPSHOT_MINUTE", "1")
    monkeypatch.setenv("CALL_AUCTION_HOUR", "1")
    monkeypatch.setenv("CALL_AUCTION_MINUTE", "2")
    monkeypatch.setenv("EOD_QUOTE_HOUR", "3")
    monkeypatch.setenv("PYTDX_POOL_REFRESH_HOURS", "4")
    monkeypatch.setenv("CALL_AUCTION_MARKET_SERIES_HOUR", "5")
    monkeypatch.setenv("CALL_AUCTION_MARKET_SERIES_MINUTE", "6")
    monkeypatch.setenv("CALL_AUCTION_MARKET_SERIES_CADENCE_SECONDS", "7")
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "fixed.sqlite",
            eod_quote_snapshot_enabled=True,
            call_auction_snapshot_enabled=True,
            _env_file=None,
        )
    )

    assert str(scheduler.get_job(CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID).trigger) == (
        "cron[day_of_week='mon-fri', hour='9', minute='26']"
    )
    assert scheduler.get_job("call-auction-snapshot-daily") is None
    assert str(scheduler.get_job(EOD_QUOTE_SNAPSHOT_JOB_ID).trigger) == (
        "cron[day_of_week='mon-fri', hour='21', minute='10']"
    )
    assert str(scheduler.get_job(PYTDX_POOL_REFRESH_JOB_ID).trigger) == "interval[12:00:00]"
    assert str(scheduler.get_job(CALL_AUCTION_MARKET_SERIES_JOB_ID).trigger) == (
        "cron[day_of_week='mon-fri', hour='9', minute='15']"
    )


def test_scheduled_job_fire_time_uses_catalog_definition(monkeypatch) -> None:
    expected = datetime(2026, 8, 11, 13, 30, tzinfo=UTC)
    captured: list[tuple[int, int, str, bool]] = []

    def fake_fire_time(
        hour: int, minute: int, timezone_name: str, *, weekdays_only: bool
    ) -> datetime:
        captured.append((hour, minute, timezone_name, weekdays_only))
        return expected

    monkeypatch.setattr(scheduler_module, "_scheduled_fire_time", fake_fire_time)

    actual = scheduler_module._scheduled_job_fire_time(
        CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID,
        SchedulerSettings(_env_file=None),
    )

    assert actual == expected
    assert captured == [(9, 26, "Asia/Shanghai", True)]


class FakeScheduler:
    running = False


class FakeAdminServer:
    def shutdown(self) -> None:
        return None

    def server_close(self) -> None:
        return None


def test_locked_worker_refreshes_before_scheduler_and_admin() -> None:
    events: list[str] = []
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

    def scheduler_factory(_settings):
        events.append("scheduler-built")
        return FakeScheduler()

    def admin_factory(_settings, _persistence, _operations):
        events.append("admin-started")
        return FakeAdminServer()

    scheduler, admin = prepare_locked_worker(
        SchedulerSettings(_env_file=None),
        cast(PostgreSQLPersistence, object()),
        cast(PostgreSQLOperationsPersistence, object()),
        startup_refresh=refresh,
        scheduler_factory=scheduler_factory,
        admin_factory=admin_factory,
    )

    assert isinstance(scheduler, FakeScheduler)
    assert isinstance(admin, FakeAdminServer)
    assert events == ["pool-refreshed", "scheduler-built", "admin-started"]


def test_locked_worker_does_not_build_runtime_when_refresh_fails() -> None:
    events: list[str] = []

    def fail_refresh(_trigger_source):
        events.append("pool-failed")
        raise ProviderError("no usable pytdx endpoint pool")

    def scheduler_factory(_settings):
        events.append("scheduler-built")
        return FakeScheduler()

    def admin_factory(_settings, _persistence, _operations):
        events.append("admin-started")
        return FakeAdminServer()

    with pytest.raises(ProviderError, match="no usable"):
        prepare_locked_worker(
            SchedulerSettings(_env_file=None),
            cast(PostgreSQLPersistence, object()),
            cast(PostgreSQLOperationsPersistence, object()),
            startup_refresh=fail_refresh,
            scheduler_factory=scheduler_factory,
            admin_factory=admin_factory,
        )

    assert events == ["pool-failed"]


def test_market_series_has_a_dedicated_morning_executor(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(scheduler_store_path=tmp_path / "executors.sqlite", _env_file=None)
    )

    series = scheduler.get_job(CALL_AUCTION_MARKET_SERIES_JOB_ID)
    snapshot_0926 = scheduler.get_job(CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID)
    assert series is not None and snapshot_0926 is not None
    assert scheduler.get_job("opening-auction-limit-up-quotes") is None
    assert series.executor == "morning_auction"
    assert snapshot_0926.executor == "default"
    assert series.max_instances == 1
    assert scheduler._executors["default"]._pool._max_workers == 1
    assert scheduler._executors["morning_auction"]._pool._max_workers == 1


def test_disabling_series_does_not_disable_0926_snapshot(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "no_series.sqlite",
            call_auction_market_series_enabled=False,
            call_auction_snapshot_enabled=True,
            _env_file=None,
        )
    )

    assert scheduler.get_job(CALL_AUCTION_MARKET_SERIES_JOB_ID) is None
    assert scheduler.get_job("opening-auction-limit-up-quotes") is None
    assert scheduler.get_job(CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID) is not None


def test_closing_high_snapshot_can_only_be_enabled_or_disabled(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "no_closing_highs.sqlite",
            close_price_new_highs_120d_enabled=False,
            _env_file=None,
        )
    )

    assert scheduler.get_job(CLOSE_PRICE_NEW_HIGHS_120D_JOB_ID) is None
    assert scheduler.get_job(DAILY_RUN_JOB_ID) is not None


def test_call_auction_market_series_runner_wires_isolated_dependencies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workflow_run_id = uuid4()
    fire_time = datetime(2026, 8, 14, 1, 15, tzinfo=UTC)
    engine = SimpleNamespace(disposed=False)
    engine.dispose = lambda: setattr(engine, "disposed", True)
    captured: dict[str, object] = {}

    class FakeExecution:
        run = SimpleNamespace(workflow_run_id=workflow_run_id)

        def step(self, code, sequence, operation):
            captured["step"] = (code, sequence)
            return operation()

        def succeed(self):
            captured["succeeded"] = True

        def fail(self, error):
            captured["failed"] = type(error).__name__

    class FakeExecutionService:
        def __init__(self, operations):
            captured["operations"] = operations

        def start(self, workflow_code, scheduled_for, trigger_source):
            captured["workflow"] = (workflow_code, scheduled_for, trigger_source)
            return FakeExecution()

    class FakeService:
        def __init__(self, **kwargs):
            captured["service"] = kwargs

        def collect(self, trade_date, actual_workflow_run_id):
            captured["collect"] = (trade_date, actual_workflow_run_id)
            captured["provider"] = captured["service"]["provider_factory"](
                captured["service"]["quote_endpoints"][0]
            )
            return None

    quote_settings = SimpleNamespace(pytdx_hq_timeout_seconds=2.0)

    def fake_quote_settings(**kwargs):
        captured["quote_kwargs"] = kwargs
        return quote_settings

    monkeypatch.setattr(
        scheduler_module,
        "WorkerSettings",
        lambda: SimpleNamespace(
            database_url=SimpleNamespace(get_secret_value=lambda: "unused"),
            raw_data_root=tmp_path / "raw",
        ),
    )
    monkeypatch.setattr(
        scheduler_module, "SchedulerSettings", lambda: SchedulerSettings(_env_file=None)
    )
    monkeypatch.setattr(
        scheduler_module,
        "PytdxPoolSettings",
        lambda: SimpleNamespace(pytdx_pool_path=tmp_path / "pool.json"),
    )
    monkeypatch.setattr(
        scheduler_module,
        "PytdxHqSettings",
        fake_quote_settings,
    )
    monkeypatch.setattr(scheduler_module, "sqlalchemy_url", lambda value: value)
    monkeypatch.setattr(scheduler_module, "create_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(
        scheduler_module, "PostgreSQLOperationsPersistence", lambda value: ("ops", value)
    )
    monkeypatch.setattr(scheduler_module, "WorkflowExecutionService", FakeExecutionService)
    monkeypatch.setattr(scheduler_module, "_scheduled_job_fire_time", lambda *args: fire_time)
    monkeypatch.setattr(scheduler_module, "load_endpoint_pool", lambda path: ("pool", path))
    monkeypatch.setattr(
        scheduler_module,
        "endpoints_for",
        lambda pool, capability: (
            captured.setdefault("pool", (pool, capability))
            and (("quote-a", 7709), ("quote-b", 7709))
        ),
    )
    monkeypatch.setattr(
        scheduler_module,
        "PostgreSQLCallAuctionMarketSeriesPersistence",
        lambda value: ("series", value),
    )
    monkeypatch.setattr(scheduler_module, "LocalRawStore", lambda root: ("raw", root))
    monkeypatch.setattr(
        scheduler_module,
        "PytdxHqProvider",
        lambda settings, endpoints: ("provider", settings, endpoints),
    )
    monkeypatch.setattr(scheduler_module, "CallAuctionMarketSeriesService", FakeService)

    scheduler_module.run_call_auction_market_series_job()

    assert captured["quote_kwargs"] == {"pytdx_hq_batch_size": 80}
    assert captured["collect"] == (date(2026, 8, 14), workflow_run_id)
    assert captured["step"] == ("collect_call_auction_market_series", 1)
    assert captured["provider"] == ("provider", quote_settings, (("quote-a", 7709),))
    assert captured["succeeded"] is True
    assert engine.disposed is True


def test_stale_recovery_includes_call_auction_market_series_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = SimpleNamespace(disposed=False)
    engine.dispose = lambda: setattr(engine, "disposed", True)
    steps: list[str] = []

    class FakeOperations:
        def recover_stale(self, stale_before):
            return 2

    class FakeExecution:
        def step(self, code, sequence, operation):
            steps.append(code)
            return operation()

        def succeed(self):
            return None

        def fail(self, error):
            raise AssertionError(error)

    monkeypatch.setattr(
        scheduler_module,
        "WorkerSettings",
        lambda: SimpleNamespace(database_url=SimpleNamespace(get_secret_value=lambda: "unused")),
    )
    monkeypatch.setattr(scheduler_module, "sqlalchemy_url", lambda value: value)
    monkeypatch.setattr(scheduler_module, "create_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(
        scheduler_module, "PostgreSQLOperationsPersistence", lambda value: FakeOperations()
    )
    monkeypatch.setattr(
        scheduler_module,
        "WorkflowExecutionService",
        lambda operations: SimpleNamespace(start=lambda *args: FakeExecution()),
    )
    monkeypatch.setattr(scheduler_module, "PostgreSQLPersistence", lambda value: object())
    monkeypatch.setattr(scheduler_module, "recover_stale_runs", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        scheduler_module,
        "PostgreSQLAuctionPersistence",
        lambda value: SimpleNamespace(recover_expired_sessions=lambda now: 3),
    )
    monkeypatch.setattr(
        scheduler_module,
        "PostgreSQLCallAuctionMarketSeriesPersistence",
        lambda value: SimpleNamespace(recover_expired_sessions=lambda now: 4),
    )

    scheduler_module.run_stale_recovery_job()

    assert steps == [
        "recover_ingestion_runs",
        "recover_workflow_runs",
        "recover_auction_sessions",
        "recover_call_auction_market_series_sessions",
    ]
    assert engine.disposed is True


def test_only_call_auction_morning_job_is_registered_when_enabled(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "call_auction.sqlite",
            call_auction_snapshot_enabled=True,
            _env_file=None,
        )
    )

    morning = scheduler.get_job(CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID)
    assert morning is not None
    assert str(morning.trigger) == "cron[day_of_week='mon-fri', hour='9', minute='26']"
    assert morning.func is scheduler_module.run_call_auction_market_snapshot_job
    assert morning.max_instances == 1
    assert morning.coalesce
    assert scheduler.get_job("call-auction-snapshot-daily") is None


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


def test_call_auction_morning_job_is_absent_when_disabled(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "no_call_auction.sqlite",
            call_auction_snapshot_enabled=False,
            _env_file=None,
        )
    )

    assert scheduler.get_job(CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID) is None
    assert scheduler.get_job("call-auction-snapshot-daily") is None


def test_scheduler_removes_persisted_retired_call_auction_job(tmp_path: Path) -> None:
    store_path = tmp_path / "retired.sqlite"
    _create_job_store(store_path, ("call-auction-snapshot-daily",))

    build_scheduler(
        SchedulerSettings(  # type: ignore[call-arg]
            scheduler_store_path=store_path,
            call_auction_snapshot_enabled=True,
            _env_file=None,
        )
    )

    with connect(store_path) as connection:
        persisted = connection.execute("select id from apscheduler_jobs").fetchall()
    assert persisted == []


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
            CLOSE_PRICE_NEW_HIGHS_120D_JOB_ID,
        ),
    )
    settings = SchedulerSettings(
        scheduler_store_path=store_path,
        eod_quote_snapshot_enabled=False,
        call_auction_snapshot_enabled=False,
        call_auction_market_series_enabled=False,
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
