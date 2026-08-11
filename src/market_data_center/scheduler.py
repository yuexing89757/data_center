"""Embedded scheduling for the unified long-lived collection Worker."""

from argparse import Namespace
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, time, timedelta
from json import dumps
from logging import INFO, basicConfig, getLogger
from pathlib import Path
from signal import SIGINT, SIGTERM, signal
from sqlite3 import Error as SQLiteError
from sqlite3 import connect
from types import FrameType
from typing import Protocol
from zoneinfo import ZoneInfo

from apscheduler.executors.pool import ThreadPoolExecutor  # type: ignore[import-untyped]
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore  # type: ignore[import-untyped]
from apscheduler.schedulers.blocking import BlockingScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]
from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-untyped]
from sqlalchemy import URL, create_engine

from market_data_center.auction_service import AuctionCollectionService
from market_data_center.call_auction_market_service import CallAuctionMarketSnapshotService
from market_data_center.cli import run_daily_workflow, run_stock_daily_indicator_workflow
from market_data_center.database_urls import sqlalchemy_url
from market_data_center.domain.operations import TriggerSource, WorkflowCode
from market_data_center.operations_service import WorkflowExecutionService
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.persistence.auction_postgres import PostgreSQLAuctionPersistence
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
from market_data_center.persistence.stock_pool_postgres import PostgreSQLStockPoolPersistence
from market_data_center.pipeline import IngestionPipeline
from market_data_center.providers.pytdx_hq import PytdxHqProvider
from market_data_center.providers.pytdx_pool import (
    PytdxCapability,
    PytdxPoolRefreshResult,
    endpoints_for,
    load_endpoint_pool,
    refresh_endpoint_pool,
)
from market_data_center.raw_store import LocalRawStore
from market_data_center.reliability import recover_stale_runs
from market_data_center.scheduling_catalog import (
    AUCTION_COLLECTION_JOB_ID,
    CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID,
    DAILY_RUN_JOB_ID,
    DEDUCTED_PROFIT_JOB_ID,
    EOD_QUOTE_SNAPSHOT_JOB_ID,
    PYTDX_POOL_REFRESH_JOB_ID,
    SCHEDULER_TIMEZONE,
    STALE_RUN_RECOVERY_JOB_ID,
    STOCK_DAILY_INDICATOR_JOB_ID,
    STOCK_POOL_JOB_ID,
    TODAY_LIMIT_UP_SNAPSHOT_JOB_ID,
    JobDefinition,
    job_definition,
    job_definitions,
)
from market_data_center.settings import (
    PytdxHqSettings,
    PytdxPoolSettings,
    SchedulerSettings,
    WorkerSettings,
)
from market_data_center.stock_pool_service import StockPoolService

SCHEDULER_LOCK_KEY = "market-data-center:scheduler"
LOGGER = getLogger(__name__)
_RETIRED_JOB_IDS = ("call-auction-snapshot-daily",)


@dataclass(frozen=True, slots=True)
class SchedulerHealthReport:
    healthy: bool
    persisted_job_ids: tuple[str, ...]
    stale_run_count: int
    latest_snapshot_date: str | None
    latest_snapshot_rows: int


@dataclass(frozen=True, slots=True)
class PersistedScheduledTask:
    task_id: str
    next_run_time: str | None


@dataclass(frozen=True, slots=True)
class JobStoreSnapshot:
    available: bool
    tasks: tuple[PersistedScheduledTask, ...]


class WorkerAdminServer(Protocol):
    def shutdown(self) -> None: ...

    def server_close(self) -> None: ...


def read_job_store_snapshot(settings: SchedulerSettings) -> JobStoreSnapshot:
    """Read only APScheduler IDs and next-run timestamps from SQLite."""
    store_uri = f"{settings.scheduler_store_path.resolve().as_uri()}?mode=ro"
    try:
        with connect(store_uri, uri=True) as connection:
            rows = connection.execute(
                "select id, next_run_time from apscheduler_jobs order by id"
            ).fetchall()
    except SQLiteError:
        return JobStoreSnapshot(available=False, tasks=())
    timezone = ZoneInfo(SCHEDULER_TIMEZONE)
    tasks = tuple(
        PersistedScheduledTask(
            task_id=row[0],
            next_run_time=(
                datetime.fromtimestamp(row[1], UTC).astimezone(timezone).isoformat()
                if row[1] is not None
                else None
            ),
        )
        for row in rows
    )
    return JobStoreSnapshot(available=True, tasks=tasks)


def execute_pytdx_pool_refresh(
    operations: PostgreSQLOperationsPersistence,
    pool_settings: PytdxPoolSettings,
    trigger_source: TriggerSource,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    refresh: Callable[[Path], PytdxPoolRefreshResult] = refresh_endpoint_pool,
) -> PytdxPoolRefreshResult:
    """Refresh the shared pool as one controlled Operations workflow."""
    scheduled_for = clock()
    execution = WorkflowExecutionService(operations).start(
        WorkflowCode.PYTDX_POOL_REFRESH,
        scheduled_for,
        trigger_source,
    )
    try:
        result = execution.step(
            "refresh_pytdx_pool",
            1,
            lambda: refresh(pool_settings.pytdx_pool_path),
        )
    except BaseException as error:
        execution.fail(error)
        raise
    execution.succeed()
    return result


def run_pytdx_pool_refresh_job(
    trigger_source: TriggerSource = TriggerSource.SCHEDULED,
) -> PytdxPoolRefreshResult:
    """Run one scheduled or startup refresh using the Worker database."""
    worker_settings = WorkerSettings()  # type: ignore[call-arg]
    engine = create_engine(
        sqlalchemy_url(worker_settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        return execute_pytdx_pool_refresh(
            PostgreSQLOperationsPersistence(engine),
            PytdxPoolSettings(),
            trigger_source,
        )
    finally:
        engine.dispose()


def run_stock_daily_indicator_job() -> None:
    """Run one idempotent trading-day collection and retention workflow."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.STOCK_DAILY_INDICATOR,
            _scheduled_job_fire_time(STOCK_DAILY_INDICATOR_JOB_ID, scheduling),
            TriggerSource.SCHEDULED,
        )
        try:
            run_stock_daily_indicator_workflow(
                Namespace(provider="tushare", as_of_date=None),
                PostgreSQLPersistence(engine),
                LocalRawStore(settings.raw_data_root),
                execution=execution,
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def run_daily_market_job() -> None:
    """Run the ordinary security, calendar, and remote pytdx Daily Bar workflow."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.DAILY_MARKET,
            _scheduled_job_fire_time(DAILY_RUN_JOB_ID, scheduling),
            TriggerSource.SCHEDULED,
        )
        try:
            run_daily_workflow(
                Namespace(
                    provider="auto",
                    as_of_date=None,
                    bar_lookback_days=1,
                    calendar_lookback_days=14,
                    shard_count=1,
                    shard_index=0,
                ),
                PostgreSQLPersistence(engine),
                LocalRawStore(settings.raw_data_root),
                execution=execution,
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def run_stale_recovery_job() -> None:
    """Fail ingestion runs left running after an interrupted worker process."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        operations_persistence = PostgreSQLOperationsPersistence(engine)
        execution = WorkflowExecutionService(operations_persistence).start(
            WorkflowCode.STALE_RUN_RECOVERY,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.RECOVERY,
        )
        try:
            ingestion_ids = execution.step(
                "recover_ingestion_runs",
                1,
                lambda: recover_stale_runs(
                    PostgreSQLPersistence(engine),
                    older_than=timedelta(minutes=60),
                    dry_run=False,
                ),
            )
            workflow_count = execution.step(
                "recover_workflow_runs",
                2,
                lambda: operations_persistence.recover_stale(
                    datetime.now(UTC) - timedelta(minutes=60)
                ),
            )
            auction_count = execution.step(
                "recover_auction_sessions",
                3,
                lambda: PostgreSQLAuctionPersistence(engine).recover_expired_sessions(
                    datetime.now(UTC)
                ),
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
        LOGGER.info(
            "recovered stale runs ingestion_count=%d workflow_count=%d auction_count=%d",
            len(ingestion_ids),
            workflow_count,
            auction_count,
        )
    finally:
        engine.dispose()


def run_deducted_profit_job() -> None:
    """Discover and ingest newly disclosed or revised deducted-profit facts."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.DEDUCTED_PROFIT,
            _scheduled_job_fire_time(DEDUCTED_PROFIT_JOB_ID, scheduling, weekdays_only=False),
            TriggerSource.SCHEDULED,
        )
        try:
            from market_data_center.providers import create_provider

            with create_provider("tushare") as provider:
                pipeline = IngestionPipeline(
                    provider=provider,
                    persistence=PostgreSQLPersistence(engine),
                    raw_store=LocalRawStore(settings.raw_data_root),
                )
                execution.step(
                    "deducted_profit",
                    1,
                    lambda: pipeline.ingest_deducted_profit_updates(
                        datetime.now(ZoneInfo("Asia/Shanghai")).date()
                    ),
                )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def run_stock_pool_job() -> None:
    """Build exact next-trading-day main-board limit-up/down pools."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        operations = PostgreSQLOperationsPersistence(engine)
        execution = WorkflowExecutionService(operations).start(
            WorkflowCode.STOCK_POOL,
            _scheduled_job_fire_time(STOCK_POOL_JOB_ID, scheduling),
            TriggerSource.SCHEDULED,
        )
        try:
            persistence = PostgreSQLStockPoolPersistence(engine)
            as_of = datetime.now(ZoneInfo(SCHEDULER_TIMEZONE)).date()
            basis, _ = persistence.resolve_basis_date(as_of)
            execution.step(
                "build_stock_pools", 1, lambda: StockPoolService(persistence).build(basis)
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def run_auction_collection_job() -> None:
    """Collect one bounded opening-auction session for today's exact limit-up pool."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    definition = job_definition(AUCTION_COLLECTION_JOB_ID, scheduling)
    if definition.cadence_seconds is None:
        raise ValueError(f"job has no cadence: {AUCTION_COLLECTION_JOB_ID}")
    quote_settings = PytdxHqSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        operations = PostgreSQLOperationsPersistence(engine)
        execution = WorkflowExecutionService(operations).start(
            WorkflowCode.AUCTION_COLLECTION,
            _scheduled_job_fire_time(AUCTION_COLLECTION_JOB_ID, scheduling),
            TriggerSource.SCHEDULED,
        )
        try:
            trade_date = datetime.now(ZoneInfo(definition.timezone)).date()
            with PytdxHqProvider(quote_settings) as provider:
                service = AuctionCollectionService(
                    PostgreSQLAuctionPersistence(engine),
                    provider,
                    LocalRawStore(settings.raw_data_root),
                    cadence_seconds=definition.cadence_seconds,
                    max_retries=quote_settings.pytdx_hq_max_retries,
                    retry_budget_seconds=quote_settings.pytdx_hq_timeout_seconds,
                )
                execution.step("collect_auction_quotes", 1, lambda: service.collect(trade_date))
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def run_eod_quote_snapshot_job() -> None:
    """Collect end-of-day five-level quotes for the latest limit-up pool."""
    from market_data_center.snapshot_collector import collect_eod_quotes

    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        fire_time = _scheduled_job_fire_time(EOD_QUOTE_SNAPSHOT_JOB_ID, scheduling)
        collect_eod_quotes(
            engine,
            fire_time.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date(),
            raw_store=LocalRawStore(settings.raw_data_root),
        )
    finally:
        engine.dispose()


def run_call_auction_market_snapshot_job() -> None:
    """Collect one complete morning auction snapshot from stable quote endpoints."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    pool_settings = PytdxPoolSettings()
    quote_settings = PytdxHqSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        fire_time = _scheduled_job_fire_time(CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID, scheduling)
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.CALL_AUCTION_MARKET_SNAPSHOT,
            fire_time,
            TriggerSource.SCHEDULED,
        )
        try:
            trade_date = fire_time.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date()
            pool = load_endpoint_pool(pool_settings.pytdx_pool_path)
            quote_endpoints = endpoints_for(pool, PytdxCapability.QUOTE)

            def provider_factory(endpoint: tuple[str, int]) -> PytdxHqProvider:
                return PytdxHqProvider(quote_settings, endpoints=(endpoint,))

            service = CallAuctionMarketSnapshotService(
                persistence=PostgreSQLPersistence(engine),
                raw_store=LocalRawStore(settings.raw_data_root),
                quote_endpoints=quote_endpoints,
                provider_factory=provider_factory,
            )
            execution.step(
                "collect_call_auction_market_snapshot",
                1,
                lambda: service.collect(trade_date),
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def run_today_limit_up_snapshot_job() -> None:
    """Fill the exact-date immutable limit-up snapshot after dependency checks."""
    from market_data_center.today_limit_up_service import fill_today_limit_up_snapshot

    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        fire_time = _scheduled_job_fire_time(TODAY_LIMIT_UP_SNAPSHOT_JOB_ID, scheduling)
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.TODAY_LIMIT_UP_SNAPSHOT,
            fire_time,
            TriggerSource.SCHEDULED,
        )
        try:
            trade_date = fire_time.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date()
            execution.step(
                "fill_today_limit_up_snapshot",
                1,
                lambda: fill_today_limit_up_snapshot(
                    engine, LocalRawStore(settings.raw_data_root), trade_date
                ),
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def _scheduled_fire_time(
    hour: int,
    minute: int,
    timezone_name: str,
    *,
    weekdays_only: bool = True,
) -> datetime:
    """Resolve the intended weekday fire time, including after-midnight misfires."""
    timezone = ZoneInfo(timezone_name)
    now = datetime.now(timezone)
    candidate = datetime.combine(now.date(), time(hour, minute), timezone)
    if candidate > now:
        candidate -= timedelta(days=1)
    while weekdays_only and candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate.astimezone(UTC)


def _scheduled_job_fire_time(
    job_code: str,
    settings: SchedulerSettings,
    *,
    weekdays_only: bool = True,
) -> datetime:
    definition = job_definition(job_code, settings)
    if definition.hour is None or definition.minute is None:
        raise ValueError(f"job has no cron time: {job_code}")
    return _scheduled_fire_time(
        definition.hour,
        definition.minute,
        definition.timezone,
        weekdays_only=weekdays_only,
    )


def build_scheduler(settings: SchedulerSettings | None = None) -> BlockingScheduler:
    settings = settings or SchedulerSettings()
    store_path = settings.scheduler_store_path.resolve()
    store_path.parent.mkdir(parents=True, exist_ok=True)
    _remove_retired_jobs(store_path)
    job_store_engine = create_engine(URL.create("sqlite", database=str(store_path)))
    scheduler = BlockingScheduler(
        jobstores={"default": SQLAlchemyJobStore(engine=job_store_engine)},
        executors={"default": ThreadPoolExecutor(max_workers=1)},
        timezone=SCHEDULER_TIMEZONE,
    )
    functions = {
        DAILY_RUN_JOB_ID: run_daily_market_job,
        STOCK_DAILY_INDICATOR_JOB_ID: run_stock_daily_indicator_job,
        STALE_RUN_RECOVERY_JOB_ID: run_stale_recovery_job,
        DEDUCTED_PROFIT_JOB_ID: run_deducted_profit_job,
        STOCK_POOL_JOB_ID: run_stock_pool_job,
        AUCTION_COLLECTION_JOB_ID: run_auction_collection_job,
        EOD_QUOTE_SNAPSHOT_JOB_ID: run_eod_quote_snapshot_job,
        CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID: run_call_auction_market_snapshot_job,
        TODAY_LIMIT_UP_SNAPSHOT_JOB_ID: run_today_limit_up_snapshot_job,
        PYTDX_POOL_REFRESH_JOB_ID: run_pytdx_pool_refresh_job,
    }
    for definition in job_definitions(settings):
        if not definition.enabled:
            continue
        scheduler.add_job(
            functions[definition.code],
            _trigger(definition),
            id=definition.code,
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=definition.timeout_seconds,
        )
    return scheduler


def _remove_retired_jobs(store_path: Path) -> None:
    with connect(store_path) as connection:
        table_exists = connection.execute(
            "select 1 from sqlite_master where type = 'table' and name = 'apscheduler_jobs'"
        ).fetchone()
        if table_exists is None:
            return
        connection.executemany(
            "delete from apscheduler_jobs where id = ?",
            ((job_id,) for job_id in _RETIRED_JOB_IDS),
        )


def _trigger(definition: JobDefinition) -> CronTrigger | IntervalTrigger:
    if definition.trigger_type == "cron":
        if definition.hour is None or definition.minute is None:
            raise ValueError(f"incomplete cron definition: {definition.code}")
        return CronTrigger(
            day_of_week=definition.day_of_week,
            hour=definition.hour,
            minute=definition.minute,
            timezone=definition.timezone,
        )
    if definition.trigger_type == "interval" and definition.interval_hours is not None:
        return IntervalTrigger(hours=definition.interval_hours, timezone=definition.timezone)
    raise ValueError(f"unsupported trigger definition: {definition.code}")


def prepare_locked_worker(
    scheduler_settings: SchedulerSettings,
    persistence: PostgreSQLPersistence,
    operations_persistence: PostgreSQLOperationsPersistence,
    *,
    startup_refresh: Callable[[TriggerSource], PytdxPoolRefreshResult] = run_pytdx_pool_refresh_job,
    scheduler_factory: Callable[[SchedulerSettings], BlockingScheduler] = build_scheduler,
    admin_factory: Callable[
        [SchedulerSettings, PostgreSQLPersistence, PostgreSQLOperationsPersistence],
        WorkerAdminServer,
    ]
    | None = None,
) -> tuple[BlockingScheduler, WorkerAdminServer]:
    """Prepare the runtime after the caller has acquired the global lock."""
    startup_refresh(TriggerSource.RECOVERY)
    scheduler = scheduler_factory(scheduler_settings)
    if admin_factory is None:
        from market_data_center.worker_admin import start_worker_admin_server

        admin_factory = start_worker_admin_server
    admin_server = admin_factory(scheduler_settings, persistence, operations_persistence)
    return scheduler, admin_server


def check_scheduler_health(
    settings: SchedulerSettings,
    persistence: PostgreSQLPersistence,
    *,
    now: datetime | None = None,
) -> SchedulerHealthReport:
    current = now or datetime.now(UTC)
    job_store = read_job_store_snapshot(settings)
    persisted_job_ids = tuple(task.task_id for task in job_store.tasks)
    stale_runs = persistence.stale_ingestion_run_ids(current - timedelta(minutes=60))
    latest_snapshot = persistence.latest_stock_daily_indicator_snapshot()
    expected_jobs = {
        definition.code for definition in job_definitions(settings) if definition.enabled
    }
    latest_date = latest_snapshot[0] if latest_snapshot is not None else None
    latest_rows = latest_snapshot[1] if latest_snapshot is not None else 0
    recent_enough = latest_date is not None and latest_date >= current.date() - timedelta(days=10)
    return SchedulerHealthReport(
        healthy=(
            job_store.available
            and expected_jobs.issubset(persisted_job_ids)
            and not stale_runs
            and latest_rows > 0
            and recent_enough
        ),
        persisted_job_ids=persisted_job_ids,
        stale_run_count=len(stale_runs),
        latest_snapshot_date=latest_date.isoformat() if latest_date is not None else None,
        latest_snapshot_rows=latest_rows,
    )


def run_worker(*, check: bool = False) -> None:
    """Run the unified long-lived Worker or its read-only health check."""
    basicConfig(level=INFO)
    scheduler_settings = SchedulerSettings()
    if check:
        worker_settings = WorkerSettings()  # type: ignore[call-arg]
        health_engine = create_engine(
            sqlalchemy_url(worker_settings.database_url.get_secret_value()), pool_pre_ping=True
        )
        try:
            report = check_scheduler_health(
                scheduler_settings, PostgreSQLPersistence(health_engine)
            )
            print(dumps(asdict(report), sort_keys=True))
        finally:
            health_engine.dispose()
        raise SystemExit(0 if report.healthy else 1)
    worker_settings = WorkerSettings()  # type: ignore[call-arg]
    lock_engine = create_engine(
        sqlalchemy_url(worker_settings.database_url.get_secret_value()), pool_pre_ping=True
    )

    scheduler: BlockingScheduler | None = None

    def stop_scheduler(signum: int, frame: FrameType | None) -> None:
        del frame
        LOGGER.info("received signal=%d; waiting for scheduled jobs to stop", signum)
        if scheduler is not None and scheduler.running:
            scheduler.shutdown(wait=True)

    signal(SIGINT, stop_scheduler)
    signal(SIGTERM, stop_scheduler)
    admin_server = None
    try:
        with PostgreSQLPersistence(lock_engine).task_lock(SCHEDULER_LOCK_KEY):
            run_stale_recovery_job()
            scheduler, admin_server = prepare_locked_worker(
                scheduler_settings,
                PostgreSQLPersistence(lock_engine),
                PostgreSQLOperationsPersistence(lock_engine),
            )
            LOGGER.info("starting Market Data Center scheduler")
            scheduler.start()
    finally:
        if admin_server is not None:
            admin_server.shutdown()
            admin_server.server_close()
        if scheduler is not None and scheduler.running:
            scheduler.shutdown(wait=True)
        lock_engine.dispose()
