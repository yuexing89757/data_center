"""Embedded scheduling for the unified long-lived collection Worker."""

from argparse import Namespace
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from json import dumps
from logging import INFO, basicConfig, getLogger
from pathlib import Path
from signal import SIGINT, SIGTERM, signal
from sqlite3 import Error as SQLiteError
from sqlite3 import connect
from time import sleep
from types import FrameType
from typing import Protocol
from zoneinfo import ZoneInfo

from apscheduler.executors.pool import ThreadPoolExecutor  # type: ignore[import-untyped]
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore  # type: ignore[import-untyped]
from apscheduler.schedulers.blocking import BlockingScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]
from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-untyped]
from sqlalchemy import URL, create_engine

from market_data_center.board_index_daily_schedule import collect_board_index_daily_bar_gap
from market_data_center.call_auction_market_series_service import (
    CallAuctionMarketSeriesService,
)
from market_data_center.call_auction_market_service import CallAuctionMarketSnapshotService
from market_data_center.cli import run_daily_workflow, run_stock_daily_indicator_workflow
from market_data_center.close_price_new_highs_service import ClosePriceNewHighsService
from market_data_center.data_cleanup_service import DataCleanupService
from market_data_center.database_urls import sqlalchemy_url
from market_data_center.domain.operations import TriggerSource, WorkflowCode
from market_data_center.dragon_tiger_service import DragonTigerService
from market_data_center.operations_service import WorkflowExecutionService
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.persistence.auction_postgres import PostgreSQLAuctionPersistence
from market_data_center.persistence.call_auction_market_series_postgres import (
    PostgreSQLCallAuctionMarketSeriesPersistence,
)
from market_data_center.persistence.close_price_new_highs_postgres import (
    PostgreSQLClosePriceNewHighsPersistence,
)
from market_data_center.persistence.dragon_tiger_postgres import (
    PostgreSQLDragonTigerPersistence,
)
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
from market_data_center.persistence.regulation_postgres import (
    PostgreSQLRegulationPersistence,
)
from market_data_center.persistence.stock_pool_postgres import PostgreSQLStockPoolPersistence
from market_data_center.pipeline import BoardIndexIngestionPipeline, IngestionPipeline
from market_data_center.providers import create_board_index_provider, create_provider
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.eastmoney_dragon_tiger import (
    EastmoneyDragonTigerAdapter,
)
from market_data_center.providers.pytdx_hq import PytdxHqProvider
from market_data_center.providers.pytdx_pool import (
    PytdxCapability,
    PytdxPoolRefreshResult,
    endpoints_for,
    load_endpoint_pool,
    refresh_endpoint_pool,
)
from market_data_center.raw_store import LocalRawStore
from market_data_center.regulation_benchmark_service import RegulationBenchmarkService
from market_data_center.regulation_service import RegulationService
from market_data_center.reliability import recover_stale_runs
from market_data_center.scheduling_catalog import (
    BOARD_INDEX_DAILY_BAR_JOB_ID,
    CALL_AUCTION_MARKET_SERIES_JOB_ID,
    CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID,
    CLOSE_PRICE_NEW_HIGHS_120D_JOB_ID,
    DAILY_RUN_JOB_ID,
    DATA_CLEANUP_JOB_ID,
    DEDUCTED_PROFIT_JOB_ID,
    DRAGON_TIGER_JOB_ID,
    EOD_QUOTE_SNAPSHOT_JOB_ID,
    PYTDX_POOL_REFRESH_JOB_ID,
    REGULATION_DAILY_CALCULATION_JOB_ID,
    SCHEDULER_TIMEZONE,
    SHAREHOLDER_COUNT_DAILY_JOB_ID,
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
from market_data_center.shareholder_count_service import ShareholderCountService
from market_data_center.stock_pool_service import StockPoolService

SCHEDULER_LOCK_KEY = "market-data-center:scheduler"
LOGGER = getLogger(__name__)
_RETIRED_JOB_IDS = (
    "call-auction-snapshot-daily",
    "opening-auction-limit-up-quotes",
    "trading-billboard-daily",
)


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


def run_close_price_new_highs_120d_job() -> None:
    """Build one immutable exact-date closing-high snapshot after daily market ingestion."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    scheduled_for = _scheduled_job_fire_time(CLOSE_PRICE_NEW_HIGHS_120D_JOB_ID, scheduling)
    trade_date = scheduled_for.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date()
    try:
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.CLOSE_PRICE_NEW_HIGHS_120D,
            scheduled_for,
            TriggerSource.SCHEDULED,
        )
        try:
            execution.step(
                "build_close_price_new_highs_120d_snapshot",
                1,
                lambda: ClosePriceNewHighsService(
                    PostgreSQLClosePriceNewHighsPersistence(engine)
                ).build(trade_date),
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def run_board_index_daily_bar_job() -> None:
    """Collect the missing THS:883423 daily-bar tail after market close."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    fire_time = _scheduled_job_fire_time(BOARD_INDEX_DAILY_BAR_JOB_ID, scheduling)
    as_of_date = fire_time.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date()
    try:
        persistence = PostgreSQLPersistence(engine)
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.BOARD_INDEX_DAILY_BAR,
            fire_time,
            TriggerSource.SCHEDULED,
        )
        try:
            expected_date = persistence.latest_trading_date(
                as_of_date - timedelta(days=14), as_of_date
            )
            if expected_date is None:
                raise RuntimeError("CN_A_SHARE trading calendar has no recent trading date")
            latest_stored_date = persistence.latest_board_index_daily_bar_date("THS:883423")

            def ingest(start_date: date, end_date: date) -> object:
                with create_board_index_provider("akshare_ths") as provider:
                    run = BoardIndexIngestionPipeline(
                        provider=provider,
                        persistence=persistence,
                        raw_store=LocalRawStore(settings.raw_data_root),
                    ).ingest_board_index_daily_bars("THS:883423", start_date, end_date)
                stored_date = persistence.latest_board_index_daily_bar_date("THS:883423")
                if stored_date is None or stored_date < end_date:
                    raise ProviderError("THS board-index response did not include expected date")
                return run

            execution.step(
                "collect_board_index_daily_bars",
                1,
                lambda: collect_board_index_daily_bar_gap(
                    expected_date=expected_date,
                    latest_stored_date=latest_stored_date,
                    ingest=ingest,
                    before_retry=lambda _attempt: sleep(2),
                ),
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
            series_count = execution.step(
                "recover_call_auction_market_series_sessions",
                4,
                lambda: PostgreSQLCallAuctionMarketSeriesPersistence(
                    engine
                ).recover_expired_sessions(datetime.now(UTC)),
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
        LOGGER.info(
            "recovered stale runs ingestion_count=%d workflow_count=%d auction_count=%d "
            "series_count=%d",
            len(ingestion_ids),
            workflow_count,
            auction_count,
            series_count,
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


def run_shareholder_count_daily_job() -> None:
    """Synchronize the rolling shareholder-count disclosure window inside the Worker."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    scheduled_for = _scheduled_job_fire_time(
        SHAREHOLDER_COUNT_DAILY_JOB_ID,
        scheduling,
        weekdays_only=False,
    )
    as_of_date = datetime.now(ZoneInfo(SCHEDULER_TIMEZONE)).date()
    try:
        persistence = PostgreSQLPersistence(engine)
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.SHAREHOLDER_COUNT_DAILY,
            scheduled_for,
            TriggerSource.SCHEDULED,
        )
        try:
            with create_provider("tushare") as provider:
                service = ShareholderCountService(
                    IngestionPipeline(
                        provider=provider,
                        persistence=persistence,
                        raw_store=LocalRawStore(settings.raw_data_root),
                    ),
                    persistence,
                )
                execution.step(
                    "shareholder_count_daily",
                    1,
                    lambda: service.sync_daily(as_of_date),
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


def run_call_auction_market_series_job() -> None:
    """Collect the fixed 32-round full-market opening-auction series."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    pool_settings = PytdxPoolSettings()
    quote_settings = PytdxHqSettings(pytdx_hq_batch_size=80)
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        fire_time = _scheduled_job_fire_time(CALL_AUCTION_MARKET_SERIES_JOB_ID, scheduling)
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.CALL_AUCTION_MARKET_SERIES,
            fire_time,
            TriggerSource.SCHEDULED,
        )
        try:
            trade_date = fire_time.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date()
            pool = load_endpoint_pool(pool_settings.pytdx_pool_path)
            quote_endpoints = endpoints_for(pool, PytdxCapability.QUOTE)

            def provider_factory(endpoint: tuple[str, int]) -> PytdxHqProvider:
                return PytdxHqProvider(quote_settings, endpoints=(endpoint,))

            service = CallAuctionMarketSeriesService(
                persistence=PostgreSQLCallAuctionMarketSeriesPersistence(engine),
                raw_store=LocalRawStore(settings.raw_data_root),
                quote_endpoints=quote_endpoints,
                provider_factory=provider_factory,
                retry_budget_seconds=quote_settings.pytdx_hq_timeout_seconds,
            )
            execution.step(
                "collect_call_auction_market_series",
                1,
                lambda: service.collect(trade_date, execution.run.workflow_run_id),
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


def run_dragon_tiger_job() -> None:
    """Collect one exact Shanghai trading-date billboard batch when opt-in is enabled."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        fire_time = _scheduled_job_fire_time(DRAGON_TIGER_JOB_ID, scheduling)
        trade_date = fire_time.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date()
        persistence = PostgreSQLDragonTigerPersistence(engine)
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.DRAGON_TIGER_DAILY,
            fire_time,
            TriggerSource.SCHEDULED,
        )
        try:
            if persistence.is_trading_day(trade_date):
                service = DragonTigerService(
                    persistence=persistence,
                    raw_store=LocalRawStore(settings.raw_data_root),
                    provider=EastmoneyDragonTigerAdapter(),
                )
                execution.step("collect_dragon_tiger", 1, lambda: service.collect(trade_date))
            else:
                execution.step("collect_dragon_tiger", 1, lambda: 0)
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def run_regulation_daily_calculation_job() -> None:
    """Calculate exact-date regulation status and T+1 condition warnings."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        fire_time = _scheduled_job_fire_time(REGULATION_DAILY_CALCULATION_JOB_ID, scheduling)
        trade_date = fire_time.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date()
        facts = PostgreSQLPersistence(engine)
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.REGULATION_DAILY_CALCULATION,
            fire_time,
            TriggerSource.SCHEDULED,
        )
        try:
            if facts.is_trading_day(trade_date):
                with create_provider("baostock") as provider:
                    execution.step(
                        "collect_regulation_benchmarks",
                        1,
                        lambda: RegulationBenchmarkService(
                            IngestionPipeline(
                                provider=provider,
                                persistence=facts,
                                raw_store=LocalRawStore(settings.raw_data_root),
                            )
                        ).collect(trade_date),
                    )
                execution.step(
                    "calculate_regulation_warnings",
                    2,
                    lambda: RegulationService(
                        PostgreSQLRegulationPersistence(engine),
                        clock=lambda: datetime.now(UTC),
                    ).calculate(trade_date),
                )
            else:
                execution.step("collect_regulation_benchmarks", 1, lambda: 0)
                execution.step("calculate_regulation_warnings", 2, lambda: 0)
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
    finally:
        engine.dispose()


def run_data_cleanup_job() -> None:
    """Delete auction-series details older than three completed trading days."""
    settings = WorkerSettings()  # type: ignore[call-arg]
    scheduling = SchedulerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        fire_time = _scheduled_job_fire_time(
            DATA_CLEANUP_JOB_ID,
            scheduling,
            weekdays_only=False,
        )
        reference_date = fire_time.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date()
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.DATA_CLEANUP,
            fire_time,
            TriggerSource.SCHEDULED,
        )
        try:
            service = DataCleanupService(PostgreSQLPersistence(engine))
            execution.step(
                "cleanup_call_auction_market_series_snapshots",
                1,
                lambda: service.run(reference_date),
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
    second: int = 0,
    weekdays_only: bool = True,
) -> datetime:
    """Resolve the intended weekday fire time, including after-midnight misfires."""
    timezone = ZoneInfo(timezone_name)
    now = datetime.now(timezone)
    candidate = datetime.combine(now.date(), time(hour, minute, second), timezone)
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
    hours = _cron_hour_values(definition.hour)
    return max(
        _scheduled_fire_time(
            hour,
            definition.minute,
            definition.timezone,
            second=definition.second or 0,
            weekdays_only=weekdays_only,
        )
        for hour in hours
    )


def _cron_hour_values(value: int | str) -> tuple[int, ...]:
    if isinstance(value, int):
        return (value,)
    if "-" in value:
        start, end = (int(part) for part in value.split("-", maxsplit=1))
        return tuple(range(start, end + 1))
    return tuple(int(part) for part in value.split(","))


def build_scheduler(settings: SchedulerSettings | None = None) -> BlockingScheduler:
    settings = settings or SchedulerSettings()
    store_path = settings.scheduler_store_path.resolve()
    store_path.parent.mkdir(parents=True, exist_ok=True)
    _remove_retired_jobs(store_path)
    job_store_engine = create_engine(URL.create("sqlite", database=str(store_path)))
    scheduler = BlockingScheduler(
        jobstores={"default": SQLAlchemyJobStore(engine=job_store_engine)},
        executors={
            "default": ThreadPoolExecutor(max_workers=1),
            "morning_auction": ThreadPoolExecutor(max_workers=1),
        },
        timezone=SCHEDULER_TIMEZONE,
    )
    functions = {
        DAILY_RUN_JOB_ID: run_daily_market_job,
        STOCK_DAILY_INDICATOR_JOB_ID: run_stock_daily_indicator_job,
        STALE_RUN_RECOVERY_JOB_ID: run_stale_recovery_job,
        DEDUCTED_PROFIT_JOB_ID: run_deducted_profit_job,
        SHAREHOLDER_COUNT_DAILY_JOB_ID: run_shareholder_count_daily_job,
        STOCK_POOL_JOB_ID: run_stock_pool_job,
        EOD_QUOTE_SNAPSHOT_JOB_ID: run_eod_quote_snapshot_job,
        CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID: run_call_auction_market_snapshot_job,
        CALL_AUCTION_MARKET_SERIES_JOB_ID: run_call_auction_market_series_job,
        TODAY_LIMIT_UP_SNAPSHOT_JOB_ID: run_today_limit_up_snapshot_job,
        CLOSE_PRICE_NEW_HIGHS_120D_JOB_ID: run_close_price_new_highs_120d_job,
        BOARD_INDEX_DAILY_BAR_JOB_ID: run_board_index_daily_bar_job,
        DRAGON_TIGER_JOB_ID: run_dragon_tiger_job,
        REGULATION_DAILY_CALCULATION_JOB_ID: run_regulation_daily_calculation_job,
        DATA_CLEANUP_JOB_ID: run_data_cleanup_job,
        PYTDX_POOL_REFRESH_JOB_ID: run_pytdx_pool_refresh_job,
    }
    for definition in job_definitions(settings):
        if not definition.enabled:
            continue
        executor = (
            "morning_auction" if definition.code == CALL_AUCTION_MARKET_SERIES_JOB_ID else "default"
        )
        scheduler.add_job(
            functions[definition.code],
            _trigger(definition),
            id=definition.code,
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=definition.timeout_seconds,
            executor=executor,
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
        trigger_options: dict[str, object] = {
            "day_of_week": definition.day_of_week,
            "hour": definition.hour,
            "minute": definition.minute,
            "timezone": definition.timezone,
        }
        if definition.second is not None:
            trigger_options["second"] = definition.second
        return CronTrigger(
            **trigger_options,
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
