"""Manual phase-one ingestion commands."""

from argparse import SUPPRESS, ArgumentParser, Namespace
from calendar import monthrange
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from functools import partial
from json import dumps
from sys import stderr, stdin
from time import monotonic
from typing import cast
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine

from market_data_center.auction_service import AuctionCollectionService
from market_data_center.close_price_new_highs_service import ClosePriceNewHighsService
from market_data_center.daily_bar_batch import DailyBarBulkSummary, PreparedDailyBarBatch
from market_data_center.database_urls import sqlalchemy_url
from market_data_center.derivation import (
    DEFAULT_ALGORITHM_VERSION,
    DerivationService,
)
from market_data_center.domain import CalculationMode
from market_data_center.domain.ingestion import DatasetCode, IngestionRun, IngestionStatus
from market_data_center.domain.operations import TriggerSource, WorkflowCode
from market_data_center.domain.regulation import REGULATION_RULES_EFFECTIVE_FROM
from market_data_center.domain.stock_pool import (
    MAINBOARD_LIMIT_DOWN_POOL,
    MAINBOARD_LIMIT_UP_POOL,
)
from market_data_center.operations_service import WorkflowExecution, WorkflowExecutionService
from market_data_center.persistence import (
    PostgreSQLDerivedPersistence,
    PostgreSQLOperationsPersistence,
    PostgreSQLPersistence,
    PostgreSQLRegulationPersistence,
    PostgreSQLStockPoolPersistence,
)
from market_data_center.persistence.auction_postgres import PostgreSQLAuctionPersistence
from market_data_center.persistence.close_price_new_highs_postgres import (
    PostgreSQLClosePriceNewHighsPersistence,
)
from market_data_center.persistence.trading_billboard_postgres import (
    PostgreSQLTradingBillboardPersistence,
)
from market_data_center.pipeline import BoardIndexIngestionPipeline, IngestionPipeline
from market_data_center.providers import (
    ManagedMarketDataProvider,
    ProviderRequestUnavailable,
    ProviderRouter,
    ProviderRoutingError,
    RoutedResult,
    TradingBillboardProvider,
    available_board_index_provider_codes,
    available_provider_codes,
    create_board_index_provider,
    create_provider,
)
from market_data_center.raw_store import LocalRawStore
from market_data_center.regulation_service import RegulationService
from market_data_center.reliability import (
    RawReplayService,
    compare_daily_bar_sources,
    recover_stale_runs,
)
from market_data_center.settings import WorkerSettings
from market_data_center.shareholder_count_batch import ShareholderCountSyncSummary
from market_data_center.shareholder_count_service import (
    ShareholderCountBackfillTarget,
    ShareholderCountService,
)
from market_data_center.stock_pool_service import StockPoolService
from market_data_center.trading_billboard_service import (
    TradingBillboardBackfillSummary,
    TradingBillboardCollectionSummary,
    TradingBillboardService,
)

AUTO_PROVIDER_CODE = "auto"
DEFAULT_BOARD_INDEX_PROVIDER_CODE = "akshare_ths"
BOARD_INDEX_DATASETS = frozenset(
    {
        "board-index",
        "board-index-daily-bar",
        "board-index-constituents",
    }
)
SHANGHAI_TIME_ZONE = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class StockDailyIndicatorWorkflowResult:
    as_of_date: date
    calendar_run: IngestionRun
    snapshot_run: IngestionRun
    cutoff_date: date
    deleted_rows: int


class _TradingBillboardBackfillStopped(RuntimeError):
    def __init__(self, summary: TradingBillboardBackfillSummary) -> None:
        super().__init__("trading billboard backfill stopped at the first failed date")
        self.summary = summary


def main() -> None:
    args = _parser().parse_args()
    if args.dataset == "worker":
        from market_data_center.scheduler import run_worker

        run_worker(check=args.check)
        return
    if args.dataset in {"shareholder-count-daily", "shareholder-count-backfill"}:
        _validate_shareholder_count_command(
            args,
            today=datetime.now(SHANGHAI_TIME_ZONE).date(),
            interactive=stdin.isatty(),
        )
    if args.dataset == "trading-billboard-collect":
        _run_trading_billboard_command(args)
        return
    settings = WorkerSettings()  # type: ignore[call-arg]
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    persistence = PostgreSQLPersistence(engine)
    raw_store = LocalRawStore(settings.raw_data_root)

    if args.dataset in {"shareholder-count-daily", "shareholder-count-backfill"}:
        workflow_code = (
            WorkflowCode.SHAREHOLDER_COUNT_DAILY
            if args.dataset == "shareholder-count-daily"
            else WorkflowCode.SHAREHOLDER_COUNT_BACKFILL
        )
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            workflow_code,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.MANUAL,
        )
        try:
            shareholder_summary = run_shareholder_count_workflow(
                args,
                persistence,
                raw_store,
                today=datetime.now(SHANGHAI_TIME_ZONE).date(),
                interactive=stdin.isatty(),
                execution=execution,
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
        print(dumps(asdict(shareholder_summary), sort_keys=True))
        return

    if args.dataset == "call-auction-indicative-detail":
        if not args.confirm_current_day_single_symbol:
            raise SystemExit(
                "operator confirmation is required; this command is disabled by default"
            )
        from market_data_center.auction_indicative_service import (
            collect_current_day_auction_indicative,
        )
        from market_data_center.persistence.auction_indicative_postgres import (
            PostgreSQLAuctionIndicativePersistence,
        )

        requested_date = date.fromisoformat(args.trade_date)
        indicative_run, version = collect_current_day_auction_indicative(
            PostgreSQLAuctionIndicativePersistence(engine),
            raw_store,
            args.symbol,
            requested_date,
            now=datetime.now(SHANGHAI_TIME_ZONE),
        )
        print(
            f"call_auction_indicative_detail {indicative_run.status.value} "
            f"ingestion_id={indicative_run.ingestion_id} version={version}"
        )
        return

    if args.dataset == "realtime-quotes":
        if not args.confirm_bounded_tencent_request:
            raise SystemExit(
                "operator confirmation is required; this command is disabled by default"
            )
        from market_data_center.persistence.realtime_quote_postgres import (
            PostgreSQLRealtimeQuotePersistence,
        )
        from market_data_center.providers.tencent_quote import TencentQuoteProvider
        from market_data_center.realtime_quote_service import collect_tencent_realtime_quotes

        quote_run = collect_tencent_realtime_quotes(
            PostgreSQLRealtimeQuotePersistence(engine),
            raw_store,
            tuple(args.symbols),
            provider=TencentQuoteProvider(),
        )
        print(
            f"five_level_quote {quote_run.status.value} "
            f"provider={quote_run.provider_code.value} ingestion_id={quote_run.ingestion_id}"
        )
        return

    if args.dataset == "auction-quotes-preflight":
        trade_date = date.fromisoformat(args.trade_date)
        repository = PostgreSQLAuctionPersistence(engine)

        class _NoNetworkProvider:
            source_code = "preflight"

            def fetch_five_level_quotes(
                self, symbols: object, *, deadline: object = None
            ) -> object:
                del symbols, deadline
                raise AssertionError("preflight must not access a quote provider")

        report = AuctionCollectionService(
            repository,
            _NoNetworkProvider(),  # type: ignore[arg-type]
            raw_store,
            cadence_seconds=args.cadence_seconds,
            max_retries=0,
        ).preflight(trade_date)
        print(dumps(report, ensure_ascii=False, sort_keys=True))
        return

    if args.dataset in BOARD_INDEX_DATASETS:
        board_run = _run_board_index_command(args, persistence, raw_store)
        print(
            f"{board_run.dataset_code.value} {board_run.status.value} "
            f"provider={board_run.provider_code.value} "
            f"ingestion_id={board_run.ingestion_id}"
        )
        return

    if args.dataset == "derived-recompute":
        try:
            summary = DerivationService(PostgreSQLDerivedPersistence(engine)).recompute(
                date.fromisoformat(args.start_date),
                date.fromisoformat(args.end_date),
                mode=CalculationMode(args.mode),
                algorithm_version=args.algorithm_version,
            )
            print(dumps(summary.as_json(), ensure_ascii=False, sort_keys=True))
        except Exception as error:
            print(
                dumps(
                    {
                        "status": "failed",
                        "operation": args.dataset,
                        "error_type": type(error).__name__,
                    },
                    sort_keys=True,
                ),
                file=stderr,
            )
            raise SystemExit(1) from None
        return

    if args.dataset == "stock-pool-check":
        try:
            if args.version is not None and args.version < 1:
                raise ValueError("stock-pool version must be positive")
            snapshot = PostgreSQLStockPoolPersistence(engine).inspect_ready_snapshot(
                args.pool_code,
                date.fromisoformat(args.effective_trade_date),
                args.version,
            )
            print(dumps(snapshot, ensure_ascii=False, sort_keys=True, default=str))
        except Exception as error:
            print(
                dumps(
                    {
                        "status": "failed",
                        "operation": args.dataset,
                        "error_type": type(error).__name__,
                    },
                    sort_keys=True,
                ),
                file=stderr,
            )
            raise SystemExit(1) from None
        return

    if args.dataset == "stock-pools-build":
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.STOCK_POOL,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.MANUAL,
        )
        try:
            pool_summary = execution.step(
                "build_stock_pools",
                1,
                lambda: StockPoolService(PostgreSQLStockPoolPersistence(engine)).build(
                    date.fromisoformat(args.basis_trade_date)
                ),
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
        print(dumps(_stock_pool_summary_json(pool_summary), sort_keys=True))
        return

    if args.dataset == "today-limit-up-snapshot":
        from market_data_center.today_limit_up_service import fill_today_limit_up_snapshot

        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.TODAY_LIMIT_UP_SNAPSHOT,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.MANUAL,
        )
        try:
            limit_up_summary = execution.step(
                "fill_today_limit_up_snapshot",
                1,
                lambda: fill_today_limit_up_snapshot(
                    engine, raw_store, date.fromisoformat(args.trade_date)
                ),
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
        print(dumps(asdict(limit_up_summary), default=str, sort_keys=True))
        return

    if args.dataset == "close-price-new-highs-120d-build":
        trade_date = date.fromisoformat(args.trade_date)
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.CLOSE_PRICE_NEW_HIGHS_120D,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.MANUAL,
        )
        try:
            closing_high_summary = execution.step(
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
        print(dumps(asdict(closing_high_summary), default=str, sort_keys=True))
        return

    if args.dataset == "regulation-calculate":
        trade_date = _validate_regulation_calculation_date(
            args, today=datetime.now(SHANGHAI_TIME_ZONE).date()
        )
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.REGULATION_DAILY_CALCULATION,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.MANUAL,
        )
        try:
            regulation_summary = execution.step(
                "calculate_regulation_warnings",
                1,
                lambda: RegulationService(
                    PostgreSQLRegulationPersistence(engine),
                    clock=lambda: datetime.now(UTC),
                ).calculate(trade_date),
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
        print(dumps(asdict(regulation_summary), default=str, sort_keys=True))
        return

    if args.dataset in {"raw-replay", "recover-stale-runs", "compare-daily-bars"}:
        try:
            _run_reliability_command(args, persistence, raw_store)
        except Exception as error:
            print(
                dumps(
                    {
                        "status": "failed",
                        "operation": args.dataset,
                        "error_type": type(error).__name__,
                    },
                    sort_keys=True,
                ),
                file=stderr,
            )
            raise SystemExit(1) from None
        return
    if args.dataset == "daily-run":
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.DAILY_MARKET,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.MANUAL,
        )
        try:
            run_daily_workflow(args, persistence, raw_store, execution=execution)
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
        return
    if args.dataset == "stock-daily-indicators-daily":
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.STOCK_DAILY_INDICATOR,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.MANUAL,
        )
        try:
            run_stock_daily_indicator_workflow(args, persistence, raw_store, execution=execution)
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
        return
    if args.dataset == "stock-daily-indicator-retention":
        cutoff_date = date.fromisoformat(args.cutoff_date)
        deleted = persistence.delete_stock_daily_indicators_before(cutoff_date)
        print(f"stock-daily-indicator-retention cutoff={cutoff_date.isoformat()} deleted={deleted}")
        return
    if args.dataset == "deducted-profit-daily":
        if args.provider != "tushare":
            raise SystemExit("deducted-profit-daily requires --provider tushare")
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.DEDUCTED_PROFIT,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.MANUAL,
        )
        try:
            run = execution.step(
                "deducted_profit",
                1,
                lambda: _run_explicit(args, persistence, raw_store),
            )
        except BaseException as error:
            execution.fail(error)
            raise
        execution.succeed()
        if not isinstance(run, IngestionRun):
            raise RuntimeError("deducted-profit workflow returned no ingestion run")
        print(
            f"{run.dataset_code.value} {run.status.value} "
            f"provider={run.provider_code.value} ingestion_id={run.ingestion_id}"
        )
        return
    if (
        args.dataset
        in {
            "stock-daily-indicator",
            "stock-daily-indicators-bulk",
            "deducted-profit-daily",
        }
        and args.provider == AUTO_PROVIDER_CODE
    ):
        raise SystemExit(f"{args.dataset} requires --provider tushare")
    if args.dataset in ("eod-quote-snapshot", "call-auction-snapshot"):
        from market_data_center.snapshot_collector import (
            collect_call_auction,
            collect_eod_quotes,
        )

        trade_date = (
            date.fromisoformat(args.trade_date)
            if args.trade_date
            else datetime.now(SHANGHAI_TIME_ZONE).date()
        )
        snapshot_engine = create_engine(
            sqlalchemy_url(WorkerSettings().database_url.get_secret_value()),  # type: ignore[call-arg]
            pool_pre_ping=True,
        )
        if args.dataset == "eod-quote-snapshot":
            collect_eod_quotes(snapshot_engine, trade_date)
        else:
            collect_call_auction(snapshot_engine, trade_date)
        snapshot_engine.dispose()
        return
    if args.provider == AUTO_PROVIDER_CODE:
        run = _run_automatic(args, persistence, raw_store)
    else:
        run = _run_explicit(args, persistence, raw_store)
    if isinstance(run, IngestionRun):
        print(
            f"{run.dataset_code.value} {run.status.value} "
            f"provider={run.provider_code.value} ingestion_id={run.ingestion_id}"
        )


def _run_reliability_command(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> None:
    if args.dataset == "raw-replay":
        summary = RawReplayService(
            raw_store=raw_store,
            persistence=persistence,
        ).replay(UUID(args.ingestion_id), dry_run=args.dry_run)
        print(dumps(summary.as_json(), ensure_ascii=False, sort_keys=True))
        return
    if args.dataset == "recover-stale-runs":
        ingestion_ids = recover_stale_runs(
            persistence,
            older_than=timedelta(minutes=args.older_than_minutes),
            dry_run=args.dry_run,
        )
        print(
            dumps(
                {
                    "status": "dry_run" if args.dry_run else "recovered",
                    "count": len(ingestion_ids),
                    "ingestion_ids": [str(ingestion_id) for ingestion_id in ingestion_ids],
                },
                sort_keys=True,
            )
        )
        return
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    report = compare_daily_bar_sources(
        persistence,
        raw_store,
        symbol=args.symbol,
        start_date=start_date,
        end_date=end_date,
    )
    print(dumps(report.as_json(), ensure_ascii=False, sort_keys=True))


def _validate_shareholder_count_command(
    args: Namespace,
    *,
    today: date,
    interactive: bool,
) -> None:
    if args.provider != "tushare":
        raise SystemExit(f"{args.dataset} requires --provider tushare")
    if args.dataset == "shareholder-count-daily":
        if args.as_of_date is not None and date.fromisoformat(args.as_of_date) > today:
            raise SystemExit("shareholder-count daily as-of date must not be in the future")
        return
    cutoff_date = date.fromisoformat(args.cutoff_date)
    if cutoff_date > today:
        raise SystemExit("shareholder-count backfill cutoff must not be in the future")
    if not args.yes and not interactive:
        raise SystemExit("shareholder-count backfill requires --yes in non-interactive mode")


def run_shareholder_count_workflow(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
    *,
    today: date | None = None,
    interactive: bool | None = None,
    execution: WorkflowExecution | None = None,
) -> ShareholderCountSyncSummary:
    current_date = today or datetime.now(SHANGHAI_TIME_ZONE).date()
    is_interactive = stdin.isatty() if interactive is None else interactive
    _validate_shareholder_count_command(
        args,
        today=current_date,
        interactive=is_interactive,
    )
    targets: tuple[ShareholderCountBackfillTarget, ...] = ()
    cutoff_date: date | None = None
    if args.dataset == "shareholder-count-backfill":
        cutoff_date = date.fromisoformat(args.cutoff_date)
        requested_symbols = set(args.symbols) if args.symbols else None
        targets = persistence.shareholder_count_backfill_targets(
            requested_symbols,
            args.resume_after_symbol,
        )
        earliest = min((target.start_date for target in targets), default=None)
        print(
            "shareholder-count-backfill "
            f"targets={len(targets)} "
            f"estimated_minimum_requests={len(targets)} "
            f"range={earliest.isoformat() if earliest else 'empty'}..{cutoff_date.isoformat()}"
        )
        if not args.yes:
            confirmation = input("Type yes to start controlled shareholder-count backfill: ")
            if confirmation.strip().lower() != "yes":
                raise SystemExit("shareholder-count backfill cancelled")

    with create_provider("tushare") as provider:
        service = ShareholderCountService(
            IngestionPipeline(
                provider=provider,
                raw_store=raw_store,
                persistence=persistence,
            ),
            persistence,
        )

        def synchronize() -> ShareholderCountSyncSummary:
            if args.dataset == "shareholder-count-daily":
                as_of_date = (
                    date.fromisoformat(args.as_of_date) if args.as_of_date else current_date
                )
                return service.sync_daily(as_of_date)
            assert cutoff_date is not None
            return service.backfill_targets(cutoff_date, targets)

        if execution is None:
            return synchronize()
        job_code = (
            "shareholder_count_daily"
            if args.dataset == "shareholder-count-daily"
            else "shareholder_count_backfill"
        )
        return execution.step(job_code, 1, synchronize)


def _run_explicit(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> IngestionRun | DailyBarBulkSummary | None:
    with create_provider(args.provider) as provider:
        pipeline = IngestionPipeline(
            provider=provider,
            raw_store=raw_store,
            persistence=persistence,
        )
        if args.dataset == "daily-bars-bulk":
            if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
                raise SystemExit("shard-index must be in [0, shard-count)")
            start_date = date.fromisoformat(args.start_date)
            end_date = date.fromisoformat(args.end_date)
            symbols = _bulk_symbols(args, persistence, start_date, end_date)
            failures = 0
            unavailable = 0
            pending: list[PreparedDailyBarBatch] = []
            batch_size = _daily_bar_write_batch_size(args)
            known_symbols = set(symbols)
            known_dates = persistence.known_trading_dates(
                {
                    start_date + timedelta(days=offset)
                    for offset in range((end_date - start_date).days + 1)
                }
            )
            for position, symbol in enumerate(symbols, start=1):
                try:
                    pending.append(
                        pipeline.prepare_daily_bars(
                            provider.source_symbol(symbol),
                            start_date,
                            end_date,
                            known_symbols=known_symbols,
                            known_trading_dates=known_dates,
                        )
                    )
                except ProviderRequestUnavailable as error:
                    if getattr(args, "allow_unavailable", False):
                        unavailable += 1
                    else:
                        failures += 1
                        print(f"failed {symbol}: {type(error).__name__}", file=stderr)
                except Exception as error:
                    failures += 1
                    print(f"failed {symbol}: {type(error).__name__}", file=stderr)
                if len(pending) >= batch_size:
                    _commit_daily_bar_batches(persistence, pending, position, len(symbols))
                    pending.clear()
                if position % 100 == 0 or position == len(symbols):
                    print(
                        f"progress={position}/{len(symbols)} failures={failures} "
                        f"unavailable={unavailable}"
                    )
            if pending:
                _commit_daily_bar_batches(persistence, pending, len(symbols), len(symbols))
            return _daily_bar_bulk_summary(len(symbols), failures, unavailable)
        return _execute(args, pipeline)


def _run_board_index_command(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> IngestionRun:
    provider_code = (
        DEFAULT_BOARD_INDEX_PROVIDER_CODE if args.provider == AUTO_PROVIDER_CODE else args.provider
    )
    if provider_code not in available_board_index_provider_codes():
        raise SystemExit(
            f"{provider_code} does not provide the BoardIndex capability; "
            f"use {DEFAULT_BOARD_INDEX_PROVIDER_CODE}"
        )
    with create_board_index_provider(provider_code) as provider:
        pipeline = BoardIndexIngestionPipeline(
            provider=provider,
            raw_store=raw_store,
            persistence=persistence,
        )
        if args.dataset == "board-index":
            return pipeline.ingest_board_indexes()
        if args.dataset == "board-index-daily-bar":
            return pipeline.ingest_board_index_daily_bars(
                args.board_id,
                date.fromisoformat(args.start_date),
                date.fromisoformat(args.end_date),
            )
        snapshot_date = (
            date.fromisoformat(args.snapshot_date)
            if args.snapshot_date
            else datetime.now(SHANGHAI_TIME_ZONE).date()
        )
        return pipeline.ingest_board_index_constituents(args.board_id, snapshot_date)


def _run_automatic(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> IngestionRun | DailyBarBulkSummary | None:
    with ProviderRouter() as router:
        if args.dataset == "daily-bars-bulk":
            return _run_automatic_bulk(args, router, persistence, raw_store)
        routed = router.route(
            _dataset_code(args.dataset),
            lambda provider: _execute_automatic_provider(args, provider, persistence, raw_store),
        )
        _report_route("request", routed)
        return routed.value


def _execute_automatic_provider(
    args: Namespace,
    provider: ManagedMarketDataProvider,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> IngestionRun:
    pipeline = IngestionPipeline(
        provider=provider,
        raw_store=raw_store,
        persistence=persistence,
    )
    if args.dataset == "security":
        return pipeline.ingest_securities()
    if args.dataset == "capital":
        return pipeline.ingest_capital(provider.source_symbol(args.source_symbol), mode=args.mode)
    if args.dataset == "classification-catalog":
        return pipeline.ingest_classification_catalog(
            args.classification_type,
            snapshot_date=datetime.now(SHANGHAI_TIME_ZONE).date(),
        )
    if args.dataset == "classification-members":
        return pipeline.ingest_classification_members(
            args.classification_type,
            args.classification_code,
            snapshot_date=datetime.now(SHANGHAI_TIME_ZONE).date(),
        )
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if args.dataset == "trading-calendar":
        return pipeline.ingest_trading_calendar(start_date, end_date)
    source_symbol = provider.source_symbol(args.source_symbol)
    return pipeline.ingest_daily_bars(source_symbol, start_date, end_date)


def _run_automatic_bulk(
    args: Namespace,
    router: ProviderRouter,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> DailyBarBulkSummary:
    if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        raise SystemExit("shard-index must be in [0, shard-count)")
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    symbols = _bulk_symbols(args, persistence, start_date, end_date)
    failures = 0
    unavailable = 0
    pending: list[PreparedDailyBarBatch] = []
    batch_size = _daily_bar_write_batch_size(args)
    known_symbols = set(symbols)
    known_dates = persistence.known_trading_dates(
        {start_date + timedelta(days=offset) for offset in range((end_date - start_date).days + 1)}
    )
    for position, symbol in enumerate(symbols, start=1):
        try:
            routed = router.route(
                DatasetCode.DAILY_BAR,
                partial(
                    _prepare_automatic_daily_bar,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    persistence=persistence,
                    raw_store=raw_store,
                    known_symbols=known_symbols,
                    known_trading_dates=known_dates,
                ),
            )
            _report_route(symbol, routed)
            pending.append(routed.value)
        except ProviderRoutingError as error:
            request_unavailable = error.attempts and all(
                attempt.error_type == "ProviderRequestUnavailable" for attempt in error.attempts
            )
            if getattr(args, "allow_unavailable", False) and request_unavailable:
                unavailable += 1
            else:
                failures += 1
                print(f"failed {symbol}: {type(error).__name__}", file=stderr)
        except Exception as error:
            failures += 1
            print(f"failed {symbol}: {type(error).__name__}", file=stderr)
        if len(pending) >= batch_size:
            _commit_daily_bar_batches(persistence, pending, position, len(symbols))
            pending.clear()
        if position % 100 == 0 or position == len(symbols):
            print(
                f"progress={position}/{len(symbols)} failures={failures} unavailable={unavailable}"
            )
    if pending:
        _commit_daily_bar_batches(persistence, pending, len(symbols), len(symbols))
    return _daily_bar_bulk_summary(len(symbols), failures, unavailable)


def _daily_bar_bulk_summary(
    expected_symbols: int, failures: int, unavailable: int
) -> DailyBarBulkSummary:
    summary = DailyBarBulkSummary(
        expected_symbols=expected_symbols,
        accepted_symbols=expected_symbols - failures - unavailable,
        failed_symbols=failures,
        unavailable_symbols=unavailable,
    )
    print(
        f"daily-bars-bulk status={summary.status} expected={summary.expected_symbols} "
        f"accepted={summary.accepted_symbols} failed={summary.failed_symbols} "
        f"unavailable={summary.unavailable_symbols}"
    )
    return summary


def _bulk_symbols(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    start_date: date,
    end_date: date,
) -> list[str]:
    if end_date < start_date:
        raise SystemExit("end-date must not precede start-date")
    if not persistence.has_complete_calendar_range(start_date, end_date):
        raise SystemExit(
            "trading calendar is incomplete for the requested daily-bar range; "
            "synchronize trading-calendar first"
        )
    missing = persistence.stock_symbols_missing_daily_bars(start_date, end_date)
    symbols = [
        symbol
        for index, symbol in enumerate(missing)
        if index % args.shard_count == args.shard_index
    ]
    if not symbols:
        print(
            f"daily-bars-bulk no incomplete symbols for "
            f"{start_date.isoformat()}..{end_date.isoformat()}"
        )
    return symbols


def run_daily_workflow(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
    *,
    execution: WorkflowExecution | None = None,
) -> date | None:
    as_of_date = (
        date.fromisoformat(args.as_of_date)
        if args.as_of_date
        else datetime.now(SHANGHAI_TIME_ZONE).date()
    )
    if args.bar_lookback_days < 1:
        raise SystemExit("bar-lookback-days must be positive")
    if args.calendar_lookback_days < 1:
        raise SystemExit("calendar-lookback-days must be positive")

    security_args = _derived_args(args, dataset="security")
    calendar_args = _derived_args(
        args,
        dataset="trading-calendar",
        start_date=(as_of_date - timedelta(days=args.calendar_lookback_days - 1)).isoformat(),
        end_date=as_of_date.isoformat(),
    )
    _execute_workflow_step(
        execution, "security", 1, lambda: _execute_operation(security_args, persistence, raw_store)
    )
    _execute_workflow_step(
        execution,
        "trading_calendar",
        2,
        lambda: _execute_operation(calendar_args, persistence, raw_store),
    )

    calendar_start_date = as_of_date - timedelta(days=args.calendar_lookback_days - 1)
    bar_end_date = persistence.latest_trading_date(calendar_start_date, as_of_date)
    if bar_end_date is None:
        print(
            f"daily-run no trading day for {calendar_start_date.isoformat()}.."
            f"{as_of_date.isoformat()}"
        )
        return None
    daily_bar_args = _derived_args(
        args,
        dataset="daily-bars-bulk",
        start_date=bar_end_date.isoformat(),
        end_date=bar_end_date.isoformat(),
        allow_unavailable=True,
    )
    _execute_workflow_step(
        execution,
        "daily_bar",
        3,
        lambda: _execute_operation(daily_bar_args, persistence, raw_store),
    )
    return bar_end_date


def run_stock_daily_indicator_workflow(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
    *,
    execution: WorkflowExecution | None = None,
) -> StockDailyIndicatorWorkflowResult | None:
    if args.provider != "tushare":
        raise SystemExit("stock-daily-indicators-daily requires --provider tushare")
    as_of_date = (
        date.fromisoformat(args.as_of_date)
        if args.as_of_date
        else datetime.now(SHANGHAI_TIME_ZONE).date()
    )
    calendar_args = _derived_args(
        args,
        dataset="trading-calendar",
        start_date=as_of_date.isoformat(),
        end_date=as_of_date.isoformat(),
    )
    calendar_run = _execute_workflow_step(
        execution,
        "trading_calendar",
        1,
        lambda: _execute_operation(calendar_args, persistence, raw_store),
    )
    if calendar_run is None:
        raise RuntimeError("trading calendar synchronization did not succeed")
    calendar_ingestion = cast(IngestionRun, calendar_run)
    if calendar_ingestion.status is not IngestionStatus.SUCCEEDED:
        raise RuntimeError("trading calendar synchronization did not succeed")
    if persistence.latest_trading_date(as_of_date, as_of_date) != as_of_date:
        print(f"stock-daily-indicators-daily market closed on {as_of_date.isoformat()}")
        return None

    snapshot_args = _derived_args(
        args,
        dataset="stock-daily-indicators-bulk",
        trade_date=as_of_date.isoformat(),
    )
    snapshot_run = _execute_workflow_step(
        execution,
        "stock_daily_indicator",
        2,
        lambda: _execute_operation(snapshot_args, persistence, raw_store),
    )
    allowed_statuses = {IngestionStatus.SUCCEEDED, IngestionStatus.PARTIAL}
    if snapshot_run is None:
        raise RuntimeError("stock daily indicator snapshot is not safe for retention")
    snapshot_ingestion = cast(IngestionRun, snapshot_run)
    if snapshot_ingestion.status not in allowed_statuses or snapshot_ingestion.accepted_rows <= 0:
        raise RuntimeError("stock daily indicator snapshot is not safe for retention")
    cutoff_date = _one_month_before(as_of_date)
    deleted = _execute_workflow_step(
        execution,
        "retention",
        3,
        lambda: persistence.delete_stock_daily_indicators_before(cutoff_date),
    )
    print(f"stock-daily-indicator-retention cutoff={cutoff_date.isoformat()} deleted={deleted}")
    return StockDailyIndicatorWorkflowResult(
        as_of_date=as_of_date,
        calendar_run=calendar_ingestion,
        snapshot_run=snapshot_ingestion,
        cutoff_date=cutoff_date,
        deleted_rows=deleted,
    )


def _one_month_before(value: date) -> date:
    year = value.year if value.month > 1 else value.year - 1
    month = value.month - 1 if value.month > 1 else 12
    return value.replace(year=year, month=month, day=min(value.day, monthrange(year, month)[1]))


def _execute_workflow_step[T](
    execution: WorkflowExecution | None,
    job_code: str,
    sequence_no: int,
    operation: Callable[[], T],
) -> T:
    return operation() if execution is None else execution.step(job_code, sequence_no, operation)


def _execute_operation(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> IngestionRun | DailyBarBulkSummary | None:
    if args.provider == AUTO_PROVIDER_CODE:
        run = _run_automatic(args, persistence, raw_store)
    else:
        run = _run_explicit(args, persistence, raw_store)
    if isinstance(run, IngestionRun):
        print(
            f"{run.dataset_code.value} {run.status.value} "
            f"provider={run.provider_code.value} ingestion_id={run.ingestion_id}"
        )
    return run


def _derived_args(args: Namespace, **overrides: object) -> Namespace:
    values = vars(args).copy()
    values.update(overrides)
    return Namespace(**values)


def _prepare_automatic_daily_bar(
    provider: ManagedMarketDataProvider,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
    known_symbols: set[str],
    known_trading_dates: set[date],
) -> PreparedDailyBarBatch:
    return IngestionPipeline(
        provider=provider,
        raw_store=raw_store,
        persistence=persistence,
    ).prepare_daily_bars(
        provider.source_symbol(symbol),
        start_date,
        end_date,
        known_symbols=known_symbols,
        known_trading_dates=known_trading_dates,
    )


def _report_route[T](label: str, routed: RoutedResult[T]) -> None:
    if not routed.failed_attempts:
        return
    failed = ",".join(
        f"{attempt.provider_code}:{attempt.error_type}" for attempt in routed.failed_attempts
    )
    print(
        f"route {label}: selected={routed.provider_code} failed_attempts={failed}",
        file=stderr,
    )


def _daily_bar_write_batch_size(args: Namespace) -> int:
    configured = getattr(args, "write_batch_size", None)
    value = configured or WorkerSettings().daily_bar_write_batch_size  # type: ignore[call-arg]
    if not 1 <= value <= 500:
        raise SystemExit("write-batch-size must be in [1, 500]")
    return value


def _commit_daily_bar_batches(
    persistence: PostgreSQLPersistence,
    pending: list[PreparedDailyBarBatch],
    position: int,
    total: int,
) -> None:
    started = monotonic()
    persistence.commit_daily_bar_batches(pending)
    elapsed = monotonic() - started
    rows = sum(batch.run.accepted_rows for batch in pending)
    print(
        f"daily_bar_commit position={position}/{total} runs={len(pending)} "
        f"rows={rows} seconds={elapsed:.3f}"
    )


def _dataset_code(dataset: str) -> DatasetCode:
    return {
        "security": DatasetCode.SECURITY,
        "trading-calendar": DatasetCode.TRADING_CALENDAR,
        "daily-bar": DatasetCode.DAILY_BAR,
        "capital": DatasetCode.CAPITAL,
        "classification-catalog": DatasetCode.CLASSIFICATION_CATALOG,
        "classification-members": DatasetCode.CLASSIFICATION_MEMBERS,
    }[dataset]


def _execute(args: Namespace, pipeline: IngestionPipeline) -> IngestionRun:
    if args.dataset == "security":
        return pipeline.ingest_securities()
    if args.dataset == "capital":
        return pipeline.ingest_capital(args.source_symbol, mode=args.mode)
    if args.dataset == "stock-daily-indicator":
        return pipeline.ingest_stock_daily_indicators(
            args.source_symbol,
            date.fromisoformat(args.start_date),
            date.fromisoformat(args.end_date),
        )
    if args.dataset == "stock-daily-indicators-bulk":
        return pipeline.ingest_stock_daily_indicator_snapshot(date.fromisoformat(args.trade_date))
    if args.dataset == "deducted-profit-daily":
        as_of_date = (
            date.fromisoformat(args.as_of_date)
            if args.as_of_date
            else datetime.now(SHANGHAI_TIME_ZONE).date()
        )
        return pipeline.ingest_deducted_profit_updates(as_of_date)
    if args.dataset == "convertible-bond":
        return pipeline.ingest_convertible_bonds()
    if args.dataset == "convertible-bond-daily-bar":
        return pipeline.ingest_convertible_bond_daily_bars(
            args.source_symbol,
            date.fromisoformat(args.start_date),
            date.fromisoformat(args.end_date),
        )
    if args.dataset == "classification-catalog":
        return pipeline.ingest_classification_catalog(
            args.classification_type,
            snapshot_date=datetime.now(SHANGHAI_TIME_ZONE).date(),
        )
    if args.dataset == "classification-members":
        return pipeline.ingest_classification_members(
            args.classification_type,
            args.classification_code,
            snapshot_date=datetime.now(SHANGHAI_TIME_ZONE).date(),
        )
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if args.dataset == "trading-calendar":
        return pipeline.ingest_trading_calendar(start_date, end_date)
    return pipeline.ingest_daily_bars(args.source_symbol, start_date, end_date)


def _validate_trading_billboard_args(
    args: Namespace,
) -> tuple[date | None, date | None, date | None]:
    if not args.confirm_eastmoney_source_terms_reviewed:
        raise ValueError("Eastmoney source terms review confirmation is required")
    exact = date.fromisoformat(args.trade_date) if args.trade_date else None
    start = date.fromisoformat(args.start_date) if args.start_date else None
    end = date.fromisoformat(args.end_date) if args.end_date else None
    if exact is not None:
        if start is not None or end is not None:
            raise ValueError("--trade-date cannot combine with a date range")
        return exact, None, None
    if start is None or end is None:
        raise ValueError("--start-date and --end-date must both be provided")
    if start > end:
        raise ValueError("start_date must not follow end_date")
    if (end - start).days > 365:
        raise ValueError("trading billboard range is bounded to 366 calendar days")
    return None, start, end


def _run_trading_billboard_command(args: Namespace) -> None:
    exact, start, end = _validate_trading_billboard_args(args)
    settings = WorkerSettings()  # type: ignore[call-arg]
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    try:
        execution = WorkflowExecutionService(PostgreSQLOperationsPersistence(engine)).start(
            WorkflowCode.TRADING_BILLBOARD_DAILY,
            datetime.now(UTC).replace(second=0, microsecond=0),
            TriggerSource.MANUAL,
        )
        service = TradingBillboardService(
            persistence=PostgreSQLTradingBillboardPersistence(engine),
            raw_store=LocalRawStore(settings.raw_data_root),
            provider=_eastmoney_trading_billboard_provider(),
        )
        try:
            result: TradingBillboardCollectionSummary | TradingBillboardBackfillSummary
            if exact is not None:
                result = execution.step(
                    "collect_trading_billboard", 1, lambda: service.collect(exact)
                )
            else:
                if start is None or end is None:  # pragma: no cover - validator invariant
                    raise AssertionError("validated trading billboard range is missing")

                def collect_range() -> TradingBillboardBackfillSummary:
                    summary = service.backfill(start, end)
                    if summary.failed_date is not None:
                        raise _TradingBillboardBackfillStopped(summary)
                    return summary

                result = execution.step("collect_trading_billboard", 1, collect_range)
        except BaseException as error:
            execution.fail(error)
            payload: object = (
                asdict(error.summary)
                if isinstance(error, _TradingBillboardBackfillStopped)
                else {
                    "status": "failed",
                    "operation": "trading-billboard-collect",
                    "error_type": type(error).__name__,
                }
            )
            print(
                dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
                file=stderr,
            )
            raise SystemExit(1) from None
        execution.succeed()
        print(dumps(asdict(result), ensure_ascii=False, sort_keys=True, default=str))
    finally:
        engine.dispose()


def _eastmoney_trading_billboard_provider() -> TradingBillboardProvider:
    from market_data_center.providers.eastmoney_trading_billboard import (
        EastmoneyTradingBillboardProvider,
    )

    return EastmoneyTradingBillboardProvider()


def _parser() -> ArgumentParser:
    parser = ArgumentParser(prog="market-data-center")
    parser.add_argument(
        "--provider",
        choices=(
            AUTO_PROVIDER_CODE,
            *available_provider_codes(),
            *available_board_index_provider_codes(),
        ),
        default=AUTO_PROVIDER_CODE,
        help="automatic routing or an explicit data provider (default: auto)",
    )
    subparsers = parser.add_subparsers(dest="dataset", required=True)
    billboard = subparsers.add_parser(
        "trading-billboard-collect",
        help="collect exact-day Eastmoney A-share trading billboard facts",
    )
    billboard_mode = billboard.add_mutually_exclusive_group(required=True)
    billboard_mode.add_argument("--trade-date", help="one exact date YYYY-MM-DD")
    billboard_mode.add_argument("--start-date", help="bounded range start YYYY-MM-DD")
    billboard.add_argument("--end-date", help="bounded range end YYYY-MM-DD")
    billboard.add_argument(
        "--confirm-eastmoney-source-terms-reviewed",
        action="store_true",
        required=True,
        help="confirm source-rights review before this explicit collection command",
    )
    subparsers.add_parser("security", help="synchronize the security master")

    calendar = subparsers.add_parser("trading-calendar", help="synchronize natural-day calendar")
    _add_date_range(calendar)

    daily_bar = subparsers.add_parser("daily-bar", help="synchronize unadjusted daily bars")
    daily_bar.add_argument(
        "--source-symbol",
        required=True,
        help=(
            "standard symbol such as SSE:600000 in auto mode; provider-specific symbol "
            "in explicit mode"
        ),
    )
    _add_date_range(daily_bar)

    daily_indicator = subparsers.add_parser(
        "stock-daily-indicator",
        help="synchronize Tushare daily valuation, share, and market-value snapshots",
    )
    daily_indicator.add_argument(
        "--source-symbol",
        required=True,
        help="Tushare symbol such as 600000.SH; requires --provider tushare",
    )
    _add_date_range(daily_indicator)

    daily_indicator_bulk = subparsers.add_parser(
        "stock-daily-indicators-bulk",
        help="synchronize one complete Tushare daily indicator market snapshot",
    )
    daily_indicator_bulk.add_argument(
        "--trade-date",
        required=True,
        help="trading date YYYY-MM-DD",
    )

    capital = subparsers.add_parser(
        "capital", help="reconcile share-capital, distribution, and rights-issue history"
    )
    capital.add_argument(
        "--source-symbol",
        required=True,
        help=(
            "standard symbol such as SSE:600000 in auto mode; provider-specific symbol "
            "in explicit mode"
        ),
    )
    capital.add_argument(
        "--mode",
        choices=("backfill", "incremental"),
        default="incremental",
        help="record operational intent; both modes reconcile the provider's complete history",
    )

    subparsers.add_parser(
        "convertible-bond", help="synchronize convertible bond basic terms (full market)"
    )
    cb_daily_bar = subparsers.add_parser(
        "convertible-bond-daily-bar", help="synchronize convertible bond daily bars for one symbol"
    )
    cb_daily_bar.add_argument(
        "--source-symbol", required=True, help="standard symbol such as SSE:113527"
    )
    _add_date_range(cb_daily_bar)

    eod_parser = subparsers.add_parser(
        "eod-quote-snapshot", help="collect end-of-day 5-level quotes for limit-up pool"
    )
    eod_parser.add_argument("--trade-date", help="YYYY-MM-DD; defaults to today")

    auction_parser = subparsers.add_parser(
        "call-auction-snapshot",
        help="finalize call-auction facts from persisted morning data",
    )
    auction_parser.add_argument("--trade-date", help="YYYY-MM-DD; defaults to today")

    indicative = subparsers.add_parser(
        "call-auction-indicative-detail",
        help="operator-controlled current-day single-stock virtual indicative detail collection",
    )
    indicative.add_argument("--symbol", required=True, help="SSE:600000 or SZSE:000001")
    indicative.add_argument("--trade-date", required=True, help="current Asia/Shanghai date")
    indicative.add_argument(
        "--confirm-current-day-single-symbol",
        action="store_true",
        help="explicitly allow one bounded provider request; no schedule is registered",
    )

    realtime_quotes = subparsers.add_parser(
        "realtime-quotes",
        help="explicitly collect a bounded Tencent batch of persisted five-level quotes",
    )
    realtime_quotes.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="one to 500 unique standard symbols such as SSE:600000 SZSE:000001",
    )
    realtime_quotes.add_argument(
        "--confirm-bounded-tencent-request",
        action="store_true",
        help="explicitly allow bounded Tencent requests; no schedule is registered",
    )

    catalog = subparsers.add_parser(
        "classification-catalog", help="capture a complete industry or concept catalog snapshot"
    )
    catalog.add_argument("--classification-type", choices=("industry", "concept"), required=True)

    members = subparsers.add_parser(
        "classification-members", help="capture one complete classification member snapshot"
    )
    members.add_argument("--classification-type", choices=("industry", "concept"), required=True)
    members.add_argument("--classification-code", required=True, help="board code such as BK0475")

    subparsers.add_parser(
        "board-index",
        help="synchronize the explicit third-party BoardIndex directory",
    )

    board_daily_bar = subparsers.add_parser(
        "board-index-daily-bar",
        help="synchronize unadjusted THS board-index daily bars",
    )
    board_daily_bar.add_argument(
        "--board-id",
        default="THS:883423",
        help="explicit board identity (default: THS:883423)",
    )
    _add_date_range(board_daily_bar)

    board_constituents = subparsers.add_parser(
        "board-index-constituents",
        help="capture today's complete THS board-index constituent snapshot",
    )
    board_constituents.add_argument(
        "--board-id",
        default="THS:883423",
        help="explicit board identity (default: THS:883423)",
    )
    board_constituents.add_argument(
        "--snapshot-date",
        help="must be today's Asia/Shanghai date; defaults to today",
    )

    derived = subparsers.add_parser(
        "derived-recompute",
        help="calculate versioned adjusted bars and objective daily metrics",
    )
    _add_date_range(derived)
    derived.add_argument(
        "--mode",
        choices=tuple(mode.value for mode in CalculationMode),
        default=CalculationMode.INCREMENTAL.value,
    )
    derived.add_argument(
        "--algorithm-version",
        default=DEFAULT_ALGORITHM_VERSION,
        help="immutable calculator contract version",
    )

    bulk = subparsers.add_parser(
        "daily-bars-bulk", help="synchronize daily bars for every currently listed stock"
    )
    _add_date_range(bulk)
    bulk.add_argument("--shard-count", type=int, default=1)
    bulk.add_argument("--shard-index", type=int, default=0)
    bulk.add_argument("--write-batch-size", type=int)

    daily_run = subparsers.add_parser(
        "daily-run",
        help="synchronize security, calendar, and an incremental daily-bar window",
    )
    daily_run.add_argument(
        "--as-of-date",
        help="inclusive YYYY-MM-DD; defaults to the current Asia/Shanghai date",
    )
    daily_run.add_argument(
        "--bar-lookback-days",
        type=int,
        default=1,
        help="deprecated compatibility option; daily-run never repairs historical Daily Bars",
    )
    daily_run.add_argument(
        "--calendar-lookback-days",
        type=int,
        default=14,
        help="calendar synchronization window ending at as-of-date (default: 14)",
    )
    daily_run.add_argument("--shard-count", type=int, default=1)
    daily_run.add_argument("--shard-index", type=int, default=0)
    daily_run.add_argument("--write-batch-size", type=int)

    worker = subparsers.add_parser(
        "worker",
        help="run the long-lived collection worker with embedded scheduling",
    )
    worker.add_argument(
        "--check",
        action="store_true",
        help="run a read-only worker health check and exit",
    )

    daily_indicator_daily = subparsers.add_parser(
        "stock-daily-indicators-daily",
        help="synchronize today's Tushare market snapshot and enforce one-month Core retention",
    )
    daily_indicator_daily.add_argument(
        "--as-of-date",
        help="YYYY-MM-DD; defaults to the current Asia/Shanghai date",
    )

    daily_indicator_retention = subparsers.add_parser(
        "stock-daily-indicator-retention",
        help="delete Core stock daily indicators before an explicit cutoff date",
    )
    daily_indicator_retention.add_argument(
        "--cutoff-date",
        required=True,
        help="delete rows with trade_date before this YYYY-MM-DD date",
    )

    deducted_profit = subparsers.add_parser(
        "deducted-profit-daily",
        help="discover and synchronize newly disclosed or revised deducted-profit facts",
    )
    deducted_profit.add_argument(
        "--as-of-date",
        help="YYYY-MM-DD disclosure discovery date; defaults to Asia/Shanghai today",
    )

    shareholder_daily = subparsers.add_parser(
        "shareholder-count-daily",
        help="synchronize the rolling 30-day Tushare shareholder-count window",
    )
    shareholder_daily.add_argument(
        "--as-of-date",
        help="inclusive YYYY-MM-DD; defaults to the current Asia/Shanghai date",
    )
    shareholder_daily.add_argument(
        "--provider",
        choices=(AUTO_PROVIDER_CODE, *available_provider_codes()),
        default=SUPPRESS,
    )

    shareholder_backfill = subparsers.add_parser(
        "shareholder-count-backfill",
        help="run an explicitly confirmed, sequential Tushare full-history backfill",
    )
    shareholder_backfill.add_argument("--cutoff-date", required=True, help="inclusive YYYY-MM-DD")
    shareholder_backfill.add_argument(
        "--symbols",
        nargs="+",
        help="optional standard-symbol subset; defaults to every known A-share stock",
    )
    shareholder_backfill.add_argument(
        "--resume-after-symbol",
        help="resume strictly after this standard symbol in deterministic order",
    )
    shareholder_backfill.add_argument(
        "--yes",
        action="store_true",
        help="confirm non-interactive controlled full-history execution",
    )
    shareholder_backfill.add_argument(
        "--provider",
        choices=(AUTO_PROVIDER_CODE, *available_provider_codes()),
        default=SUPPRESS,
    )

    stock_pools = subparsers.add_parser(
        "stock-pools-build",
        help="build immutable main-board previous-day limit-up and limit-down pools",
    )
    stock_pools.add_argument(
        "--basis-trade-date",
        required=True,
        help="exact price-limit event trading date YYYY-MM-DD",
    )

    closing_highs = subparsers.add_parser(
        "close-price-new-highs-120d-build",
        help="idempotently build one exact-date immutable 120-session closing-high snapshot",
    )
    closing_highs.add_argument("--trade-date", required=True, help="exact YYYY-MM-DD")

    regulation = subparsers.add_parser(
        "regulation-calculate",
        help="calculate exact-date regulation conditions and next-session scenarios",
    )
    regulation.add_argument("--trade-date", required=True, help="exact YYYY-MM-DD")

    today_limit_up = subparsers.add_parser(
        "today-limit-up-snapshot",
        help="idempotently fill one exact-date immutable same-day limit-up snapshot",
    )
    today_limit_up.add_argument("--trade-date", required=True, help="exact YYYY-MM-DD")

    stock_pool_check = subparsers.add_parser(
        "stock-pool-check",
        help="read one exact ready stock-pool snapshot without date fallback",
    )
    stock_pool_check.add_argument(
        "--pool-code",
        choices=(MAINBOARD_LIMIT_UP_POOL, MAINBOARD_LIMIT_DOWN_POOL),
        required=True,
    )

    auction_preflight = subparsers.add_parser(
        "auction-quotes-preflight",
        help="read-only check of calendar, exact frozen pool, and expected collection size",
    )
    auction_preflight.add_argument("--trade-date", required=True, help="exact YYYY-MM-DD")
    auction_preflight.add_argument("--cadence-seconds", type=int, default=30)
    stock_pool_check.add_argument(
        "--effective-trade-date",
        required=True,
        help="exact effective trading date YYYY-MM-DD",
    )
    stock_pool_check.add_argument(
        "--version",
        type=int,
        help="optional exact positive snapshot version; defaults to latest ready version",
    )

    replay = subparsers.add_parser(
        "raw-replay", help="validate and replay an immutable Raw ingestion batch"
    )
    replay.add_argument("--ingestion-id", required=True)
    replay.add_argument("--dry-run", action="store_true")

    stale = subparsers.add_parser(
        "recover-stale-runs", help="fail ingestion runs left running beyond an age limit"
    )
    stale.add_argument("--older-than-minutes", type=int, default=60)
    stale.add_argument("--dry-run", action="store_true")

    comparison = subparsers.add_parser(
        "compare-daily-bars", help="compare provider Raw daily bars without changing Core"
    )
    comparison.add_argument("--symbol", required=True, help="standard symbol such as SSE:600000")
    _add_date_range(comparison)
    return parser


def _validate_regulation_calculation_date(args: Namespace, *, today: date) -> date:
    trade_date = date.fromisoformat(args.trade_date)
    if trade_date < REGULATION_RULES_EFFECTIVE_FROM:
        raise ValueError("regulation calculation cannot precede 2026-07-06")
    if trade_date > today:
        raise ValueError("regulation calculation trade date cannot be in the future")
    return trade_date


def _add_date_range(parser: ArgumentParser) -> None:
    parser.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")


def _stock_pool_summary_json(summary: object) -> dict[str, object]:
    return {
        name: (
            [str(item) for item in value]
            if isinstance(value, tuple)
            else value.isoformat()
            if isinstance(value, (date, datetime))
            else str(value)
            if isinstance(value, UUID)
            else value
        )
        for name in summary.__slots__  # type: ignore[attr-defined]
        for value in (getattr(summary, name),)
    }
