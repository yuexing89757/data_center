"""Manual phase-one ingestion commands."""

from argparse import ArgumentParser, Namespace
from datetime import date, datetime, timedelta
from functools import partial
from json import dumps
from sys import stderr
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine

from market_data_center.database_urls import sqlalchemy_url
from market_data_center.derivation import (
    DEFAULT_ALGORITHM_VERSION,
    DerivationService,
)
from market_data_center.domain import CalculationMode
from market_data_center.domain.ingestion import DatasetCode, IngestionRun
from market_data_center.persistence import PostgreSQLDerivedPersistence, PostgreSQLPersistence
from market_data_center.pipeline import IngestionPipeline
from market_data_center.providers import (
    ManagedMarketDataProvider,
    ProviderRouter,
    RoutedResult,
    available_provider_codes,
    create_provider,
)
from market_data_center.raw_store import LocalRawStore
from market_data_center.reliability import (
    RawReplayService,
    compare_daily_bar_sources,
    recover_stale_runs,
)
from market_data_center.settings import WorkerSettings

AUTO_PROVIDER_CODE = "auto"
SHANGHAI_TIME_ZONE = ZoneInfo("Asia/Shanghai")


def main() -> None:
    args = _parser().parse_args()
    settings = WorkerSettings()  # type: ignore[call-arg]
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), pool_pre_ping=True
    )
    persistence = PostgreSQLPersistence(engine)
    raw_store = LocalRawStore(settings.raw_data_root)

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
        _run_daily_workflow(args, persistence, raw_store)
        return
    if args.provider == AUTO_PROVIDER_CODE:
        run = _run_automatic(args, persistence, raw_store)
    else:
        run = _run_explicit(args, persistence, raw_store)
    if run is not None:
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


def _run_explicit(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> IngestionRun | None:
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
            for position, symbol in enumerate(symbols, start=1):
                try:
                    pipeline.ingest_daily_bars(
                        provider.source_symbol(symbol),
                        start_date,
                        end_date,
                    )
                except Exception as error:
                    failures += 1
                    print(f"failed {symbol}: {type(error).__name__}", file=stderr)
                if position % 100 == 0 or position == len(symbols):
                    print(f"progress={position}/{len(symbols)} failures={failures}")
            if failures:
                raise SystemExit(f"bulk ingestion completed with {failures} failures")
            return None
        return _execute(args, pipeline)


def _run_automatic(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> IngestionRun | None:
    with ProviderRouter() as router:
        if args.dataset == "daily-bars-bulk":
            _run_automatic_bulk(args, router, persistence, raw_store)
            return None
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
) -> None:
    if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        raise SystemExit("shard-index must be in [0, shard-count)")
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    symbols = _bulk_symbols(args, persistence, start_date, end_date)
    failures = 0
    for position, symbol in enumerate(symbols, start=1):
        try:
            routed = router.route(
                DatasetCode.DAILY_BAR,
                partial(
                    _ingest_automatic_daily_bar,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    persistence=persistence,
                    raw_store=raw_store,
                ),
            )
            _report_route(symbol, routed)
        except Exception as error:
            failures += 1
            print(f"failed {symbol}: {type(error).__name__}", file=stderr)
        if position % 100 == 0 or position == len(symbols):
            print(f"progress={position}/{len(symbols)} failures={failures}")
    if failures:
        raise SystemExit(f"bulk ingestion completed with {failures} failures")


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


def _run_daily_workflow(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> None:
    as_of_date = (
        date.fromisoformat(args.as_of_date)
        if args.as_of_date
        else datetime.now(SHANGHAI_TIME_ZONE).date()
    )
    if args.bar_lookback_days < 1:
        raise SystemExit("bar-lookback-days must be positive")
    if args.calendar_lookback_days < args.bar_lookback_days:
        raise SystemExit("calendar-lookback-days must be at least bar-lookback-days")

    security_args = _derived_args(args, dataset="security")
    calendar_args = _derived_args(
        args,
        dataset="trading-calendar",
        start_date=(as_of_date - timedelta(days=args.calendar_lookback_days - 1)).isoformat(),
        end_date=as_of_date.isoformat(),
    )
    _execute_operation(security_args, persistence, raw_store)
    _execute_operation(calendar_args, persistence, raw_store)

    bar_start_date = as_of_date - timedelta(days=args.bar_lookback_days - 1)
    bar_end_date = persistence.latest_trading_date(bar_start_date, as_of_date)
    if bar_end_date is None:
        print(
            f"daily-run no trading day for {bar_start_date.isoformat()}..{as_of_date.isoformat()}"
        )
        return
    daily_bar_args = _derived_args(
        args,
        dataset="daily-bars-bulk",
        start_date=bar_start_date.isoformat(),
        end_date=bar_end_date.isoformat(),
    )
    _execute_operation(daily_bar_args, persistence, raw_store)


def _execute_operation(
    args: Namespace,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> None:
    if args.provider == AUTO_PROVIDER_CODE:
        run = _run_automatic(args, persistence, raw_store)
    else:
        run = _run_explicit(args, persistence, raw_store)
    if run is not None:
        print(
            f"{run.dataset_code.value} {run.status.value} "
            f"provider={run.provider_code.value} ingestion_id={run.ingestion_id}"
        )


def _derived_args(args: Namespace, **overrides: object) -> Namespace:
    values = vars(args).copy()
    values.update(overrides)
    return Namespace(**values)


def _ingest_automatic_daily_bar(
    provider: ManagedMarketDataProvider,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    persistence: PostgreSQLPersistence,
    raw_store: LocalRawStore,
) -> IngestionRun:
    return IngestionPipeline(
        provider=provider,
        raw_store=raw_store,
        persistence=persistence,
    ).ingest_daily_bars(
        provider.source_symbol(symbol),
        start_date,
        end_date,
    )


def _report_route(label: str, routed: RoutedResult[IngestionRun]) -> None:
    if not routed.failed_attempts:
        return
    failed = ",".join(
        f"{attempt.provider_code}:{attempt.error_type}" for attempt in routed.failed_attempts
    )
    print(
        f"route {label}: selected={routed.provider_code} failed_attempts={failed}",
        file=stderr,
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


def _parser() -> ArgumentParser:
    parser = ArgumentParser(prog="market-data-center")
    parser.add_argument(
        "--provider",
        choices=(AUTO_PROVIDER_CODE, *available_provider_codes()),
        default=AUTO_PROVIDER_CODE,
        help="automatic routing or an explicit data provider (default: auto)",
    )
    subparsers = parser.add_subparsers(dest="dataset", required=True)
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

    catalog = subparsers.add_parser(
        "classification-catalog", help="capture a complete industry or concept catalog snapshot"
    )
    catalog.add_argument("--classification-type", choices=("industry", "concept"), required=True)

    members = subparsers.add_parser(
        "classification-members", help="capture one complete classification member snapshot"
    )
    members.add_argument("--classification-type", choices=("industry", "concept"), required=True)
    members.add_argument("--classification-code", required=True, help="board code such as BK0475")

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
        default=7,
        help="calendar-day window used to repair missed daily bars (default: 7)",
    )
    daily_run.add_argument(
        "--calendar-lookback-days",
        type=int,
        default=14,
        help="calendar synchronization window ending at as-of-date (default: 14)",
    )
    daily_run.add_argument("--shard-count", type=int, default=1)
    daily_run.add_argument("--shard-index", type=int, default=0)

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


def _add_date_range(parser: ArgumentParser) -> None:
    parser.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")
