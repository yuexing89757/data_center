"""Manual phase-one ingestion commands."""

from argparse import ArgumentParser, Namespace
from datetime import date
from sys import stderr

from sqlalchemy import create_engine

from market_data_center.domain.ingestion import IngestionRun
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.pipeline import IngestionPipeline
from market_data_center.providers import available_provider_codes, create_provider
from market_data_center.raw_store import LocalRawStore
from market_data_center.settings import WorkerSettings


def main() -> None:
    args = _parser().parse_args()
    settings = WorkerSettings()  # type: ignore[call-arg]
    engine = create_engine(settings.database_url.get_secret_value(), pool_pre_ping=True)
    persistence = PostgreSQLPersistence(engine)
    raw_store = LocalRawStore(settings.raw_data_root)

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
            completed = persistence.symbols_with_daily_bars(start_date, end_date)
            symbols = [
                symbol
                for index, symbol in enumerate(persistence.listed_stock_symbols())
                if index % args.shard_count == args.shard_index and symbol not in completed
            ]
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
            return
        run = _execute(args, pipeline)
    print(f"{run.dataset_code.value} {run.status.value} ingestion_id={run.ingestion_id}")


def _execute(args: Namespace, pipeline: IngestionPipeline) -> IngestionRun:
    if args.dataset == "security":
        return pipeline.ingest_securities()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if args.dataset == "trading-calendar":
        return pipeline.ingest_trading_calendar(start_date, end_date)
    return pipeline.ingest_daily_bars(args.source_symbol, start_date, end_date)


def _parser() -> ArgumentParser:
    parser = ArgumentParser(prog="market-data-center")
    parser.add_argument(
        "--provider",
        choices=available_provider_codes(),
        default="baostock",
        help="explicit data provider (default: baostock)",
    )
    subparsers = parser.add_subparsers(dest="dataset", required=True)
    subparsers.add_parser("security", help="synchronize the security master")

    calendar = subparsers.add_parser("trading-calendar", help="synchronize natural-day calendar")
    _add_date_range(calendar)

    daily_bar = subparsers.add_parser("daily-bar", help="synchronize unadjusted daily bars")
    daily_bar.add_argument(
        "--source-symbol",
        required=True,
        help="provider symbol: sh.600000 for BaoStock, 600000 for AKShare",
    )
    _add_date_range(daily_bar)

    bulk = subparsers.add_parser(
        "daily-bars-bulk", help="synchronize daily bars for every currently listed stock"
    )
    _add_date_range(bulk)
    bulk.add_argument("--shard-count", type=int, default=1)
    bulk.add_argument("--shard-index", type=int, default=0)
    return parser


def _add_date_range(parser: ArgumentParser) -> None:
    parser.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")
