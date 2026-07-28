"""Manual phase-one ingestion commands."""

from argparse import ArgumentParser, Namespace
from datetime import date

from sqlalchemy import create_engine

from market_data_center.domain.ingestion import IngestionRun
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.pipeline import IngestionPipeline
from market_data_center.providers import BaoStockProvider
from market_data_center.raw_store import LocalRawStore
from market_data_center.settings import WorkerSettings


def main() -> None:
    args = _parser().parse_args()
    settings = WorkerSettings()  # type: ignore[call-arg]
    engine = create_engine(settings.database_url.get_secret_value(), pool_pre_ping=True)
    persistence = PostgreSQLPersistence(engine)
    raw_store = LocalRawStore(settings.raw_data_root)

    with BaoStockProvider.default() as provider:
        pipeline = IngestionPipeline(
            provider=provider,
            raw_store=raw_store,
            persistence=persistence,
        )
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
    subparsers = parser.add_subparsers(dest="dataset", required=True)
    subparsers.add_parser("security", help="synchronize the security master")

    calendar = subparsers.add_parser("trading-calendar", help="synchronize natural-day calendar")
    _add_date_range(calendar)

    daily_bar = subparsers.add_parser("daily-bar", help="synchronize unadjusted daily bars")
    daily_bar.add_argument("--source-symbol", required=True, help="BaoStock symbol, e.g. sh.600000")
    _add_date_range(daily_bar)
    return parser


def _add_date_range(parser: ArgumentParser) -> None:
    parser.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")
