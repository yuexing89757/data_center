"""Read-only verification for the production database and PostgREST pipeline."""

from argparse import ArgumentParser
from json import loads
from os import environ
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg

from market_data_center.database_urls import psycopg_url

API_VIEWS = (
    "adjusted_daily_bars",
    "board_index_constituents",
    "board_index_daily_bars",
    "board_indexes",
    "calculation_runs",
    "classification_catalog_snapshots",
    "classification_daily_metrics",
    "classification_member_intervals",
    "classification_member_snapshot_status",
    "classification_member_snapshots",
    "convertible_bond_daily_bars",
    "convertible_bonds",
    "daily_bars",
    "daily_metrics",
    "distributions",
    "market_capitalizations",
    "rights_issues",
    "securities",
    "share_capital",
    "stock_daily_indicators",
    "trading_calendar",
)
BASE_REQUIRED_METRICS = {
    "security",
    "security_name_history",
    "trading_calendar",
    "daily_bar",
    "raw_manifest",
    "succeeded_runs",
}
BOARD_REQUIRED_METRICS = {
    "board_index",
    "board_index_daily_bar",
    "board_index_constituent_snapshot",
}


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument(
        "--require-board-index",
        action="store_true",
        help="require THS board catalog, daily bars, and a constituent snapshot",
    )
    args = parser.parse_args()
    database_url = environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required")
    metrics, orphan_facts, api_rows = _database_smoke(database_url)

    print(f"metrics={metrics}")
    print(f"orphan_facts={orphan_facts}")
    print(f"api_rows={api_rows}")
    required_metrics = set(BASE_REQUIRED_METRICS)
    if args.require_board_index:
        required_metrics.update(BOARD_REQUIRED_METRICS)
    empty_required = sorted(metric for metric in required_metrics if not metrics.get(metric))
    if empty_required:
        raise SystemExit(f"required production facts are empty: {', '.join(empty_required)}")
    if orphan_facts != 0:
        raise SystemExit("traceability smoke check failed")
    required_api_views = {"securities", "trading_calendar", "daily_bars"}
    if args.require_board_index:
        required_api_views.update(
            {"board_indexes", "board_index_daily_bars", "board_index_constituents"}
        )
    empty_api_views = sorted(view for view in required_api_views if not api_rows.get(view))
    if empty_api_views:
        raise SystemExit(f"required api_v1 views are empty: {', '.join(empty_api_views)}")

    supabase_url = environ.get("SUPABASE_URL", "").strip()
    publishable_key = environ.get("SUPABASE_PUBLISHABLE_KEY", "").strip()
    if bool(supabase_url) != bool(publishable_key):
        raise SystemExit("SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY must be configured together")
    if supabase_url:
        postgrest_rows = {
            view: _postgrest_sample(supabase_url, publishable_key, view) for view in API_VIEWS
        }
        print(f"postgrest_rows={postgrest_rows}")
        empty_postgrest_views = sorted(
            view for view in required_api_views if not postgrest_rows.get(view)
        )
        if empty_postgrest_views:
            raise SystemExit(
                "required PostgREST views are empty: " + ", ".join(empty_postgrest_views)
            )


def _database_smoke(database_url: str) -> tuple[dict[str, int], int, dict[str, int]]:
    with psycopg.connect(
        psycopg_url(database_url),
        connect_timeout=10,
        options="-c default_transaction_read_only=on",
    ) as connection:
        metrics = {
            str(metric): int(count)
            for metric, count in connection.execute("""
                select 'security' as metric, count(*) from core.security
                union all
                select 'security_name_history', count(*) from core.security_name_history
                union all
                select 'trading_calendar', count(*) from core.trading_calendar
                union all
                select 'daily_bar', count(*) from core.daily_bar
                union all
                select 'board_index', count(*) from core.board_index
                union all
                select 'board_index_daily_bar', count(*) from core.board_index_daily_bar
                union all
                select 'board_index_constituent_snapshot', count(*)
                from core.board_index_constituent_snapshot
                union all
                select 'share_capital', count(*) from capital.share_capital
                union all
                select 'distribution', count(*) from capital.distribution
                union all
                select 'rights_issue', count(*) from capital.rights_issue
                union all
                select 'classification_catalog_snapshot', count(*)
                from classification.catalog_snapshot
                union all
                select 'classification_member_snapshot', count(*)
                from classification.member_snapshot
                union all
                select 'calculation_run', count(*) from derived.calculation_run
                union all
                select 'adjusted_daily_bar', count(*) from derived.adjusted_daily_bar
                union all
                select 'daily_metric', count(*) from derived.daily_metric
                union all
                select 'market_capitalization', count(*)
                from derived.market_capitalization
                union all
                select 'classification_daily_metric', count(*)
                from metrics.classification_daily_metric
                union all
                select 'raw_manifest', count(*) from ingestion.raw_manifest
                union all
                select 'succeeded_runs', count(*)
                from ingestion.ingestion_run where status = 'succeeded'
            """).fetchall()
        }
        orphan_row = connection.execute("""
            select count(*)
            from (
                select ingestion_id from core.security
                union all
                select ingestion_id from core.trading_calendar
                union all
                select ingestion_id from core.daily_bar
                union all
                select ingestion_id from core.board_index
                union all
                select ingestion_id from core.board_index_daily_bar
                union all
                select ingestion_id from core.board_index_constituent_snapshot
                union all
                select ingestion_id from capital.share_capital
                union all
                select ingestion_id from capital.distribution
                union all
                select ingestion_id from capital.rights_issue
                union all
                select ingestion_id from classification.catalog_snapshot
                union all
                select ingestion_id from classification.definition_snapshot
                union all
                select ingestion_id from classification.member_snapshot
                union all
                select ingestion_id from classification.member_snapshot_item
                union all
                select ingestion_id from classification.member_interval
            ) facts
            left join ingestion.ingestion_run run using (ingestion_id)
            where run.ingestion_id is null
        """).fetchone()
        api_rows = {view: _view_count(connection, view) for view in API_VIEWS}
    if orphan_row is None:
        raise RuntimeError("smoke check query returned no row")
    return metrics, int(orphan_row[0]), api_rows


def _view_count(connection: psycopg.Connection[tuple[object, ...]], view: str) -> int:
    if view not in API_VIEWS:
        raise ValueError(f"unsupported api_v1 view: {view}")
    row = connection.execute(f"select count(*) from api_v1.{view}").fetchone()
    if row is None:
        raise RuntimeError(f"api_v1.{view} count returned no row")
    return int(cast(int, row[0]))


def _postgrest_sample(supabase_url: str, publishable_key: str, view: str) -> int:
    query = urlencode({"select": "*", "limit": "1"})
    url = f"{supabase_url.rstrip('/')}/rest/v1/{view}?{query}"
    request = Request(
        url,
        headers={
            "apikey": publishable_key,
            "Authorization": f"Bearer {publishable_key}",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=10) as response:
            payload = loads(response.read().decode("utf-8"))
    except HTTPError as error:
        raise RuntimeError(f"PostgREST {view} returned HTTP {error.code}") from error
    except URLError as error:
        raise RuntimeError(f"PostgREST {view} request failed") from error
    if not isinstance(payload, list):
        raise RuntimeError(f"PostgREST {view} returned an unexpected payload")
    return len(payload)


if __name__ == "__main__":
    main()
