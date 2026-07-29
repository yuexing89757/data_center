"""Read-only verification for the phase-one database and PostgREST pipeline."""

from json import loads
from os import environ
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg

from market_data_center.database_urls import psycopg_url


def main() -> None:
    database_url = environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required")
    metrics, orphan_facts, api_daily_bar_rows = _database_smoke(database_url)

    print(f"metrics={metrics}")
    print(f"orphan_facts={orphan_facts}")
    print(f"api_daily_bar_rows={api_daily_bar_rows}")
    if not metrics.get("security") or not metrics.get("security_name_history"):
        raise SystemExit("security smoke check failed")
    if not metrics.get("trading_calendar") or not metrics.get("daily_bar"):
        raise SystemExit("market smoke check failed")
    if orphan_facts != 0:
        raise SystemExit("traceability smoke check failed")
    if api_daily_bar_rows == 0:
        raise SystemExit("api_v1 database smoke check failed")

    supabase_url = environ.get("SUPABASE_URL", "").strip()
    publishable_key = environ.get("SUPABASE_PUBLISHABLE_KEY", "").strip()
    if bool(supabase_url) != bool(publishable_key):
        raise SystemExit("SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY must be configured together")
    if supabase_url:
        postgrest_rows = {
            view: _postgrest_sample(supabase_url, publishable_key, view)
            for view in ("securities", "trading_calendar", "daily_bars")
        }
        print(f"postgrest_rows={postgrest_rows}")
        if not all(postgrest_rows.values()):
            raise SystemExit("PostgREST smoke check failed")


def _database_smoke(database_url: str) -> tuple[dict[str, int], int, int]:
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
            ) facts
            left join ingestion.ingestion_run run using (ingestion_id)
            where run.ingestion_id is null
        """).fetchone()
        api_row = connection.execute("select count(*) from api_v1.daily_bars").fetchone()
    if orphan_row is None or api_row is None:
        raise RuntimeError("smoke check query returned no row")
    return metrics, int(orphan_row[0]), int(api_row[0])


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
