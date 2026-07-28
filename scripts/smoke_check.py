"""Read-only verification for the phase-one database pipeline."""

from os import environ

import psycopg


def main() -> None:
    database_url = environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required")
    with psycopg.connect(
        database_url,
        connect_timeout=10,
        options="-c default_transaction_read_only=on",
    ) as connection:
        metrics: dict[str, int] = {
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
        orphan_facts = connection.execute("""
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
        api_rows = connection.execute("""
            select count(*)
            from api_v1.daily_bars
            where symbol = 'SSE:600000'
              and trade_date between date '2024-01-02' and date '2024-01-10'
        """).fetchone()

    print(f"metrics={metrics}")
    print(f"orphan_facts={orphan_facts[0] if orphan_facts else None}")
    print(f"api_daily_bar_rows={api_rows[0] if api_rows else None}")
    if not metrics.get("security") or not metrics.get("security_name_history"):
        raise SystemExit("security smoke check failed")
    if not metrics.get("trading_calendar") or not metrics.get("daily_bar"):
        raise SystemExit("market smoke check failed")
    if orphan_facts is None or orphan_facts[0] != 0:
        raise SystemExit("traceability smoke check failed")
    if api_rows is None or api_rows[0] == 0:
        raise SystemExit("api_v1 smoke check failed")


if __name__ == "__main__":
    main()
