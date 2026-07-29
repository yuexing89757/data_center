"""Apply repository SQL migrations to a self-hosted Supabase database."""

from argparse import ArgumentParser
from os import environ

import psycopg

from market_data_center.database_urls import psycopg_url
from market_data_center.migrations import MIGRATION_DIR, apply_migrations

TARGET_SCHEMAS = (
    "api_v1",
    "audit",
    "capital",
    "classification",
    "core",
    "derived",
    "ingestion",
    "metrics",
)
EXPECTED_TABLES = {
    ("audit", "quality_result"),
    ("capital", "distribution"),
    ("capital", "rights_issue"),
    ("capital", "share_capital"),
    ("classification", "catalog_snapshot"),
    ("classification", "definition_snapshot"),
    ("classification", "member_interval"),
    ("classification", "member_snapshot"),
    ("classification", "member_snapshot_item"),
    ("core", "board_index"),
    ("core", "board_index_constituent_snapshot"),
    ("core", "board_index_daily_bar"),
    ("core", "daily_bar"),
    ("core", "security"),
    ("core", "security_name_history"),
    ("core", "trading_calendar"),
    ("derived", "adjusted_daily_bar"),
    ("derived", "calculation_run"),
    ("derived", "daily_metric"),
    ("derived", "market_capitalization"),
    ("ingestion", "ingestion_run"),
    ("ingestion", "raw_manifest"),
    ("metrics", "classification_daily_metric"),
}
EXPECTED_VIEWS = {
    ("api_v1", "adjusted_daily_bars"),
    ("api_v1", "board_index_constituents"),
    ("api_v1", "board_index_daily_bars"),
    ("api_v1", "board_indexes"),
    ("api_v1", "calculation_runs"),
    ("api_v1", "classification_catalog_snapshots"),
    ("api_v1", "classification_daily_metrics"),
    ("api_v1", "classification_member_intervals"),
    ("api_v1", "classification_member_snapshot_status"),
    ("api_v1", "classification_member_snapshots"),
    ("api_v1", "daily_bars"),
    ("api_v1", "daily_metrics"),
    ("api_v1", "distributions"),
    ("api_v1", "market_capitalizations"),
    ("api_v1", "rights_issues"),
    ("api_v1", "securities"),
    ("api_v1", "share_capital"),
    ("api_v1", "trading_calendar"),
}


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("mode", choices=("check", "apply"))
    args = parser.parse_args()
    database_url = environ.get("MIGRATION_DATABASE_URL")
    if not database_url:
        raise SystemExit("MIGRATION_DATABASE_URL is required")

    options = "-c default_transaction_read_only=on" if args.mode == "check" else ""
    with psycopg.connect(
        psycopg_url(database_url), connect_timeout=10, options=options
    ) as connection:
        if args.mode == "check":
            _check(connection)
        else:
            apply_migrations(connection, sorted(MIGRATION_DIR.glob("*.sql")))


def _check(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    schemas = connection.execute(
        "select nspname from pg_namespace where nspname = any(%s) order by 1",
        (list(TARGET_SCHEMAS),),
    ).fetchall()
    tables = connection.execute(
        """
        select schemaname, tablename
        from pg_tables
        where schemaname = any(%s)
        order by 1, 2
        """,
        (list(TARGET_SCHEMAS),),
    ).fetchall()
    worker_role = connection.execute(
        "select exists(select 1 from pg_roles where rolname = 'market_data_worker')"
    ).fetchone()
    history_table = connection.execute(
        "select to_regclass('supabase_migrations.schema_migrations')"
    ).fetchone()
    views = connection.execute(
        """
        select schemaname, viewname
        from pg_views
        where schemaname = 'api_v1'
        order by viewname
        """
    ).fetchall()
    rls_tables = connection.execute(
        """
        select schemaname, tablename
        from pg_tables
        where schemaname = any(%s) and rowsecurity
        order by 1, 2
        """,
        (list(TARGET_SCHEMAS),),
    ).fetchall()
    versions = (
        connection.execute(
            "select version from supabase_migrations.schema_migrations order by version"
        ).fetchall()
        if history_table and history_table[0]
        else []
    )
    print(f"target_schemas={schemas}")
    print(f"target_tables={tables}")
    print(f"worker_role_exists={worker_role[0] if worker_role else False}")
    print(f"migration_history={history_table[0] if history_table else None}")
    print(f"api_views={views}")
    print(f"rls_tables={rls_tables}")
    print(f"migration_versions={versions}")

    actual_tables = {(str(schema), str(table)) for schema, table in tables}
    actual_views = {(str(schema), str(view)) for schema, view in views}
    actual_rls_tables = {(str(schema), str(table)) for schema, table in rls_tables}
    actual_versions = {str(version) for (version,) in versions}
    repository_versions = {
        migration.stem.split("_", maxsplit=1)[0] for migration in MIGRATION_DIR.glob("*.sql")
    }
    failures: list[str] = []
    if actual_tables != EXPECTED_TABLES:
        failures.append("application table set differs from the accepted schema")
    if actual_views != EXPECTED_VIEWS:
        failures.append("api_v1 view set differs from the accepted contract")
    if actual_rls_tables != EXPECTED_TABLES:
        failures.append("not every internal application table has RLS enabled")
    if not worker_role or not worker_role[0]:
        failures.append("market_data_worker role is missing")
    if not history_table or not history_table[0]:
        failures.append("migration history table is missing")
    if actual_versions != repository_versions:
        failures.append("database migration versions differ from this repository")
    if failures:
        raise SystemExit("migration check failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()
