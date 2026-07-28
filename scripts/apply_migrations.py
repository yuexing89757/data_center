"""Apply repository SQL migrations to a self-hosted Supabase database."""

from argparse import ArgumentParser
from collections.abc import Sequence
from os import environ
from pathlib import Path

import psycopg

MIGRATION_DIR = Path(__file__).resolve().parents[1] / "supabase" / "migrations"
TARGET_SCHEMAS = ("api_v1", "audit", "core", "ingestion")


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("mode", choices=("check", "apply"))
    args = parser.parse_args()
    database_url = environ.get("MIGRATION_DATABASE_URL")
    if not database_url:
        raise SystemExit("MIGRATION_DATABASE_URL is required")

    options = "-c default_transaction_read_only=on" if args.mode == "check" else ""
    with psycopg.connect(database_url, connect_timeout=10, options=options) as connection:
        if args.mode == "check":
            _check(connection)
        else:
            _apply(connection, sorted(MIGRATION_DIR.glob("*.sql")))


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


def _apply(connection: psycopg.Connection[tuple[object, ...]], migrations: Sequence[Path]) -> None:
    _ensure_history_table(connection)
    applied = {
        row[0]
        for row in connection.execute(
            "select version from supabase_migrations.schema_migrations"
        ).fetchall()
    }
    for migration in migrations:
        version, name = migration.stem.split("_", maxsplit=1)
        if version in applied:
            print(f"skip {migration.name}")
            continue
        sql = migration.read_text(encoding="utf-8")
        with connection.transaction():
            connection.execute(sql)
            connection.execute(
                """
                insert into supabase_migrations.schema_migrations(version, name, statements)
                values (%s, %s, %s)
                """,
                (version, name, [sql]),
            )
        print(f"applied {migration.name}")


def _ensure_history_table(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    with connection.transaction():
        connection.execute("create schema if not exists supabase_migrations")
        connection.execute("""
            create table if not exists supabase_migrations.schema_migrations (
                version text not null primary key
            )
        """)
        connection.execute("""
            alter table supabase_migrations.schema_migrations
            add column if not exists statements text[]
        """)
        connection.execute("""
            alter table supabase_migrations.schema_migrations
            add column if not exists name text
        """)


if __name__ == "__main__":
    main()
