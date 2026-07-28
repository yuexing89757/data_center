"""Versioned SQL migration application shared by operations and integration tests."""

from collections.abc import Sequence
from pathlib import Path

import psycopg

MIGRATION_DIR = Path(__file__).resolve().parents[2] / "supabase" / "migrations"


def apply_migrations(
    connection: psycopg.Connection[tuple[object, ...]], migrations: Sequence[Path]
) -> None:
    """Apply each unapplied migration in its own transaction."""
    _ensure_history_table(connection)
    applied = {
        row[0]
        for row in connection.execute(
            "select version from supabase_migrations.schema_migrations"
        ).fetchall()
    }
    connection.commit()
    for migration in migrations:
        version, name = migration.stem.split("_", maxsplit=1)
        if version in applied:
            print(f"skip {migration.name}")
            continue
        migration_sql = migration.read_text(encoding="utf-8")
        with connection.transaction():
            connection.execute(migration_sql)
            connection.execute(
                """
                insert into supabase_migrations.schema_migrations(version, name, statements)
                values (%s, %s, %s)
                """,
                (version, name, [migration_sql]),
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
