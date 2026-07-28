"""Application-scoped PostgreSQL backup, restore, and verification helpers."""

from dataclasses import dataclass
from hashlib import sha256
from os import environ
from pathlib import Path
from subprocess import CalledProcessError, run
from urllib.parse import parse_qs, unquote, urlsplit

import psycopg

APPLICATION_SCHEMAS = ("audit", "core", "ingestion")
COUNT_QUERIES = {
    "quality_result": "select count(*) from audit.quality_result",
    "daily_bar": "select count(*) from core.daily_bar",
    "security": "select count(*) from core.security",
    "security_name_history": "select count(*) from core.security_name_history",
    "trading_calendar": "select count(*) from core.trading_calendar",
    "ingestion_run": "select count(*) from ingestion.ingestion_run",
    "raw_manifest": "select count(*) from ingestion.raw_manifest",
}


@dataclass(frozen=True, slots=True)
class DatabaseSnapshot:
    row_counts: tuple[tuple[str, int], ...]
    migration_versions: tuple[str, ...]
    api_views: tuple[str, ...]
    orphan_facts: int


def capture_database_snapshot(database_url: str) -> DatabaseSnapshot:
    """Capture stable recovery invariants without returning credentials or row content."""
    with psycopg.connect(
        database_url,
        connect_timeout=10,
        options="-c default_transaction_read_only=on",
    ) as connection:
        row_counts = tuple(
            (name, _count(connection, query)) for name, query in COUNT_QUERIES.items()
        )
        migration_versions = tuple(
            str(version)
            for (version,) in connection.execute(
                "select version from supabase_migrations.schema_migrations order by version"
            ).fetchall()
        )
        api_views = tuple(
            str(view)
            for (view,) in connection.execute("""
                select viewname
                from pg_views
                where schemaname = 'api_v1'
                order by viewname
            """).fetchall()
        )
        orphan_facts = _count(
            connection,
            """
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
            """,
        )
    return DatabaseSnapshot(row_counts, migration_versions, api_views, orphan_facts)


def verify_restored_snapshot(source: DatabaseSnapshot, restored: DatabaseSnapshot) -> None:
    """Reject a restore whose observable application invariants differ."""
    if source != restored:
        raise ValueError(f"restored database snapshot mismatch: {_snapshot_diff(source, restored)}")
    if restored.orphan_facts:
        raise ValueError(f"restored database contains {restored.orphan_facts} orphan fact rows")


def backup_application_data(
    database_url: str,
    output_path: Path,
    *,
    pg_dump_executable: str = "pg_dump",
) -> str:
    """Write an application-data-only custom dump without overwriting an existing file."""
    if output_path.exists():
        raise FileExistsError(f"backup already exists: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    arguments = [
        pg_dump_executable,
        "--format=custom",
        "--data-only",
        "--no-owner",
        "--no-privileges",
        "--file",
        str(output_path),
    ]
    for schema in APPLICATION_SCHEMAS:
        arguments.extend(("--schema", schema))
    _run_postgres_command(arguments, database_url, "application backup")
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise RuntimeError("application backup did not produce a non-empty file")
    return _file_sha256(output_path)


def restore_application_data(
    database_url: str,
    backup_path: Path,
    *,
    pg_restore_executable: str = "pg_restore",
) -> None:
    """Restore application data into an already-migrated empty database."""
    if not backup_path.is_file():
        raise FileNotFoundError(f"backup does not exist: {backup_path}")
    arguments = [
        pg_restore_executable,
        "--data-only",
        "--no-owner",
        "--no-privileges",
        "--single-transaction",
        "--exit-on-error",
        "--disable-triggers",
        "--dbname",
        _database_name(database_url),
        str(backup_path),
    ]
    _run_postgres_command(arguments, database_url, "application restore")


def _count(connection: psycopg.Connection[tuple[object, ...]], statement: str) -> int:
    row = connection.execute(statement).fetchone()
    if row is None:
        raise RuntimeError("database verification query returned no row")
    value = row[0]
    if not isinstance(value, int):
        raise RuntimeError("database verification count is not an integer")
    return value


def _postgres_environment(database_url: str) -> dict[str, str]:
    parsed = urlsplit(database_url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise ValueError("database URL must use the postgres or postgresql scheme")
    if not parsed.username or parsed.password is None:
        raise ValueError("database URL must include a username and password")
    command_environment = dict(environ)
    command_environment.update(
        {
            "PGHOST": parsed.hostname,
            "PGPORT": str(parsed.port or 5432),
            "PGUSER": unquote(parsed.username),
            "PGPASSWORD": unquote(parsed.password),
            "PGDATABASE": _database_name(database_url),
        }
    )
    query = parse_qs(parsed.query)
    sslmode = query.get("sslmode")
    if sslmode:
        command_environment["PGSSLMODE"] = sslmode[-1]
    return command_environment


def _database_name(database_url: str) -> str:
    database = urlsplit(database_url).path.removeprefix("/")
    if not database:
        raise ValueError("database URL must include a database name")
    return unquote(database)


def _run_postgres_command(arguments: list[str], database_url: str, operation: str) -> None:
    try:
        run(
            arguments,
            check=True,
            capture_output=True,
            text=True,
            env=_postgres_environment(database_url),
        )
    except FileNotFoundError as error:
        raise RuntimeError(f"{operation} executable is not installed") from error
    except CalledProcessError as error:
        detail = (error.stderr or "").strip().splitlines()
        summary = detail[-1] if detail else "command failed"
        raise RuntimeError(f"{operation} failed: {summary}") from error


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as backup:
        for chunk in iter(lambda: backup.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_diff(source: DatabaseSnapshot, restored: DatabaseSnapshot) -> dict[str, object]:
    return {
        field: (getattr(source, field), getattr(restored, field))
        for field in ("row_counts", "migration_versions", "api_views", "orphan_facts")
        if getattr(source, field) != getattr(restored, field)
    }
