"""Application-scoped PostgreSQL backup, restore, and verification helpers."""

from dataclasses import dataclass
from hashlib import sha256
from os import environ
from pathlib import Path
from subprocess import CalledProcessError, run
from urllib.parse import parse_qs, unquote, urlsplit

import psycopg

from market_data_center.database_urls import psycopg_url

APPLICATION_SCHEMAS = (
    "audit",
    "capital",
    "classification",
    "core",
    "derived",
    "ingestion",
    "metrics",
    "operations",
    "realtime",
    "stock_pool",
)
COUNT_QUERIES = {
    "auction_collection_round": "select count(*) from realtime.auction_collection_round",
    "auction_collection_session": "select count(*) from realtime.auction_collection_session",
    "auction_quote_metric": "select count(*) from derived.auction_quote_metric",
    "adjusted_daily_bar": "select count(*) from derived.adjusted_daily_bar",
    "calculation_run": "select count(*) from derived.calculation_run",
    "classification_catalog_snapshot": ("select count(*) from classification.catalog_snapshot"),
    "classification_daily_metric": ("select count(*) from metrics.classification_daily_metric"),
    "classification_definition_snapshot": (
        "select count(*) from classification.definition_snapshot"
    ),
    "classification_member_interval": ("select count(*) from classification.member_interval"),
    "classification_member_snapshot": ("select count(*) from classification.member_snapshot"),
    "classification_member_snapshot_item": (
        "select count(*) from classification.member_snapshot_item"
    ),
    "board_index": "select count(*) from core.board_index",
    "board_index_constituent_snapshot": (
        "select count(*) from core.board_index_constituent_snapshot"
    ),
    "board_index_daily_bar": "select count(*) from core.board_index_daily_bar",
    "quality_result": "select count(*) from audit.quality_result",
    "daily_bar": "select count(*) from core.daily_bar",
    "daily_price_limit": "select count(*) from derived.daily_price_limit",
    "daily_metric": "select count(*) from derived.daily_metric",
    "deducted_profit": "select count(*) from core.deducted_profit",
    "distribution": "select count(*) from capital.distribution",
    "market_capitalization": "select count(*) from derived.market_capitalization",
    "job_execution": "select count(*) from operations.job_execution",
    "price_limit_event": "select count(*) from derived.price_limit_event",
    "rights_issue": "select count(*) from capital.rights_issue",
    "security": "select count(*) from core.security",
    "security_name_history": "select count(*) from core.security_name_history",
    "share_capital": "select count(*) from capital.share_capital",
    "stock_daily_indicator": "select count(*) from core.stock_daily_indicator",
    "stock_pool_calculation_quality": ("select count(*) from stock_pool.calculation_quality"),
    "stock_pool_member": "select count(*) from stock_pool.member",
    "stock_pool_snapshot": "select count(*) from stock_pool.snapshot",
    "trading_calendar": "select count(*) from core.trading_calendar",
    "workflow_run": "select count(*) from operations.workflow_run",
    "ingestion_run": "select count(*) from ingestion.ingestion_run",
    "raw_manifest": "select count(*) from ingestion.raw_manifest",
    "call_auction_market_snapshot": ("select count(*) from realtime.call_auction_market_snapshot"),
    "five_level_quote_snapshot": "select count(*) from realtime.five_level_quote_snapshot",
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
        psycopg_url(database_url),
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
                union all
                select ingestion_id from core.board_index
                union all
                select ingestion_id from core.board_index_daily_bar
                union all
                select ingestion_id from core.board_index_constituent_snapshot
                union all
                select ingestion_id from core.stock_daily_indicator
                union all
                select ingestion_id from core.deducted_profit
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
                union all
                select ingestion_id from realtime.call_auction_market_snapshot
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
    parsed = urlsplit(psycopg_url(database_url))
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
    database = urlsplit(psycopg_url(database_url)).path.removeprefix("/")
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
