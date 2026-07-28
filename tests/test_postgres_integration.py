from collections.abc import Iterator, Mapping
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from os import environ
from typing import cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError

from market_data_center.domain import (
    CalculatedTradingDay,
    DailyBarRecord,
    DatasetCode,
    Exchange,
    IngestionRun,
    IngestionStatus,
    Market,
    ProviderCode,
    RawFileFormat,
    RawManifest,
    SecurityRecord,
    SecurityStatus,
    SecurityType,
    TradeStatus,
)
from market_data_center.migrations import MIGRATION_DIR, apply_migrations
from market_data_center.persistence import PostgreSQLPersistence

pytestmark = pytest.mark.integration

MIGRATIONS = tuple(sorted(MIGRATION_DIR.glob("*.sql")))
NOW = datetime(2026, 7, 28, 8, tzinfo=UTC)
TRADE_DATE = date(2026, 7, 28)
SYMBOL = "SSE:600000"


@pytest.fixture
def empty_database_url() -> Iterator[str]:
    admin_url = environ.get("TEST_DATABASE_URL")
    if not admin_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    database_name = f"market_data_center_test_{uuid4().hex}"
    database_url = _replace_database_name(admin_url, database_name)
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("create database {}").format(sql.Identifier(database_name)))
        try:
            yield database_url
        finally:
            admin.execute(
                sql.SQL("drop database {} with (force)").format(sql.Identifier(database_name))
            )


@pytest.fixture
def migrated_database_url(empty_database_url: str) -> str:
    with psycopg.connect(empty_database_url) as connection:
        apply_migrations(connection, MIGRATIONS)
    return empty_database_url


@pytest.fixture
def database_engine(migrated_database_url: str) -> Iterator[Engine]:
    engine = create_engine(_sqlalchemy_url(migrated_database_url))
    try:
        yield engine
    finally:
        engine.dispose()


def test_migrations_apply_to_empty_database_and_are_idempotent(
    empty_database_url: str, capsys: pytest.CaptureFixture[str]
) -> None:
    expected_versions = [migration.stem.split("_", maxsplit=1)[0] for migration in MIGRATIONS]
    with psycopg.connect(empty_database_url) as connection:
        apply_migrations(connection, MIGRATIONS)
        capsys.readouterr()
        versions = [
            cast(str, row[0])
            for row in connection.execute(
                "select version from supabase_migrations.schema_migrations order by version"
            ).fetchall()
        ]
        first_snapshot = _schema_snapshot(connection)
        connection.commit()

        apply_migrations(connection, MIGRATIONS)
        second_output = capsys.readouterr().out
        second_snapshot = _schema_snapshot(connection)

    assert versions == expected_versions
    assert all(f"skip {migration.name}" in second_output for migration in MIGRATIONS)
    assert first_snapshot == second_snapshot
    assert ("api_v1", "daily_bars") in first_snapshot["views"]
    assert ("api_v1", "securities") in first_snapshot["views"]
    assert ("api_v1", "trading_calendar") in first_snapshot["views"]


def test_security_batch_commit_is_atomic(database_engine: Engine) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    running = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(running)
    completed = _completed_run(running)

    persistence.commit_security_batch(
        completed,
        _manifest(running.ingestion_id, "security"),
        [_security()],
    )

    assert (
        _scalar(
            database_engine,
            "select status from ingestion.ingestion_run where ingestion_id = :ingestion_id",
            {"ingestion_id": running.ingestion_id},
        )
        == "succeeded"
    )
    assert _scalar(database_engine, "select count(*) from ingestion.raw_manifest") == 1
    assert _scalar(database_engine, "select current_name from core.security") == "浦发银行"
    assert _scalar(database_engine, "select count(*) from core.security_name_history") == 1


def test_trading_calendar_batch_commit_is_atomic(database_engine: Engine) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    running = _running_run(DatasetCode.TRADING_CALENDAR)
    persistence.create_ingestion_run(running)
    completed = _completed_run(running)
    trading_day = CalculatedTradingDay(
        market=Market.CN_A_SHARE,
        trade_date=TRADE_DATE,
        is_trading_day=True,
        previous_trading_day=None,
        next_trading_day=None,
        source_code="baostock",
    )

    persistence.commit_trading_calendar_batch(
        completed,
        _manifest(running.ingestion_id, "trading-calendar"),
        [trading_day],
    )

    assert (
        _scalar(
            database_engine,
            "select is_trading_day from core.trading_calendar where trade_date = :trade_date",
            {"trade_date": TRADE_DATE},
        )
        is True
    )
    assert _scalar(database_engine, "select count(*) from ingestion.raw_manifest") == 1


def test_daily_bar_batch_commit_is_atomic(database_engine: Engine) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    _commit_security_prerequisite(persistence)
    _commit_calendar_prerequisite(persistence)
    running = _running_run(DatasetCode.DAILY_BAR)
    persistence.create_ingestion_run(running)
    completed = _completed_run(running)

    persistence.commit_daily_bar_batch(
        completed,
        _manifest(running.ingestion_id, "daily-bar"),
        [_daily_bar()],
        [],
    )

    assert _scalar(
        database_engine,
        "select close from core.daily_bar where symbol = :symbol and trade_date = :trade_date",
        {"symbol": SYMBOL, "trade_date": TRADE_DATE},
    ) == Decimal("10.5000")
    assert (
        _scalar(
            database_engine,
            "select source_code from core.daily_bar where symbol = :symbol",
            {"symbol": SYMBOL},
        )
        == "baostock"
    )


def test_failed_batch_rolls_back_raw_and_core_writes(database_engine: Engine) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    running = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(running)
    invalid_source = replace(_security(), source_code="invalid-provider")

    with pytest.raises(IntegrityError):
        persistence.commit_security_batch(
            _completed_run(running),
            _manifest(running.ingestion_id, "invalid-security"),
            [invalid_source],
        )

    assert _scalar(database_engine, "select count(*) from ingestion.raw_manifest") == 0
    assert _scalar(database_engine, "select count(*) from core.security") == 0
    assert _scalar(database_engine, "select count(*) from core.security_name_history") == 0
    assert (
        _scalar(
            database_engine,
            "select status from ingestion.ingestion_run where ingestion_id = :ingestion_id",
            {"ingestion_id": running.ingestion_id},
        )
        == "running"
    )

    failed = replace(
        running,
        status=IngestionStatus.FAILED,
        finished_at=NOW,
        error_summary="IntegrityError: ingestion failed",
    )
    persistence.fail_ingestion_run(failed)
    assert (
        _scalar(
            database_engine,
            "select status from ingestion.ingestion_run where ingestion_id = :ingestion_id",
            {"ingestion_id": running.ingestion_id},
        )
        == "failed"
    )


def _replace_database_name(database_url: str, database_name: str) -> str:
    parsed = urlsplit(database_url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.netloc:
        raise ValueError("TEST_DATABASE_URL must be a PostgreSQL URL")
    return urlunsplit(
        (parsed.scheme, parsed.netloc, f"/{database_name}", parsed.query, parsed.fragment)
    )


def _sqlalchemy_url(database_url: str) -> str:
    parsed = urlsplit(database_url)
    return urlunsplit(
        ("postgresql+psycopg", parsed.netloc, parsed.path, parsed.query, parsed.fragment)
    )


def _schema_snapshot(
    connection: psycopg.Connection[tuple[object, ...]],
) -> dict[str, list[tuple[object, ...]]]:
    schemas = ("api_v1", "audit", "core", "ingestion")
    return {
        "relations": connection.execute(
            """
            select n.nspname, c.relname, c.relkind
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = any(%s) and c.relkind in ('r', 'v')
            order by 1, 2, 3
            """,
            (list(schemas),),
        ).fetchall(),
        "columns": connection.execute(
            """
            select table_schema, table_name, ordinal_position, column_name,
                   data_type, is_nullable, coalesce(column_default, '')
            from information_schema.columns
            where table_schema = any(%s)
            order by 1, 2, 3
            """,
            (list(schemas),),
        ).fetchall(),
        "constraints": connection.execute(
            """
            select n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid)
            from pg_constraint con
            join pg_class c on c.oid = con.conrelid
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = any(%s)
            order by 1, 2, 3
            """,
            (list(schemas),),
        ).fetchall(),
        "indexes": connection.execute(
            """
            select schemaname, tablename, indexname, indexdef
            from pg_indexes
            where schemaname = any(%s)
            order by 1, 2, 3
            """,
            (list(schemas),),
        ).fetchall(),
        "policies": connection.execute(
            """
            select schemaname, tablename, policyname, cmd, roles, qual, with_check
            from pg_policies
            where schemaname = any(%s)
            order by 1, 2, 3
            """,
            (list(schemas),),
        ).fetchall(),
        "views": connection.execute(
            """
            select schemaname, viewname
            from pg_views
            where schemaname = 'api_v1'
            order by 1, 2
            """
        ).fetchall(),
    }


def _running_run(dataset_code: DatasetCode) -> IngestionRun:
    return IngestionRun(
        ingestion_id=uuid4(),
        provider_code=ProviderCode.BAOSTOCK,
        dataset_code=dataset_code,
        status=IngestionStatus.RUNNING,
        requested_at=NOW,
        started_at=NOW,
    )


def _completed_run(run: IngestionRun) -> IngestionRun:
    return replace(
        run,
        status=IngestionStatus.SUCCEEDED,
        finished_at=NOW,
        fetched_rows=1,
        accepted_rows=1,
    )


def _manifest(ingestion_id: UUID, dataset: str) -> RawManifest:
    return RawManifest(
        raw_id=uuid4(),
        ingestion_id=ingestion_id,
        object_path=f"baostock/{dataset}/2026-07-28/{ingestion_id}.jsonl",
        file_format=RawFileFormat.JSONL,
        content_sha256="0" * 64,
        byte_size=10,
        row_count=1,
        schema_version=f"{dataset}.v1",
    )


def _security() -> SecurityRecord:
    return SecurityRecord(
        symbol=SYMBOL,
        code="600000",
        exchange=Exchange.SSE,
        name="浦发银行",
        security_type=SecurityType.STOCK,
        status=SecurityStatus.LISTED,
        ipo_date=date(1999, 11, 10),
        delisting_date=None,
        source_code="baostock",
    )


def _daily_bar() -> DailyBarRecord:
    return DailyBarRecord(
        symbol=SYMBOL,
        trade_date=TRADE_DATE,
        market=Market.CN_A_SHARE,
        open=Decimal("10.00"),
        high=Decimal("11.00"),
        low=Decimal("9.00"),
        close=Decimal("10.50"),
        previous_close=Decimal("9.90"),
        volume=100,
        amount=Decimal("1050.00"),
        trade_status=TradeStatus.TRADING,
        is_st=False,
        source_code="baostock",
    )


def _commit_security_prerequisite(persistence: PostgreSQLPersistence) -> None:
    running = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(running)
    persistence.commit_security_batch(
        _completed_run(running),
        _manifest(running.ingestion_id, "security-prerequisite"),
        [_security()],
    )


def _commit_calendar_prerequisite(persistence: PostgreSQLPersistence) -> None:
    running = _running_run(DatasetCode.TRADING_CALENDAR)
    persistence.create_ingestion_run(running)
    trading_day = CalculatedTradingDay(
        market=Market.CN_A_SHARE,
        trade_date=TRADE_DATE,
        is_trading_day=True,
        previous_trading_day=None,
        next_trading_day=None,
        source_code="baostock",
    )
    persistence.commit_trading_calendar_batch(
        _completed_run(running),
        _manifest(running.ingestion_id, "calendar-prerequisite"),
        [trading_day],
    )


def _scalar(
    engine: Engine, statement: str, parameters: Mapping[str, object] | None = None
) -> object:
    with engine.connect() as connection:
        return cast(object, connection.execute(text(statement), parameters or {}).scalar_one())
