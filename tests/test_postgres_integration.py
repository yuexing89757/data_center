from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from os import environ
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.errors import InsufficientPrivilege
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError

from market_data_center.domain import (
    CalculatedTradingDay,
    DailyBarRecord,
    DatasetCode,
    Exchange,
    IngestionEnvelope,
    IngestionRun,
    IngestionStatus,
    Market,
    ProviderCode,
    QualityResult,
    QualitySeverity,
    QualityStatus,
    RawFileFormat,
    RawManifest,
    SecurityRecord,
    SecurityStatus,
    SecurityType,
    TradeStatus,
)
from market_data_center.migrations import MIGRATION_DIR, apply_migrations
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.quality_audit import audit_daily_bars
from market_data_center.recovery import (
    backup_application_data,
    capture_database_snapshot,
    restore_application_data,
    verify_restored_snapshot,
)

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
    with _temporary_database_url(admin_url) as database_url:
        yield database_url


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
        _envelopes(running.ingestion_id, [_security()]),
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
        _envelopes(running.ingestion_id, [trading_day]),
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
        _envelopes(running.ingestion_id, [_daily_bar()]),
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


def test_bulk_resume_requires_complete_eligible_trading_range(database_engine: Engine) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    _prepare_api_data(database_engine)

    security_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(security_run)
    second_security = replace(
        _security(),
        symbol="SZSE:000001",
        code="000001",
        exchange=Exchange.SZSE,
        name="平安银行",
        ipo_date=date(2026, 7, 28),
    )
    persistence.commit_security_batch(
        _completed_run(security_run),
        _manifest(security_run.ingestion_id, "resume-security"),
        _envelopes(security_run.ingestion_id, [second_security]),
    )

    assert persistence.stock_symbols_missing_daily_bars(date(2026, 7, 27), date(2026, 7, 29)) == [
        "SZSE:000001"
    ]

    first_bar_run = _running_run(DatasetCode.DAILY_BAR)
    persistence.create_ingestion_run(first_bar_run)
    persistence.commit_daily_bar_batch(
        _completed_run(first_bar_run),
        _manifest(first_bar_run.ingestion_id, "resume-first-bar"),
        _envelopes(
            first_bar_run.ingestion_id,
            [replace(_daily_bar(date(2026, 7, 28)), symbol="SZSE:000001")],
        ),
        [],
    )
    assert persistence.stock_symbols_missing_daily_bars(date(2026, 7, 27), date(2026, 7, 29)) == [
        "SZSE:000001"
    ]

    second_bar_run = _running_run(DatasetCode.DAILY_BAR)
    persistence.create_ingestion_run(second_bar_run)
    persistence.commit_daily_bar_batch(
        _completed_run(second_bar_run),
        _manifest(second_bar_run.ingestion_id, "resume-second-bar"),
        _envelopes(
            second_bar_run.ingestion_id,
            [replace(_daily_bar(date(2026, 7, 29)), symbol="SZSE:000001")],
        ),
        [],
    )
    assert persistence.stock_symbols_missing_daily_bars(date(2026, 7, 27), date(2026, 7, 29)) == []
    assert persistence.has_complete_calendar_range(date(2026, 7, 27), date(2026, 7, 29))
    assert not persistence.has_complete_calendar_range(date(2026, 7, 26), date(2026, 7, 29))
    assert persistence.latest_trading_date(date(2026, 7, 27), date(2026, 7, 30)) == date(
        2026, 7, 29
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
            _envelopes(running.ingestion_id, [invalid_source]),
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


def test_rejected_normalization_batch_commits_raw_and_quality(database_engine: Engine) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    running = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(running)
    failed = replace(
        running,
        status=IngestionStatus.FAILED,
        finished_at=NOW,
        fetched_rows=1,
        rejected_rows=1,
        error_summary="ProviderError: provider normalization failed",
    )
    quality_result = QualityResult(
        quality_result_id=uuid4(),
        ingestion_id=running.ingestion_id,
        dataset_code=DatasetCode.SECURITY,
        rule_code="security.provider_normalization",
        severity=QualitySeverity.ERROR,
        status=QualityStatus.FAILED,
        message="provider response normalization failed",
    )

    persistence.commit_rejected_batch(
        failed,
        _manifest(running.ingestion_id, "rejected-security"),
        [quality_result],
    )

    assert _scalar(database_engine, "select count(*) from ingestion.raw_manifest") == 1
    assert _scalar(database_engine, "select count(*) from audit.quality_result") == 1
    assert _scalar(database_engine, "select count(*) from core.security") == 0
    assert (
        _scalar(
            database_engine,
            "select status from ingestion.ingestion_run where ingestion_id = :ingestion_id",
            {"ingestion_id": running.ingestion_id},
        )
        == "failed"
    )


def test_api_v1_contract_supports_symbol_and_closed_date_range(
    database_engine: Engine,
) -> None:
    _prepare_api_data(database_engine)

    with database_engine.connect() as connection:
        rows = connection.execute(
            text("""
                select symbol, trade_date
                from api_v1.daily_bars
                where symbol = :symbol
                  and trade_date between :start_date and :end_date
                order by trade_date
            """),
            {
                "symbol": SYMBOL,
                "start_date": date(2026, 7, 27),
                "end_date": date(2026, 7, 28),
            },
        ).all()

    assert [tuple(row) for row in rows] == [
        (SYMBOL, date(2026, 7, 27)),
        (SYMBOL, date(2026, 7, 28)),
    ]
    assert _view_columns(database_engine, "securities") == [
        "symbol",
        "code",
        "exchange",
        "current_name",
        "security_type",
        "status",
        "ipo_date",
        "delisting_date",
    ]
    assert _view_columns(database_engine, "trading_calendar") == [
        "market",
        "trade_date",
        "is_trading_day",
        "previous_trading_day",
        "next_trading_day",
    ]
    assert _view_columns(database_engine, "daily_bars") == [
        "symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "previous_close",
        "volume",
        "amount",
        "trade_status",
        "is_st",
    ]


@pytest.mark.parametrize("client_role", ["anon", "authenticated"])
def test_client_roles_can_only_read_api_v1(
    migrated_database_url: str, database_engine: Engine, client_role: str
) -> None:
    _prepare_api_data(database_engine)

    with psycopg.connect(migrated_database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("set role {}").format(sql.Identifier(client_role)))
        api_count = connection.execute("select count(*) from api_v1.daily_bars").fetchone()
        assert api_count is not None and api_count[0] == 3

        denied_statements = (
            "select count(*) from core.daily_bar",
            "select count(*) from ingestion.ingestion_run",
            "select count(*) from audit.quality_result",
            """
            insert into api_v1.daily_bars(symbol, trade_date)
            values ('SSE:600000', date '2026-07-30')
            """,
        )
        for statement in denied_statements:
            with pytest.raises(InsufficientPrivilege):
                connection.execute(statement)


def test_worker_has_only_ingestion_permissions(
    migrated_database_url: str, database_engine: Engine
) -> None:
    _prepare_api_data(database_engine)
    expected = {
        ("audit", "quality_result", "INSERT"),
        ("audit", "quality_result", "SELECT"),
        ("core", "daily_bar", "INSERT"),
        ("core", "daily_bar", "SELECT"),
        ("core", "daily_bar", "UPDATE"),
        ("core", "security", "INSERT"),
        ("core", "security", "SELECT"),
        ("core", "security", "UPDATE"),
        ("core", "security_name_history", "INSERT"),
        ("core", "security_name_history", "SELECT"),
        ("core", "security_name_history", "UPDATE"),
        ("core", "trading_calendar", "INSERT"),
        ("core", "trading_calendar", "SELECT"),
        ("core", "trading_calendar", "UPDATE"),
        ("ingestion", "ingestion_run", "INSERT"),
        ("ingestion", "ingestion_run", "SELECT"),
        ("ingestion", "ingestion_run", "UPDATE"),
        ("ingestion", "raw_manifest", "INSERT"),
        ("ingestion", "raw_manifest", "SELECT"),
    }
    with psycopg.connect(migrated_database_url, autocommit=True) as connection:
        privileges = {
            (cast(str, schema), cast(str, table), cast(str, privilege))
            for schema, table, privilege in connection.execute("""
                select table_schema, table_name, privilege_type
                from information_schema.role_table_grants
                where grantee = 'market_data_worker'
                  and table_schema in ('audit', 'core', 'ingestion', 'api_v1')
            """).fetchall()
        }
        assert privileges == expected

        connection.execute("set role market_data_worker")
        assert connection.execute("select count(*) from core.daily_bar").fetchone() == (3,)
        with pytest.raises(InsufficientPrivilege):
            connection.execute("delete from core.daily_bar")
        with pytest.raises(InsufficientPrivilege):
            connection.execute("select count(*) from api_v1.daily_bars")


def test_internal_tables_have_rls_with_worker_only_policies(database_engine: Engine) -> None:
    expected_tables = {
        ("audit", "quality_result"),
        ("core", "daily_bar"),
        ("core", "security"),
        ("core", "security_name_history"),
        ("core", "trading_calendar"),
        ("ingestion", "ingestion_run"),
        ("ingestion", "raw_manifest"),
    }
    with database_engine.connect() as connection:
        rls_tables = {
            (cast(str, schema), cast(str, table))
            for schema, table in connection.execute(
                text("""
                select n.nspname, c.relname
                from pg_class c
                join pg_namespace n on n.oid = c.relnamespace
                where n.nspname in ('audit', 'core', 'ingestion')
                  and c.relkind = 'r'
                  and c.relrowsecurity
            """)
            ).all()
        }
        policies = connection.execute(
            text("""
            select schemaname, tablename, roles
            from pg_policies
            where schemaname in ('audit', 'core', 'ingestion')
        """)
        ).all()

    assert rls_tables == expected_tables
    assert {(cast(str, row[0]), cast(str, row[1])) for row in policies} == expected_tables
    assert all(row[2] == ["market_data_worker"] for row in policies)


def test_application_backup_restores_to_independent_database(
    migrated_database_url: str, database_engine: Engine, tmp_path: Path
) -> None:
    _prepare_api_data(database_engine)
    source_snapshot = capture_database_snapshot(migrated_database_url)
    backup_path = tmp_path / "application-data.dump"

    digest = backup_application_data(migrated_database_url, backup_path)

    admin_url = environ["TEST_DATABASE_URL"]
    with _temporary_database_url(admin_url) as target_url:
        with psycopg.connect(target_url) as connection:
            apply_migrations(connection, MIGRATIONS)
        restore_application_data(target_url, backup_path)
        restored_snapshot = capture_database_snapshot(target_url)

    verify_restored_snapshot(source_snapshot, restored_snapshot)
    assert len(digest) == 64
    assert backup_path.stat().st_size > 0


def test_daily_bar_quality_audit_reports_coverage_and_traceability(
    migrated_database_url: str, database_engine: Engine
) -> None:
    _prepare_api_data(database_engine)

    report = audit_daily_bars(
        migrated_database_url,
        date(2026, 7, 27),
        date(2026, 7, 29),
    )

    assert not report.has_errors
    assert not report.has_warnings
    assert report.total_rows == 3
    assert report.coverage.stock_count == 1
    assert report.coverage.covered_stock_count == 1
    assert report.coverage.eligible_symbol_days == 3
    assert report.coverage.observed_eligible_rows == 3
    assert report.coverage.coverage_percent == Decimal("100.00")
    assert report.traceability.ingestion_run_count == 1
    assert report.traceability.raw_manifest_count == 1
    assert report.traceability.manifest_row_count_mismatch_runs == 0
    assert [(item.source_code, item.row_count) for item in report.source_distribution] == [
        ("baostock", 3)
    ]


def test_daily_bar_quality_audit_excludes_pre_ipo_days_and_reports_active_gaps(
    migrated_database_url: str, database_engine: Engine
) -> None:
    _prepare_api_data(database_engine)
    persistence = PostgreSQLPersistence(database_engine)
    running = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(running)
    security = replace(
        _security(),
        symbol="SZSE:000001",
        code="000001",
        exchange=Exchange.SZSE,
        name="平安银行",
        ipo_date=date(2026, 7, 28),
    )
    persistence.commit_security_batch(
        _completed_run(running),
        _manifest(running.ingestion_id, "second-security"),
        _envelopes(running.ingestion_id, [security]),
    )

    report = audit_daily_bars(
        migrated_database_url,
        date(2026, 7, 27),
        date(2026, 7, 29),
    )

    assert not report.has_errors
    assert report.has_warnings
    assert report.coverage.all_stock_calendar_days == 6
    assert report.coverage.pre_ipo_symbol_days_excluded == 1
    assert report.coverage.eligible_symbol_days == 5
    assert report.coverage.missing_eligible_symbol_days == 2
    assert report.gap_candidates[0].symbol == "SZSE:000001"
    assert report.gap_candidates[0].first_missing_date == date(2026, 7, 28)
    assert report.gap_candidates[0].last_missing_date == date(2026, 7, 29)


@contextmanager
def _temporary_database_url(admin_url: str) -> Iterator[str]:
    database_name = f"market_data_center_test_{uuid4().hex}"
    database_url = _replace_database_name(admin_url, database_name)
    with psycopg.connect(admin_url, autocommit=True) as admin:
        _ensure_supabase_client_roles(admin)
        admin.execute(sql.SQL("create database {}").format(sql.Identifier(database_name)))
        try:
            yield database_url
        finally:
            admin.execute(
                sql.SQL("drop database {} with (force)").format(sql.Identifier(database_name))
            )


def _ensure_supabase_client_roles(
    connection: psycopg.Connection[tuple[object, ...]],
) -> None:
    for role in ("anon", "authenticated"):
        existing = connection.execute(
            "select exists(select 1 from pg_roles where rolname = %s)", (role,)
        ).fetchone()
        if existing is None or not existing[0]:
            connection.execute(sql.SQL("create role {} nologin").format(sql.Identifier(role)))


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


def _completed_run(run: IngestionRun, row_count: int = 1) -> IngestionRun:
    return replace(
        run,
        status=IngestionStatus.SUCCEEDED,
        finished_at=NOW,
        fetched_rows=row_count,
        accepted_rows=row_count,
    )


def _manifest(ingestion_id: UUID, dataset: str, row_count: int = 1) -> RawManifest:
    return RawManifest(
        raw_id=uuid4(),
        ingestion_id=ingestion_id,
        object_path=f"baostock/{dataset}/2026-07-28/{ingestion_id}.jsonl",
        file_format=RawFileFormat.JSONL,
        content_sha256="0" * 64,
        byte_size=10,
        row_count=row_count,
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


def _daily_bar(trade_date: date = TRADE_DATE) -> DailyBarRecord:
    return DailyBarRecord(
        symbol=SYMBOL,
        trade_date=trade_date,
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
        _envelopes(running.ingestion_id, [_security()]),
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
        _envelopes(running.ingestion_id, [trading_day]),
    )


def _prepare_api_data(engine: Engine) -> None:
    persistence = PostgreSQLPersistence(engine)
    _commit_security_prerequisite(persistence)

    calendar_run = _running_run(DatasetCode.TRADING_CALENDAR)
    persistence.create_ingestion_run(calendar_run)
    dates = [date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29)]
    trading_days = [
        CalculatedTradingDay(
            market=Market.CN_A_SHARE,
            trade_date=trade_date,
            is_trading_day=True,
            previous_trading_day=dates[index - 1] if index else None,
            next_trading_day=dates[index + 1] if index + 1 < len(dates) else None,
            source_code="baostock",
        )
        for index, trade_date in enumerate(dates)
    ]
    persistence.commit_trading_calendar_batch(
        _completed_run(calendar_run, len(trading_days)),
        _manifest(calendar_run.ingestion_id, "api-calendar"),
        _envelopes(calendar_run.ingestion_id, trading_days),
    )

    daily_run = _running_run(DatasetCode.DAILY_BAR)
    persistence.create_ingestion_run(daily_run)
    daily_bars = [_daily_bar(trade_date) for trade_date in dates]
    persistence.commit_daily_bar_batch(
        _completed_run(daily_run, len(daily_bars)),
        _manifest(daily_run.ingestion_id, "api-daily-bar", len(daily_bars)),
        _envelopes(daily_run.ingestion_id, daily_bars),
        [],
    )


def _envelopes[RecordT: SecurityRecord | CalculatedTradingDay | DailyBarRecord](
    ingestion_id: UUID, records: list[RecordT]
) -> list[IngestionEnvelope[RecordT]]:
    return [IngestionEnvelope(ingestion_id, record) for record in records]


def _view_columns(engine: Engine, view_name: str) -> list[str]:
    with engine.connect() as connection:
        return [
            cast(str, column)
            for column in connection.execute(
                text("""
                    select column_name
                    from information_schema.columns
                    where table_schema = 'api_v1' and table_name = :view_name
                    order by ordinal_position
                """),
                {"view_name": view_name},
            ).scalars()
        ]


def _scalar(
    engine: Engine, statement: str, parameters: Mapping[str, object] | None = None
) -> object:
    with engine.connect() as connection:
        return cast(object, connection.execute(text(statement), parameters or {}).scalar_one())
