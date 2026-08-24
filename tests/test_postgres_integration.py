from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from os import environ
from pathlib import Path
from shutil import which
from typing import cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.errors import CheckViolation, InsufficientPrivilege
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from market_data_center.close_price_new_highs_service import ClosePriceNewHighsService
from market_data_center.daily_bar_batch import PreparedDailyBarBatch
from market_data_center.derivation import DerivationService
from market_data_center.domain import (
    BoardIndexConstituentSnapshotRecord,
    BoardIndexDailyBarRecord,
    BoardIndexRecord,
    BoardIndexStatus,
    BoardIndexType,
    CalculatedTradingDay,
    CallAuctionMarketSnapshotRecord,
    CapitalRecord,
    ClassificationCatalogSnapshotRecord,
    ClassificationDefinition,
    ClassificationMemberSnapshotRecord,
    ClassificationRecord,
    ClassificationType,
    CorporateActionStatus,
    DailyBarRecord,
    DatasetCode,
    DeductedProfitRecord,
    DistributionRecord,
    Exchange,
    IngestionEnvelope,
    IngestionRun,
    IngestionStatus,
    Market,
    OrderBookLevel,
    PriceLimitStatus,
    ProviderCode,
    QualityResult,
    QualitySeverity,
    QualityStatus,
    RawFileFormat,
    RawManifest,
    RightsIssueRecord,
    SecurityRecord,
    SecurityStatus,
    SecurityType,
    ShareCapitalRecord,
    StockDailyIndicatorSnapshotRecord,
    TradeStatus,
    deducted_profit_revision_key,
)
from market_data_center.domain.call_auction_market_series import (
    MarketSeriesRound,
    MarketSeriesSession,
    MarketSeriesSnapshotRecord,
    MarketSeriesStatus,
    MarketSeriesValueSemantics,
    series_batch_code,
    series_slots,
    universe_hash,
)
from market_data_center.domain.operations import ExecutionStatus, TriggerSource, WorkflowCode
from market_data_center.migrations import MIGRATION_DIR, apply_migrations
from market_data_center.persistence import PostgreSQLDerivedPersistence, PostgreSQLPersistence
from market_data_center.persistence.call_auction_market_series_postgres import (
    PostgreSQLCallAuctionMarketSeriesPersistence,
)
from market_data_center.persistence.close_price_new_highs_postgres import (
    PostgreSQLClosePriceNewHighsPersistence,
)
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
from market_data_center.quality_audit import audit_daily_bars
from market_data_center.recovery import (
    backup_application_data,
    capture_database_snapshot,
    restore_application_data,
    verify_restored_snapshot,
)

pytestmark = pytest.mark.integration


def _market_series_levels(first_price: str, second_volume: int) -> tuple[OrderBookLevel, ...]:
    return (
        OrderBookLevel(1, Decimal(first_price), 100),
        OrderBookLevel(2, None, second_volume),
        OrderBookLevel(3, None, None),
        OrderBookLevel(4, None, None),
        OrderBookLevel(5, None, None),
    )


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
    assert ("api_v1", "share_capital") in first_snapshot["views"]
    assert ("api_v1", "distributions") in first_snapshot["views"]
    assert ("api_v1", "rights_issues") in first_snapshot["views"]
    assert ("api_v1", "classification_catalog_snapshots") in first_snapshot["views"]
    assert ("api_v1", "classification_member_snapshots") in first_snapshot["views"]
    assert ("api_v1", "classification_member_intervals") in first_snapshot["views"]
    assert ("api_v1", "calculation_runs") in first_snapshot["views"]
    assert ("api_v1", "adjusted_daily_bars") in first_snapshot["views"]
    assert ("api_v1", "daily_metrics") in first_snapshot["views"]
    assert ("api_v1", "market_capitalizations") in first_snapshot["views"]
    assert ("api_v1", "classification_daily_metrics") in first_snapshot["views"]
    assert ("api_v1", "classification_member_snapshot_status") in first_snapshot["views"]
    assert ("api_v1", "board_indexes") in first_snapshot["views"]
    assert ("api_v1", "board_index_daily_bars") in first_snapshot["views"]
    assert ("api_v1", "board_index_constituents") in first_snapshot["views"]
    assert {
        "query_securities",
        "query_daily_bars",
        "query_recent_daily_bars",
        "query_latest_stock_daily_indicators",
        "query_adjusted_daily_bars",
        "query_market_snapshot",
        "query_classification_members_as_of",
        "query_deducted_profits_as_of",
        "query_stock_pool_snapshot",
        "query_limit_up_pool",
        "query_auction_quotes",
    }.issubset({row[1] for row in first_snapshot["routines"]})


def test_persisted_latest_stock_quotes_rpc_is_retired(
    database_engine: Engine,
) -> None:
    with database_engine.connect() as connection:
        assert (
            connection.scalar(
                text("select to_regprocedure('api_v1.query_latest_stock_quotes(text[],integer)')")
            )
            is None
        )
        assert connection.scalar(
            text(
                "select relrowsecurity from pg_class where oid = "
                "'realtime.stock_quote_snapshot'::regclass"
            )
        )


def test_recent_daily_bars_rpc_permission_boundary(migrated_database_url: str) -> None:
    signature = "api_v1.query_recent_daily_bars(text,date,integer)"
    with psycopg.connect(migrated_database_url) as connection:
        assert connection.execute(
            "select has_function_privilege('market_data_api', %s, 'execute')",
            (signature,),
        ).fetchone() == (True,)
        assert connection.execute(
            "select has_function_privilege('anon', %s, 'execute')",
            (signature,),
        ).fetchone() == (False,)
        assert connection.execute(
            "select has_function_privilege('authenticated', %s, 'execute')",
            (signature,),
        ).fetchone() == (False,)


def test_latest_stock_daily_indicators_rpc_returns_each_symbol_latest_in_request_order(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    securities = [
        _security(),
        SecurityRecord(
            symbol="SZSE:000001",
            code="000001",
            exchange=Exchange.SZSE,
            name="平安银行",
            security_type=SecurityType.STOCK,
            status=SecurityStatus.LISTED,
            ipo_date=date(1991, 4, 3),
            delisting_date=None,
            source_code="baostock",
        ),
    ]
    security_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(security_run)
    persistence.commit_security_batch(
        _completed_run(security_run, len(securities)),
        _manifest(security_run.ingestion_id, "latest-indicator-securities", len(securities)),
        _envelopes(security_run.ingestion_id, securities),
    )

    trading_dates = [date(2026, 8, 20), date(2026, 8, 21)]
    calendar_run = _running_run(DatasetCode.TRADING_CALENDAR)
    persistence.create_ingestion_run(calendar_run)
    persistence.commit_trading_calendar_batch(
        _completed_run(calendar_run, len(trading_dates)),
        _manifest(calendar_run.ingestion_id, "latest-indicator-calendar", len(trading_dates)),
        _envelopes(
            calendar_run.ingestion_id,
            [
                CalculatedTradingDay(
                    market=Market.CN_A_SHARE,
                    trade_date=trade_date,
                    is_trading_day=True,
                    previous_trading_day=trading_dates[index - 1] if index else None,
                    next_trading_day=(
                        trading_dates[index + 1] if index + 1 < len(trading_dates) else None
                    ),
                    source_code="baostock",
                )
                for index, trade_date in enumerate(trading_dates)
            ],
        ),
    )

    indicator_run = _running_run(DatasetCode.STOCK_DAILY_INDICATOR, ProviderCode.TUSHARE)
    persistence.create_ingestion_run(indicator_run)
    indicators = [
        StockDailyIndicatorSnapshotRecord(
            symbol=symbol,
            trade_date=trade_date,
            market=Market.CN_A_SHARE,
            close=close,
            turnover_rate_pct=Decimal("1.25"),
            free_float_turnover_rate_pct=None,
            volume_ratio=None,
            pe=None,
            pe_ttm=None,
            pb=None,
            ps=None,
            ps_ttm=None,
            dividend_yield_pct=None,
            dividend_yield_ttm_pct=None,
            total_shares=10_000_000,
            circulating_shares=8_000_000,
            free_float_shares=7_000_000,
            total_market_value=Decimal("100000000.0000"),
            circulating_market_value=Decimal("80000000.0000"),
            price_limit_status=PriceLimitStatus.RISE,
            source_code="tushare",
        )
        for symbol, trade_date, close in (
            ("SSE:600000", trading_dates[0], Decimal("10.0000")),
            ("SSE:600000", trading_dates[1], Decimal("10.5000")),
            ("SZSE:000001", trading_dates[0], Decimal("12.0000")),
        )
    ]
    persistence.commit_stock_daily_indicator_batch(
        _completed_run(indicator_run, len(indicators)),
        _manifest(indicator_run.ingestion_id, "latest-indicators", len(indicators)),
        [IngestionEnvelope(indicator_run.ingestion_id, item) for item in indicators],
        [],
    )

    with database_engine.begin() as connection:
        connection.execute(text("set local role market_data_api"))
        payload = connection.execute(
            text("select api_v1.query_latest_stock_daily_indicators(:codes)"),
            {"codes": ["000001", "999999", "600000", "000001"]},
        ).scalar_one()

    assert payload["requested_count"] == 3
    assert payload["found_count"] == 2
    assert payload["missing_codes"] == ["999999"]
    assert [item["code"] for item in payload["items"]] == ["000001", "600000"]
    assert [item["trade_date"] for item in payload["items"]] == ["2026-08-20", "2026-08-21"]
    assert [item["close"] for item in payload["items"]] == [12.0, 10.5]


def test_latest_stock_daily_indicators_rpc_permission_boundary(
    migrated_database_url: str,
) -> None:
    signature = "api_v1.query_latest_stock_daily_indicators(text[])"
    with psycopg.connect(migrated_database_url) as connection:
        privileges = connection.execute(
            """
            select
                has_function_privilege('market_data_api', %s, 'execute'),
                has_function_privilege('anon', %s, 'execute'),
                has_function_privilege('authenticated', %s, 'execute')
            """,
            (signature, signature, signature),
        ).fetchone()

    assert privileges == (True, False, False)


def test_pysnowball_auction_session_and_quote_are_valid_source_facts(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    _commit_security_prerequisite(persistence)
    calculation_id = uuid4()
    snapshot_id = uuid4()
    session_id = uuid4()
    ingestion_id = uuid4()
    raw_id = uuid4()
    basis = date(2026, 8, 14)
    effective = date(2026, 8, 17)
    scheduled_at = datetime(2026, 8, 17, 1, 15, tzinfo=UTC)

    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into derived.calculation_run (
 calculation_id, calculation_code, algorithm_version, mode, start_date, end_date,
 status, input_watermark, input_hash, requested_at, calculated_at, finished_at, output_rows
) values (
 :calculation_id, 'cn_a_mainboard_price_limit_pools', '1.0.0', 'incremental',
 :basis, :basis, 'succeeded', '{}'::jsonb, :input_hash, now(), now(), now(), 1
)
"""),
            {"calculation_id": calculation_id, "basis": basis, "input_hash": "0" * 64},
        )
        connection.execute(
            text("""
insert into stock_pool.snapshot (
 snapshot_id, calculation_id, pool_code, basis_trade_date, effective_trade_date,
 version, status, member_count, candidate_count, rejected_count, content_hash,
 input_hash, rule_version, algorithm_version, generated_at
) values (
 :snapshot_id, :calculation_id, 'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP',
 :basis, :effective, 1, 'ready', 1, 1, 0, :content_hash,
 :input_hash, 'CN_MAINBOARD_2026_07_06', '1.0.0', now()
)
"""),
            {
                "snapshot_id": snapshot_id,
                "calculation_id": calculation_id,
                "basis": basis,
                "effective": effective,
                "content_hash": "1" * 64,
                "input_hash": "0" * 64,
            },
        )
        connection.execute(
            text("""
insert into realtime.auction_collection_session (
 session_id, pool_snapshot_id, pool_snapshot_version, basis_trade_date,
 effective_trade_date, window_start, window_end, cadence_seconds, expected_rounds,
 expected_quotes, provider_code, status, started_at
) values (
 :session_id, :snapshot_id, 1, :basis, :effective, :window_start,
 :window_end, 30, 21, 21, 'pysnowball', 'running', :window_start
)
"""),
            {
                "session_id": session_id,
                "snapshot_id": snapshot_id,
                "basis": basis,
                "effective": effective,
                "window_start": scheduled_at,
                "window_end": scheduled_at + timedelta(minutes=10),
            },
        )
        connection.execute(
            text("""
insert into ingestion.ingestion_run (
 ingestion_id, provider_code, dataset_code, status, requested_at, started_at
) values (:ingestion_id, 'pysnowball', 'five_level_quote', 'running', :at, :at)
"""),
            {"ingestion_id": ingestion_id, "at": scheduled_at},
        )
        connection.execute(
            text("""
insert into ingestion.raw_manifest (
 raw_id, ingestion_id, object_path, file_format, content_sha256,
 byte_size, row_count, schema_version
) values (
 :raw_id, :ingestion_id, 'pysnowball/auction.jsonl', 'jsonl', :sha,
 1, 1, 'pysnowball.pankou.v1'
)
"""),
            {"raw_id": raw_id, "ingestion_id": ingestion_id, "sha": "2" * 64},
        )
        connection.execute(
            text("""
insert into realtime.five_level_quote_snapshot (
 session_id, pool_snapshot_id, ingestion_id, raw_id, symbol, sample_seq,
 scheduled_at, collected_at, phase, quote_semantics, quote_status,
 last_price, bid1_price, bid1_volume, ask1_price, ask1_volume, source_code
) values (
 :session_id, :snapshot_id, :ingestion_id, :raw_id, :symbol, 0,
 :at, :at, 'cancellable', 'auction_indicative', 'trading',
 10.00, 9.99, 100, 10.01, 200, 'pysnowball'
)
"""),
            {
                "session_id": session_id,
                "snapshot_id": snapshot_id,
                "ingestion_id": ingestion_id,
                "raw_id": raw_id,
                "symbol": SYMBOL,
                "at": scheduled_at,
            },
        )

    assert (
        _scalar(
            database_engine,
            "select source_code from realtime.five_level_quote_snapshot where session_id=:id",
            {"id": session_id},
        )
        == "pysnowball"
    )


def test_auction_series_semantics_migration_labels_existing_rows_without_rewrite(
    empty_database_url: str,
) -> None:
    migration = next(
        migration
        for migration in MIGRATIONS
        if migration.name == "20260817000200_add_auction_series_value_semantics.sql"
    )
    enrichment_migration = next(
        item
        for item in MIGRATIONS
        if item.name == "20260818000100_enrich_call_auction_market_series.sql"
    )
    with psycopg.connect(empty_database_url) as connection:
        apply_migrations(
            connection, tuple(item for item in MIGRATIONS if item.name < migration.name)
        )

    trade_date = date(2026, 8, 17)
    slots = series_slots(trade_date)
    workflow_id = uuid4()
    session_id = uuid4()
    ingestion_id = uuid4()
    engine = create_engine(_sqlalchemy_url(empty_database_url))
    try:
        with engine.begin() as connection:
            _insert_call_auction_security_universe(connection)
            connection.execute(
                text("""
                    insert into operations.workflow_run (
                        workflow_run_id, workflow_code, scheduled_for, trigger_source,
                        attempt, status, started_at, finished_at
                    ) values (
                        :workflow_id, 'call_auction_market_series', :started_at,
                        'scheduled', 1, 'succeeded', :started_at, :finished_at
                    )
                """),
                {
                    "workflow_id": workflow_id,
                    "started_at": slots[0],
                    "finished_at": slots[0] + timedelta(seconds=2),
                },
            )
            connection.execute(
                text("""
                    insert into realtime.call_auction_market_series_session (
                        session_id, workflow_run_id, trade_date, window_start, window_end,
                        cadence_seconds, expected_rounds, universe_symbols, universe_count,
                        universe_hash, status, started_at, finished_at, successful_rounds,
                        successful_quotes
                    ) values (
                        :session_id, :workflow_id, :trade_date, :window_start, :window_end,
                        20, 32, array['SSE:600000'], 1, :universe_hash, 'succeeded',
                        :window_start, :finished_at, 1, 1
                    )
                """),
                {
                    "session_id": session_id,
                    "workflow_id": workflow_id,
                    "trade_date": trade_date,
                    "window_start": slots[0],
                    "window_end": slots[-1] + timedelta(seconds=20),
                    "universe_hash": universe_hash(("SSE:600000",)),
                    "finished_at": slots[0] + timedelta(seconds=2),
                },
            )
            connection.execute(
                text("""
                    insert into ingestion.ingestion_run (
                        ingestion_id, provider_code, dataset_code, status, requested_at,
                        started_at, finished_at, fetched_rows, accepted_rows
                    ) values (
                        :ingestion_id, 'pytdx_hq', 'call_auction_market_series',
                        'succeeded', :started_at, :started_at, :finished_at, 1, 1
                    )
                """),
                {
                    "ingestion_id": ingestion_id,
                    "started_at": slots[0],
                    "finished_at": slots[0] + timedelta(seconds=2),
                },
            )
            connection.execute(
                text("""
                    insert into realtime.call_auction_market_series_round (
                        session_id, sample_seq, scheduled_at, collected_at, status,
                        attempt_count, expected_quotes, successful_quotes, failed_quotes,
                        selected_ingestion_id
                    ) values (
                        :session_id, 0, :scheduled_at, :collected_at, 'succeeded',
                        1, 1, 1, 0, :ingestion_id
                    )
                """),
                {
                    "session_id": session_id,
                    "scheduled_at": slots[0],
                    "collected_at": slots[0] + timedelta(seconds=2),
                    "ingestion_id": ingestion_id,
                },
            )
            connection.execute(
                text("""
                    insert into realtime.call_auction_market_series_snapshot (
                        trade_date, ingestion_id, session_id, sample_seq, scheduled_at,
                        symbol, observed_at, last_price, cumulative_volume,
                        cumulative_amount, source_code
                    ) values (
                        :trade_date, :ingestion_id, :session_id, 0, :scheduled_at,
                        'SSE:600000', :observed_at, 9.8700, 32100, 316827.0000,
                        'pytdx_hq'
                    )
                """),
                {
                    "trade_date": trade_date,
                    "ingestion_id": ingestion_id,
                    "session_id": session_id,
                    "scheduled_at": slots[0],
                    "observed_at": slots[0] + timedelta(seconds=1),
                },
            )
            before = tuple(
                connection.execute(
                    text("""
                        select last_price, cumulative_volume, cumulative_amount
                        from realtime.call_auction_market_series_snapshot
                    """)
                ).one()
            )
    finally:
        engine.dispose()

    with psycopg.connect(empty_database_url) as connection:
        apply_migrations(connection, (migration,))
        after = connection.execute("""
            select last_price, cumulative_volume, cumulative_amount, value_semantics
            from realtime.call_auction_market_series_snapshot
        """).fetchone()

    assert after is not None
    assert tuple(after[:3]) == before
    assert after[3] == "legacy_source_quote"

    with psycopg.connect(empty_database_url) as connection:
        apply_migrations(connection, (enrichment_migration,))
        enriched = connection.execute("""
            select batch_code, bid1_price, bid1_volume, ask5_price, ask5_volume
            from realtime.call_auction_market_series_snapshot
        """).fetchone()

    assert enriched == ("091500", None, None, None, None)


def test_call_auction_market_series_schema_is_partitioned_and_internal(
    database_engine: Engine,
) -> None:
    table_names = (
        "call_auction_market_series_session",
        "call_auction_market_series_round",
        "call_auction_market_series_snapshot",
    )
    with database_engine.connect() as connection:
        assert {
            name: connection.scalar(
                text("select to_regclass('realtime.' || :name)"), {"name": name}
            )
            for name in table_names
        } == {name: f"realtime.{name}" for name in table_names}
        assert (
            connection.scalar(
                text("""
                select partstrat from pg_partitioned_table
                where partrelid='realtime.call_auction_market_series_snapshot'::regclass
            """)
            )
            == "r"
        )
        assert (
            connection.scalar(
                text("""
                select count(*) from pg_inherits
                where inhparent='realtime.call_auction_market_series_snapshot'::regclass
            """)
            )
            == 14
        )
        rls_tables = set(
            connection.execute(
                text("""
                    select tablename from pg_tables
                    where schemaname='realtime' and rowsecurity
                      and tablename like 'call_auction_market_series%'
                """)
            ).scalars()
        )
        assert rls_tables == {
            *table_names,
            *(f"call_auction_market_series_snapshot_{month}" for month in range(202608, 202613)),
            *(f"call_auction_market_series_snapshot_{month}" for month in range(202701, 202710)),
        }
        constraints = {
            row.name: row.definition
            for row in connection.execute(
                text("""
                    select conname name, pg_get_constraintdef(oid) definition
                    from pg_constraint
                    where conrelid in (
                      'realtime.call_auction_market_series_session'::regclass,
                      'realtime.call_auction_market_series_round'::regclass,
                      'realtime.call_auction_market_series_snapshot'::regclass
                    )
                """)
            )
        }
        assert constraints["call_auction_market_series_session_workflow_run_id_key"] == (
            "UNIQUE (workflow_run_id)"
        )
        assert constraints["call_auction_market_series_round_pkey"] == (
            "PRIMARY KEY (session_id, sample_seq)"
        )
        assert constraints["call_auction_market_series_snapshot_pkey"] == (
            "PRIMARY KEY (trade_date, ingestion_id, symbol)"
        )
        expected_quote_columns = {
            "batch_code",
            *(
                f"{side}{level}_{field}"
                for side in ("bid", "ask")
                for level in range(1, 6)
                for field in ("price", "volume")
            ),
        }
        for table_name in (
            "call_auction_market_series_snapshot",
            "call_auction_market_series_snapshot_202608",
        ):
            columns = set(
                connection.execute(
                    text("""
                        select column_name from information_schema.columns
                        where table_schema='realtime' and table_name=:table_name
                    """),
                    {"table_name": table_name},
                ).scalars()
            )
            assert expected_quote_columns <= columns
        grants = {
            (row.table_name, row.privilege_type)
            for row in connection.execute(
                text("""
                    select table_name, privilege_type
                    from information_schema.role_table_grants
                    where table_schema='realtime' and grantee='market_data_worker'
                      and table_name = any(:table_names)
                """),
                {"table_names": list(table_names)},
            )
        }
        assert grants == {
            ("call_auction_market_series_session", "SELECT"),
            ("call_auction_market_series_session", "INSERT"),
            ("call_auction_market_series_session", "UPDATE"),
            ("call_auction_market_series_round", "SELECT"),
            ("call_auction_market_series_round", "INSERT"),
            ("call_auction_market_series_round", "UPDATE"),
            ("call_auction_market_series_snapshot", "SELECT"),
            ("call_auction_market_series_snapshot", "INSERT"),
        }
        assert (
            connection.scalar(
                text("""
                select count(*) from information_schema.role_table_grants
                where table_schema='realtime'
                  and grantee in ('anon','authenticated','market_data_api')
                  and table_name = any(:table_names)
            """),
                {"table_names": list(table_names)},
            )
            == 0
        )
        assert (
            connection.scalar(
                text("""
                select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace
                where n.nspname='api_v1' and p.proname like 'call_auction_market_series%'
            """)
            )
            == 0
        )

        connection.execute(
            text("""
                insert into ingestion.ingestion_run (
                  ingestion_id,provider_code,dataset_code,status,requested_at
                ) values (
                  :ingestion_id,'pytdx_hq','call_auction_market_series','running',now()
                )
            """),
            {"ingestion_id": uuid4()},
        )
        connection.execute(
            text("""
                insert into operations.workflow_run (
                  workflow_run_id,workflow_code,scheduled_for,trigger_source,
                  attempt,status,started_at
                ) values (
                  :workflow_run_id,'call_auction_market_series',now(),'scheduled',1,'running',now()
                )
            """),
            {"workflow_run_id": uuid4()},
        )

    inventory = dict(
        capture_database_snapshot(
            database_engine.url.render_as_string(hide_password=False)
        ).row_counts
    )
    assert inventory["call_auction_market_series_session"] == 0
    assert inventory["call_auction_market_series_round"] == 0
    assert inventory["call_auction_market_series_snapshot"] == 0


def test_market_series_persistence_commits_attempt_and_finishes_partial_session(
    database_engine: Engine,
) -> None:
    trade_date = date(2026, 8, 17)
    slots = series_slots(trade_date)
    persistence = PostgreSQLCallAuctionMarketSeriesPersistence(database_engine)
    operations = PostgreSQLOperationsPersistence(database_engine)
    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
        _insert_trading_calendar_day(connection, trade_date, is_trading_day=True)

    workflow = operations.start_workflow(
        WorkflowCode.CALL_AUCTION_MARKET_SERIES,
        slots[0],
        TriggerSource.SCHEDULED,
    )
    symbols = ("SSE:600000", "SZSE:000001")
    session = MarketSeriesSession(
        session_id=uuid4(),
        workflow_run_id=workflow.workflow_run_id,
        trade_date=trade_date,
        window_start=slots[0],
        window_end=slots[-1] + timedelta(seconds=20),
        cadence_seconds=20,
        expected_rounds=32,
        universe_symbols=symbols,
        universe_count=2,
        universe_hash=universe_hash(symbols),
        status=MarketSeriesStatus.RUNNING,
        started_at=slots[0],
    )

    assert persistence.is_trading_day(trade_date)
    assert persistence.listed_sse_szse_stock_symbols() == list(symbols)
    assert persistence.load_recovery_universe(trade_date) is None
    persistence.create_session(session)
    assert persistence.load_recovery_universe(trade_date) == symbols

    running_round = MarketSeriesRound(
        session_id=session.session_id,
        sample_seq=0,
        scheduled_at=slots[0],
        collected_at=None,
        status=MarketSeriesStatus.RUNNING,
        attempt_count=0,
        expected_quotes=2,
        successful_quotes=0,
        failed_quotes=0,
        selected_ingestion_id=None,
    )
    persistence.start_round(running_round)
    ingestion_id = uuid4()
    running_run = IngestionRun(
        ingestion_id=ingestion_id,
        provider_code=ProviderCode.PYTDX_HQ,
        dataset_code=DatasetCode.CALL_AUCTION_MARKET_SERIES,
        status=IngestionStatus.RUNNING,
        requested_at=slots[0],
        started_at=slots[0],
        request_params={
            "trade_date": trade_date.isoformat(),
            "session_id": str(session.session_id),
            "sample_seq": 0,
            "scheduled_at": slots[0].isoformat(),
            "endpoint": "first.quote:7709",
            "expected_rows": 2,
        },
    )
    persistence.create_ingestion_run(running_run)
    completed_run = replace(
        running_run,
        status=IngestionStatus.PARTIAL,
        finished_at=slots[0] + timedelta(seconds=2),
        fetched_rows=1,
        accepted_rows=1,
    )
    records = (
        MarketSeriesSnapshotRecord(
            symbol="SSE:600000",
            trade_date=trade_date,
            session_id=session.session_id,
            sample_seq=0,
            batch_code="091500",
            scheduled_at=slots[0],
            observed_at=slots[0] + timedelta(seconds=1),
            source_code="pytdx_hq",
            value_semantics=MarketSeriesValueSemantics.AUCTION_INDICATIVE,
            bid_levels=_market_series_levels("10.00", 10_743_200),
            ask_levels=_market_series_levels("10.01", 13_300),
            last_price=Decimal("10.10"),
            previous_close=Decimal("10.00"),
            high_price=Decimal("10.10"),
            low_price=Decimal("10.00"),
            cumulative_volume=100,
            cumulative_amount=Decimal("1010.00"),
        ),
    )
    persistence.commit_attempt(
        completed_run,
        records,
        _manifest(ingestion_id, "market-series-partial", row_count=1, provider="pytdx_hq"),
        (),
    )
    persistence.finish_round(
        replace(
            running_round,
            collected_at=slots[0] + timedelta(seconds=2),
            status=MarketSeriesStatus.PARTIAL,
            attempt_count=1,
            successful_quotes=1,
            failed_quotes=1,
            selected_ingestion_id=ingestion_id,
        )
    )
    finished = persistence.finish_session(session.session_id, slots[-1] + timedelta(seconds=20))

    assert finished.status is MarketSeriesStatus.PARTIAL
    assert (finished.successful_rounds, finished.partial_rounds, finished.failed_rounds) == (
        0,
        1,
        31,
    )
    assert (finished.successful_quotes, finished.failed_quotes) == (1, 63)
    with database_engine.connect() as connection:
        assert (
            connection.scalar(
                text("select count(*) from realtime.call_auction_market_series_snapshot")
            )
            == 1
        )
        stored = connection.execute(
            text(
                """
                select last_price, cumulative_volume, cumulative_amount, value_semantics,
                       batch_code, bid2_price, bid2_volume, ask2_price, ask2_volume
                from realtime.call_auction_market_series_snapshot
                """
            )
        ).one()
        assert tuple(stored) == (
            Decimal("10.1000"),
            100,
            Decimal("1010.0000"),
            "auction_indicative",
            "091500",
            None,
            10_743_200,
            None,
            13_300,
        )
        assert (
            connection.scalar(
                text("select status from ingestion.ingestion_run where ingestion_id=:id"),
                {"id": ingestion_id},
            )
            == "partial"
        )

    invalid_updates = (
        "update realtime.call_auction_market_series_snapshot set batch_code='091501'",
        "update realtime.call_auction_market_series_snapshot set bid2_volume=-1",
        """
        update realtime.call_auction_market_series_snapshot
        set bid2_price=9.9900, bid2_volume=null
        """,
    )
    for statement in invalid_updates:
        with pytest.raises(IntegrityError), database_engine.begin() as connection:
            connection.execute(text(statement))


def test_market_series_attempt_rolls_back_manifest_quality_and_facts(
    database_engine: Engine,
) -> None:
    trade_date = date(2026, 8, 17)
    slots = series_slots(trade_date)
    persistence = PostgreSQLCallAuctionMarketSeriesPersistence(database_engine)
    operations = PostgreSQLOperationsPersistence(database_engine)
    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
    workflow = operations.start_workflow(
        WorkflowCode.CALL_AUCTION_MARKET_SERIES, slots[0], TriggerSource.SCHEDULED
    )
    symbols = ("SSE:600000", "SZSE:000001")
    session = MarketSeriesSession(
        uuid4(),
        workflow.workflow_run_id,
        trade_date,
        slots[0],
        slots[-1] + timedelta(seconds=20),
        20,
        32,
        symbols,
        2,
        universe_hash(symbols),
        MarketSeriesStatus.RUNNING,
        slots[0],
    )
    persistence.create_session(session)
    running_round = MarketSeriesRound(
        session.session_id,
        0,
        slots[0],
        None,
        MarketSeriesStatus.RUNNING,
        0,
        2,
        0,
        0,
        None,
    )
    persistence.start_round(running_round)
    running_run = IngestionRun(
        uuid4(),
        ProviderCode.PYTDX_HQ,
        DatasetCode.CALL_AUCTION_MARKET_SERIES,
        IngestionStatus.RUNNING,
        slots[0],
        slots[0],
        request_params={"session_id": str(session.session_id), "sample_seq": 0},
    )
    persistence.create_ingestion_run(running_run)
    record = MarketSeriesSnapshotRecord(
        symbol="SSE:600000",
        trade_date=trade_date,
        session_id=session.session_id,
        sample_seq=0,
        batch_code="091500",
        scheduled_at=slots[0],
        observed_at=slots[0] + timedelta(seconds=1),
        source_code="pytdx_hq",
        value_semantics=MarketSeriesValueSemantics.AUCTION_INDICATIVE,
        bid_levels=_market_series_levels("10.00", 10_743_200),
        ask_levels=_market_series_levels("10.01", 13_300),
        last_price=Decimal("10.00"),
        previous_close=Decimal("9.90"),
        high_price=Decimal("10.00"),
        low_price=Decimal("10.00"),
        cumulative_volume=100,
        cumulative_amount=Decimal("1000.00"),
    )
    completed = replace(
        running_run,
        status=IngestionStatus.SUCCEEDED,
        finished_at=slots[0] + timedelta(seconds=2),
        fetched_rows=2,
        accepted_rows=2,
    )
    quality = QualityResult(
        uuid4(),
        running_run.ingestion_id,
        DatasetCode.CALL_AUCTION_MARKET_SERIES,
        "call_auction_market_series.complete",
        QualitySeverity.INFO,
        QualityStatus.PASSED,
        "complete",
    )

    with pytest.raises(IntegrityError):
        persistence.commit_attempt(
            completed,
            (record, record),
            _manifest(
                running_run.ingestion_id,
                "market-series-rollback",
                row_count=2,
                provider="pytdx_hq",
            ),
            (quality,),
        )

    with database_engine.connect() as connection:
        counts = connection.execute(
            text("""
                select
                  (select count(*) from ingestion.raw_manifest where ingestion_id=:id),
                  (select count(*) from audit.quality_result where ingestion_id=:id),
                  (select count(*) from realtime.call_auction_market_series_snapshot
                   where ingestion_id=:id),
                  (select status from ingestion.ingestion_run where ingestion_id=:id)
            """),
            {"id": running_run.ingestion_id},
        ).one()
    assert tuple(counts) == (0, 0, 0, "running")

    mismatched_ingestion_id = uuid4()
    with database_engine.begin() as connection:
        connection.execute(
            text("""
                insert into ingestion.ingestion_run (
                  ingestion_id,provider_code,dataset_code,status,requested_at,started_at,
                  finished_at,request_params
                ) values (
                  :ingestion_id,'pytdx_hq','call_auction_market_series','partial',
                  :started_at,:started_at,:finished_at,
                  jsonb_build_object(
                    'session_id',cast(:wrong_session_id as text),'sample_seq',0
                  )
                )
            """),
            {
                "ingestion_id": mismatched_ingestion_id,
                "started_at": slots[0],
                "finished_at": slots[0] + timedelta(seconds=1),
                "wrong_session_id": str(uuid4()),
            },
        )
    with pytest.raises(ValueError, match="does not match"):
        persistence.finish_round(
            replace(
                running_round,
                collected_at=slots[0] + timedelta(seconds=2),
                status=MarketSeriesStatus.PARTIAL,
                attempt_count=1,
                failed_quotes=2,
                selected_ingestion_id=mismatched_ingestion_id,
            )
        )

    recovered = persistence.recover_expired_sessions(slots[-1] + timedelta(seconds=21))
    assert recovered == 1
    with database_engine.connect() as connection:
        terminal = connection.execute(
            text("""
                select session.status,round.status,round.failed_quotes
                from realtime.call_auction_market_series_session session
                join realtime.call_auction_market_series_round round using (session_id)
                where session.session_id=:session_id
            """),
            {"session_id": session.session_id},
        ).one()
    assert tuple(terminal) == ("failed", "failed", 2)


def test_auction_indicative_schema_and_api_permission_boundary(
    migrated_database_url: str,
) -> None:
    with psycopg.connect(migrated_database_url) as connection:
        assert connection.execute(
            "select to_regclass('realtime.call_auction_indicative_snapshot')"
        ).fetchone() == ("realtime.call_auction_indicative_snapshot",)
        assert connection.execute(
            "select to_regclass('realtime.call_auction_indicative_detail')"
        ).fetchone() == ("realtime.call_auction_indicative_detail",)
        assert connection.execute(
            "select has_function_privilege('market_data_api', "
            "'api_v1.query_call_auction_indicative_details(text,date,integer,integer)', "
            "'execute')"
        ).fetchone() == (True,)
        assert connection.execute(
            "select has_table_privilege('market_data_api', "
            "'realtime.call_auction_indicative_detail', 'select')"
        ).fetchone() == (False,)


def test_auction_indicative_database_response_matches_read_through_contract(
    migrated_database_url: str,
) -> None:
    security_ingestion_id = uuid4()
    ingestion_id = uuid4()
    raw_id = uuid4()
    with psycopg.connect(migrated_database_url) as connection:
        trade_date = connection.execute(
            "select (now() at time zone 'Asia/Shanghai')::date"
        ).fetchone()[0]
        connection.execute(
            """
insert into ingestion.ingestion_run (
    ingestion_id, provider_code, dataset_code, status, started_at
) values (%s, 'baostock', 'security', 'running', now())
""",
            (security_ingestion_id,),
        )
        connection.execute(
            """
insert into core.security (
    symbol, code, exchange, current_name, security_type, status,
    source_code, ingestion_id
) values ('SSE:688796', '688796', 'SSE', '测试股票', 'stock', 'listed',
          'baostock', %s)
""",
            (security_ingestion_id,),
        )
        connection.execute("set local role market_data_api")
        connection.execute(
            """
select api_v1.persist_call_auction_indicative_details(
    %s, %s, 'SSE:688796', %s, now(), %s, %s, %s, 100, 1,
    jsonb_build_array(jsonb_build_object(
        'source_sequence', 0,
        'observed_at', (%s::text || ' 09:15:05+08')::timestamptz,
        'indicative_price', '133.9900',
        'displayed_volume_shares', 200,
        'source_display_classification', 'unknown'
    ))
)
""",
            (
                ingestion_id,
                raw_id,
                trade_date,
                "a" * 64,
                (
                    "eastmoney/call_auction_indicative_detail/"
                    f"year={trade_date:%Y}/month={trade_date:%m}/day={trade_date:%d}/"
                    f"{raw_id}.jsonl"
                ),
                "b" * 64,
                trade_date,
            ),
        )
        payload = connection.execute(
            """
select api_v1.query_call_auction_indicative_details(
    'SSE:688796', %s, 0, 200
)
""",
            (trade_date,),
        ).fetchone()[0]

    assert payload["data_origin"] == "database"
    assert payload["persistence_status"] == "persisted"
    assert payload["version"] == 1
    assert payload["ingestion_status"] == "succeeded"
    assert payload["quality"]["database_persistence"] == "persisted"
    assert payload["quality"]["accepted_auction_row_count"] == 1


def test_today_limit_up_schema_is_internal_and_append_only(
    database_engine: Engine,
) -> None:
    with database_engine.connect() as connection:
        tables = {
            connection.scalar(text("select to_regclass(:name)"), {"name": name})
            for name in (
                "today_limit_up.source_observation",
                "today_limit_up.snapshot",
                "today_limit_up.member",
                "today_limit_up.calculation_quality",
            )
        }
        assert None not in tables
        assert connection.scalar(
            text("""
select count(*) = 4 from pg_class c join pg_namespace n on n.oid=c.relnamespace
where n.nspname='today_limit_up' and c.relname in
 ('source_observation','snapshot','member','calculation_quality') and c.relrowsecurity
""")
        )
        assert not connection.scalar(
            text("""
select has_schema_privilege('public','today_limit_up','usage')
 or has_table_privilege('public','today_limit_up.snapshot','select')
""")
        )


def test_daily_limit_up_list_rpc_exposes_only_bounded_domain_projection(
    database_engine: Engine,
) -> None:
    snapshot_id = uuid4()
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into today_limit_up.snapshot (
    snapshot_id, calculation_id, trade_date, version, status,
    member_count, candidate_count, rejected_count, content_hash, input_hash,
    rule_version, algorithm_version, source_ingestion_id, generated_at
) values (
    :snapshot_id, null, :trade_date, 1, 'deferred',
    0, 0, 0, :content_hash, :input_hash,
    'cn-a-share-limit-up-v1', 'today-limit-up-snapshot-v1', null, now()
)
"""),
            {
                "snapshot_id": snapshot_id,
                "trade_date": TRADE_DATE,
                "content_hash": "0" * 64,
                "input_hash": "1" * 64,
            },
        )
        connection.execute(
            text("""
insert into today_limit_up.calculation_quality (
    snapshot_id, rule_code, severity, symbol, message
) values (
    :snapshot_id, 'missing_daily_market', 'error', '',
    'daily_market dependency is not ready'
)
"""),
            {"snapshot_id": snapshot_id},
        )
        assert connection.scalar(
            text("""
select has_function_privilege(
    'market_data_api',
    'api_v1.query_daily_limit_up_list(date,integer,integer,integer)',
    'execute'
)
""")
        )
        assert not connection.scalar(
            text("""
select has_table_privilege(
    'market_data_api', 'today_limit_up.snapshot', 'select'
)
""")
        )
        connection.execute(text("set local role market_data_api"))
        payload = connection.scalar(
            text("""
select api_v1.query_daily_limit_up_list(
    :trade_date, 1, 0, 20
)
"""),
            {"trade_date": TRADE_DATE},
        )

    assert payload["snapshot_id"] == str(snapshot_id)
    assert payload["trade_date"] == TRADE_DATE.isoformat()
    assert payload["version"] == 1
    assert payload["status"] == "deferred"
    assert payload["member_count"] == 0
    assert payload["returned_count"] == 0
    assert payload["has_more"] is False
    assert payload["quality"] == {
        "total_findings": 1,
        "by_rule": {"missing_daily_market": 1},
    }
    assert payload["items"] == []


def test_call_auction_market_snapshot_rpc_selects_one_preferred_batch(
    database_engine: Engine,
) -> None:
    security_ingestion_id = uuid4()
    succeeded_ingestion_id = uuid4()
    partial_ingestion_id = uuid4()
    trade_date = date(2026, 8, 13)
    observed_at = datetime(2026, 8, 13, 1, 26, tzinfo=UTC)
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into ingestion.ingestion_run (
    ingestion_id, provider_code, dataset_code, status,
    requested_at, started_at, finished_at, fetched_rows, accepted_rows
) values
    (:security_id, 'baostock', 'security', 'running',
     :security_at, :security_at, null, 0, 0),
    (:succeeded_id, 'pytdx_hq', 'call_auction_market_snapshot', 'succeeded',
     :succeeded_at, :succeeded_at, :succeeded_finished_at, 2, 2),
    (:partial_id, 'pytdx_hq', 'call_auction_market_snapshot', 'partial',
     :partial_at, :partial_at, :partial_finished_at, 1, 1)
"""),
            {
                "security_id": security_ingestion_id,
                "security_at": observed_at - timedelta(days=1),
                "succeeded_id": succeeded_ingestion_id,
                "succeeded_at": observed_at - timedelta(minutes=2),
                "succeeded_finished_at": observed_at - timedelta(minutes=1),
                "partial_id": partial_ingestion_id,
                "partial_at": observed_at,
                "partial_finished_at": observed_at + timedelta(minutes=1),
            },
        )
        connection.execute(
            text("""
insert into core.security (
    symbol, code, exchange, current_name, security_type, status,
    source_code, ingestion_id
) values
    ('SSE:600000', '600000', 'SSE', '上海样本', 'stock', 'listed',
     'baostock', :security_id),
    ('SZSE:600000', '600000', 'SZSE', '深圳样本', 'stock', 'listed',
     'baostock', :security_id)
"""),
            {"security_id": security_ingestion_id},
        )
        connection.execute(
            text("""
insert into realtime.call_auction_market_snapshot (
    ingestion_id, symbol, trade_date, observed_at, last_price,
    previous_close, high_price, low_price, cumulative_volume,
    cumulative_amount, bid1_price, bid1_volume, bid2_volume,
    ask1_volume, ask2_volume, seal_amount, source_code
) values
    (:succeeded_id, 'SSE:600000', :trade_date, :observed_at,
     10.1200, 10.0000, 10.1500, 9.9800, 123400, 1248808.0000,
     10.1200, 560200, 10743200, 0, 13300, 5673224.0000, 'pytdx_hq'),
    (:succeeded_id, 'SZSE:600000', :trade_date, :observed_at,
     20.1200, 20.0000, 20.1500, 19.9800, 223400, 4494808.0000,
     null, null, null, null, null, null, 'pytdx_hq'),
    (:partial_id, 'SSE:600000', :trade_date, :observed_at,
     99.0000, 98.0000, 99.0000, 98.0000, 1, 99.0000,
     null, null, null, null, null, null, 'pytdx_hq')
"""),
            {
                "succeeded_id": succeeded_ingestion_id,
                "partial_id": partial_ingestion_id,
                "trade_date": trade_date,
                "observed_at": observed_at,
            },
        )
        assert connection.scalar(
            text("""
select has_function_privilege(
    'market_data_api',
    'api_v1.query_call_auction_market_snapshots(date,text[])',
    'execute'
)
""")
        )
        assert not connection.scalar(
            text("""
select has_table_privilege(
    'market_data_api', 'realtime.call_auction_market_snapshot', 'select'
)
""")
        )
        connection.execute(text("set local role market_data_api"))
        payload = connection.scalar(
            text("""
select api_v1.query_call_auction_market_snapshots(
    :trade_date, array['600000', '300001', '600000']::text[]
)
"""),
            {"trade_date": trade_date},
        )
        connection.execute(text("reset role"))
        connection.execute(
            text("""
update ingestion.ingestion_run
set status = 'failed'
where ingestion_id = :succeeded_id
"""),
            {"succeeded_id": succeeded_ingestion_id},
        )
        connection.execute(text("set local role market_data_api"))
        partial_payload = connection.scalar(
            text("""
select api_v1.query_call_auction_market_snapshots(
    :trade_date, array['600000']::text[]
)
"""),
            {"trade_date": trade_date},
        )

    assert payload["ingestion_id"] == str(succeeded_ingestion_id)
    assert payload["ingestion_status"] == "succeeded"
    assert payload["requested_count"] == 2
    assert payload["returned_count"] == 2
    assert payload["missing_codes"] == ["300001"]
    assert [(item["code"], item["symbol"]) for item in payload["items"]] == [
        ("600000", "SSE:600000"),
        ("600000", "SZSE:600000"),
    ]
    assert payload["items"][0]["last_price"] == 10.1200
    assert payload["items"][0]["bid2_price"] is None
    assert payload["items"][0]["bid2_volume"] == 10_743_200
    assert payload["items"][0]["ask2_volume"] == 13_300
    assert payload["items"][0]["seal_amount"] == 5_673_224.0000
    assert partial_payload["ingestion_id"] == str(partial_ingestion_id)
    assert partial_payload["ingestion_status"] == "partial"
    assert partial_payload["returned_count"] == 1
    assert partial_payload["items"][0]["last_price"] == 99.0000


def test_auction_one_price_limits_calculates_mainboard_limits_from_092530_snapshot(
    database_engine: Engine,
) -> None:
    security_ingestion_id = uuid4()
    calendar_ingestion_id = uuid4()
    bar_ingestion_id = uuid4()
    snapshot_ingestion_id = uuid4()
    partial_ingestion_id = uuid4()
    trade_date = date(2026, 8, 17)
    observed_at = datetime(2026, 8, 17, 1, 25, 30, tzinfo=UTC)
    prior_dates = tuple(date(2026, 8, day) for day in range(10, 15))
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into ingestion.ingestion_run (
 ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at,
 fetched_rows,accepted_rows
) values
 (:security_id,'baostock','security','running',:started_at,:started_at,null,0,0),
 (:calendar_id,'baostock','trading_calendar','running',:started_at,:started_at,null,0,0),
 (:bar_id,'baostock','daily_bar','running',:started_at,:started_at,null,0,0),
 (:snapshot_id,'pytdx_hq','call_auction_market_snapshot','succeeded',
  :started_at,:started_at,:finished_at,16,16),
 (:partial_id,'pytdx_hq','call_auction_market_snapshot','partial',
  :started_at,:started_at,:partial_finished_at,1,1)
"""),
            {
                "security_id": security_ingestion_id,
                "calendar_id": calendar_ingestion_id,
                "bar_id": bar_ingestion_id,
                "snapshot_id": snapshot_ingestion_id,
                "partial_id": partial_ingestion_id,
                "started_at": observed_at - timedelta(minutes=1),
                "finished_at": observed_at + timedelta(seconds=10),
                "partial_finished_at": observed_at + timedelta(seconds=20),
            },
        )
        connection.execute(
            text("""
insert into core.security (
 symbol,code,exchange,current_name,security_type,status,ipo_date,source_code,ingestion_id
) values
 ('SSE:600000','600000','SSE','上海涨停','stock','listed','2026-08-03','baostock',:id),
 ('SZSE:000001','000001','SZSE','深圳跌停','stock','listed','2026-08-03','baostock',:id),
 ('SSE:600001','600001','SSE','上海普通','stock','listed','2026-08-03','baostock',:id),
 ('SSE:600002','600002','SSE','低价涨停','stock','listed','2026-08-03','baostock',:id),
 ('SSE:600003','600003','SSE','缺IPO','stock','listed',null,'baostock',:id),
 ('SSE:600004','600004','SSE','上市五日内','stock','listed','2026-08-14','baostock',:id),
 ('SSE:600005','600005','SSE','半分涨停','stock','listed','2026-08-03','baostock',:id),
 ('SSE:600006','600006','SSE','最低跌停','stock','listed','2026-08-03','baostock',:id),
 ('SSE:600007','600007','SSE','缺一根日K','stock','listed','2026-08-03','baostock',:id),
 ('SSE:603999','603999','SSE','沪市边界一','stock','listed','2026-08-03','baostock',:id),
 ('SSE:605000','605000','SSE','沪市边界二','stock','listed','2026-08-03','baostock',:id),
 ('SZSE:004999','004999','SZSE','深市边界','stock','listed','2026-08-03','baostock',:id),
 ('SSE:604000','604000','SSE','沪市排除','stock','listed','2026-08-03','baostock',:id),
 ('SZSE:001001','001001','SZSE','深市排除一','stock','listed','2026-08-03','baostock',:id),
 ('SZSE:300001','300001','SZSE','创业排除','stock','listed','2026-08-03','baostock',:id),
 ('SSE:688001','688001','SSE','科创排除','stock','listed','2026-08-03','baostock',:id)
"""),
            {"id": security_ingestion_id},
        )
        connection.execute(
            text("""
insert into core.security_name_history (
 symbol,name,effective_from,source_code,ingestion_id
)
select symbol,current_name,'2026-08-03','baostock',:id
from core.security where ingestion_id=:id
"""),
            {"id": security_ingestion_id},
        )
        connection.execute(
            text("""
insert into core.trading_calendar (
 market,trade_date,is_trading_day,source_code,ingestion_id
)
select 'CN_A_SHARE',day,true,'baostock',:id
from unnest(cast(:days as date[])) day
"""),
            {
                "id": calendar_ingestion_id,
                "days": [date(2026, 8, 3), *prior_dates, trade_date],
            },
        )
        connection.execute(
            text("""
insert into core.daily_bar (
 symbol,trade_date,market,open,high,low,close,previous_close,volume,amount,
 trade_status,is_st,source_code,ingestion_id
)
select symbol,day,'CN_A_SHARE',10,10,10,10,10,100,1000,
       'unknown',false,'baostock',:bar_id
from unnest(cast(:symbols as text[])) symbol
cross join unnest(cast(:days as date[])) day
"""),
            {
                "bar_id": bar_ingestion_id,
                "symbols": [
                    "SSE:600000",
                    "SZSE:000001",
                    "SSE:600001",
                    "SSE:600002",
                    "SSE:600005",
                    "SSE:600006",
                    "SSE:603999",
                    "SSE:605000",
                    "SZSE:004999",
                ],
                "days": list(prior_dates),
            },
        )
        connection.execute(
            text("""
insert into core.daily_bar (
 symbol,trade_date,market,open,high,low,close,previous_close,volume,amount,
 trade_status,is_st,source_code,ingestion_id
)
select 'SSE:600007',day,'CN_A_SHARE',10,10,10,10,10,100,1000,
       'unknown',false,'baostock',:bar_id
from unnest(cast(:days as date[])) day
"""),
            {"bar_id": bar_ingestion_id, "days": list(prior_dates[:4])},
        )
        connection.execute(
            text("""
insert into realtime.call_auction_market_snapshot (
 ingestion_id,symbol,trade_date,observed_at,last_price,previous_close,
 high_price,low_price,cumulative_volume,cumulative_amount,source_code
) values
 (:id,'SSE:600000',:day,:at,11,10,11,11,100,1100,'pytdx_hq'),
 (:id,'SZSE:000001',:day,:at,9,10,9,9,100,900,'pytdx_hq'),
 (:id,'SSE:600001',:day,:at,10.5,10,10.5,10.5,100,1050,'pytdx_hq'),
 (:id,'SSE:600002',:day,:at,0.05,0.04,0.05,0.05,100,5,'pytdx_hq'),
 (:id,'SSE:600003',:day,:at,11,10,11,11,100,1100,'pytdx_hq'),
 (:id,'SSE:600004',:day,:at,11,10,11,11,100,1100,'pytdx_hq'),
 (:id,'SSE:600005',:day,:at,11.06,10.05,11.06,11.06,100,1106,'pytdx_hq'),
 (:id,'SSE:600006',:day,:at,0.01,0.01,0.01,0.01,100,1,'pytdx_hq'),
 (:id,'SSE:600007',:day,:at,11,10,11,11,100,1100,'pytdx_hq'),
 (:id,'SSE:603999',:day,:at,10,10,10,10,100,1000,'pytdx_hq'),
 (:id,'SSE:605000',:day,:at,10,10,10,10,100,1000,'pytdx_hq'),
 (:id,'SZSE:004999',:day,:at,10,10,10,10,100,1000,'pytdx_hq'),
 (:id,'SSE:604000',:day,:at,11,10,11,11,100,1100,'pytdx_hq'),
 (:id,'SZSE:001001',:day,:at,11,10,11,11,100,1100,'pytdx_hq'),
 (:id,'SZSE:300001',:day,:at,12,10,12,12,100,1200,'pytdx_hq'),
 (:id,'SSE:688001',:day,:at,12,10,12,12,100,1200,'pytdx_hq'),
 (:partial_id,'SSE:600001',:day,:at,10,10,10,10,100,1000,'pytdx_hq')
"""),
            {
                "id": snapshot_ingestion_id,
                "partial_id": partial_ingestion_id,
                "day": trade_date,
                "at": observed_at,
            },
        )
        connection.execute(
            text("""
update realtime.call_auction_market_snapshot
set bid1_price=11, bid1_volume=100, seal_amount=1100
where ingestion_id=:id and symbol='SSE:600000'
"""),
            {"id": snapshot_ingestion_id},
        )
        assert connection.scalar(
            text(
                "select has_function_privilege('market_data_api', "
                "'api_v1.query_auction_one_price_limits(date)', 'execute')"
            )
        )
        assert not connection.scalar(
            text(
                "select has_table_privilege('market_data_api', "
                "'realtime.call_auction_market_snapshot', 'select')"
            )
        )
        assert not connection.scalar(
            text(
                "select has_table_privilege('market_data_api', "
                "'core.trading_calendar', 'select') "
                "or has_table_privilege('market_data_api', 'core.daily_bar', 'select') "
                "or has_table_privilege('market_data_api', "
                "'derived.daily_price_limit', 'select')"
            )
        )
        connection.execute(text("set local role market_data_api"))
        payload = connection.scalar(
            text("select api_v1.query_auction_one_price_limits(:day)"),
            {"day": trade_date},
        )
        connection.execute(text("reset role"))
        connection.execute(
            text("update ingestion.ingestion_run set status='failed' where ingestion_id=:id"),
            {"id": snapshot_ingestion_id},
        )
        connection.execute(text("set local role market_data_api"))
        partial_payload = connection.scalar(
            text("select api_v1.query_auction_one_price_limits(:day)"),
            {"day": trade_date},
        )

    assert payload["ingestion_id"] == str(snapshot_ingestion_id)
    assert payload["price_limit_calculation_id"] is None
    assert payload["price_limit_rule_version"] == "CN_MAINBOARD_2026_07_06"
    assert payload["price_limit_algorithm_version"] == "1.0.0"
    assert payload["calculation_mode"] == "realtime_read"
    assert payload["candidate_count"] == 12
    assert payload["omitted_incomplete_count"] == 3
    assert [item["symbol"] for item in payload["up"]] == [
        "SSE:600000",
        "SSE:600002",
        "SSE:600005",
    ]
    assert payload["up"][1]["limit_price"] == 0.05
    assert payload["up"][2]["limit_price"] == 11.06
    assert payload["up"][0]["seal_amount"] == 1100
    assert [item["symbol"] for item in payload["down"]] == [
        "SSE:600006",
        "SZSE:000001",
    ]
    assert payload["down"][0]["limit_price"] == 0.01
    assert payload["down"][0]["seal_amount"] is None
    assert partial_payload["ingestion_id"] == str(partial_ingestion_id)
    assert partial_payload["ingestion_status"] == "partial"
    assert partial_payload["candidate_count"] == 1
    assert partial_payload["up"] == []
    assert partial_payload["down"] == []


def test_auction_one_price_limits_requires_exact_092550_snapshot(
    database_engine: Engine,
) -> None:
    with database_engine.connect() as connection:
        connection.execute(text("set local role market_data_api"))
        with pytest.raises(DBAPIError) as raised:
            connection.scalar(
                text("select api_v1.query_auction_one_price_limits(:day)"),
                {"day": date(2026, 8, 17)},
            )

    assert raised.value.orig.sqlstate == "P0002"


def test_call_auction_one_price_patterns_requires_a_complete_window_session(
    database_engine: Engine,
) -> None:
    with database_engine.connect() as connection:
        connection.execute(text("set local role market_data_api"))
        with pytest.raises(DBAPIError) as raised:
            connection.scalar(
                text("select api_v1.query_call_auction_one_price_patterns(:day)"),
                {"day": date(2026, 8, 18)},
            )

    assert raised.value.orig.sqlstate == "P0002"


def test_call_auction_one_price_patterns_filters_strict_29_round_window(
    database_engine: Engine,
) -> None:
    trade_date = date(2026, 8, 18)
    session_id = uuid4()

    with database_engine.begin() as connection:
        _seed_one_price_pattern_session(connection, trade_date, session_id)
        connection.execute(text("set local role market_data_api"))
        payload = connection.scalar(
            text("select api_v1.query_call_auction_one_price_patterns(:day)"),
            {"day": trade_date},
        )
        latest_payload = connection.scalar(
            text("select api_v1.query_call_auction_one_price_patterns(null)"),
        )
        connection.execute(text("reset role"))

        assert connection.scalar(
            text("""
select has_function_privilege(
    'market_data_api',
    'api_v1.query_call_auction_one_price_patterns(date)',
    'execute'
)
""")
        )
        assert not connection.scalar(
            text("""
select has_function_privilege(
    'authenticated',
    'api_v1.query_call_auction_one_price_patterns(date)',
    'execute'
)
""")
        )
        assert connection.scalar(
            text("""
select 'statement_timeout=10s' = any(proconfig)
from pg_proc
where oid = 'api_v1.query_call_auction_one_price_patterns(date)'::regprocedure
""")
        )
        assert connection.scalar(
            text("""
select to_regclass(
    'realtime.call_auction_market_series_snapshot_session_symbol_idx'
) is not null
""")
        )

        connection.execute(
            text("""
update realtime.call_auction_market_series_snapshot
set last_price = 10.5000
where session_id = :session_id
  and symbol in ('SSE:600000','SZSE:000001','SZSE:000002')
"""),
            {"session_id": session_id},
        )
        connection.execute(text("set local role market_data_api"))
        empty_payload = connection.scalar(
            text("select api_v1.query_call_auction_one_price_patterns(:day)"),
            {"day": trade_date},
        )

    assert payload["trade_date"] == trade_date.isoformat()
    assert payload["session_id"] == str(session_id)
    assert payload["session_status"] == "partial"
    assert payload["round_count"] == 29
    assert payload["candidate_count"] == 3
    assert [item["code"] for item in payload["items"]] == [
        "000002",
        "600000",
        "000001",
    ]
    assert [Decimal(str(item["change_pct"])) for item in payload["items"]] == [
        Decimal("4.0000000000"),
        Decimal("2.0000000000"),
        Decimal("-4.0000000000"),
    ]
    assert payload["items"][1]["name"] == "浦发银行"
    assert all(item["sample_count"] == 29 for item in payload["items"])
    assert latest_payload == payload
    assert empty_payload["candidate_count"] == 0
    assert empty_payload["items"] == []


def test_call_auction_market_series_rpc_selects_one_session_and_orders_rounds(
    database_engine: Engine,
) -> None:
    trade_date = date(2026, 8, 14)
    slots = series_slots(trade_date)
    succeeded_session_id = uuid4()
    partial_session_id = uuid4()
    succeeded_workflow_id = uuid4()
    partial_workflow_id = uuid4()
    round_zero_ingestion_id = uuid4()
    round_one_ingestion_id = uuid4()
    partial_ingestion_id = uuid4()

    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
        connection.execute(
            text("""
insert into operations.workflow_run (
    workflow_run_id, workflow_code, scheduled_for, trigger_source,
    attempt, status, started_at, finished_at
) values
    (:succeeded_workflow_id, 'call_auction_market_series', :started_at,
     'scheduled', 1, 'succeeded', :started_at, :succeeded_finished_at),
    (:partial_workflow_id, 'call_auction_market_series', :started_at,
     'scheduled', 2, 'succeeded', :started_at, :partial_finished_at)
"""),
            {
                "succeeded_workflow_id": succeeded_workflow_id,
                "partial_workflow_id": partial_workflow_id,
                "started_at": slots[0],
                "succeeded_finished_at": slots[2],
                "partial_finished_at": slots[3],
            },
        )
        connection.execute(
            text("""
insert into realtime.call_auction_market_series_session (
    session_id, workflow_run_id, trade_date, window_start, window_end,
    cadence_seconds, expected_rounds, universe_symbols, universe_count,
    universe_hash, status, started_at, finished_at, successful_rounds,
    partial_rounds, failed_rounds, successful_quotes, failed_quotes
) values
    (:succeeded_session_id, :succeeded_workflow_id, :trade_date, :window_start,
     :window_end, 20, 32, array['SSE:600000','SZSE:000001'], 2, :universe_hash,
     'succeeded', :window_start, :succeeded_finished_at, 2, 0, 0, 4, 0),
    (:partial_session_id, :partial_workflow_id, :trade_date, :window_start,
     :window_end, 20, 32, array['SSE:600000','SZSE:000001'], 2, :universe_hash,
     'partial', :window_start, :partial_finished_at, 0, 1, 0, 1, 1)
"""),
            {
                "succeeded_session_id": succeeded_session_id,
                "partial_session_id": partial_session_id,
                "succeeded_workflow_id": succeeded_workflow_id,
                "partial_workflow_id": partial_workflow_id,
                "trade_date": trade_date,
                "window_start": slots[0],
                "window_end": slots[-1] + timedelta(seconds=20),
                "universe_hash": universe_hash(("SSE:600000", "SZSE:000001")),
                "succeeded_finished_at": slots[2],
                "partial_finished_at": slots[3],
            },
        )
        connection.execute(
            text("""
insert into ingestion.ingestion_run (
    ingestion_id, provider_code, dataset_code, status, requested_at,
    started_at, finished_at, fetched_rows, accepted_rows
) values
    (:round_zero_id, 'pytdx_hq', 'call_auction_market_series', 'succeeded',
     :slot_zero, :slot_zero, :round_zero_finished, 2, 2),
    (:round_one_id, 'pytdx_hq', 'call_auction_market_series', 'succeeded',
     :slot_one, :slot_one, :round_one_finished, 2, 2),
    (:partial_id, 'pytdx_hq', 'call_auction_market_series', 'partial',
     :slot_zero, :slot_zero, :partial_finished, 1, 1)
"""),
            {
                "round_zero_id": round_zero_ingestion_id,
                "round_one_id": round_one_ingestion_id,
                "partial_id": partial_ingestion_id,
                "slot_zero": slots[0],
                "slot_one": slots[1],
                "round_zero_finished": slots[0] + timedelta(seconds=2),
                "round_one_finished": slots[1] + timedelta(seconds=2),
                "partial_finished": slots[0] + timedelta(seconds=3),
            },
        )
        connection.execute(
            text("""
insert into realtime.call_auction_market_series_round (
    session_id, sample_seq, scheduled_at, collected_at, status, attempt_count,
    expected_quotes, successful_quotes, failed_quotes, selected_ingestion_id
) values
    (:succeeded_session_id, 1, :slot_one, :round_one_finished, 'succeeded',
     1, 2, 2, 0, :round_one_id),
    (:succeeded_session_id, 0, :slot_zero, :round_zero_finished, 'succeeded',
     1, 2, 2, 0, :round_zero_id),
    (:partial_session_id, 0, :slot_zero, :partial_finished, 'partial',
     1, 2, 1, 1, :partial_id)
"""),
            {
                "succeeded_session_id": succeeded_session_id,
                "partial_session_id": partial_session_id,
                "slot_zero": slots[0],
                "slot_one": slots[1],
                "round_zero_finished": slots[0] + timedelta(seconds=2),
                "round_one_finished": slots[1] + timedelta(seconds=2),
                "partial_finished": slots[0] + timedelta(seconds=3),
                "round_zero_id": round_zero_ingestion_id,
                "round_one_id": round_one_ingestion_id,
                "partial_id": partial_ingestion_id,
            },
        )
        connection.execute(
            text("""
insert into realtime.call_auction_market_series_snapshot (
    trade_date, ingestion_id, session_id, sample_seq, batch_code, scheduled_at, symbol,
    observed_at, last_price, previous_close, high_price, low_price,
    cumulative_volume, cumulative_amount, source_code, value_semantics,
    bid1_price, bid1_volume, bid2_price, bid2_volume,
    ask1_price, ask1_volume, ask2_price, ask2_volume
) values
    (:trade_date, :round_zero_id, :succeeded_session_id, 0, '091500', :slot_zero,
     'SSE:600000', :round_zero_observed, 10.1000, 10.0000, 10.1000, 10.0000,
     100, 1010.0000, 'pytdx_hq', 'auction_indicative',
     10.0000, 100, null, 10743200, 10.0100, 100, null, 13300),
    (:trade_date, :round_one_id, :succeeded_session_id, 1, '091520', :slot_one,
     'SSE:600000', :round_one_observed, 10.2000, 10.0000, 10.2000, 10.0000,
     200, 2040.0000, 'pytdx_hq', 'auction_indicative',
     10.0000, 100, null, 10743200, 10.0100, 100, null, 13300),
    (:trade_date, :partial_id, :partial_session_id, 0, '091500', :slot_zero,
     'SSE:600000', :partial_observed, 99.0000, 98.0000, 99.0000, 98.0000,
     1, 99.0000, 'pytdx_hq', 'auction_indicative',
     98.0000, 100, null, 10743200, 99.0000, 100, null, 13300)
"""),
            {
                "trade_date": trade_date,
                "round_zero_id": round_zero_ingestion_id,
                "round_one_id": round_one_ingestion_id,
                "partial_id": partial_ingestion_id,
                "succeeded_session_id": succeeded_session_id,
                "partial_session_id": partial_session_id,
                "slot_zero": slots[0],
                "slot_one": slots[1],
                "round_zero_observed": slots[0] + timedelta(seconds=1),
                "round_one_observed": slots[1] + timedelta(seconds=1),
                "partial_observed": slots[0] + timedelta(seconds=1),
            },
        )

        assert connection.scalar(
            text("""
select has_function_privilege(
    'market_data_api',
    'api_v1.query_call_auction_market_series_snapshots(date,text[],text)',
    'execute'
)
""")
        )
        assert not connection.scalar(
            text("""
select has_table_privilege(
    'market_data_api', 'realtime.call_auction_market_series_snapshot', 'select'
)
""")
        )
        connection.execute(text("set local role market_data_api"))
        payload = connection.scalar(
            text("""
select api_v1.query_call_auction_market_series_snapshots(
    :trade_date, array['600000','000001','600000']::text[], '091520'
)
"""),
            {"trade_date": trade_date},
        )
        unfiltered_payload = connection.scalar(
            text("""
select api_v1.query_call_auction_market_series_snapshots(
    :trade_date, array['600000']::text[]
)
"""),
            {"trade_date": trade_date},
        )
        missing_batch_payload = connection.scalar(
            text("""
select api_v1.query_call_auction_market_series_snapshots(
    :trade_date, array['600000']::text[], '091540'
)
"""),
            {"trade_date": trade_date},
        )
        connection.execute(text("reset role"))
        connection.execute(
            text("""
update realtime.call_auction_market_series_session
set status='failed'
where session_id=:session_id
"""),
            {"session_id": succeeded_session_id},
        )
        connection.execute(text("set local role market_data_api"))
        partial_payload = connection.scalar(
            text("""
select api_v1.query_call_auction_market_series_snapshots(
    :trade_date, array['600000']::text[]
)
"""),
            {"trade_date": trade_date},
        )

    assert payload["session_id"] == str(succeeded_session_id)
    assert payload["session_status"] == "succeeded"
    assert payload["requested_count"] == 2
    assert payload["returned_rounds"] == 1
    assert [item["sample_seq"] for item in payload["rounds"]] == [1]
    assert payload["rounds"][0]["missing_codes"] == ["000001"]
    assert payload["rounds"][0]["items"][0]["last_price"] == 10.2000
    assert payload["rounds"][0]["items"][0]["value_semantics"] == "auction_indicative"
    assert payload["rounds"][0]["items"][0]["batch_code"] == "091520"
    assert payload["rounds"][0]["items"][0]["bid2_price"] is None
    assert payload["rounds"][0]["items"][0]["bid2_volume"] == 10_743_200
    assert payload["rounds"][0]["items"][0]["ask2_volume"] == 13_300
    assert unfiltered_payload["returned_rounds"] == 2
    assert [item["sample_seq"] for item in unfiltered_payload["rounds"]] == [0, 1]
    assert missing_batch_payload["returned_rounds"] == 0
    assert missing_batch_payload["rounds"] == []
    assert partial_payload["session_id"] == str(partial_session_id)
    assert partial_payload["session_status"] == "partial"
    assert partial_payload["rounds"][0]["items"][0]["last_price"] == 99.0000


def test_operations_repository_records_attempts_steps_and_stale_recovery(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLOperationsPersistence(database_engine)
    scheduled_for = datetime(2026, 8, 2, 10, tzinfo=UTC)
    first = persistence.start_workflow(
        WorkflowCode.DAILY_MARKET, scheduled_for, TriggerSource.SCHEDULED
    )
    job = persistence.start_job(first.workflow_run_id, "security", 1)
    persistence.finish_job(
        job.finish(
            ExecutionStatus.SUCCEEDED,
            job.started_at + timedelta(seconds=2),
            fetched_rows=10,
            accepted_rows=10,
        )
    )
    persistence.finish_workflow(
        first.finish(
            ExecutionStatus.SUCCEEDED,
            first.started_at + timedelta(seconds=3),
            accepted_rows=10,
        )
    )
    retry = persistence.start_workflow(
        WorkflowCode.DAILY_MARKET, scheduled_for, TriggerSource.MANUAL
    )

    recovered = persistence.recover_stale(datetime.now(UTC) + timedelta(minutes=1))
    history = persistence.recent_workflows()

    assert retry.attempt == 2
    assert recovered == 1
    assert history[0].status is ExecutionStatus.FAILED
    assert history[0].error_summary == "worker_interrupted_or_timed_out"


def test_operations_stale_recovery_preserves_time_order_when_database_clock_trails_worker(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLOperationsPersistence(database_engine)
    run = persistence.start_workflow(
        WorkflowCode.DAILY_MARKET,
        datetime(2026, 8, 2, 10, tzinfo=UTC),
        TriggerSource.MANUAL,
    )
    persistence.start_job(run.workflow_run_id, "security", 1)
    with database_engine.begin() as connection:
        worker_started_at = connection.execute(
            text("select now() + interval '1 hour'")
        ).scalar_one()
        connection.execute(
            text("update operations.workflow_run set started_at=:started_at"),
            {"started_at": worker_started_at},
        )
        connection.execute(
            text("update operations.job_execution set started_at=:started_at"),
            {"started_at": worker_started_at},
        )

    recovered = persistence.recover_stale(worker_started_at + timedelta(minutes=1))

    with database_engine.connect() as connection:
        workflow_row = connection.execute(
            text("select status, started_at, finished_at from operations.workflow_run")
        ).one()
        job_row = connection.execute(
            text("select status, started_at, finished_at from operations.job_execution")
        ).one()

    assert recovered == 1
    assert workflow_row.status == "failed"
    assert workflow_row.finished_at >= workflow_row.started_at
    assert job_row.status == "failed"
    assert job_row.finished_at >= job_row.started_at


def test_operations_workflow_codes_are_constrained(database_engine: Engine) -> None:
    statement = text("""
        insert into operations.workflow_run (
            workflow_run_id,
            workflow_code,
            scheduled_for,
            trigger_source,
            attempt,
            status,
            started_at
        ) values (
            :workflow_run_id,
            :workflow_code,
            :scheduled_for,
            'scheduled',
            1,
            'running',
            :scheduled_for
        )
    """)
    with database_engine.begin() as connection:
        connection.execute(
            statement,
            {
                "workflow_run_id": uuid4(),
                "workflow_code": "pytdx_pool_refresh",
                "scheduled_for": datetime(2026, 8, 11, 2, tzinfo=UTC),
            },
        )

    with pytest.raises(DBAPIError) as captured, database_engine.begin() as connection:
        connection.execute(
            statement,
            {
                "workflow_run_id": uuid4(),
                "workflow_code": "unknown_workflow",
                "scheduled_for": datetime(2026, 8, 11, 3, tzinfo=UTC),
            },
        )

    assert isinstance(captured.value.orig, CheckViolation)


def test_call_auction_market_schema_enforces_append_only_source_facts(
    database_engine: Engine,
) -> None:
    trade_date = date(2026, 8, 12)
    observed_at = datetime(2026, 8, 12, 1, 26, tzinfo=UTC)
    ingestion_id = uuid4()
    security_ingestion_id = uuid4()
    workflow_run_id = uuid4()

    with database_engine.connect() as connection:
        assert (
            connection.scalar(text("select to_regclass('realtime.call_auction_market_snapshot')"))
            == "realtime.call_auction_market_snapshot"
        )
        columns = (
            connection.execute(
                text("""
                select column_name, data_type, numeric_precision, numeric_scale, is_nullable
                from information_schema.columns
                where table_schema = 'realtime'
                  and table_name = 'call_auction_market_snapshot'
                order by ordinal_position
            """)
            )
            .mappings()
            .all()
        )
        column_names = {cast(str, row["column_name"]) for row in columns}
        assert column_names >= {
            "ingestion_id",
            "symbol",
            "trade_date",
            "observed_at",
            "last_price",
            "previous_close",
            "high_price",
            "low_price",
            "cumulative_volume",
            "cumulative_amount",
            "bid1_price",
            "bid1_volume",
            "bid2_price",
            "bid2_volume",
            "bid3_price",
            "bid3_volume",
            "bid4_price",
            "bid4_volume",
            "bid5_price",
            "bid5_volume",
            "ask1_price",
            "ask1_volume",
            "ask2_price",
            "ask2_volume",
            "ask3_price",
            "ask3_volume",
            "ask4_price",
            "ask4_volume",
            "ask5_price",
            "ask5_volume",
            "seal_amount",
            "source_code",
            "created_at",
        }
        numeric_columns = {
            cast(str, row["column_name"]): (
                row["numeric_precision"],
                row["numeric_scale"],
                row["is_nullable"],
            )
            for row in columns
            if row["data_type"] == "numeric"
        }
        assert numeric_columns == {
            "last_price": (18, 4, "YES"),
            "previous_close": (18, 4, "YES"),
            "high_price": (18, 4, "YES"),
            "low_price": (18, 4, "YES"),
            "cumulative_amount": (30, 4, "YES"),
            "bid1_price": (18, 4, "YES"),
            "bid2_price": (18, 4, "YES"),
            "bid3_price": (18, 4, "YES"),
            "bid4_price": (18, 4, "YES"),
            "bid5_price": (18, 4, "YES"),
            "ask1_price": (18, 4, "YES"),
            "ask2_price": (18, 4, "YES"),
            "ask3_price": (18, 4, "YES"),
            "ask4_price": (18, 4, "YES"),
            "ask5_price": (18, 4, "YES"),
            "seal_amount": (30, 4, "YES"),
        }
        nonnumeric_columns = {
            cast(str, row["column_name"]): (row["data_type"], row["is_nullable"])
            for row in columns
            if row["column_name"]
            in {
                "ingestion_id",
                "symbol",
                "trade_date",
                "observed_at",
                "cumulative_volume",
                "source_code",
                "created_at",
            }
        }
        assert nonnumeric_columns == {
            "ingestion_id": ("uuid", "NO"),
            "symbol": ("text", "NO"),
            "trade_date": ("date", "NO"),
            "observed_at": ("timestamp with time zone", "NO"),
            "cumulative_volume": ("bigint", "YES"),
            "source_code": ("text", "NO"),
            "created_at": ("timestamp with time zone", "NO"),
        }
        primary_key = connection.execute(
            text("""
                select array_agg(attribute.attname order by key.ordinality)
                from pg_constraint constraint_definition
                cross join lateral unnest(constraint_definition.conkey)
                    with ordinality as key(attnum, ordinality)
                join pg_attribute attribute
                  on attribute.attrelid = constraint_definition.conrelid
                 and attribute.attnum = key.attnum
                where constraint_definition.conrelid =
                      'realtime.call_auction_market_snapshot'::regclass
                  and constraint_definition.contype = 'p'
            """)
        ).scalar_one()
        assert primary_key == ["ingestion_id", "symbol"]
        foreign_tables = set(
            connection.execute(
                text("""
                    select referenced_namespace.nspname, referenced_table.relname
                    from pg_constraint constraint_definition
                    join pg_class referenced_table
                      on referenced_table.oid = constraint_definition.confrelid
                    join pg_namespace referenced_namespace
                      on referenced_namespace.oid = referenced_table.relnamespace
                    where constraint_definition.conrelid =
                          'realtime.call_auction_market_snapshot'::regclass
                      and constraint_definition.contype = 'f'
                """)
            ).all()
        )
        assert foreign_tables == {("ingestion", "ingestion_run"), ("core", "security")}
        check_constraints = {
            cast(str, name): cast(bool, validated)
            for name, validated in connection.execute(
                text("""
                    select conname, convalidated
                    from pg_constraint
                    where conrelid = 'realtime.call_auction_market_snapshot'::regclass
                      and contype = 'c'
                """)
            ).all()
        }
        assert check_constraints == {
            "call_auction_market_nonnegative": True,
            "call_auction_market_observation_window": True,
            "call_auction_market_order_book_nonnegative": True,
            "call_auction_market_order_book_price_requires_volume": True,
            "call_auction_market_order_book_volume_only_positive": True,
            "call_auction_market_price_range": True,
            "call_auction_market_seal_amount_rule": True,
            "call_auction_market_snapshot_source_code_check": True,
        }
        index_columns = connection.execute(
            text("""
                select array_agg(attribute.attname order by key.ordinality)
                from pg_index index_definition
                cross join lateral unnest(index_definition.indkey)
                    with ordinality as key(attnum, ordinality)
                join pg_attribute attribute
                  on attribute.attrelid = index_definition.indrelid
                 and attribute.attnum = key.attnum
                where index_definition.indrelid =
                      'realtime.call_auction_market_snapshot'::regclass
                  and not index_definition.indisprimary
                group by index_definition.indexrelid
            """)
        ).scalar_one()
        assert index_columns == ["trade_date", "ingestion_id", "symbol"]
        assert (
            connection.scalar(
                text("""
                select relrowsecurity
                from pg_class
                where oid = 'realtime.call_auction_market_snapshot'::regclass
            """)
            )
            is True
        )
        policies = {
            (cast(str, command), tuple(cast(list[str], roles)))
            for command, roles in connection.execute(
                text("""
                    select cmd, roles
                    from pg_policies
                    where schemaname = 'realtime'
                      and tablename = 'call_auction_market_snapshot'
                """)
            ).all()
        }
        assert policies == {
            ("INSERT", ("market_data_worker",)),
            ("SELECT", ("market_data_worker",)),
        }
        worker_grants = {
            cast(str, privilege)
            for (privilege,) in connection.execute(
                text("""
                    select privilege_type
                    from information_schema.role_table_grants
                    where grantee = 'market_data_worker'
                      and table_schema = 'realtime'
                      and table_name = 'call_auction_market_snapshot'
                """)
            ).all()
        }
        assert worker_grants == {"INSERT", "SELECT"}
        final_observed_at = connection.execute(
            text("""
                select data_type, is_nullable
                from information_schema.columns
                where table_schema = 'realtime'
                  and table_name = 'call_auction_snapshot'
                  and column_name = 'observed_at'
            """)
        ).one()
        assert tuple(final_observed_at) == ("timestamp with time zone", "YES")
        assert (
            connection.scalar(
                text("""
                select has_table_privilege(
                    'market_data_worker', 'realtime.call_auction_snapshot', 'delete'
                )
            """)
            )
            is True
        )
        assert (
            connection.execute(
                text("""
                select table_type
                from information_schema.tables
                where table_schema = 'api_v1'
                  and table_name = 'call_auction_market_snapshot'
            """)
            ).all()
            == []
        )

    with database_engine.begin() as connection:
        connection.execute(
            text("""
                insert into ingestion.ingestion_run (
                    ingestion_id, provider_code, dataset_code, status
                ) values
                    (:security_ingestion_id, 'baostock', 'security', 'running'),
                    (:ingestion_id, 'pytdx_hq', 'call_auction_market_snapshot', 'running')
            """),
            {
                "security_ingestion_id": security_ingestion_id,
                "ingestion_id": ingestion_id,
            },
        )
        connection.execute(
            text("""
                insert into core.security (
                    symbol, code, exchange, current_name, security_type, status,
                    source_code, ingestion_id
                ) values (
                    :symbol, '600000', 'SSE', '浦发银行', 'stock', 'listed',
                    'baostock', :security_ingestion_id
                )
            """),
            {"symbol": SYMBOL, "security_ingestion_id": security_ingestion_id},
        )
        connection.execute(
            text("""
                insert into audit.quality_result (
                    ingestion_id, dataset_code, rule_code, severity, status, message
                ) values (
                    :ingestion_id, 'call_auction_market_snapshot',
                    'market_snapshot.complete', 'info', 'passed', 'complete'
                )
            """),
            {"ingestion_id": ingestion_id},
        )
        connection.execute(
            text("""
                insert into operations.workflow_run (
                    workflow_run_id, workflow_code, scheduled_for, trigger_source,
                    attempt, status, started_at
                ) values (
                    :workflow_run_id, 'call_auction_market_snapshot', :scheduled_for,
                    'scheduled', 1, 'running', :scheduled_for
                )
            """),
            {
                "workflow_run_id": workflow_run_id,
                "scheduled_for": observed_at,
            },
        )
        connection.execute(text("set local role market_data_worker"))
        connection.execute(
            text("""
                insert into realtime.call_auction_market_snapshot (
                    ingestion_id, symbol, trade_date, observed_at, last_price,
                    previous_close, high_price, low_price, cumulative_volume,
                    cumulative_amount, source_code
                ) values (
                    :ingestion_id, :symbol, :trade_date, :observed_at, 10.0000,
                    9.8000, 10.1000, 9.9000, 100, 1000.0000, 'pytdx_hq'
                )
            """),
            {
                "ingestion_id": ingestion_id,
                "symbol": SYMBOL,
                "trade_date": trade_date,
                "observed_at": observed_at,
            },
        )
        worker_row = connection.execute(
            text("""
                select symbol, cumulative_volume, source_code
                from realtime.call_auction_market_snapshot
                where ingestion_id=:ingestion_id
            """),
            {"ingestion_id": ingestion_id},
        ).one()
        connection.execute(text("reset role"))
        assert tuple(worker_row) == (SYMBOL, 100, "pytdx_hq")

    invalid_rows = (
        ("call_auction_market_price_range", {"high_price": "9.8000", "low_price": "9.9000"}),
        ("call_auction_market_price_range", {"last_price": "9.8000", "low_price": "9.9000"}),
        ("call_auction_market_price_range", {"last_price": "10.2000", "high_price": "10.1000"}),
        ("call_auction_market_nonnegative", {"previous_close": "-0.0001"}),
        (
            "call_auction_market_observation_window",
            {"observed_at": datetime(2026, 8, 12, 1, 30, tzinfo=UTC)},
        ),
    )
    statement = text("""
        insert into realtime.call_auction_market_snapshot (
            ingestion_id, symbol, trade_date, observed_at, last_price,
            previous_close, high_price, low_price, cumulative_volume,
            cumulative_amount, source_code
        ) values (
            :ingestion_id, :symbol, :trade_date, :observed_at, :last_price,
            :previous_close, :high_price, :low_price, :cumulative_volume,
            :cumulative_amount, 'pytdx_hq'
        )
    """)
    defaults: dict[str, object] = {
        "symbol": SYMBOL,
        "trade_date": trade_date,
        "observed_at": observed_at,
        "last_price": "10.0000",
        "previous_close": "9.8000",
        "high_price": None,
        "low_price": None,
        "cumulative_volume": 100,
        "cumulative_amount": "1000.0000",
    }
    for expected_constraint, overrides in invalid_rows:
        rejected_ingestion_id = uuid4()
        parameters = defaults | overrides | {"ingestion_id": rejected_ingestion_id}
        with pytest.raises(DBAPIError) as captured, database_engine.begin() as connection:
            connection.execute(
                text("""
                    insert into ingestion.ingestion_run (
                        ingestion_id, provider_code, dataset_code, status
                    ) values (
                        :ingestion_id, 'pytdx_hq',
                        'call_auction_market_snapshot', 'running'
                    )
                """),
                {"ingestion_id": rejected_ingestion_id},
            )
            connection.execute(statement, parameters)
        assert isinstance(captured.value.orig, CheckViolation)
        assert captured.value.orig.diag.constraint_name == expected_constraint

    for forbidden_statement in (
        "update realtime.call_auction_market_snapshot set last_price = last_price",
        "delete from realtime.call_auction_market_snapshot",
    ):
        with pytest.raises(DBAPIError) as captured, database_engine.begin() as connection:
            connection.execute(text("set role market_data_worker"))
            connection.execute(text(forbidden_statement))
        assert isinstance(captured.value.orig, InsufficientPrivilege)

    database_url = database_engine.url.render_as_string(hide_password=False)
    snapshot = capture_database_snapshot(database_url)
    assert dict(snapshot.row_counts)["call_auction_market_snapshot"] == 1
    assert snapshot.orphan_facts == 0

    with database_engine.begin() as connection:
        connection.execute(text("set session_replication_role = replica"))
        connection.execute(
            statement,
            defaults
            | {
                "ingestion_id": uuid4(),
                "observed_at": datetime(2026, 8, 12, 1, 27, tzinfo=UTC),
            },
        )
        connection.execute(text("set session_replication_role = origin"))

    orphaned_snapshot = capture_database_snapshot(database_url)
    assert orphaned_snapshot.orphan_facts == 1


def test_call_auction_market_seal_rule_preserves_legacy_and_checks_three_ask_levels(
    database_engine: Engine,
) -> None:
    security_ingestion_id = uuid4()
    legacy_ingestion_id = uuid4()
    current_ingestion_id = uuid4()
    invalid_ingestion_id = uuid4()
    with database_engine.begin() as connection:
        connection.execute(
            text("""
                insert into ingestion.ingestion_run (
                    ingestion_id, provider_code, dataset_code, status
                ) values
                    (:security_id, 'baostock', 'security', 'running'),
                    (:legacy_id, 'pytdx_hq', 'call_auction_market_snapshot', 'running'),
                    (:current_id, 'pytdx_hq', 'call_auction_market_snapshot', 'running'),
                    (:invalid_id, 'pytdx_hq', 'call_auction_market_snapshot', 'running')
            """),
            {
                "security_id": security_ingestion_id,
                "legacy_id": legacy_ingestion_id,
                "current_id": current_ingestion_id,
                "invalid_id": invalid_ingestion_id,
            },
        )
        connection.execute(
            text("""
                insert into core.security (
                    symbol, code, exchange, current_name, security_type, status,
                    source_code, ingestion_id
                ) values (
                    'SSE:600000', '600000', 'SSE', '浦发银行', 'stock', 'listed',
                    'baostock', :security_id
                )
            """),
            {"security_id": security_ingestion_id},
        )
        insert_snapshot = text("""
            insert into realtime.call_auction_market_snapshot (
                ingestion_id, symbol, trade_date, observed_at,
                bid1_price, bid1_volume,
                ask1_volume, ask2_volume, ask3_volume,
                seal_amount, source_code
            ) values (
                :ingestion_id, 'SSE:600000', :trade_date, :observed_at,
                10, 100, null, 100, null, :seal_amount, 'pytdx_hq'
            )
        """)
        connection.execute(
            insert_snapshot,
            [
                {
                    "ingestion_id": legacy_ingestion_id,
                    "trade_date": date(2026, 8, 19),
                    "observed_at": datetime(2026, 8, 19, 1, 25, 50, tzinfo=UTC),
                    "seal_amount": Decimal("1000"),
                },
                {
                    "ingestion_id": current_ingestion_id,
                    "trade_date": date(2026, 8, 20),
                    "observed_at": datetime(2026, 8, 20, 1, 25, 30, tzinfo=UTC),
                    "seal_amount": None,
                },
            ],
        )

    with pytest.raises(DBAPIError) as raised, database_engine.begin() as connection:
        connection.execute(
            insert_snapshot,
            {
                "ingestion_id": invalid_ingestion_id,
                "trade_date": date(2026, 8, 20),
                "observed_at": datetime(2026, 8, 20, 1, 25, 30, tzinfo=UTC),
                "seal_amount": Decimal("1000"),
            },
        )

    assert isinstance(raised.value.orig, CheckViolation)
    assert raised.value.orig.diag.constraint_name == "call_auction_market_seal_amount_rule"


def test_call_auction_market_attempts_are_append_only_and_finalize_latest_success(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    trade_date = date(2026, 8, 12)
    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
        _insert_trading_calendar_day(connection, trade_date, is_trading_day=True)

    assert persistence.is_trading_day(trade_date) is True
    assert persistence.is_trading_day(trade_date - timedelta(days=1)) is False
    assert persistence.listed_sse_szse_stock_symbols() == ["SSE:600000", "SZSE:000001"]

    partial = _call_auction_market_run(
        IngestionStatus.PARTIAL,
        finished_at=datetime(2026, 8, 12, 1, 27, tzinfo=UTC),
        fetched_rows=2,
        accepted_rows=1,
        rejected_rows=1,
    )
    partial_record = _call_auction_market_record(
        "SSE:600000",
        trade_date,
        datetime(2026, 8, 12, 1, 26, tzinfo=UTC),
        last_price=Decimal("10.00"),
        previous_close=Decimal("10.00"),
        high_price=Decimal("10.05"),
        low_price=Decimal("9.95"),
        cumulative_volume=100,
        cumulative_amount=Decimal("1000.00"),
    )
    _commit_call_auction_market_run(persistence, partial, [partial_record])

    succeeded = _call_auction_market_run(
        IngestionStatus.SUCCEEDED,
        finished_at=datetime(2026, 8, 12, 1, 28, tzinfo=UTC),
        fetched_rows=2,
        accepted_rows=2,
        request_params={"trade_date": "1999-01-01"},
    )
    succeeded_records = [
        _call_auction_market_record(
            "SSE:600000",
            trade_date,
            datetime(2026, 8, 12, 1, 27, tzinfo=UTC),
            last_price=Decimal("10.10"),
            previous_close=Decimal("10.00"),
            high_price=Decimal("10.10"),
            low_price=Decimal("10.00"),
            cumulative_volume=123_400,
            cumulative_amount=Decimal("1246340.00"),
        ),
        _call_auction_market_record(
            "SZSE:000001",
            trade_date,
            datetime(2026, 8, 12, 1, 27, 1, tzinfo=UTC),
            last_price=Decimal("8.80"),
            previous_close=Decimal("8.00"),
            high_price=Decimal("8.90"),
            low_price=Decimal("8.00"),
            cumulative_volume=200_000,
            cumulative_amount=Decimal("1760000.00"),
        ),
    ]
    _commit_call_auction_market_run(persistence, succeeded, succeeded_records)

    with database_engine.begin() as connection:
        _insert_ready_limit_up_pool(
            connection,
            trade_date - timedelta(days=1),
            ["BSE:920000"],
            version=1,
        )
        _insert_ready_limit_up_pool(
            connection,
            trade_date,
            ["SSE:600000"],
            version=1,
        )
        _insert_ready_limit_up_pool(
            connection,
            trade_date,
            ["SSE:600000", "SZSE:000001"],
            version=2,
        )

    written = persistence.finalize_call_auction_snapshot(trade_date)

    assert written == 2
    with database_engine.connect() as connection:
        source_rows = connection.execute(
            text("""
                select symbol, observed_at, high_price, low_price, ingestion_id
                from realtime.call_auction_market_snapshot
                order by ingestion_id, symbol
            """)
        ).all()
        final_rows = connection.execute(
            text("""
                select symbol, cumulative_volume, cumulative_amount, auction_premium_pct,
                       observed_at, ingestion_id
                from realtime.call_auction_snapshot order by symbol
            """)
        ).all()
        audit_counts = connection.execute(
            text("""
                select
                    (select count(*) from ingestion.raw_manifest) manifests,
                    (select count(*) from audit.quality_result) quality_results
            """)
        ).one()

    assert len(source_rows) == 3
    assert all(row.high_price is not None and row.low_price is not None for row in source_rows)
    assert {row.ingestion_id for row in final_rows} == {succeeded.ingestion_id}
    assert [row.symbol for row in final_rows] == ["SSE:600000", "SZSE:000001"]
    assert [row.cumulative_volume for row in final_rows] == [123_400, 200_000]
    assert [row.cumulative_amount for row in final_rows] == [
        Decimal("1246340.0000"),
        Decimal("1760000.0000"),
    ]
    assert [row.auction_premium_pct for row in final_rows] == [
        Decimal("1.0000000000"),
        Decimal("10.0000000000"),
    ]
    assert [row.observed_at for row in final_rows] == [
        datetime(2026, 8, 12, 1, 27, tzinfo=UTC),
        datetime(2026, 8, 12, 1, 27, 1, tzinfo=UTC),
    ]
    assert tuple(audit_counts) == (2, 2)


def test_call_auction_market_attempt_rejects_incoherent_manifest_and_fact_counts(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    trade_date = date(2026, 8, 12)
    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
    record = _call_auction_market_record(
        "SSE:600000",
        trade_date,
        datetime(2026, 8, 12, 1, 26, tzinfo=UTC),
    )
    cases = (
        (2, 1, 1, "Raw manifest row_count"),
        (2, 2, 2, "accepted_rows"),
    )
    for fetched_rows, accepted_rows, manifest_rows, message in cases:
        run = _call_auction_market_run(
            IngestionStatus.SUCCEEDED,
            finished_at=datetime(2026, 8, 12, 1, 28, tzinfo=UTC),
            fetched_rows=fetched_rows,
            accepted_rows=accepted_rows,
        )
        persistence.create_ingestion_run(
            replace(
                run,
                status=IngestionStatus.RUNNING,
                finished_at=None,
                fetched_rows=0,
                accepted_rows=0,
            )
        )

        with pytest.raises(ValueError, match=message):
            persistence.commit_call_auction_market_attempt(
                run,
                [record],
                _manifest(
                    run.ingestion_id,
                    f"call-auction-market-counts-{run.ingestion_id}",
                    row_count=manifest_rows,
                    provider="pytdx_hq",
                ),
                [],
            )

        with database_engine.connect() as connection:
            counts = connection.execute(
                text("""
                    select
                        (select count(*) from ingestion.raw_manifest
                         where ingestion_id=:ingestion_id),
                        (select count(*) from realtime.call_auction_market_snapshot
                         where ingestion_id=:ingestion_id),
                        (select status from ingestion.ingestion_run
                         where ingestion_id=:ingestion_id)
                """),
                {"ingestion_id": run.ingestion_id},
            ).one()
        assert tuple(counts) == (0, 0, "running")


def test_call_auction_market_attempt_rolls_back_manifest_quality_and_facts(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    trade_date = date(2026, 8, 12)
    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
    run = _call_auction_market_run(
        IngestionStatus.SUCCEEDED,
        finished_at=datetime(2026, 8, 12, 1, 28, tzinfo=UTC),
        fetched_rows=2,
        accepted_rows=2,
    )
    running = replace(
        run,
        status=IngestionStatus.RUNNING,
        finished_at=None,
        fetched_rows=0,
        accepted_rows=0,
    )
    persistence.create_ingestion_run(running)
    duplicate = _call_auction_market_record(
        "SSE:600000",
        trade_date,
        datetime(2026, 8, 12, 1, 26, tzinfo=UTC),
        last_price=Decimal("10.00"),
        previous_close=Decimal("9.90"),
        high_price=Decimal("10.00"),
        low_price=Decimal("9.90"),
        cumulative_volume=100,
        cumulative_amount=Decimal("1000.00"),
    )
    manifest = _manifest(
        run.ingestion_id,
        "call-auction-market-rollback",
        row_count=2,
        provider="pytdx_hq",
    )
    quality = _call_auction_quality_result(run.ingestion_id)

    with pytest.raises(IntegrityError):
        persistence.commit_call_auction_market_attempt(
            run,
            [duplicate, duplicate],
            manifest,
            [quality],
        )

    with database_engine.connect() as connection:
        counts = connection.execute(
            text("""
                select
                    (select count(*) from ingestion.raw_manifest
                     where ingestion_id=:ingestion_id) manifests,
                    (select count(*) from audit.quality_result
                     where ingestion_id=:ingestion_id) quality_results,
                    (select count(*) from realtime.call_auction_market_snapshot
                     where ingestion_id=:ingestion_id) source_facts,
                    (select status from ingestion.ingestion_run
                     where ingestion_id=:ingestion_id) status
            """),
            {"ingestion_id": run.ingestion_id},
        ).one()
    assert tuple(counts) == (0, 0, 0, "running")


def test_finalize_call_auction_rejects_partial_and_old_date_inputs(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    trade_date = date(2026, 8, 12)
    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
    old_date = trade_date - timedelta(days=1)
    old_succeeded = _call_auction_market_run(
        IngestionStatus.SUCCEEDED,
        finished_at=datetime(2026, 8, 11, 1, 28, tzinfo=UTC),
        fetched_rows=1,
        accepted_rows=1,
    )
    _commit_call_auction_market_run(
        persistence,
        old_succeeded,
        [
            _call_auction_market_record(
                "SSE:600000",
                old_date,
                datetime(2026, 8, 11, 1, 26, tzinfo=UTC),
            )
        ],
    )
    partial = _call_auction_market_run(
        IngestionStatus.PARTIAL,
        finished_at=datetime(2026, 8, 12, 1, 28, tzinfo=UTC),
        fetched_rows=1,
        accepted_rows=1,
    )
    _commit_call_auction_market_run(
        persistence,
        partial,
        [
            _call_auction_market_record(
                "SSE:600000",
                trade_date,
                datetime(2026, 8, 12, 1, 26, tzinfo=UTC),
            )
        ],
    )
    with database_engine.begin() as connection:
        _insert_ready_limit_up_pool(connection, trade_date, ["SSE:600000"])

    with pytest.raises(LookupError, match="successful call-auction market snapshot"):
        persistence.finalize_call_auction_snapshot(trade_date)

    with database_engine.connect() as connection:
        assert connection.scalar(text("select count(*) from realtime.call_auction_snapshot")) == 0


def test_finalize_call_auction_incomplete_pool_preserves_existing_final_rows(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    trade_date = date(2026, 8, 12)
    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
    succeeded = _call_auction_market_run(
        IngestionStatus.SUCCEEDED,
        finished_at=datetime(2026, 8, 12, 1, 28, tzinfo=UTC),
        fetched_rows=1,
        accepted_rows=1,
    )
    _commit_call_auction_market_run(
        persistence,
        succeeded,
        [
            _call_auction_market_record(
                "SSE:600000",
                trade_date,
                datetime(2026, 8, 12, 1, 26, tzinfo=UTC),
            )
        ],
    )
    with database_engine.begin() as connection:
        _insert_ready_limit_up_pool(
            connection,
            trade_date,
            ["SSE:600000", "SZSE:000001"],
        )
        legacy_ingestion_id = _insert_existing_call_auction_final(connection, trade_date)

    with pytest.raises(RuntimeError, match="complete coverage"):
        persistence.finalize_call_auction_snapshot(trade_date)

    with database_engine.connect() as connection:
        existing = connection.execute(
            text("""
                select symbol, ingestion_id, cumulative_volume
                from realtime.call_auction_snapshot where trade_date=:trade_date
            """),
            {"trade_date": trade_date},
        ).one()
    assert tuple(existing) == ("SSE:600000", legacy_ingestion_id, 999)


def test_finalize_call_auction_rejects_inconsistent_pool_member_metadata(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    trade_date = date(2026, 8, 12)
    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
    succeeded = _call_auction_market_run(
        IngestionStatus.SUCCEEDED,
        finished_at=datetime(2026, 8, 12, 1, 28, tzinfo=UTC),
        fetched_rows=1,
        accepted_rows=1,
    )
    _commit_call_auction_market_run(
        persistence,
        succeeded,
        [
            _call_auction_market_record(
                "SSE:600000",
                trade_date,
                datetime(2026, 8, 12, 1, 26, tzinfo=UTC),
            )
        ],
    )
    with database_engine.begin() as connection:
        snapshot_id = _insert_ready_limit_up_pool(
            connection,
            trade_date,
            ["SSE:600000", "SZSE:000001"],
        )
        connection.execute(
            text("update stock_pool.snapshot set member_count=1 where snapshot_id=:snapshot_id"),
            {"snapshot_id": snapshot_id},
        )
        legacy_ingestion_id = _insert_existing_call_auction_final(connection, trade_date)

    with pytest.raises(RuntimeError, match="complete coverage"):
        persistence.finalize_call_auction_snapshot(trade_date)

    with database_engine.connect() as connection:
        existing = connection.execute(
            text("""
                select symbol, ingestion_id, cumulative_volume
                from realtime.call_auction_snapshot where trade_date=:trade_date
            """),
            {"trade_date": trade_date},
        ).one()
    assert tuple(existing) == ("SSE:600000", legacy_ingestion_id, 999)


def test_finalize_call_auction_ready_empty_pool_clears_only_exact_date(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    trade_date = date(2026, 8, 12)
    with database_engine.begin() as connection:
        _insert_call_auction_security_universe(connection)
    succeeded = _call_auction_market_run(
        IngestionStatus.SUCCEEDED,
        finished_at=datetime(2026, 8, 12, 1, 28, tzinfo=UTC),
        fetched_rows=1,
        accepted_rows=1,
    )
    _commit_call_auction_market_run(
        persistence,
        succeeded,
        [
            _call_auction_market_record(
                "SSE:600000",
                trade_date,
                datetime(2026, 8, 12, 1, 26, tzinfo=UTC),
            )
        ],
    )
    with database_engine.begin() as connection:
        _insert_ready_limit_up_pool(connection, trade_date, [])
        _insert_existing_call_auction_final(connection, trade_date)
        _insert_existing_call_auction_final(connection, trade_date - timedelta(days=1))

    written = persistence.finalize_call_auction_snapshot(trade_date)

    assert written == 0
    with database_engine.connect() as connection:
        remaining_dates = list(
            connection.execute(
                text("select trade_date from realtime.call_auction_snapshot order by trade_date")
            ).scalars()
        )
    assert remaining_dates == [trade_date - timedelta(days=1)]


def test_stock_pool_rpc_requires_exact_ready_date_and_preserves_empty_snapshot(
    database_engine: Engine,
) -> None:
    calculation_id = uuid4()
    snapshot_id = uuid4()
    basis = date(2026, 7, 31)
    effective = date(2026, 8, 3)
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into derived.calculation_run (
 calculation_id, calculation_code, algorithm_version, mode, start_date, end_date,
 status, input_watermark, input_hash, requested_at, calculated_at, finished_at, output_rows
) values (
 :calculation_id, 'cn_a_mainboard_price_limit_pools', '1.0.0', 'incremental',
 :basis, :basis, 'succeeded', '{}'::jsonb, :input_hash, now(), now(), now(), 0
)
"""),
            {"calculation_id": calculation_id, "basis": basis, "input_hash": "0" * 64},
        )
        connection.execute(
            text("""
insert into stock_pool.snapshot (
 snapshot_id, calculation_id, pool_code, basis_trade_date, effective_trade_date,
 version, status, member_count, candidate_count, rejected_count, content_hash,
 input_hash, rule_version, algorithm_version, generated_at
) values (
 :snapshot_id, :calculation_id, 'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP',
 :basis, :effective, 1, 'ready', 0, 10, 0, :content_hash,
 :input_hash, 'CN_MAINBOARD_2026_07_06', '1.0.0', now()
)
"""),
            {
                "snapshot_id": snapshot_id,
                "calculation_id": calculation_id,
                "basis": basis,
                "effective": effective,
                "content_hash": "1" * 64,
                "input_hash": "0" * 64,
            },
        )
    with database_engine.connect() as connection:
        payload = connection.execute(
            text("select api_v1.query_stock_pool_snapshot(:code, :effective, null, 5000)"),
            {"code": "CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP", "effective": effective},
        ).scalar_one()

    assert payload["snapshot_id"] == str(snapshot_id)
    assert payload["member_count"] == 0
    assert payload["members"] == []

    with database_engine.connect() as connection, pytest.raises(DBAPIError) as error:
        connection.execute(
            text("select api_v1.query_stock_pool_snapshot(:code, :effective, null, 5000)"),
            {
                "code": "CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP",
                "effective": effective - timedelta(days=1),
            },
        ).scalar_one()
    assert getattr(error.value.orig, "sqlstate", None) == "P0002"


def test_deducted_profit_is_idempotent_and_as_of_excludes_later_observation(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    security_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(security_run)
    persistence.commit_security_batch(
        _completed_run(security_run),
        _manifest(security_run.ingestion_id, "security"),
        _envelopes(security_run.ingestion_id, [_security()]),
    )
    values = {
        "symbol": SYMBOL,
        "report_period": date(2025, 6, 30),
        "announcement_date": date(2025, 8, 2),
        "actual_announcement_date": date(2025, 8, 2),
        "cumulative_deducted_profit": Decimal("123.45"),
        "quarterly_deducted_profit": Decimal("23.45"),
        "update_flag": "1",
    }
    record = DeductedProfitRecord(
        **values,
        revision_key=deducted_profit_revision_key(**values),
        source_code="tushare",
    )
    run = _running_run(DatasetCode.DEDUCTED_PROFIT, ProviderCode.TUSHARE)
    persistence.create_ingestion_run(run)
    completed = _completed_run(run, row_count=1)
    envelope = IngestionEnvelope(run.ingestion_id, record)
    persistence.commit_deducted_profit_batch(
        completed,
        _manifest(run.ingestion_id, "profit", provider="tushare"),
        [envelope],
    )

    with database_engine.connect() as connection:
        past = connection.execute(
            text("select * from api_v1.query_deducted_profits_as_of(:as_of, null, 10)"),
            {"as_of": date(2025, 8, 2)},
        ).all()
        current = (
            connection.execute(
                text("select * from api_v1.query_deducted_profits_as_of(:as_of, null, 10)"),
                {"as_of": date.today()},
            )
            .mappings()
            .all()
        )
        count = connection.execute(text("select count(*) from core.deducted_profit")).scalar_one()

    assert past == []
    assert count == 1
    assert current[0]["cumulative_deducted_profit_positive"] is True


def test_replay_lineage_and_source_manifest_are_queryable(database_engine: Engine) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    original = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(original)
    manifest = _manifest(original.ingestion_id, "security")
    persistence.commit_security_batch(
        _completed_run(original),
        manifest,
        _envelopes(original.ingestion_id, [_security()]),
    )
    replay = replace(
        _running_run(DatasetCode.SECURITY),
        replayed_from_raw_id=manifest.raw_id,
        request_params={"replay_source_ingestion_id": str(original.ingestion_id)},
    )

    persistence.create_ingestion_run(replay)
    source = persistence.replay_source(original.ingestion_id)

    assert source.source_ingestion_id == original.ingestion_id
    assert source.manifest == manifest
    with database_engine.connect() as connection:
        replayed_from = connection.execute(
            text("""
                select replayed_from_raw_id
                from ingestion.ingestion_run
                where ingestion_id = :ingestion_id
            """),
            {"ingestion_id": replay.ingestion_id},
        ).scalar_one()
    assert replayed_from == manifest.raw_id


def test_capital_facts_are_idempotent_revision_aware_and_exposed_by_views(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    security_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(security_run)
    persistence.commit_security_batch(
        _completed_run(security_run),
        _manifest(security_run.ingestion_id, "security"),
        _envelopes(security_run.ingestion_id, [_security()]),
    )
    capital_run = _running_run(DatasetCode.CAPITAL)
    persistence.create_ingestion_run(capital_run)
    records: list[CapitalRecord] = [
        ShareCapitalRecord(
            symbol=SYMBOL,
            effective_date=date(2024, 1, 15),
            total_shares=1_000_000,
            restricted_shares=100_000,
            circulating_shares=900_000,
            listed_a_shares=900_000,
            change_reason="initial",
            source_code="akshare",
        ),
        DistributionRecord(
            symbol=SYMBOL,
            report_period=date(2023, 12, 31),
            announcement_date=date(2024, 5, 31),
            record_date=date(2024, 6, 5),
            ex_date=date(2024, 6, 6),
            cash_dividend_per_share=Decimal("0.35"),
            bonus_share_ratio=Decimal("0.1"),
            transfer_share_ratio=Decimal("0.2"),
            status=CorporateActionStatus.IMPLEMENTED,
            source_code="akshare",
        ),
        RightsIssueRecord(
            symbol=SYMBOL,
            record_date=date(2020, 1, 9),
            announcement_date=date(2020, 1, 2),
            ex_date=date(2020, 1, 10),
            payment_start_date=date(2020, 1, 10),
            payment_end_date=date(2020, 1, 16),
            listing_date=date(2020, 2, 1),
            rights_ratio=Decimal("0.25"),
            rights_price=Decimal("8.5"),
            base_shares=1_000_000,
            proceeds=Decimal("2125000"),
            source_code="akshare",
        ),
    ]
    persistence.commit_capital_batch(
        _completed_run(capital_run, row_count=3),
        _manifest(capital_run.ingestion_id, "capital"),
        _envelopes(capital_run.ingestion_id, records),
        [],
    )
    revision_run = _running_run(DatasetCode.CAPITAL)
    persistence.create_ingestion_run(revision_run)
    revised_share_capital = replace(
        cast(ShareCapitalRecord, records[0]),
        total_shares=1_100_000,
        change_reason="revised",
    )
    persistence.commit_capital_batch(
        _completed_run(revision_run),
        _manifest(revision_run.ingestion_id, "capital-revision"),
        _envelopes(revision_run.ingestion_id, [revised_share_capital]),
        [],
    )

    with database_engine.connect() as connection:
        share_row = connection.execute(
            text(
                "select total_shares, change_reason, ingestion_id "
                "from capital.share_capital where symbol = :symbol"
            ),
            {"symbol": SYMBOL},
        ).one()
        counts = connection.execute(
            text(
                "select "
                "(select count(*) from api_v1.share_capital), "
                "(select count(*) from api_v1.distributions), "
                "(select count(*) from api_v1.rights_issues)"
            )
        ).one()

    assert tuple(share_row) == (1_100_000, "revised", revision_run.ingestion_id)
    assert tuple(counts) == (1, 1, 1)


def test_classification_snapshots_are_versioned_idempotent_and_interval_safe(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    security_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(security_run)
    persistence.commit_security_batch(
        _completed_run(security_run),
        _manifest(security_run.ingestion_id, "security"),
        _envelopes(security_run.ingestion_id, [_security()]),
    )
    snapshot_date = date(2026, 7, 29)
    catalog_record = ClassificationCatalogSnapshotRecord(
        namespace="eastmoney",
        classification_type=ClassificationType.INDUSTRY,
        snapshot_date=snapshot_date,
        definitions=(ClassificationDefinition("BK0475", "银行"),),
        source_code="akshare",
    )
    catalog_run = _running_run(DatasetCode.CLASSIFICATION_CATALOG)
    persistence.create_ingestion_run(catalog_run)
    persistence.commit_classification_catalog_batch(
        _completed_run(catalog_run),
        _manifest(catalog_run.ingestion_id, "classification-catalog"),
        IngestionEnvelope(catalog_run.ingestion_id, catalog_record),
        [],
    )
    key = ("eastmoney", ClassificationType.INDUSTRY, "BK0475", snapshot_date)
    assert persistence.known_classification_snapshots({key}) == {key}

    member_record = ClassificationMemberSnapshotRecord(
        namespace="eastmoney",
        classification_type=ClassificationType.INDUSTRY,
        classification_code="BK0475",
        snapshot_date=snapshot_date,
        members=(SYMBOL,),
        source_code="akshare",
    )
    member_run = _running_run(DatasetCode.CLASSIFICATION_MEMBERS)
    persistence.create_ingestion_run(member_run)
    persistence.commit_classification_members_batch(
        _completed_run(member_run),
        _manifest(member_run.ingestion_id, "classification-members"),
        IngestionEnvelope(member_run.ingestion_id, member_record),
        [],
    )
    catalog_replay_run = _running_run(DatasetCode.CLASSIFICATION_CATALOG)
    persistence.create_ingestion_run(catalog_replay_run)
    persistence.commit_classification_catalog_batch(
        _completed_run(catalog_replay_run),
        _manifest(catalog_replay_run.ingestion_id, "classification-catalog-replay"),
        IngestionEnvelope(catalog_replay_run.ingestion_id, catalog_record),
        [],
    )
    with database_engine.connect() as connection:
        assert (
            connection.execute(
                text("select count(*) from api_v1.classification_member_snapshots")
            ).scalar_one()
            == 1
        )
    revision_run = _running_run(DatasetCode.CLASSIFICATION_MEMBERS)
    persistence.create_ingestion_run(revision_run)
    persistence.commit_classification_members_batch(
        _completed_run(revision_run, row_count=0),
        _manifest(revision_run.ingestion_id, "classification-members-revision", row_count=0),
        IngestionEnvelope(revision_run.ingestion_id, replace(member_record, members=())),
        [],
    )

    with database_engine.connect() as connection:
        counts = connection.execute(
            text(
                "select "
                "(select count(*) from api_v1.classification_catalog_snapshots), "
                "(select count(*) from classification.member_snapshot), "
                "(select count(*) from api_v1.classification_member_snapshots)"
            )
        ).one()
    assert tuple(counts) == (1, 1, 0)

    interval_params = {
        "namespace": "official",
        "classification_type": "industry",
        "classification_code": "TEST",
        "symbol": SYMBOL,
        "valid_from": date(2024, 1, 1),
        "valid_to": date(2024, 12, 31),
        "source_code": "akshare",
        "ingestion_id": revision_run.ingestion_id,
    }
    interval_insert = text("""
insert into classification.member_interval (
    namespace, classification_type, classification_code, symbol,
    valid_from, valid_to, source_code, ingestion_id
) values (
    :namespace, :classification_type, :classification_code, :symbol,
    :valid_from, :valid_to, :source_code, :ingestion_id
)
""")
    with database_engine.begin() as connection:
        connection.execute(interval_insert, interval_params)
    with pytest.raises(IntegrityError), database_engine.begin() as connection:
        connection.execute(
            interval_insert,
            {
                **interval_params,
                "valid_from": date(2024, 6, 1),
                "valid_to": None,
            },
        )


def test_board_index_facts_are_idempotent_and_queryable(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    security_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(security_run)
    persistence.commit_security_batch(
        _completed_run(security_run),
        _manifest(security_run.ingestion_id, "board-security"),
        _envelopes(security_run.ingestion_id, [_security()]),
    )
    calendar_run = _running_run(DatasetCode.TRADING_CALENDAR)
    persistence.create_ingestion_run(calendar_run)
    persistence.commit_trading_calendar_batch(
        _completed_run(calendar_run),
        _manifest(calendar_run.ingestion_id, "board-calendar"),
        _envelopes(
            calendar_run.ingestion_id,
            [
                CalculatedTradingDay(
                    market=Market.CN_A_SHARE,
                    trade_date=TRADE_DATE,
                    is_trading_day=True,
                    previous_trading_day=None,
                    next_trading_day=None,
                    source_code="baostock",
                )
            ],
        ),
    )

    board = BoardIndexRecord(
        board_id="THS:883423",
        board_code="883423",
        namespace="THS",
        name="沪深主板昨日涨停",
        board_type=BoardIndexType.DYNAMIC_THEME,
        market=Market.CN_A_SHARE,
        status=BoardIndexStatus.ACTIVE,
        source_code="akshare_ths",
    )
    board_run = _running_run(DatasetCode.BOARD_INDEX, ProviderCode.AKSHARE_THS)
    persistence.create_ingestion_run(board_run)
    persistence.commit_board_index_batch(
        _completed_run(board_run),
        _manifest(board_run.ingestion_id, "board-index", provider="akshare_ths"),
        _envelopes(board_run.ingestion_id, [board]),
    )

    bar = BoardIndexDailyBarRecord(
        board_id=board.board_id,
        trade_date=TRADE_DATE,
        market=Market.CN_A_SHARE,
        open=Decimal("225.229"),
        high=Decimal("225.772"),
        low=Decimal("220.542"),
        close=Decimal("223.554"),
        volume=7_365_903_100,
        amount=Decimal("147481890000"),
        source_code="akshare_ths",
    )
    first_bar_run = _running_run(DatasetCode.BOARD_INDEX_DAILY_BAR, ProviderCode.AKSHARE_THS)
    persistence.create_ingestion_run(first_bar_run)
    persistence.commit_board_index_daily_bar_batch(
        _completed_run(first_bar_run),
        _manifest(
            first_bar_run.ingestion_id,
            "board-daily-bar",
            provider="akshare_ths",
        ),
        _envelopes(first_bar_run.ingestion_id, [bar]),
        [],
    )
    revised_bar_run = _running_run(DatasetCode.BOARD_INDEX_DAILY_BAR, ProviderCode.AKSHARE_THS)
    persistence.create_ingestion_run(revised_bar_run)
    persistence.commit_board_index_daily_bar_batch(
        _completed_run(revised_bar_run),
        _manifest(
            revised_bar_run.ingestion_id,
            "board-daily-bar-revision",
            provider="akshare_ths",
        ),
        _envelopes(
            revised_bar_run.ingestion_id,
            [replace(bar, close=Decimal("223.600"))],
        ),
        [],
    )

    snapshot = BoardIndexConstituentSnapshotRecord(
        board_id=board.board_id,
        trade_date=TRADE_DATE,
        members=(SYMBOL,),
        source_code="akshare_ths",
    )
    for dataset in ("board-members", "board-members-revision"):
        member_run = _running_run(
            DatasetCode.BOARD_INDEX_CONSTITUENT_SNAPSHOT,
            ProviderCode.AKSHARE_THS,
        )
        persistence.create_ingestion_run(member_run)
        persistence.commit_board_index_constituents_batch(
            _completed_run(member_run),
            _manifest(
                member_run.ingestion_id,
                dataset,
                provider="akshare_ths",
            ),
            IngestionEnvelope(member_run.ingestion_id, snapshot),
            [],
        )

    with database_engine.connect() as connection:
        result = connection.execute(
            text(
                "select "
                "(select count(*) from api_v1.board_indexes), "
                "(select count(*) from api_v1.board_index_daily_bars), "
                "(select count(*) from api_v1.board_index_constituents), "
                "(select close from api_v1.board_index_daily_bars "
                " where board_id = 'THS:883423')"
            )
        ).one()

    assert tuple(result[:3]) == (1, 1, 1)
    assert result[3] == Decimal("223.6000")


def test_board_index_bias_rpc_uses_latest_30_sessions_and_decimal_math(
    database_engine: Engine,
) -> None:
    start_date = date(2026, 6, 1)
    closes = [Decimal(index) for index in range(1, 36)]
    _commit_board_bias_bars(database_engine, start_date, closes)

    with database_engine.connect() as connection:
        payload = connection.execute(
            text("select api_v1.query_board_index_bias_latest() as payload")
        ).scalar_one()
        privileges = connection.execute(
            text(
                "select "
                "has_function_privilege('market_data_api', "
                "'api_v1.query_board_index_bias_latest()', 'EXECUTE'), "
                "has_function_privilege('anon', "
                "'api_v1.query_board_index_bias_latest()', 'EXECUTE'), "
                "has_function_privilege('authenticated', "
                "'api_v1.query_board_index_bias_latest()', 'EXECUTE')"
            )
        ).one()

    fetched_at = payload.pop("fetched_at")
    assert datetime.fromisoformat(fetched_at).tzinfo is not None
    assert payload == {
        "board_id": "THS:883423",
        "board_code": "883423",
        "board_name": "沪深主板昨日涨停",
        "trade_date": "2026-07-05",
        "close": "35.0000",
        "moving_average_5": "33.000000",
        "bias_5_pct": "6.060606",
        "previous_trade_date": "2026-07-04",
        "previous_bias_5_pct": "6.250000",
        "bias_direction": "down",
        "window_trading_days": 30,
        "bias_sample_count": 30,
        "highest_bias_5_pct": "50.000000",
        "highest_bias_trade_date": "2026-06-06",
        "lowest_bias_5_pct": "6.060606",
        "lowest_bias_trade_date": "2026-07-05",
        "algorithm_version": "board_index_bias_v1",
        "data_origin": "database",
        "persistence_status": "persisted",
    }
    assert tuple(privileges) == (True, False, False)


def test_board_index_bias_rpc_requests_fallback_with_fewer_than_34_bars(
    database_engine: Engine,
) -> None:
    _commit_board_bias_bars(
        database_engine,
        date(2026, 8, 1),
        [Decimal("10"), Decimal("11"), Decimal("12"), Decimal("13")],
    )

    with pytest.raises(DBAPIError) as captured, database_engine.connect() as connection:
        connection.execute(text("select api_v1.query_board_index_bias_latest()"))

    assert captured.value.orig.sqlstate == "P0002"


def test_board_index_bias_rpc_returns_latest_stored_date_when_current_date_is_missing(
    database_engine: Engine,
) -> None:
    start_date = date(2026, 6, 1)
    _commit_board_bias_bars(
        database_engine,
        start_date,
        [Decimal(index) for index in range(1, 36)],
    )
    with database_engine.begin() as connection:
        connection.execute(
            text(
                "delete from core.board_index_daily_bar "
                "where board_id='THS:883423' and trade_date=:trade_date"
            ),
            {"trade_date": start_date + timedelta(days=34)},
        )

    with database_engine.connect() as connection:
        payload = connection.execute(
            text("select api_v1.query_board_index_bias_latest()")
        ).scalar_one()

    assert payload["trade_date"] == (start_date + timedelta(days=33)).isoformat()


def test_board_index_bias_rpc_fails_when_no_board_bars_exist(database_engine: Engine) -> None:
    with pytest.raises(DBAPIError) as captured, database_engine.connect() as connection:
        connection.execute(text("select api_v1.query_board_index_bias_latest()"))

    assert captured.value.orig.sqlstate == "P0002"


def test_top_gainers_accepts_pytdx_unknown_bars_and_excludes_suspended(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    securities = [
        _security(),
        replace(
            _security(),
            symbol="SZSE:000001",
            code="000001",
            exchange=Exchange.SZSE,
            name="平安银行",
        ),
    ]
    security_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(security_run)
    persistence.commit_security_batch(
        _completed_run(security_run, len(securities)),
        _manifest(security_run.ingestion_id, "top-gainers-securities", len(securities)),
        _envelopes(security_run.ingestion_id, securities),
    )

    trading_days: list[date] = []
    candidate_date = date(2026, 7, 20)
    while len(trading_days) < 20:
        if candidate_date.weekday() < 5:
            trading_days.append(candidate_date)
        candidate_date += timedelta(days=1)
    calendar_run = _running_run(DatasetCode.TRADING_CALENDAR)
    persistence.create_ingestion_run(calendar_run)
    calendar = [
        CalculatedTradingDay(
            market=Market.CN_A_SHARE,
            trade_date=trade_date,
            is_trading_day=True,
            previous_trading_day=trading_days[index - 1] if index else None,
            next_trading_day=(trading_days[index + 1] if index + 1 < len(trading_days) else None),
            source_code="baostock",
        )
        for index, trade_date in enumerate(trading_days)
    ]
    persistence.commit_trading_calendar_batch(
        _completed_run(calendar_run, len(calendar)),
        _manifest(calendar_run.ingestion_id, "top-gainers-calendar", len(calendar)),
        _envelopes(calendar_run.ingestion_id, calendar),
    )

    start_date, end_date = trading_days[0], trading_days[-1]
    unknown_bars = [
        replace(
            _daily_bar(start_date),
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            trade_status=TradeStatus.UNKNOWN,
            source_code="pytdx",
        ),
        replace(
            _daily_bar(end_date),
            open=Decimal("12"),
            high=Decimal("12"),
            low=Decimal("12"),
            close=Decimal("12"),
            trade_status=TradeStatus.UNKNOWN,
            source_code="pytdx",
        ),
    ]
    suspended_bars = [
        replace(bar, symbol="SZSE:000001", trade_status=TradeStatus.SUSPENDED)
        for bar in unknown_bars
    ]
    bars = [*unknown_bars, *suspended_bars]
    bar_run = _running_run(DatasetCode.DAILY_BAR, ProviderCode.PYTDX)
    persistence.create_ingestion_run(bar_run)
    persistence.commit_daily_bar_batch(
        _completed_run(bar_run, len(bars)),
        _manifest(
            bar_run.ingestion_id,
            "top-gainers-bars",
            len(bars),
            provider="pytdx",
        ),
        _envelopes(bar_run.ingestion_id, bars),
        [],
    )

    with database_engine.connect() as connection:
        payload = connection.scalar(
            text("select api_v1.query_top_gainers_20d(:end_date, 10)"),
            {"end_date": end_date},
        )

    assert payload["eligible_count"] == 1
    assert payload["omissions"]["non_trading_bar"] == 1
    assert [item["symbol"] for item in payload["items"]] == [SYMBOL]


def test_close_price_new_highs_rpc_requires_complete_history_and_strict_breakout(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    securities = [
        _security(),
        replace(
            _security(),
            symbol="SZSE:000001",
            code="000001",
            exchange=Exchange.SZSE,
            name="平安银行",
        ),
        replace(
            _security(),
            symbol="SSE:600001",
            code="600001",
            name="邯郸钢铁",
        ),
    ]
    security_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(security_run)
    persistence.commit_security_batch(
        _completed_run(security_run, len(securities)),
        _manifest(security_run.ingestion_id, "new-high-securities", len(securities)),
        _envelopes(security_run.ingestion_id, securities),
    )

    trading_days: list[date] = []
    candidate_date = date(2026, 1, 1)
    while len(trading_days) < 120:
        if candidate_date.weekday() < 5:
            trading_days.append(candidate_date)
        candidate_date += timedelta(days=1)
    calendar_run = _running_run(DatasetCode.TRADING_CALENDAR)
    persistence.create_ingestion_run(calendar_run)
    calendar = [
        CalculatedTradingDay(
            market=Market.CN_A_SHARE,
            trade_date=trade_date,
            is_trading_day=True,
            previous_trading_day=trading_days[index - 1] if index else None,
            next_trading_day=(trading_days[index + 1] if index + 1 < len(trading_days) else None),
            source_code="baostock",
        )
        for index, trade_date in enumerate(trading_days)
    ]
    persistence.commit_trading_calendar_batch(
        _completed_run(calendar_run, len(calendar)),
        _manifest(calendar_run.ingestion_id, "new-high-calendar", len(calendar)),
        _envelopes(calendar_run.ingestion_id, calendar),
    )

    bars: list[DailyBarRecord] = []
    for index, trade_date in enumerate(trading_days):
        breakout_close = Decimal("11") if index == 119 else Decimal("10")
        equal_high_close = Decimal("11") if index >= 118 else Decimal("10")
        for symbol, close, status in (
            (SYMBOL, breakout_close, TradeStatus.UNKNOWN),
            ("SZSE:000001", equal_high_close, TradeStatus.UNKNOWN),
            (
                "SSE:600001",
                Decimal("12") if index == 119 else Decimal("10"),
                TradeStatus.SUSPENDED if index == 50 else TradeStatus.UNKNOWN,
            ),
        ):
            bars.append(
                replace(
                    _daily_bar(trade_date),
                    symbol=symbol,
                    open=close,
                    high=close,
                    low=close,
                    close=close,
                    trade_status=status,
                    source_code="pytdx",
                )
            )

    bar_run = _running_run(DatasetCode.DAILY_BAR, ProviderCode.PYTDX)
    persistence.create_ingestion_run(bar_run)
    persistence.commit_daily_bar_batch(
        _completed_run(bar_run, len(bars)),
        _manifest(
            bar_run.ingestion_id,
            "new-high-bars",
            len(bars),
            provider="pytdx",
        ),
        _envelopes(bar_run.ingestion_id, bars),
        [],
    )

    operations = PostgreSQLOperationsPersistence(database_engine)
    scheduled_for = datetime.combine(trading_days[-1], time(12), tzinfo=UTC)
    workflow = operations.start_workflow(
        WorkflowCode.DAILY_MARKET, scheduled_for, TriggerSource.SCHEDULED
    )
    operations.finish_workflow(
        workflow.finish(ExecutionStatus.SUCCEEDED, scheduled_for + timedelta(minutes=1))
    )
    first = ClosePriceNewHighsService(
        PostgreSQLClosePriceNewHighsPersistence(database_engine)
    ).build(trading_days[-1])
    unchanged = ClosePriceNewHighsService(
        PostgreSQLClosePriceNewHighsPersistence(database_engine)
    ).build(trading_days[-1])

    with database_engine.connect() as connection:
        payload = connection.scalar(text("select api_v1.query_close_price_new_highs_120d()"))

    assert first.status == "succeeded"
    assert unchanged.status == "unchanged"
    assert unchanged.snapshot_id == first.snapshot_id
    assert payload["trade_date"] == trading_days[-1].isoformat()
    assert payload["total_candidate_count"] == 3
    assert payload["eligible_history_count"] == 2
    assert payload["omitted_count"] == 1
    assert payload["returned_count"] == 1
    assert payload["omissions"]["incomplete_history"] == 1
    assert payload["omissions"]["non_trading_bar"] == 1
    assert payload["items"] == [
        {
            "symbol": SYMBOL,
            "code": "600000",
            "name": "浦发银行",
            "close": 11.0,
            "previous_119d_high": 10.0,
            "breakout_pct": 10.0,
        }
    ]

    revised_close = Decimal("12")
    revised_bar = replace(
        _daily_bar(trading_days[-1]),
        symbol=SYMBOL,
        open=revised_close,
        high=revised_close,
        low=revised_close,
        close=revised_close,
        trade_status=TradeStatus.UNKNOWN,
        source_code="pytdx",
    )
    revision_run = _running_run(DatasetCode.DAILY_BAR, ProviderCode.PYTDX)
    persistence.create_ingestion_run(revision_run)
    persistence.commit_daily_bar_batch(
        _completed_run(revision_run, 1),
        _manifest(
            revision_run.ingestion_id,
            "new-high-bars-revision",
            1,
            provider="pytdx",
        ),
        _envelopes(revision_run.ingestion_id, [revised_bar]),
        [],
    )
    revised = ClosePriceNewHighsService(
        PostgreSQLClosePriceNewHighsPersistence(database_engine)
    ).build(trading_days[-1])
    with database_engine.connect() as connection:
        revised_payload = connection.scalar(
            text("select api_v1.query_close_price_new_highs_120d()")
        )
        selected_version = connection.scalar(
            text("""
select version
from derived.close_price_new_high_120d_snapshot
where snapshot_id=:snapshot_id
"""),
            {"snapshot_id": revised.snapshot_id},
        )

    assert revised.status == "succeeded"
    assert revised.snapshot_id != first.snapshot_id
    assert selected_version == 2
    assert revised_payload["items"][0]["close"] == 12.0
    assert revised_payload["items"][0]["breakout_pct"] == 20.0


def test_live_board_index_persistence_rpc_is_removed(
    database_engine: Engine,
) -> None:
    with database_engine.connect() as connection:
        function_oid = connection.scalar(
            text(
                "select to_regprocedure("
                "'api_v1.persist_board_index_daily_bars_live("
                "uuid,uuid,timestamptz,text,text,text,bigint,integer,jsonb,jsonb)')"
            )
        )

    assert function_oid is None


def test_derived_calculation_is_versioned_idempotent_and_revision_aware(
    database_engine: Engine,
) -> None:
    _prepare_api_data(database_engine)
    persistence = PostgreSQLPersistence(database_engine)
    capital_run = _running_run(DatasetCode.CAPITAL)
    persistence.create_ingestion_run(capital_run)
    persistence.commit_capital_batch(
        _completed_run(capital_run),
        _manifest(capital_run.ingestion_id, "derived-capital"),
        _envelopes(
            capital_run.ingestion_id,
            [
                ShareCapitalRecord(
                    symbol=SYMBOL,
                    effective_date=date(2024, 1, 1),
                    total_shares=1_000_000,
                    restricted_shares=None,
                    circulating_shares=900_000,
                    listed_a_shares=900_000,
                    change_reason="derived fixture",
                    source_code="akshare",
                )
            ],
        ),
        [],
    )
    snapshot_date = date(2026, 7, 27)
    catalog_run = _running_run(DatasetCode.CLASSIFICATION_CATALOG)
    persistence.create_ingestion_run(catalog_run)
    persistence.commit_classification_catalog_batch(
        _completed_run(catalog_run),
        _manifest(catalog_run.ingestion_id, "derived-catalog"),
        IngestionEnvelope(
            catalog_run.ingestion_id,
            ClassificationCatalogSnapshotRecord(
                namespace="eastmoney",
                classification_type=ClassificationType.INDUSTRY,
                snapshot_date=snapshot_date,
                definitions=(ClassificationDefinition("BK0475", "银行"),),
                source_code="akshare",
            ),
        ),
        [],
    )
    member_run = _running_run(DatasetCode.CLASSIFICATION_MEMBERS)
    persistence.create_ingestion_run(member_run)
    persistence.commit_classification_members_batch(
        _completed_run(member_run),
        _manifest(member_run.ingestion_id, "derived-members"),
        IngestionEnvelope(
            member_run.ingestion_id,
            ClassificationMemberSnapshotRecord(
                namespace="eastmoney",
                classification_type=ClassificationType.INDUSTRY,
                classification_code="BK0475",
                snapshot_date=snapshot_date,
                members=(SYMBOL,),
                source_code="akshare",
            ),
        ),
        [],
    )

    service = DerivationService(PostgreSQLDerivedPersistence(database_engine))
    first = service.recompute(snapshot_date, date(2026, 7, 29))
    unchanged = service.recompute(snapshot_date, date(2026, 7, 29))

    assert first.status == "succeeded"
    assert first.output_rows == 15
    assert unchanged.status == "unchanged"
    assert unchanged.calculation_id == first.calculation_id

    revision_run = _running_run(DatasetCode.DAILY_BAR)
    persistence.create_ingestion_run(revision_run)
    revised_bar = replace(
        _daily_bar(date(2026, 7, 28)),
        close=Decimal("10.60"),
    )
    persistence.commit_daily_bar_batch(
        _completed_run(revision_run),
        _manifest(revision_run.ingestion_id, "derived-daily-revision"),
        _envelopes(revision_run.ingestion_id, [revised_bar]),
        [],
    )
    revised = service.recompute(snapshot_date, date(2026, 7, 29))

    assert revised.status == "succeeded"
    assert revised.calculation_id != first.calculation_id
    assert revised.input_hash != first.input_hash
    with database_engine.connect() as connection:
        counts = connection.execute(
            text(
                "select "
                "(select count(*) from api_v1.calculation_runs where status = 'succeeded'), "
                "(select count(*) from api_v1.adjusted_daily_bars), "
                "(select count(*) from api_v1.daily_metrics), "
                "(select count(*) from api_v1.market_capitalizations), "
                "(select count(*) from api_v1.classification_daily_metrics)"
            )
        ).one()
    assert tuple(counts) == (2, 12, 6, 6, 6)


def test_postgrest_query_contracts_are_bounded_version_coherent_and_as_of(
    database_engine: Engine,
) -> None:
    calculation_id = _prepare_query_contract_data(database_engine)

    with database_engine.connect() as connection:
        securities = (
            connection.execute(
                text("select symbol from api_v1.query_securities(:query, :limit)"),
                {"query": "600000", "limit": 10},
            )
            .scalars()
            .all()
        )
        raw_dates = (
            connection.execute(
                text("""
                select trade_date
                from api_v1.query_daily_bars(:symbol, :start_date, :end_date, :limit)
            """),
                {
                    "symbol": SYMBOL,
                    "start_date": date(2026, 7, 27),
                    "end_date": date(2026, 7, 29),
                    "limit": 2,
                },
            )
            .scalars()
            .all()
        )
        recent_payload = connection.execute(
            text("select api_v1.query_recent_daily_bars(:code, :trade_date, :limit)"),
            {"code": "600000", "trade_date": date(2026, 7, 29), "limit": 2},
        ).scalar_one()
        adjusted = connection.execute(
            text("""
                select trade_date, adjustment_type, calculation_id
                from api_v1.query_adjusted_daily_bars(
                    :symbol, :start_date, :end_date, 'forward', '1.1.0', null, 100
                )
            """),
            {
                "symbol": SYMBOL,
                "start_date": date(2026, 7, 27),
                "end_date": date(2026, 7, 29),
            },
        ).all()
        snapshot = connection.execute(
            text("""
                select symbol, calculation_id
                from api_v1.query_market_snapshot(:trade_date, '1.1.0', null, 100)
            """),
            {"trade_date": date(2026, 7, 28)},
        ).one()
        classification = connection.execute(
            text("""
                select snapshot_date, member_count, returned_count, members
                from api_v1.query_classification_members_as_of(
                    'eastmoney', 'industry', 'BK0475', :as_of_date, 100
                )
            """),
            {"as_of_date": date(2026, 7, 29)},
        ).one()

    assert securities == [SYMBOL]
    assert raw_dates == [date(2026, 7, 27), date(2026, 7, 28)]
    assert recent_payload["code"] == "600000"
    assert recent_payload["symbol"] == SYMBOL
    assert recent_payload["trade_date"] == "2026-07-29"
    assert recent_payload["limit"] == 2
    assert recent_payload["count"] == 2
    assert [item["trade_date"] for item in recent_payload["items"]] == [
        "2026-07-29",
        "2026-07-28",
    ]
    assert [tuple(row) for row in adjusted] == [
        (date(2026, 7, 27), "forward", calculation_id),
        (date(2026, 7, 28), "forward", calculation_id),
        (date(2026, 7, 29), "forward", calculation_id),
    ]
    assert tuple(snapshot) == (SYMBOL, calculation_id)
    assert tuple(classification) == (date(2026, 7, 27), 1, 1, [SYMBOL])

    with database_engine.connect() as connection, pytest.raises(DBAPIError) as error:
        connection.execute(
            text("select * from api_v1.query_daily_bars(:symbol, :start, :end, 5001)"),
            {"symbol": SYMBOL, "start": date(2026, 7, 27), "end": date(2026, 7, 29)},
        ).all()
    assert getattr(error.value.orig, "sqlstate", None) == "22023"


@pytest.mark.parametrize("client_role", ["anon", "authenticated"])
def test_client_roles_can_execute_bounded_query_contracts(
    migrated_database_url: str, database_engine: Engine, client_role: str
) -> None:
    _prepare_api_data(database_engine)

    with psycopg.connect(migrated_database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("set role {}").format(sql.Identifier(client_role)))
        rows = connection.execute(
            "select symbol from api_v1.query_securities(%s, %s)",
            ("600000", 10),
        ).fetchall()
        assert rows == [(SYMBOL,)]
        with pytest.raises(InsufficientPrivilege):
            connection.execute("select count(*) from core.security")


def test_stale_running_ingestions_are_recovered_atomically(database_engine: Engine) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    stale_time = NOW - timedelta(hours=2)
    stale = replace(
        _running_run(DatasetCode.SECURITY),
        requested_at=stale_time,
        started_at=stale_time,
    )
    recent = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(stale)
    persistence.create_ingestion_run(recent)

    candidates = persistence.stale_ingestion_run_ids(NOW - timedelta(hours=1))
    recovered = persistence.recover_stale_ingestion_runs(
        NOW - timedelta(hours=1), NOW, "StaleRunRecovery: integration test"
    )

    assert candidates == [stale.ingestion_id]
    assert recovered == [stale.ingestion_id]
    with database_engine.connect() as connection:
        rows = connection.execute(
            text("""
                    select ingestion_id, status
                    from ingestion.ingestion_run
                    where ingestion_id in (:stale_id, :recent_id)
                """),
            {"stale_id": stale.ingestion_id, "recent_id": recent.ingestion_id},
        ).all()
        states: dict[UUID, str] = {cast(UUID, row[0]): cast(str, row[1]) for row in rows}
    assert states == {stale.ingestion_id: "failed", recent.ingestion_id: "running"}


def test_daily_bar_comparison_sources_match_standard_symbol_and_range(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    _commit_security_prerequisite(persistence)
    _commit_calendar_prerequisite(persistence)
    running = replace(
        _running_run(DatasetCode.DAILY_BAR),
        request_params={
            "source_symbol": "sh.600000",
            "start_date": "2026-07-01",
            "end_date": "2026-07-28",
        },
    )
    persistence.create_ingestion_run(running)
    manifest = _manifest(running.ingestion_id, "daily_bar")
    persistence.commit_daily_bar_batch(
        _completed_run(running),
        manifest,
        _envelopes(running.ingestion_id, [_daily_bar()]),
        [],
    )

    sources = persistence.daily_bar_replay_sources(SYMBOL, date(2026, 7, 28), date(2026, 7, 28))

    assert len(sources) == 1
    assert sources[0].manifest == manifest


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


def test_unknown_security_lifecycle_does_not_replace_known_values(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    known_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(known_run)
    persistence.commit_security_batch(
        _completed_run(known_run),
        _manifest(known_run.ingestion_id, "security-known"),
        _envelopes(known_run.ingestion_id, [_security()]),
    )

    unknown_run = _running_run(DatasetCode.SECURITY, ProviderCode.AKSHARE)
    persistence.create_ingestion_run(unknown_run)
    unknown = replace(
        _security(),
        status=SecurityStatus.UNKNOWN,
        ipo_date=None,
        delisting_date=None,
        source_code="akshare",
    )
    persistence.commit_security_batch(
        _completed_run(unknown_run),
        _manifest(unknown_run.ingestion_id, "security-unknown"),
        _envelopes(unknown_run.ingestion_id, [unknown]),
    )

    with database_engine.connect() as connection:
        row = connection.execute(
            text(
                "select status, ipo_date, delisting_date from core.security where symbol = :symbol"
            ),
            {"symbol": SYMBOL},
        ).one()
    assert tuple(row) == ("listed", date(1999, 11, 10), None)


def test_older_security_replay_preserves_current_fact_and_name_intervals(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    original_time = NOW - timedelta(days=1)
    original = replace(
        _running_run(DatasetCode.SECURITY),
        requested_at=original_time,
        started_at=original_time,
    )
    persistence.create_ingestion_run(original)
    manifest = _manifest(original.ingestion_id, "security-original")
    original_security = replace(_security(), name="浦发银行旧名")
    persistence.commit_security_batch(
        _completed_run(original),
        manifest,
        _envelopes(original.ingestion_id, [original_security]),
    )

    current = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(current)
    current_security = replace(_security(), name="浦发银行新名")
    persistence.commit_security_batch(
        _completed_run(current),
        _manifest(current.ingestion_id, "security-current"),
        _envelopes(current.ingestion_id, [current_security]),
    )

    replay_time = NOW + timedelta(days=1)
    replay = replace(
        _running_run(DatasetCode.SECURITY),
        requested_at=replay_time,
        started_at=replay_time,
        request_params={
            "replay_source_ingestion_id": str(original.ingestion_id),
            "replay_source_requested_at": original_time.isoformat(),
        },
        replayed_from_raw_id=manifest.raw_id,
    )
    persistence.create_ingestion_run(replay)
    completed_replay = replace(
        replay,
        status=IngestionStatus.SUCCEEDED,
        finished_at=replay_time,
        fetched_rows=1,
        accepted_rows=1,
    )
    persistence.commit_security_batch(
        completed_replay,
        None,
        _envelopes(replay.ingestion_id, [original_security]),
    )

    with database_engine.connect() as connection:
        current_name = connection.execute(
            text("select current_name from core.security where symbol = :symbol"),
            {"symbol": SYMBOL},
        ).scalar_one()
        history = connection.execute(
            text("""
                select name, effective_from, effective_to
                from core.security_name_history
                where symbol = :symbol
                order by effective_from
            """),
            {"symbol": SYMBOL},
        ).fetchall()

    assert current_name == "浦发银行新名"
    assert [tuple(row) for row in history] == [
        ("浦发银行旧名", original_time.date(), original_time.date()),
        ("浦发银行新名", NOW.date(), None),
    ]


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


def test_multi_run_daily_bar_commit_preserves_lineage_and_is_atomic(
    database_engine: Engine,
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    _commit_security_prerequisite(persistence)
    _commit_calendar_prerequisite(persistence)
    security_run = _running_run(DatasetCode.SECURITY)
    persistence.create_ingestion_run(security_run)
    second_security = replace(
        _security(), symbol="SZSE:000001", code="000001", exchange=Exchange.SZSE
    )
    persistence.commit_security_batch(
        _completed_run(security_run),
        _manifest(security_run.ingestion_id, "batch-security"),
        _envelopes(security_run.ingestion_id, [second_security]),
    )
    first_run = _running_run(DatasetCode.DAILY_BAR)
    second_run = _running_run(DatasetCode.DAILY_BAR)
    persistence.create_ingestion_run(first_run)
    persistence.create_ingestion_run(second_run)
    batches = [
        PreparedDailyBarBatch(
            _completed_run(first_run),
            _manifest(first_run.ingestion_id, "daily-bar-first"),
            tuple(_envelopes(first_run.ingestion_id, [_daily_bar()])),
            (),
        ),
        PreparedDailyBarBatch(
            _completed_run(second_run),
            _manifest(second_run.ingestion_id, "daily-bar-second"),
            tuple(
                _envelopes(
                    second_run.ingestion_id,
                    [replace(_daily_bar(), symbol="SZSE:000001")],
                )
            ),
            (),
        ),
    ]

    persistence.commit_daily_bar_batches(batches)

    with database_engine.connect() as connection:
        rows = connection.execute(
            text(
                "select symbol,ingestion_id from core.daily_bar "
                "where trade_date=:trade_date order by symbol"
            ),
            {"trade_date": TRADE_DATE},
        ).all()
    assert rows == [(SYMBOL, first_run.ingestion_id), ("SZSE:000001", second_run.ingestion_id)]


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


def test_stock_daily_indicator_retention_uses_explicit_exclusive_cutoff(
    database_engine: Engine,
) -> None:
    _prepare_api_data(database_engine)
    persistence = PostgreSQLPersistence(database_engine)
    running = _running_run(DatasetCode.STOCK_DAILY_INDICATOR)
    persistence.create_ingestion_run(running)
    with database_engine.begin() as connection:
        connection.execute(
            text("""
                insert into core.stock_daily_indicator (
                    symbol, trade_date, market, price_limit_status, source_code, ingestion_id
                ) values (
                    :symbol, :trade_date, 'CN_A_SHARE', 'unknown', 'tushare', :ingestion_id
                )
            """),
            [
                {
                    "symbol": SYMBOL,
                    "trade_date": trade_date,
                    "ingestion_id": running.ingestion_id,
                }
                for trade_date in (date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29))
            ],
        )

    deleted = persistence.delete_stock_daily_indicators_before(date(2026, 7, 28))

    assert deleted == 1
    assert _scalar(database_engine, "select count(*) from core.stock_daily_indicator") == 2
    assert _scalar(
        database_engine, "select min(trade_date) from core.stock_daily_indicator"
    ) == date(2026, 7, 28)


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
        assert connection.execute("select count(*) from api_v1.board_indexes").fetchone() == (0,)

        denied_statements = (
            "select count(*) from core.daily_bar",
            "select count(*) from core.board_index",
            "select count(*) from derived.daily_price_limit",
            "select count(*) from stock_pool.snapshot",
            "select count(*) from operations.workflow_run",
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
        ("core", "deducted_profit", "INSERT"),
        ("core", "deducted_profit", "SELECT"),
        ("core", "board_index", "INSERT"),
        ("core", "board_index", "SELECT"),
        ("core", "board_index", "UPDATE"),
        ("core", "board_index_daily_bar", "INSERT"),
        ("core", "board_index_daily_bar", "SELECT"),
        ("core", "board_index_daily_bar", "UPDATE"),
        ("core", "board_index_constituent_snapshot", "DELETE"),
        ("core", "board_index_constituent_snapshot", "INSERT"),
        ("core", "board_index_constituent_snapshot", "SELECT"),
        ("core", "security", "INSERT"),
        ("core", "security", "SELECT"),
        ("core", "security", "UPDATE"),
        ("core", "security_name_history", "INSERT"),
        ("core", "security_name_history", "SELECT"),
        ("core", "security_name_history", "UPDATE"),
        ("core", "stock_daily_indicator", "DELETE"),
        ("core", "stock_daily_indicator", "INSERT"),
        ("core", "stock_daily_indicator", "SELECT"),
        ("core", "stock_daily_indicator", "UPDATE"),
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

        assert connection.execute(
            "select has_table_privilege('market_data_worker', 'core.daily_bar', 'select')"
        ).fetchone() == (True,)
        assert connection.execute(
            "select has_table_privilege('market_data_worker', 'core.daily_bar', 'delete')"
        ).fetchone() == (False,)
        assert connection.execute(
            "select has_table_privilege('market_data_worker', 'api_v1.daily_bars', 'select')"
        ).fetchone() == (False,)


def test_internal_tables_have_rls_with_worker_only_policies(database_engine: Engine) -> None:
    expected_tables = {
        ("audit", "quality_result"),
        ("core", "daily_bar"),
        ("core", "deducted_profit"),
        ("core", "board_index"),
        ("core", "board_index_daily_bar"),
        ("core", "board_index_constituent_snapshot"),
        ("core", "security"),
        ("core", "security_name_history"),
        ("core", "stock_daily_indicator"),
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


@pytest.mark.skipif(which("pg_dump") is None, reason="pg_dump is not installed")
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


def _seed_one_price_pattern_session(
    connection: Connection,
    trade_date: date,
    session_id: UUID,
) -> None:
    _insert_call_auction_security_universe(connection)
    security_ingestion_id = uuid4()
    workflow_id = uuid4()
    slots = series_slots(trade_date)
    extra_symbols = tuple(f"SSE:60000{suffix}" for suffix in range(4, 10))
    symbols = tuple(
        sorted(
            (
                "SSE:600000",
                "SZSE:000001",
                "SZSE:000002",
                *extra_symbols,
            )
        )
    )
    snapshot_symbols = (*symbols, "BSE:920000", "SSE:510300")
    connection.execute(
        text("""
insert into ingestion.ingestion_run (
    ingestion_id, provider_code, dataset_code, status, requested_at, started_at
) values (
    :ingestion_id, 'baostock', 'security', 'running', :started_at, :started_at
)
"""),
        {"ingestion_id": security_ingestion_id, "started_at": slots[0]},
    )
    connection.execute(
        text("""
insert into core.security (
    symbol, code, exchange, current_name, security_type, status,
    source_code, ingestion_id
) values (
    :symbol, :code, 'SSE', :current_name, 'stock', 'listed',
    'baostock', :ingestion_id
)
"""),
        [
            {
                "symbol": symbol,
                "code": symbol.split(":", maxsplit=1)[1],
                "current_name": f"测试{symbol[-2:]}",
                "ingestion_id": security_ingestion_id,
            }
            for symbol in extra_symbols
        ],
    )
    connection.execute(
        text("""
insert into core.security_name_history (
    symbol, name, effective_from, effective_to, source_code, ingestion_id
)
select symbol, current_name, date '2020-01-01', null, 'baostock', ingestion_id
from core.security
"""),
    )
    connection.execute(
        text("""
insert into operations.workflow_run (
    workflow_run_id, workflow_code, scheduled_for, trigger_source,
    attempt, status, started_at, finished_at
) values (
    :workflow_id, 'call_auction_market_series', :started_at, 'scheduled',
    1, 'succeeded', :started_at, :finished_at
)
"""),
        {
            "workflow_id": workflow_id,
            "started_at": slots[0],
            "finished_at": slots[-1] + timedelta(seconds=2),
        },
    )
    connection.execute(
        text("""
insert into realtime.call_auction_market_series_session (
    session_id, workflow_run_id, trade_date, window_start, window_end,
    cadence_seconds, expected_rounds, universe_symbols, universe_count,
    universe_hash, status, started_at, finished_at, successful_rounds,
    partial_rounds, failed_rounds, successful_quotes, failed_quotes
) values (
    :session_id, :workflow_id, :trade_date, :window_start, :window_end,
    20, 32, :universe_symbols, :universe_count,
    :universe_hash, 'partial', :window_start, :finished_at, 29,
    0, 3, :successful_quotes, :failed_quotes
)
"""),
        {
            "session_id": session_id,
            "workflow_id": workflow_id,
            "trade_date": trade_date,
            "window_start": slots[0],
            "window_end": slots[-1] + timedelta(seconds=20),
            "universe_symbols": list(symbols),
            "universe_count": len(symbols),
            "universe_hash": universe_hash(symbols),
            "finished_at": slots[-1] + timedelta(seconds=2),
            "successful_quotes": len(symbols) * 29,
            "failed_quotes": len(symbols) * 3,
        },
    )

    ingestion_ids = {sample_seq: uuid4() for sample_seq in range(1, 30)}
    connection.execute(
        text("""
insert into ingestion.ingestion_run (
    ingestion_id, provider_code, dataset_code, status, requested_at,
    started_at, finished_at, fetched_rows, accepted_rows
) values (
    :ingestion_id, 'pytdx_hq', 'call_auction_market_series', 'succeeded',
    :scheduled_at, :scheduled_at, :finished_at, :row_count, :row_count
)
"""),
        [
            {
                "ingestion_id": ingestion_id,
                "scheduled_at": slots[sample_seq],
                "finished_at": slots[sample_seq] + timedelta(seconds=2),
                "row_count": len(snapshot_symbols),
            }
            for sample_seq, ingestion_id in ingestion_ids.items()
        ],
    )
    connection.execute(
        text("""
insert into realtime.call_auction_market_series_round (
    session_id, sample_seq, scheduled_at, collected_at, status,
    attempt_count, expected_quotes, successful_quotes, failed_quotes,
    selected_ingestion_id
) values (
    :session_id, :sample_seq, :scheduled_at, :collected_at, :status,
    1, :expected_quotes, :successful_quotes, :failed_quotes,
    :selected_ingestion_id
)
"""),
        [
            {
                "session_id": session_id,
                "sample_seq": sample_seq,
                "scheduled_at": slots[sample_seq],
                "collected_at": slots[sample_seq] + timedelta(seconds=2),
                "status": "succeeded" if sample_seq in ingestion_ids else "failed",
                "expected_quotes": len(symbols),
                "successful_quotes": len(symbols) if sample_seq in ingestion_ids else 0,
                "failed_quotes": 0 if sample_seq in ingestion_ids else len(symbols),
                "selected_ingestion_id": ingestion_ids.get(sample_seq),
            }
            for sample_seq in range(32)
        ],
    )

    base_prices = {
        "SSE:600000": (Decimal("10.20"), Decimal("10.00")),
        "SZSE:000001": (Decimal("9.60"), Decimal("10.00")),
        "SZSE:000002": (Decimal("10.40"), Decimal("10.00")),
        "BSE:920000": (Decimal("10.20"), Decimal("10.00")),
        "SSE:510300": (Decimal("10.20"), Decimal("10.00")),
        "SSE:600004": (Decimal("10.4001"), Decimal("10.00")),
        "SSE:600005": (Decimal("10.20"), Decimal("10.00")),
        "SSE:600006": (Decimal("10.20"), Decimal("10.00")),
        "SSE:600007": (Decimal("10.20"), Decimal("10.00")),
        "SSE:600008": (Decimal("10.20"), Decimal("10.00")),
        "SSE:600009": (Decimal("10.20"), Decimal("10.00")),
    }
    snapshots: list[dict[str, object]] = []
    for sample_seq, ingestion_id in ingestion_ids.items():
        for symbol in snapshot_symbols:
            if symbol == "SSE:600009" and sample_seq == 29:
                continue
            last_price, previous_close = base_prices[symbol]
            if symbol == "SSE:600005" and sample_seq == 7:
                last_price = Decimal("10.21")
            if symbol == "SSE:600006" and sample_seq == 7:
                previous_close = Decimal("9.99")
            if symbol == "SSE:600007" and sample_seq == 7:
                last_price = None
            value_semantics = (
                "opening_trade"
                if symbol == "SSE:600008" and sample_seq == 7
                else "auction_indicative"
            )
            snapshots.append(
                {
                    "trade_date": trade_date,
                    "ingestion_id": ingestion_id,
                    "session_id": session_id,
                    "sample_seq": sample_seq,
                    "batch_code": series_batch_code(slots[sample_seq]),
                    "scheduled_at": slots[sample_seq],
                    "symbol": symbol,
                    "observed_at": slots[sample_seq] + timedelta(seconds=1),
                    "last_price": last_price,
                    "previous_close": previous_close,
                    "value_semantics": value_semantics,
                }
            )
    connection.execute(
        text("""
insert into realtime.call_auction_market_series_snapshot (
    trade_date, ingestion_id, session_id, sample_seq, batch_code,
    scheduled_at, symbol, observed_at, last_price, previous_close,
    source_code, value_semantics
) values (
    :trade_date, :ingestion_id, :session_id, :sample_seq, :batch_code,
    :scheduled_at, :symbol, :observed_at, :last_price, :previous_close,
    'pytdx_hq', :value_semantics
)
"""),
        snapshots,
    )


def _insert_call_auction_security_universe(connection: Connection) -> None:
    ingestion_id = uuid4()
    connection.execute(
        text("""
            insert into ingestion.ingestion_run (
                ingestion_id, provider_code, dataset_code, status, started_at
            ) values (:ingestion_id, 'baostock', 'security', 'running', now())
        """),
        {"ingestion_id": ingestion_id},
    )
    connection.execute(
        text("""
            insert into core.security (
                symbol, code, exchange, current_name, security_type, status,
                source_code, ingestion_id
            ) values (
                :symbol, :code, :exchange, :current_name, :security_type, :status,
                'baostock', :ingestion_id
            )
        """),
        [
            {
                "symbol": "SSE:600000",
                "code": "600000",
                "exchange": "SSE",
                "current_name": "浦发银行",
                "security_type": "stock",
                "status": "listed",
                "ingestion_id": ingestion_id,
            },
            {
                "symbol": "SZSE:000001",
                "code": "000001",
                "exchange": "SZSE",
                "current_name": "平安银行",
                "security_type": "stock",
                "status": "listed",
                "ingestion_id": ingestion_id,
            },
            {
                "symbol": "BSE:920000",
                "code": "920000",
                "exchange": "BSE",
                "current_name": "北交样本",
                "security_type": "stock",
                "status": "listed",
                "ingestion_id": ingestion_id,
            },
            {
                "symbol": "SSE:510300",
                "code": "510300",
                "exchange": "SSE",
                "current_name": "ETF样本",
                "security_type": "etf",
                "status": "listed",
                "ingestion_id": ingestion_id,
            },
            {
                "symbol": "SZSE:000002",
                "code": "000002",
                "exchange": "SZSE",
                "current_name": "退市样本",
                "security_type": "stock",
                "status": "delisted",
                "ingestion_id": ingestion_id,
            },
        ],
    )


def _insert_trading_calendar_day(
    connection: Connection,
    trade_date: date,
    *,
    is_trading_day: bool,
) -> None:
    ingestion_id = uuid4()
    connection.execute(
        text("""
            insert into ingestion.ingestion_run (
                ingestion_id, provider_code, dataset_code, status, started_at
            ) values (:ingestion_id, 'baostock', 'trading_calendar', 'running', now())
        """),
        {"ingestion_id": ingestion_id},
    )
    connection.execute(
        text("""
            insert into core.trading_calendar (
                market, trade_date, is_trading_day, source_code, ingestion_id
            ) values (
                'CN_A_SHARE', :trade_date, :is_trading_day, 'baostock', :ingestion_id
            )
        """),
        {
            "trade_date": trade_date,
            "is_trading_day": is_trading_day,
            "ingestion_id": ingestion_id,
        },
    )


def _call_auction_market_run(
    status: IngestionStatus,
    *,
    finished_at: datetime,
    fetched_rows: int,
    accepted_rows: int,
    rejected_rows: int = 0,
    request_params: Mapping[str, object] | None = None,
) -> IngestionRun:
    started_at = finished_at - timedelta(minutes=2)
    return IngestionRun(
        ingestion_id=uuid4(),
        provider_code=ProviderCode.PYTDX_HQ,
        dataset_code=DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT,
        status=status,
        requested_at=started_at,
        started_at=started_at,
        finished_at=finished_at,
        request_params=request_params or {},
        fetched_rows=fetched_rows,
        accepted_rows=accepted_rows,
        rejected_rows=rejected_rows,
    )


def _call_auction_market_record(
    symbol: str,
    trade_date: date,
    observed_at: datetime,
    *,
    last_price: Decimal = Decimal("10.00"),
    previous_close: Decimal = Decimal("10.00"),
    high_price: Decimal = Decimal("10.00"),
    low_price: Decimal = Decimal("10.00"),
    cumulative_volume: int = 100,
    cumulative_amount: Decimal = Decimal("1000.00"),
) -> CallAuctionMarketSnapshotRecord:
    return CallAuctionMarketSnapshotRecord(
        symbol=symbol,
        trade_date=trade_date,
        observed_at=observed_at,
        source_code="pytdx_hq",
        last_price=last_price,
        previous_close=previous_close,
        high_price=high_price,
        low_price=low_price,
        cumulative_volume=cumulative_volume,
        cumulative_amount=cumulative_amount,
    )


def _call_auction_quality_result(
    ingestion_id: UUID,
    *,
    failed: bool = False,
) -> QualityResult:
    return QualityResult(
        quality_result_id=uuid4(),
        ingestion_id=ingestion_id,
        dataset_code=DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT,
        rule_code="call_auction_market.complete",
        severity=QualitySeverity.ERROR if failed else QualitySeverity.INFO,
        status=QualityStatus.FAILED if failed else QualityStatus.PASSED,
        message="incomplete source response" if failed else "complete source response",
    )


def _commit_call_auction_market_run(
    persistence: PostgreSQLPersistence,
    run: IngestionRun,
    records: list[CallAuctionMarketSnapshotRecord],
) -> None:
    running = replace(
        run,
        status=IngestionStatus.RUNNING,
        finished_at=None,
        fetched_rows=0,
        accepted_rows=0,
        rejected_rows=0,
    )
    persistence.create_ingestion_run(running)
    persistence.commit_call_auction_market_attempt(
        run,
        records,
        _manifest(
            run.ingestion_id,
            f"call-auction-market-{run.ingestion_id}",
            row_count=run.fetched_rows,
            provider="pytdx_hq",
        ),
        [
            _call_auction_quality_result(
                run.ingestion_id,
                failed=run.status is IngestionStatus.PARTIAL,
            )
        ],
    )


def _insert_ready_limit_up_pool(
    connection: Connection,
    effective_trade_date: date,
    symbols: list[str],
    *,
    version: int = 1,
) -> UUID:
    calculation_id = uuid4()
    snapshot_id = uuid4()
    basis_trade_date = effective_trade_date - timedelta(days=1)
    input_hash = format(version, "x") * 64
    connection.execute(
        text("""
            insert into derived.calculation_run (
                calculation_id, calculation_code, algorithm_version, mode,
                start_date, end_date, status, input_watermark, input_hash,
                requested_at, calculated_at, finished_at, output_rows
            ) values (
                :calculation_id, 'cn_a_mainboard_price_limit_pools', '1.0.0',
                'incremental', :basis_trade_date, :basis_trade_date, 'succeeded',
                '{}'::jsonb, :input_hash, now(), now(), now(), :output_rows
            )
        """),
        {
            "calculation_id": calculation_id,
            "basis_trade_date": basis_trade_date,
            "input_hash": input_hash,
            "output_rows": len(symbols),
        },
    )
    connection.execute(
        text("""
            insert into stock_pool.snapshot (
                snapshot_id, calculation_id, pool_code, basis_trade_date,
                effective_trade_date, version, status, member_count, candidate_count,
                rejected_count, content_hash, input_hash, rule_version,
                algorithm_version, generated_at
            ) values (
                :snapshot_id, :calculation_id,
                'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP', :basis_trade_date,
                :effective_trade_date, :version, 'ready', :member_count, :member_count,
                0, :content_hash, :input_hash, 'CN_MAINBOARD_2026_07_06',
                '1.0.0', :generated_at
            )
        """),
        {
            "snapshot_id": snapshot_id,
            "calculation_id": calculation_id,
            "basis_trade_date": basis_trade_date,
            "effective_trade_date": effective_trade_date,
            "version": version,
            "member_count": len(symbols),
            "content_hash": f"{version}" * 64,
            "input_hash": input_hash,
            "generated_at": datetime.combine(
                effective_trade_date,
                datetime.min.time(),
                tzinfo=UTC,
            )
            + timedelta(seconds=version),
        },
    )
    if symbols:
        connection.execute(
            text("""
                insert into stock_pool.member (snapshot_id, symbol, direction)
                values (:snapshot_id, :symbol, 'up')
            """),
            [{"snapshot_id": snapshot_id, "symbol": symbol} for symbol in symbols],
        )
    return snapshot_id


def _insert_existing_call_auction_final(
    connection: Connection,
    trade_date: date,
) -> UUID:
    ingestion_id = uuid4()
    observed_at = datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=1, minutes=26
    )
    connection.execute(
        text("""
            insert into ingestion.ingestion_run (
                ingestion_id, provider_code, dataset_code, status, requested_at,
                started_at, finished_at, fetched_rows, accepted_rows
            ) values (
                :ingestion_id, 'pytdx_hq', 'call_auction_snapshot', 'succeeded',
                :observed_at, :observed_at, :observed_at, 1, 1
            )
        """),
        {"ingestion_id": ingestion_id, "observed_at": observed_at},
    )
    connection.execute(
        text("""
            insert into realtime.call_auction_snapshot (
                symbol, trade_date, last_price, previous_close, cumulative_volume,
                cumulative_amount, auction_premium_pct, source_code, ingestion_id,
                observed_at
            ) values (
                'SSE:600000', :trade_date, 9.99, 9.90, 999, 9980.01, 0.9090909091,
                'pytdx_hq', :ingestion_id, :observed_at
            )
        """),
        {
            "trade_date": trade_date,
            "ingestion_id": ingestion_id,
            "observed_at": observed_at,
        },
    )
    return ingestion_id


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
    schemas = (
        "api_v1",
        "audit",
        "capital",
        "classification",
        "core",
        "derived",
        "ingestion",
        "metrics",
        "operations",
        "stock_pool",
    )
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
        "routines": connection.execute(
            """
            select n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
            from pg_proc p
            join pg_namespace n on n.oid = p.pronamespace
            where n.nspname = 'api_v1'
            order by 1, 2, 3
            """
        ).fetchall(),
    }


def _running_run(
    dataset_code: DatasetCode,
    provider_code: ProviderCode = ProviderCode.BAOSTOCK,
) -> IngestionRun:
    return IngestionRun(
        ingestion_id=uuid4(),
        provider_code=provider_code,
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


def _manifest(
    ingestion_id: UUID,
    dataset: str,
    row_count: int = 1,
    provider: str = "baostock",
) -> RawManifest:
    return RawManifest(
        raw_id=uuid4(),
        ingestion_id=ingestion_id,
        object_path=f"{provider}/{dataset}/2026-07-28/{ingestion_id}.jsonl",
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


def _commit_board_bias_bars(
    database_engine: Engine,
    start_date: date,
    closes: list[Decimal],
) -> None:
    persistence = PostgreSQLPersistence(database_engine)
    trading_days = [start_date + timedelta(days=index) for index in range(len(closes))]

    calendar_run = _running_run(DatasetCode.TRADING_CALENDAR)
    persistence.create_ingestion_run(calendar_run)
    persistence.commit_trading_calendar_batch(
        _completed_run(calendar_run, row_count=len(trading_days)),
        _manifest(
            calendar_run.ingestion_id,
            "board-bias-calendar",
            row_count=len(trading_days),
        ),
        _envelopes(
            calendar_run.ingestion_id,
            [
                CalculatedTradingDay(
                    market=Market.CN_A_SHARE,
                    trade_date=trade_date,
                    is_trading_day=True,
                    previous_trading_day=(trading_days[index - 1] if index > 0 else None),
                    next_trading_day=(
                        trading_days[index + 1] if index + 1 < len(trading_days) else None
                    ),
                    source_code="baostock",
                )
                for index, trade_date in enumerate(trading_days)
            ],
        ),
    )

    board = BoardIndexRecord(
        board_id="THS:883423",
        board_code="883423",
        namespace="THS",
        name="沪深主板昨日涨停",
        board_type=BoardIndexType.DYNAMIC_THEME,
        market=Market.CN_A_SHARE,
        status=BoardIndexStatus.ACTIVE,
        source_code="akshare_ths",
    )
    board_run = _running_run(DatasetCode.BOARD_INDEX, ProviderCode.AKSHARE_THS)
    persistence.create_ingestion_run(board_run)
    persistence.commit_board_index_batch(
        _completed_run(board_run),
        _manifest(board_run.ingestion_id, "board-bias-index", provider="akshare_ths"),
        _envelopes(board_run.ingestion_id, [board]),
    )

    bars = [
        BoardIndexDailyBarRecord(
            board_id=board.board_id,
            trade_date=trade_date,
            market=Market.CN_A_SHARE,
            open=close,
            high=close,
            low=close,
            close=close,
            volume=0,
            amount=Decimal("0"),
            source_code="akshare_ths",
        )
        for trade_date, close in zip(trading_days, closes, strict=True)
    ]
    bar_run = _running_run(DatasetCode.BOARD_INDEX_DAILY_BAR, ProviderCode.AKSHARE_THS)
    persistence.create_ingestion_run(bar_run)
    persistence.commit_board_index_daily_bar_batch(
        _completed_run(bar_run, row_count=len(bars)),
        _manifest(
            bar_run.ingestion_id,
            "board-bias-bars",
            row_count=len(bars),
            provider="akshare_ths",
        ),
        _envelopes(bar_run.ingestion_id, bars),
        [],
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


def _prepare_query_contract_data(engine: Engine) -> UUID:
    _prepare_api_data(engine)
    persistence = PostgreSQLPersistence(engine)
    capital_run = _running_run(DatasetCode.CAPITAL)
    persistence.create_ingestion_run(capital_run)
    persistence.commit_capital_batch(
        _completed_run(capital_run),
        _manifest(capital_run.ingestion_id, "query-contract-capital"),
        _envelopes(
            capital_run.ingestion_id,
            [
                ShareCapitalRecord(
                    symbol=SYMBOL,
                    effective_date=date(2024, 1, 1),
                    total_shares=1_000_000,
                    restricted_shares=None,
                    circulating_shares=900_000,
                    listed_a_shares=900_000,
                    change_reason="query contract fixture",
                    source_code="akshare",
                )
            ],
        ),
        [],
    )
    catalog_run = _running_run(DatasetCode.CLASSIFICATION_CATALOG)
    persistence.create_ingestion_run(catalog_run)
    persistence.commit_classification_catalog_batch(
        _completed_run(catalog_run),
        _manifest(catalog_run.ingestion_id, "query-contract-catalog"),
        IngestionEnvelope(
            catalog_run.ingestion_id,
            ClassificationCatalogSnapshotRecord(
                namespace="eastmoney",
                classification_type=ClassificationType.INDUSTRY,
                snapshot_date=date(2026, 7, 27),
                definitions=(ClassificationDefinition("BK0475", "银行"),),
                source_code="akshare",
            ),
        ),
        [],
    )
    member_run = _running_run(DatasetCode.CLASSIFICATION_MEMBERS)
    persistence.create_ingestion_run(member_run)
    persistence.commit_classification_members_batch(
        _completed_run(member_run),
        _manifest(member_run.ingestion_id, "query-contract-members"),
        IngestionEnvelope(
            member_run.ingestion_id,
            ClassificationMemberSnapshotRecord(
                namespace="eastmoney",
                classification_type=ClassificationType.INDUSTRY,
                classification_code="BK0475",
                snapshot_date=date(2026, 7, 27),
                members=(SYMBOL,),
                source_code="akshare",
            ),
        ),
        [],
    )
    summary = DerivationService(PostgreSQLDerivedPersistence(engine)).recompute(
        date(2026, 7, 27), date(2026, 7, 29)
    )
    return summary.calculation_id


def test_trading_billboard_constraints_and_bounded_rpcs(database_engine: Engine) -> None:
    _prepare_api_data(database_engine)
    ingestion_id = uuid4()
    entry_id = uuid4()
    with database_engine.begin() as connection:
        connection.execute(
            text("""
                insert into ingestion.ingestion_run (
                    ingestion_id, provider_code, dataset_code, status,
                    requested_at, started_at, finished_at,
                    fetched_rows, accepted_rows, rejected_rows
                ) values (
                    :ingestion_id, 'eastmoney', 'trading_billboard', 'succeeded',
                    now(), now(), now(), 11, 1, 0
                )
            """),
            {"ingestion_id": ingestion_id},
        )
        connection.execute(
            text("""
                insert into billboard.entry (
                    entry_id, symbol, trade_date, source_event_id,
                    reason_code, reason_text, close_price, change_rate_pct,
                    turnover_rate_pct, market_amount, buy_amount, sell_amount,
                    net_amount, deal_amount, deal_to_market_pct,
                    net_to_market_pct, free_float_market_value,
                    source_code, ingestion_id, content_hash
                ) values (
                    :entry_id, :symbol, :trade_date, 'event-1',
                    '106001', '测试上榜原因', 10.50, 9.9,
                    12.5, 10000, 600, 400,
                    200, 1000, 10, 2, 50000,
                    'eastmoney', :ingestion_id, :content_hash
                )
            """),
            {
                "entry_id": entry_id,
                "symbol": SYMBOL,
                "trade_date": TRADE_DATE,
                "ingestion_id": ingestion_id,
                "content_hash": "a" * 64,
            },
        )
        for side in ("buy", "sell"):
            for rank in range(1, 6):
                connection.execute(
                    text("""
                        insert into billboard.seat (
                            entry_id, source_code, source_event_id, symbol, trade_date,
                            side, rank, seat_code, seat_name,
                            buy_amount, sell_amount, net_amount,
                            buy_to_market_pct, sell_to_market_pct, ingestion_id
                        ) values (
                            :entry_id, 'eastmoney', 'event-1', :symbol, :trade_date,
                            :side, :rank, :seat_code, :seat_name,
                            120, 20, 100, 1.2, 0.2, :ingestion_id
                        )
                    """),
                    {
                        "entry_id": entry_id,
                        "symbol": SYMBOL,
                        "trade_date": TRADE_DATE,
                        "side": side,
                        "rank": rank,
                        "seat_code": "80000001" if rank == 1 else None,
                        "seat_name": "测试营业部" if rank == 1 else "机构专用",
                        "ingestion_id": ingestion_id,
                    },
                )

    with database_engine.connect() as connection:
        connection.execute(text("set local role market_data_api"))
        by_date = cast(
            Mapping[str, object],
            connection.scalar(
                text("select api_v1.query_trading_billboard_by_date(:day, 100, 0)"),
                {"day": TRADE_DATE},
            ),
        )
        no_fallback = cast(
            Mapping[str, object],
            connection.scalar(
                text("select api_v1.query_trading_billboard_by_date(:day, 100, 0)"),
                {"day": TRADE_DATE + timedelta(days=1)},
            ),
        )
        by_symbol = cast(
            Mapping[str, object],
            connection.scalar(
                text("""
                    select api_v1.query_trading_billboard_by_symbol(
                        :symbol, :start_date, :end_date, 100, 0
                    )
                """),
                {"symbol": SYMBOL, "start_date": TRADE_DATE, "end_date": TRADE_DATE},
            ),
        )
        by_code = cast(
            Mapping[str, object],
            connection.scalar(
                text("""
                    select api_v1.query_trading_billboard_by_seat(
                        '80000001', null, :start_date, :end_date, null, 100, 0
                    )
                """),
                {"start_date": TRADE_DATE, "end_date": TRADE_DATE},
            ),
        )
        by_name = cast(
            Mapping[str, object],
            connection.scalar(
                text("""
                    select api_v1.query_trading_billboard_by_seat(
                        null, '机构专用', :start_date, :end_date, 'buy', 100, 0
                    )
                """),
                {"start_date": TRADE_DATE, "end_date": TRADE_DATE},
            ),
        )

        assert by_date["total_count"] == 1
        assert len(cast(list[object], by_date["items"])) == 1
        assert no_fallback["items"] == []
        assert by_symbol["total_count"] == 1
        assert by_code["total_count"] == 2
        assert by_name["total_count"] == 4
        assert connection.scalar(
            text("""
                select has_function_privilege(
                    'market_data_api',
                    'api_v1.query_trading_billboard_by_seat(text,text,date,date,text,integer,integer)',
                    'execute'
                )
            """)
        )
        assert not connection.scalar(
            text("select has_table_privilege('market_data_api', 'billboard.entry', 'select')")
        )
        with pytest.raises(DBAPIError), connection.begin_nested():
            connection.execute(text("select * from billboard.entry"))
        with pytest.raises(DBAPIError) as invalid:
            connection.execute(
                text("""
                    select api_v1.query_trading_billboard_by_seat(
                        '80000001', '机构专用', :start_date, :end_date, null, 100, 0
                    )
                """),
                {"start_date": TRADE_DATE, "end_date": TRADE_DATE},
            )
        assert getattr(invalid.value.orig, "sqlstate", None) == "22023"


def test_trading_billboard_seat_composite_parent_key_is_enforced(
    database_engine: Engine,
) -> None:
    _prepare_api_data(database_engine)
    ingestion_id = uuid4()
    entry_id = uuid4()
    with database_engine.begin() as connection:
        connection.execute(
            text("""
                insert into ingestion.ingestion_run (
                    ingestion_id, provider_code, dataset_code, status,
                    requested_at, started_at, finished_at
                ) values (
                    :ingestion_id, 'eastmoney', 'trading_billboard', 'succeeded',
                    now(), now(), now()
                )
            """),
            {"ingestion_id": ingestion_id},
        )
        connection.execute(
            text("""
                insert into billboard.entry (
                    entry_id, symbol, trade_date, source_event_id,
                    reason_code, reason_text, buy_amount, sell_amount,
                    net_amount, deal_amount, source_code, ingestion_id, content_hash
                ) values (
                    :entry_id, :symbol, :trade_date, 'event-parent',
                    '106001', '测试上榜原因', 60, 40,
                    20, 100, 'eastmoney', :ingestion_id, :content_hash
                )
            """),
            {
                "entry_id": entry_id,
                "symbol": SYMBOL,
                "trade_date": TRADE_DATE,
                "ingestion_id": ingestion_id,
                "content_hash": "b" * 64,
            },
        )
        connection.execute(
            text("""
                insert into billboard.seat (
                    entry_id, source_code, source_event_id, symbol, trade_date,
                    side, rank, seat_name, ingestion_id
                ) values (
                    :entry_id, 'eastmoney', 'event-parent', :symbol, :trade_date,
                    'buy', 1, '测试营业部', :ingestion_id
                )
            """),
            {
                "entry_id": entry_id,
                "symbol": SYMBOL,
                "trade_date": TRADE_DATE,
                "ingestion_id": ingestion_id,
            },
        )
        with pytest.raises(IntegrityError), connection.begin_nested():
            connection.execute(
                text("""
                    insert into billboard.seat (
                        entry_id, source_code, source_event_id, symbol, trade_date,
                        side, rank, seat_name, ingestion_id
                    ) values (
                        :entry_id, 'eastmoney', 'event-parent', :symbol, :trade_date,
                        'buy', 1, '重复名次营业部', :ingestion_id
                    )
                """),
                {
                    "entry_id": entry_id,
                    "symbol": SYMBOL,
                    "trade_date": TRADE_DATE,
                    "ingestion_id": ingestion_id,
                },
            )
        with pytest.raises(IntegrityError), connection.begin_nested():
            connection.execute(
                text("""
                        insert into billboard.seat (
                            entry_id, source_code, source_event_id, symbol, trade_date,
                            side, rank, seat_name, ingestion_id
                        ) values (
                            :entry_id, 'eastmoney', 'wrong-event', :symbol, :trade_date,
                            'buy', 1, '测试营业部', :ingestion_id
                        )
                    """),
                {
                    "entry_id": entry_id,
                    "symbol": SYMBOL,
                    "trade_date": TRADE_DATE,
                    "ingestion_id": ingestion_id,
                },
            )


def _envelopes[
    RecordT: SecurityRecord
    | CalculatedTradingDay
    | DailyBarRecord
    | CapitalRecord
    | ClassificationRecord
    | BoardIndexRecord
    | BoardIndexDailyBarRecord
    | BoardIndexConstituentSnapshotRecord
](ingestion_id: UUID, records: list[RecordT]) -> list[IngestionEnvelope[RecordT]]:
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
