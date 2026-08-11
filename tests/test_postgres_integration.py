from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
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
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from market_data_center.derivation import DerivationService
from market_data_center.domain import (
    BoardIndexConstituentSnapshotRecord,
    BoardIndexDailyBarRecord,
    BoardIndexRecord,
    BoardIndexStatus,
    BoardIndexType,
    CalculatedTradingDay,
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
    TradeStatus,
    deducted_profit_revision_key,
)
from market_data_center.domain.operations import ExecutionStatus, TriggerSource, WorkflowCode
from market_data_center.migrations import MIGRATION_DIR, apply_migrations
from market_data_center.persistence import PostgreSQLDerivedPersistence, PostgreSQLPersistence
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
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
        "query_adjusted_daily_bars",
        "query_market_snapshot",
        "query_classification_members_as_of",
        "query_deducted_profits_as_of",
        "query_stock_pool_snapshot",
        "query_limit_up_pool",
        "query_auction_quotes",
    }.issubset({row[1] for row in first_snapshot["routines"]})


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

    with pytest.raises(DBAPIError) as captured:
        with database_engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "workflow_run_id": uuid4(),
                    "workflow_code": "unknown_workflow",
                    "scheduled_for": datetime(2026, 8, 11, 3, tzinfo=UTC),
                },
            )

    assert isinstance(captured.value.orig, CheckViolation)


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
        ("core", "board_index"),
        ("core", "board_index_daily_bar"),
        ("core", "board_index_constituent_snapshot"),
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
