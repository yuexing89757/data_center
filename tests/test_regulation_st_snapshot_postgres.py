"""Daily ST facts and grants on an isolated migrated PostgreSQL database."""

from dataclasses import replace
from datetime import UTC, date, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from test_postgres_integration import (  # noqa: F401
    database_engine,
    empty_database_url,
    migrated_database_url,
)

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    RawFileFormat,
    RawManifest,
)
from market_data_center.domain.records import RegulationStSnapshotRecord
from market_data_center.persistence.postgres import PostgreSQLPersistence

pytestmark = pytest.mark.integration
DAY = date(2026, 7, 28)
NOW = datetime(2026, 7, 28, 22, 0, tzinfo=UTC)


@pytest.fixture
def st_calendar(database_engine):  # noqa: F811
    ingestion_id = uuid4()
    with database_engine.begin() as connection:
        connection.execute(
            text("""insert into ingestion.ingestion_run
                (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
                values (:id,'baostock','trading_calendar','succeeded',now(),now(),now())"""),
            {"id": ingestion_id},
        )
        connection.execute(
            text("""insert into core.trading_calendar
                (market,trade_date,is_trading_day,source_code,ingestion_id)
                values ('CN_A_SHARE',:day,true,'baostock',:id)"""),
            {"day": DAY, "id": ingestion_id},
        )


def _publish(persistence, symbols, *, day=DAY, ingestion_id=None):
    run = IngestionRun(
        ingestion_id=ingestion_id or uuid4(),
        provider_code=ProviderCode.TUSHARE,
        dataset_code=DatasetCode.REGULATION_ST_SNAPSHOT,
        status=IngestionStatus.RUNNING,
        requested_at=NOW,
        started_at=NOW,
        request_params={"trade_date": day.isoformat()},
    )
    persistence.create_ingestion_run(run)
    manifest = RawManifest(
        raw_id=uuid4(),
        ingestion_id=run.ingestion_id,
        object_path=f"st/{run.ingestion_id}.jsonl",
        file_format=RawFileFormat.JSONL,
        content_sha256="a" * 64,
        byte_size=100,
        row_count=len(symbols),
        schema_version="tushare.regulation_st_snapshot.v1",
    )
    completed = replace(
        run,
        status=IngestionStatus.SUCCEEDED,
        finished_at=NOW,
        fetched_rows=len(symbols),
        accepted_rows=1,
    )
    persistence.commit_regulation_st_snapshot_batch(
        completed, manifest, RegulationStSnapshotRecord(day, symbols, "tushare")
    )
    return run.ingestion_id


def test_st_snapshot_is_atomic_idempotent_and_private(database_engine, st_calendar):  # noqa: F811
    persistence = PostgreSQLPersistence(database_engine)
    first = _publish(persistence, ("SSE:600000",))
    duplicate = _publish(persistence, ("SSE:600000",))
    with database_engine.connect() as connection:
        row = connection.execute(
            text("select symbols, symbol_count, ingestion_id from regulation.st_day_snapshot")
        ).one()
        assert row == (["SSE:600000"], 1, first)
        assert (
            connection.execute(
                text("""select count(*) from ingestion.raw_manifest
                    where ingestion_id in (:first,:second)"""),
                {"first": first, "second": duplicate},
            ).scalar_one()
            == 2
        )
    corrected = _publish(persistence, ("SSE:600000", "SZSE:000001"))
    with database_engine.connect() as connection:
        row = connection.execute(
            text("select symbols, symbol_count, ingestion_id from regulation.st_day_snapshot")
        ).one()
        assert row == (["SSE:600000", "SZSE:000001"], 2, corrected)
        assert (
            connection.execute(text("select count(*) from ingestion.raw_manifest")).scalar_one()
            == 3
        )
        assert connection.execute(
            text("""select
                has_table_privilege('market_data_api','regulation.st_day_snapshot','SELECT'),
                has_table_privilege('market_data_worker','regulation.st_day_snapshot','SELECT'),
                has_table_privilege('market_data_worker','regulation.st_day_snapshot','INSERT'),
                has_table_privilege('market_data_worker','regulation.st_day_snapshot','UPDATE'),
                has_table_privilege('market_data_worker','regulation.st_day_snapshot','DELETE')
            """)
        ).one() == (False, True, True, True, False)


def test_st_snapshot_rejects_missing_calendar_and_rolls_back_invalid_batch(
    database_engine,  # noqa: F811
    st_calendar,
):
    persistence = PostgreSQLPersistence(database_engine)
    missing_day_id = uuid4()
    with pytest.raises(ValueError, match="known trading day"):
        _publish(persistence, ("SSE:600000",), day=date(2026, 7, 29), ingestion_id=missing_day_id)
    invalid_id = uuid4()
    with pytest.raises(DBAPIError):
        _publish(
            persistence,
            tuple(f"SSE:{code:06d}" for code in range(1000)),
            ingestion_id=invalid_id,
        )
    with database_engine.connect() as connection:
        assert (
            connection.execute(
                text("select count(*) from ingestion.raw_manifest where ingestion_id in (:a,:b)"),
                {"a": missing_day_id, "b": invalid_id},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(text("select count(*) from regulation.st_day_snapshot")).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text(
                    """select count(*) from ingestion.ingestion_run
                        where ingestion_id in (:a,:b) and status='running'"""
                ),
                {"a": missing_day_id, "b": invalid_id},
            ).scalar_one()
            == 2
        )
