"""Offline Raw is explicit and never retried as a missing compression candidate."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_postgres_integration import database_engine as database_engine
from test_postgres_integration import empty_database_url as empty_database_url
from test_postgres_integration import migrated_database_url as migrated_database_url

from market_data_center.domain.ingestion import DatasetCode, ProviderCode, ReplaySource
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.persistence.raw_compression_postgres import RawCompressionPersistence
from market_data_center.raw_store import LocalRawStore, RawIntegrityError
from market_data_center.reliability import RawReplayService


def test_offline_replay_stops_before_creating_run_or_reading_raw(tmp_path):
    source = ReplaySource(
        uuid4(),
        ProviderCode.PYTDX,
        DatasetCode.DAILY_BAR,
        datetime.now(UTC),
        {},
        None,
        raw_offline=True,
    )

    class Repository:
        def replay_source(self, _):
            return source

        def create_ingestion_run(self, _):
            pytest.fail("offline replay must not create an ingestion run")

    for dry_run in (True, False):
        with pytest.raises(RawIntegrityError, match="archived offline"):
            RawReplayService(raw_store=LocalRawStore(tmp_path), persistence=Repository()).replay(
                source.source_ingestion_id, dry_run=dry_run
            )


@pytest.mark.integration
def test_retirement_candidates_replay_flags_restore_and_permissions(database_engine):
    run_id, raw_id = uuid4(), uuid4()
    cutoff = datetime.now(UTC) - timedelta(days=30)
    old = cutoff - timedelta(days=1)
    with database_engine.begin() as c:
        c.execute(
            text("""insert into ingestion.ingestion_run
          (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
          values(:id,'pytdx','daily_bar','succeeded',:old,:old,:old)"""),
            {"id": run_id, "old": old},
        )
        c.execute(
            text("""insert into ingestion.raw_manifest
          (raw_id,ingestion_id,object_path,file_format,content_sha256,byte_size,row_count,
           schema_version,created_at)
          values(:raw,:run,'test/offline.jsonl','jsonl',:hash,0,0,'test.v1',:old)"""),
            {"raw": raw_id, "run": run_id, "hash": "0" * 64, "old": old},
        )
        original = c.execute(
            text("select row_to_json(m)::text from ingestion.raw_manifest m")
        ).scalar_one()
    compression = RawCompressionPersistence(database_engine)
    repository = PostgreSQLPersistence(database_engine)
    assert len(compression.candidates(cutoff).manifests) == 1
    assert not repository.replay_source(run_id).raw_offline
    with database_engine.begin() as c:
        c.execute(
            text("""insert into ingestion.raw_retirement
          (raw_id,archive_ref,archive_manifest_sha256,content_sha256,content_bytes,row_count,
           retirement_cutoff)
          values(:id,'synthetic-offline-set',:hash,:hash,0,0,:cutoff)"""),
            {"id": raw_id, "hash": "0" * 64, "cutoff": cutoff.date()},
        )
    assert not compression.candidates(cutoff).manifests
    assert repository.replay_source(run_id).raw_offline
    with database_engine.begin() as c:
        assert (
            c.execute(
                text("select row_to_json(m)::text from ingestion.raw_manifest m")
            ).scalar_one()
            == original
        )
        assert c.execute(
            text(
                "select has_table_privilege('market_data_worker',"
                "'ingestion.raw_retirement','SELECT')"
            )
        ).scalar_one()
        for role in ("market_data_api", "anon", "authenticated"):
            assert not c.execute(
                text("select has_table_privilege(:role,'ingestion.raw_retirement','SELECT')"),
                {"role": role},
            ).scalar_one()
        for privilege in ("INSERT", "UPDATE", "DELETE"):
            assert not c.execute(
                text(
                    "select has_table_privilege('market_data_worker',"
                    "'ingestion.raw_retirement',:privilege)"
                ),
                {"privilege": privilege},
            ).scalar_one()
        c.execute(
            text("update ingestion.raw_retirement set restored_at=now() where raw_id=:id"),
            {"id": raw_id},
        )
    assert len(compression.candidates(cutoff).manifests) == 1
    assert not repository.replay_source(run_id).raw_offline
