"""Isolated PostgreSQL coverage of sealed approval, permissions and crash resumption."""

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from time import monotonic
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from test_postgres_integration import database_engine as database_engine
from test_postgres_integration import empty_database_url as empty_database_url
from test_postgres_integration import migrated_database_url as migrated_database_url

from market_data_center.persistence.raw_retention_postgres import RawRetentionPersistence
from market_data_center.quality_cleanup_service import CleanupBudget
from market_data_center.raw_retention_service import RawRetentionService, retention_cutoff
from market_data_center.raw_retirement import approved_storage_hash
from market_data_center.raw_store import LocalRawStore


@pytest.mark.integration
def test_sealed_queue_idempotence_reference_drift_and_worker_grants(
    database_engine, tmp_path, monkeypatch
):
    now = datetime.now(UTC)
    cutoff = retention_cutoff(now.date())
    old = cutoff - timedelta(days=1)
    run_id, raw_id = uuid4(), uuid4()
    content = b'{"amount":null}\n'
    root_hash = approved_storage_hash(LocalRawStore(tmp_path))
    path = tmp_path / "sealed.jsonl"
    path.write_bytes(content)
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
            values(:id,:run,'sealed.jsonl','jsonl',:hash,:bytes,1,'test.v1',:old)"""),
            {
                "id": raw_id,
                "run": run_id,
                "hash": sha256(content).hexdigest(),
                "bytes": len(content),
                "old": old,
            },
        )
        c.execute(
            text("""insert into ingestion.raw_retirement
            (raw_id,archive_ref,archive_manifest_sha256,content_sha256,content_bytes,row_count,
             retirement_cutoff,approved_storage_sha256)
            values(:id,'independent-verified',:index,:hash,:bytes,1,:cutoff,:root)"""),
            {
                "id": raw_id,
                "index": "0" * 64,
                "hash": sha256(content).hexdigest(),
                "bytes": len(content),
                "cutoff": cutoff.date(),
                "root": root_hash,
            },
        )
        c.execute(text("create table realtime.retention_drift(ingestion_id uuid)"))
        c.execute(text("insert into realtime.retention_drift values(:id)"), {"id": run_id})
    repo = RawRetentionPersistence(database_engine)
    service = RawRetentionService(repo, tmp_path)
    result = service.run(now.date(), CleanupBudget(monotonic() + 30))
    assert result.partial and path.exists()  # A new reference after approval blocks removal.
    with database_engine.begin() as c:
        c.execute(text("delete from realtime.retention_drift"))
        c.execute(text("set local role market_data_worker"))
        for privilege in ("INSERT", "UPDATE", "DELETE"):
            assert not c.execute(
                text("""select has_table_privilege(current_user,
                'ingestion.raw_retirement',:privilege)"""),
                {"privilege": privilege},
            ).scalar_one()
        with pytest.raises(DBAPIError), c.begin_nested():
            c.execute(
                text("select ingestion.lock_approved_raw_retirement(:id,:future,:root)"),
                {"id": raw_id, "future": now + timedelta(days=60), "root": root_hash},
            )
    with repo.locked(raw_id, cutoff, "f" * 64) as item:
        assert item is None
    # Supported restore acquires this lock before touching files; a concurrent cleanup waits.
    with database_engine.begin() as restoring:
        restoring.execute(
            text("select pg_advisory_xact_lock(hashtextextended(:key,0))"),
            {"key": "raw-retirement:" + str(raw_id)},
        )
        with database_engine.begin() as cleaning:
            cleaning.execute(text("set local role market_data_worker"))
            cleaning.execute(text("set local lock_timeout='50ms'"))
            with pytest.raises(DBAPIError), cleaning.begin_nested():
                cleaning.execute(
                    text("select ingestion.lock_approved_raw_retirement(:id,:cutoff,:root)"),
                    {"id": raw_id, "cutoff": cutoff, "root": root_hash},
                )
        assert path.exists()

    def as_worker(connection):
        connection.exec_driver_sql("set local role market_data_worker")

    from market_data_center import raw_retention_service as module

    real_remove = module.remove_retired_raw

    def interrupted_remove(*args, **kwargs):
        def interrupt():
            raise RuntimeError("interrupted_after_unlink")

        kwargs["confirm"] = interrupt
        return real_remove(*args, **kwargs)

    event.listen(database_engine, "begin", as_worker)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(module, "remove_retired_raw", interrupted_remove)
            with pytest.raises(RuntimeError, match="interrupted_after_unlink"):
                service.run(now.date(), CleanupBudget(monotonic() + 30))
        assert not path.exists()  # Approval survived rollback, so absence can resume safely.
        result = service.run(now.date(), CleanupBudget(monotonic() + 30))
        assert result.removed_bytes == 0
    finally:
        event.remove(database_engine, "begin", as_worker)
    assert result.completed == 1 and not path.exists()
    assert service.run(now.date(), CleanupBudget(monotonic() + 30)).completed == 0
    with database_engine.begin() as c:
        assert c.execute(text("select count(*) from ingestion.raw_manifest")).scalar_one() == 1
        c.execute(
            text("update ingestion.raw_retirement set restored_at=now() where raw_id=:id"),
            {"id": raw_id},
        )
    path.write_bytes(content)
    assert service.run(now.date(), CleanupBudget(monotonic() + 30)).completed == 0
    assert path.read_bytes() == content
