"""Permanent expiry preserves facts, rejects replay and resumes after committed deletion."""

from calendar import timegm
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

from market_data_center.persistence.postgres import PostgreSQLPersistence
from market_data_center.persistence.raw_compression_postgres import RawCompressionPersistence
from market_data_center.persistence.raw_retention_postgres import RawRetentionPersistence
from market_data_center.quality_cleanup_service import CleanupBudget
from market_data_center.raw_retention_service import RawRetentionService, retention_cutoff
from market_data_center.raw_store import LocalRawStore, RawIntegrityError
from market_data_center.reliability import RawReplayService


@pytest.mark.integration
def test_monthly_purge_worker_permissions_replay_guard_cutoff_and_resume(
    database_engine, tmp_path, monkeypatch
):
    now = datetime.now(UTC)
    cutoff = retention_cutoff(now.date())
    content = b'{"volume":null}\n'
    old_run, old_raw, boundary_run, boundary_raw, replay_run = [uuid4() for _ in range(5)]
    with database_engine.begin() as c:
        for run, raw, when, path in [
            (old_run, old_raw, cutoff - timedelta(days=1), "old.jsonl"),
            (boundary_run, boundary_raw, cutoff, "boundary.jsonl"),
        ]:
            (tmp_path / path).write_bytes(content)
            c.execute(
                text("""insert into ingestion.ingestion_run
                (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
                values(:id,'pytdx','daily_bar','succeeded',:when,:when,:when)"""),
                {"id": run, "when": when},
            )
            c.execute(
                text("""insert into ingestion.raw_manifest
                (raw_id,ingestion_id,object_path,file_format,content_sha256,byte_size,row_count,
                 schema_version,created_at)
                 values(:id,:run,:path,'jsonl',:hash,:bytes,1,'test.v1',:when)"""),
                {
                    "id": raw,
                    "run": run,
                    "path": path,
                    "hash": sha256(content).hexdigest(),
                    "bytes": len(content),
                    "when": when,
                },
            )
        c.execute(text("create table realtime.retention_reference(ingestion_id uuid)"))
        c.execute(text("insert into realtime.retention_reference values(:id)"), {"id": old_run})
        c.execute(
            text("""insert into ingestion.ingestion_run
            (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,replayed_from_raw_id)
            values(:id,'pytdx','daily_bar','running',:now,:now,:raw)"""),
            {"id": replay_run, "now": now, "raw": old_raw},
        )
    repo = RawRetentionPersistence(database_engine)
    service = RawRetentionService(repo, tmp_path)
    assert (
        "raw_purge_active_replay" in service.run(now.date(), CleanupBudget(monotonic() + 30)).errors
    )
    assert (tmp_path / "old.jsonl").exists()
    with database_engine.begin() as c:
        c.execute(
            text(
                "update ingestion.ingestion_run set status='succeeded',finished_at=now() "
                "where ingestion_id=:id"
            ),
            {"id": replay_run},
        )
        c.execute(text("set local role market_data_worker"))
        for privilege in ("INSERT", "UPDATE", "DELETE"):
            assert not c.execute(
                text("select has_table_privilege(current_user,'ingestion.raw_purge',:p)"),
                {"p": privilege},
            ).scalar_one()
        with pytest.raises(DBAPIError), c.begin_nested():
            c.execute(
                text(
                    "select ingestion.declare_expired_raw_purge(:id,:future,:root,:hash,:bytes,1)"
                ),
                {
                    "id": old_raw,
                    "future": now + timedelta(days=60),
                    "root": "0" * 64,
                    "hash": sha256(content).hexdigest(),
                    "bytes": len(content),
                },
            )

    from market_data_center import raw_retention_service as module

    real_remove = module.remove_purged_raw

    def interrupted(*args, **kwargs):
        def stop():
            raise RuntimeError("after_unlink")

        kwargs["confirm"] = stop
        return real_remove(*args, **kwargs)

    def worker(c):
        c.exec_driver_sql("set local role market_data_worker")

    event.listen(database_engine, "begin", worker)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(module, "remove_purged_raw", interrupted)
            with pytest.raises(RuntimeError, match="after_unlink"):
                service.run(now.date(), CleanupBudget(monotonic() + 30))
        assert not (tmp_path / "old.jsonl").exists()
        source = PostgreSQLPersistence(database_engine).replay_source(old_run)
        assert source.raw_purged and not source.raw_offline
        for dry_run in (False, True):
            with pytest.raises(RawIntegrityError, match="permanently removed"):
                RawReplayService(
                    raw_store=LocalRawStore(tmp_path),
                    persistence=PostgreSQLPersistence(database_engine),
                ).replay(old_run, dry_run=dry_run)
        result = service.run(now.date(), CleanupBudget(monotonic() + 30))
        assert result.completed == 1 and result.removed_bytes == 0
        assert service.run(now.date(), CleanupBudget(monotonic() + 30)).completed == 0
    finally:
        event.remove(database_engine, "begin", worker)
    assert (tmp_path / "boundary.jsonl").read_bytes() == content
    with database_engine.begin() as c:
        assert c.execute(text("select count(*) from ingestion.raw_manifest")).scalar_one() == 2
        assert (
            c.execute(text("select count(*) from realtime.retention_reference")).scalar_one() == 1
        )
        assert c.execute(text("select count(*) from ingestion.raw_retirement")).scalar_one() == 0
        assert (
            c.execute(
                text("select count(*) from ingestion.raw_purge where online_removed_at is not null")
            ).scalar_one()
            == 1
        )
        with pytest.raises(DBAPIError), c.begin_nested():
            c.execute(text("select ingestion.assert_raw_restorable(:id)"), {"id": old_raw})
        with pytest.raises(DBAPIError), c.begin_nested():
            c.execute(
                text("""insert into ingestion.ingestion_run(ingestion_id,provider_code,dataset_code,
                status,requested_at,started_at,replayed_from_raw_id)
                values(:id,'pytdx','daily_bar','running',:now,:now,:raw)"""),
                {"id": uuid4(), "now": now, "raw": old_raw},
            )
    assert not RawCompressionPersistence(database_engine).candidates(cutoff).manifests


@pytest.mark.integration
def test_orphan_purge_is_bounded_and_blocks_late_registration(database_engine):
    now = datetime.now(UTC)
    cutoff = retention_cutoff(now.date())
    cutoff_ns = timegm(cutoff.utctimetuple()) * 1_000_000_000
    run_id = uuid4()
    path = f"pytdx/daily_bar/year=2026/month=01/day=01/{run_id}.jsonl"
    repo = RawRetentionPersistence(database_engine)
    row = {
        "object_path": path,
        "physical_sha256": sha256(b"").hexdigest(),
        "physical_bytes": 0,
        "mtime_ns": cutoff_ns - 1,
        "ctime_ns": cutoff_ns - 1,
    }
    with database_engine.begin() as c:
        c.execute(
            text("""insert into ingestion.ingestion_run(ingestion_id,provider_code,
            dataset_code,status,requested_at,started_at,finished_at)
            values(:id,'pytdx','daily_bar','succeeded',:old,:old,:old)"""),
            {"id": run_id, "old": cutoff - timedelta(days=1)},
        )

    def worker(c):
        c.exec_driver_sql("set local role market_data_worker")

    event.listen(database_engine, "begin", worker)
    try:
        audit_id = repo.register_orphan("0" * 64, row, cutoff)
        assert audit_id is not None
        assert repo.register_orphan("0" * 64, row, cutoff) == audit_id
        with pytest.raises(DBAPIError):
            repo.register_orphan(
                "0" * 64,
                row | {"object_path": path.replace("pytdx/", "other/"), "mtime_ns": cutoff_ns},
                cutoff,
            )
        repo.confirm_orphan(audit_id, "0" * 64)
        repo.confirm_orphan(audit_id, "0" * 64)
    finally:
        event.remove(database_engine, "begin", worker)
    with database_engine.begin() as c:
        with pytest.raises(DBAPIError), c.begin_nested():
            c.execute(
                text("""insert into ingestion.raw_manifest(raw_id,ingestion_id,object_path,
                file_format,content_sha256,byte_size,row_count,schema_version)
                values(:raw,:run,:path,'jsonl',:hash,0,0,'test.v1')"""),
                {"raw": uuid4(), "run": run_id, "path": path, "hash": row["physical_sha256"]},
            )
        assert c.execute(text("select count(*) from ingestion.raw_manifest")).scalar_one() == 0
        assert c.execute(text("select count(*) from ingestion.raw_orphan_purge")).scalar_one() == 1
