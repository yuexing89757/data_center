"""Administrator retirement binds archive evidence, current references and exact Raw."""

import gzip
import importlib.util
import json
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from test_postgres_integration import empty_database_url as empty_database_url
from test_postgres_integration import migrated_database_url as migrated_database_url

from market_data_center.raw_store import LocalRawStore


@pytest.fixture
def operator():
    path = Path(__file__).parents[1] / "scripts" / "retire_raw_before.py"
    spec = importlib.util.spec_from_file_location("raw_operator", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_offline_index_rejects_corruption_and_duplicate_paths(operator, tmp_path):
    row = {"path": "old/file.jsonl", "bytes": 12, "rows": 1, "sha256": "1" * 64}
    logical = json.dumps(row).encode() + b"\n"
    path = tmp_path / "index.gz"
    path.write_bytes(gzip.compress(logical))
    receipt = {
        "files": 1,
        "source_bytes": 12,
        "archive_sha256": "0" * 64,
        "index_sha256": sha256(path.read_bytes()).hexdigest(),
        "logical_index_sha256": sha256(logical).hexdigest(),
    }
    assert operator.index_proofs(path, receipt)[row["path"]] == row
    path.write_bytes(b"corrupt")
    with pytest.raises(RuntimeError, match="hash_mismatch"):
        operator.index_proofs(path, receipt)
    path.write_bytes(gzip.compress(logical * 2))
    receipt.update(
        files=2,
        source_bytes=24,
        index_sha256=sha256(path.read_bytes()).hexdigest(),
        logical_index_sha256=sha256(logical * 2).hexdigest(),
    )
    with pytest.raises(RuntimeError, match="counts_mismatch"):
        operator.index_proofs(path, receipt)


@pytest.mark.integration
def test_apply_rechecks_references_is_idempotent_and_keeps_manifests(
    operator, migrated_database_url, tmp_path
):
    store = LocalRawStore(tmp_path)
    index = {}
    runs, raws = [uuid4() for _ in range(4)], [uuid4() for _ in range(4)]
    data = b'{"volume":"1.5"}\n'
    with psycopg.connect(migrated_database_url, autocommit=True) as c:
        c.execute("create table realtime.retirement_test_reference(ingestion_id uuid)")
        for n, (run, raw) in enumerate(zip(runs, raws, strict=True)):
            path = f"{n}.jsonl"
            store.resolve_path(path).write_bytes(data)
            when = "2026-09-23 00:00:00+08" if n == 3 else "2026-09-01 00:00:00+08"
            c.execute(
                """insert into ingestion.ingestion_run(ingestion_id,provider_code,dataset_code,
                status,requested_at,started_at,finished_at)
                values(%s,'pytdx','daily_bar','succeeded',%s,%s,%s)""",
                (run, when, when, when),
            )
            c.execute(
                """insert into ingestion.raw_manifest(raw_id,ingestion_id,object_path,file_format,
                content_sha256,byte_size,row_count,schema_version,created_at)
                values(%s,%s,%s,'jsonl',%s,%s,1,'source.v1',%s)""",
                (raw, run, path, sha256(data).hexdigest(), len(data), when),
            )
            index[path] = {
                "path": path,
                "sha256": sha256(data).hexdigest(),
                "bytes": len(data),
                "rows": 1,
            }
        c.execute("insert into realtime.retirement_test_reference values(%s)", (runs[0],))
        before = c.execute(
            "select row_to_json(m)::text from ingestion.raw_manifest m order by raw_id"
        ).fetchall()
        receipt = {"index_sha256": "0" * 64, "archive_sha256": "1" * 64}
        plan = operator.preview(c, index, receipt, store)
        assert {r["raw_id"] for r in plan["eligible"]} == {raws[1], raws[2]}
        c.execute("insert into realtime.retirement_test_reference values(%s)", (runs[2],))
        wrong_root = tmp_path / "wrong-root"
        wrong_root.mkdir()
        with pytest.raises(RuntimeError, match="storage_identity_changed"):
            operator.apply(c, LocalRawStore(wrong_root), plan, index, receipt, tmp_path / "journal")
        path = store.resolve_path("1.jsonl")
        path.unlink()
        with pytest.raises(RuntimeError, match="first_retirement_online_raw_missing"):
            operator.apply(c, store, plan, index, receipt, tmp_path / "journal")
        assert c.execute("select count(*) from ingestion.raw_retirement").fetchone()[0] == 0
        path.write_bytes(data)
        staged = operator.apply(
            c, store, plan, index, receipt, tmp_path / "journal", register_only=True
        )
        assert staged["approved"] == 1 and staged["completed"] == 0
        assert path.exists()
        assert (
            c.execute(
                "select approved_storage_sha256 from ingestion.raw_retirement where raw_id=%s",
                (raws[1],),
            ).fetchone()[0]
            == sha256(operator.encoded(plan["storage_identity"])).hexdigest()
        )
        c.execute("update ingestion.raw_retirement set approved_storage_sha256=%s", ("f" * 64,))
        with pytest.raises(RuntimeError, match="approved_storage_conflict"):
            operator.apply(c, store, plan, index, receipt, tmp_path / "journal", register_only=True)
        assert path.exists()
        c.execute(
            "update ingestion.raw_retirement set approved_storage_sha256=%s",
            (sha256(operator.encoded(plan["storage_identity"])).hexdigest(),),
        )
        result = operator.apply(c, store, plan, index, receipt, tmp_path / "journal")
        assert result == {
            "completed": 1,
            "physical_bytes_removed_this_run": len(data),
            "newly_protected": 1,
        }
        assert not store.resolve_path("1.jsonl").exists()
        assert all(store.resolve_path(f"{n}.jsonl").exists() for n in (0, 2, 3))
        assert (
            operator.apply(c, store, plan, index, receipt, tmp_path / "journal")[
                "physical_bytes_removed_this_run"
            ]
            == 0
        )
        assert (
            c.execute(
                "select row_to_json(m)::text from ingestion.raw_manifest m order by raw_id"
            ).fetchall()
            == before
        )
        assert (
            c.execute(
                "select count(*) from ingestion.raw_retirement where online_removed_at is not null"
            ).fetchone()[0]
            == 1
        )
        c.execute("update ingestion.raw_retirement set restored_at=now()")
        path.write_bytes(data)
        with pytest.raises(RuntimeError, match="proof_changed_or_restored"):
            operator.apply(c, store, plan, index, receipt, tmp_path / "journal")


@pytest.mark.integration
def test_replay_waiting_on_retirement_cannot_create_new_reference(operator, migrated_database_url):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    raw, run = uuid4(), uuid4()
    with psycopg.connect(migrated_database_url, autocommit=True) as c:
        c.execute(
            """insert into ingestion.ingestion_run(ingestion_id,provider_code,dataset_code,
            status,requested_at,started_at,finished_at) values(%s,'pytdx','daily_bar',
            'succeeded','2026-09-01','2026-09-01','2026-09-01')""",
            (run,),
        )
        c.execute(
            """insert into ingestion.raw_manifest(raw_id,ingestion_id,object_path,
            file_format,content_sha256,byte_size,row_count,schema_version)
            values(%s,%s,'race.jsonl','jsonl',%s,0,0,'source.v1')""",
            (raw, run, "0" * 64),
        )
        ready = Event()

        def replay():
            with psycopg.connect(migrated_database_url, autocommit=True) as other:
                other.execute("set statement_timeout='5s'")
                ready.set()
                with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                    other.execute(
                        """insert into ingestion.ingestion_run(ingestion_id,
                        provider_code,dataset_code,status,requested_at,replayed_from_raw_id)
                        values(%s,'pytdx','daily_bar','pending',now(),%s)""",
                        (uuid4(), raw),
                    )

        with ThreadPoolExecutor(max_workers=1) as pool:
            with c.transaction():
                c.execute(
                    "select pg_advisory_xact_lock(hashtextextended(%s,0))",
                    ("raw-retirement:" + str(raw),),
                )
                pending = pool.submit(replay)
                assert ready.wait(2)
                # A replay that saw the old read-side eligibility now waits at creation.
                c.execute(
                    """insert into ingestion.raw_retirement(raw_id,archive_ref,
                    archive_manifest_sha256,content_sha256,content_bytes,row_count,
                    retirement_cutoff) values(%s,'test',%s,%s,0,0,'2026-09-23')""",
                    (raw, "0" * 64, "0" * 64),
                )
            pending.result(timeout=6)
        assert (
            c.execute(
                "select count(*) from ingestion.ingestion_run where replayed_from_raw_id=%s", (raw,)
            ).fetchone()[0]
            == 0
        )
