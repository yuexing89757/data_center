"""Disposable PostgreSQL only: certificate publication never rewrites facts."""

from copy import deepcopy
from dataclasses import asdict
from datetime import timedelta
from json import dumps
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from test_auction_quantity_certification import fixture
from test_postgres_integration import (
    _insert_call_auction_security_universe,
    _insert_series_round,
)
from test_postgres_integration import (
    database_engine as database_engine,
)
from test_postgres_integration import (
    empty_database_url as empty_database_url,
)
from test_postgres_integration import (
    migrated_database_url as migrated_database_url,
)

from market_data_center.auction_quantity_certification import certify_existing
from market_data_center.raw_store import LocalRawStore, RawIntegrityError

pytestmark = pytest.mark.integration


def seed(engine, root):
    session, run, _round_state, rows, facts = fixture()
    store = LocalRawStore(root)
    with engine.begin() as c:
        _insert_call_auction_security_universe(c)
        sid = _insert_series_round(
            c,
            trade_date=session["trade_date"],
            ingestion_id=run["ingestion_id"],
            ingestion_status="succeeded",
            symbols=("SSE:600000",),
            started_at=run["started_at"],
            workflow_attempt=1,
        )
        run["request_params"]["session_id"] = str(sid)
        rows[0]["session_id"] = str(sid)
        facts[0]["session_id"] = sid
        c.execute(
            text("delete from audit.quantity_unit_certification where ingestion_id=:id"),
            {"id": run["ingestion_id"]},
        )
        c.execute(
            text("delete from ingestion.raw_manifest where ingestion_id=:id"),
            {"id": run["ingestion_id"]},
        )
        c.execute(
            text(
                "update ingestion.ingestion_run set request_params=cast(:params as jsonb) "
                "where ingestion_id=:id"
            ),
            {"id": run["ingestion_id"], "params": dumps(run["request_params"])},
        )
        c.execute(
            text(
                "update realtime.call_auction_market_series_session "
                "set status='partial',finished_at=:finished where session_id=:id"
            ),
            {"id": sid, "finished": run["finished_at"] + timedelta(seconds=10)},
        )
        columns = ",".join(facts[0])
        values = ",".join(":" + k for k in facts[0])
        c.execute(
            text(
                f"insert into realtime.call_auction_market_series_snapshot ({columns}) "
                f"values ({values})"
            ),
            facts,
        )
        c.execute(
            text(
                "insert into realtime.call_auction_market_series_snapshot_history "
                "select * from realtime.call_auction_market_series_snapshot"
            )
        )
        stored = store.write_jsonl(
            provider="pytdx_hq",
            dataset="call_auction_market_series",
            partition_date=session["trade_date"],
            ingestion_id=run["ingestion_id"],
            rows=rows,
            schema_version="market_data_center.call_auction_market_series.raw.v2",
        )
        c.execute(
            text("""insert into ingestion.raw_manifest
                    (raw_id,ingestion_id,storage_backend,object_path,file_format,content_sha256,
                     byte_size,row_count,schema_version)
                    values (:raw_id,:id,'local',:object_path,:file_format,:content_sha256,
                            :byte_size,:row_count,:schema_version)"""),
            {"raw_id": uuid4(), "id": run["ingestion_id"], **asdict(stored)},
        )
    return store, run["ingestion_id"], stored


def read_api(c):
    return c.execute(
        text("select api_v1.query_call_auction_market_snapshots('2026-10-23',array['600000'])")
    ).scalar_one()


def test_preview_apply_idempotence_and_public_amount(database_engine, tmp_path):
    store, ingestion_id, stored = seed(database_engine, tmp_path)
    before = (tmp_path / stored.object_path).read_bytes()
    plan = certify_existing(database_engine, store, ingestion_id)
    assert len(plan["evidence"]["proofs"]) == 2
    with database_engine.connect() as c:
        assert read_api(c)["items"][0]["cumulative_amount"] is None
        assert (
            c.execute(text("select count(*) from audit.quantity_unit_repair_run")).scalar_one() == 0
        )
        # Exercise the actual restricted production writer role.
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    # Dedicated role engine avoids affecting other fixture connections.
    role_engine = create_engine(
        database_engine.url,
        connect_args={"options": "-c role=market_data_worker"},
        poolclass=StaticPool,
    )
    try:
        # The immutable tables intentionally deny UPDATE/row locks to Worker.
        # Maintenance apply uses an authorized administrative connection; no grants widen.
        with pytest.raises(ProgrammingError, match="permission denied"):
            certify_existing(role_engine, store, ingestion_id, plan=plan)
    finally:
        role_engine.dispose()
    assert (
        certify_existing(database_engine, store, ingestion_id, plan=plan)["status"] == "certified"
    )
    assert (
        certify_existing(database_engine, store, ingestion_id, plan=plan)["status"]
        == "already_applied"
    )
    with database_engine.connect() as c:
        item = read_api(c)["items"][0]
        assert item["cumulative_volume"] == 12300
        assert item["cumulative_amount"] == 123000
        assert (
            c.execute(text("select count(*) from audit.quantity_unit_repair_run")).scalar_one() == 1
        )
        assert (
            c.execute(
                text("select count(*) from realtime.call_auction_market_series_snapshot")
            ).scalar_one()
            == 1
        )
        assert (
            c.execute(
                text("select count(*) from realtime.call_auction_market_series_snapshot_history")
            ).scalar_one()
            == 1
        )
    assert (tmp_path / stored.object_path).read_bytes() == before


def test_changed_history_or_corrupt_raw_never_publishes_certificate(database_engine, tmp_path):
    store, ingestion_id, stored = seed(database_engine, tmp_path)
    plan = certify_existing(database_engine, store, ingestion_id)
    tampered = deepcopy(plan)
    tampered["evidence"]["sample_seq"] = 0
    with pytest.raises(ValueError):
        certify_existing(database_engine, store, ingestion_id, plan=tampered)
    with database_engine.begin() as c:
        c.execute(
            text("update realtime.call_auction_market_series_snapshot_history set bid1_volume=1")
        )
    with pytest.raises(ValueError, match="projection"):
        certify_existing(database_engine, store, ingestion_id, plan=plan)
    with database_engine.begin() as c:
        c.execute(
            text("update realtime.call_auction_market_series_snapshot_history set bid1_volume=100")
        )
    (tmp_path / stored.object_path).write_text("{}\n")
    with pytest.raises(RawIntegrityError):
        certify_existing(database_engine, store, ingestion_id, plan=plan)
    with database_engine.connect() as c:
        assert (
            c.execute(text("select count(*) from audit.quantity_unit_certification")).scalar_one()
            == 0
        )
        assert (
            c.execute(text("select count(*) from audit.quantity_unit_repair_run")).scalar_one() == 0
        )
