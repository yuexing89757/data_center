"""Only the disposable TEST_DATABASE_URL fixture may run these mutation tests."""

from datetime import date, timedelta
from json import dumps
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError
from test_postgres_integration import (
    database_engine as database_engine,
)
from test_postgres_integration import (
    empty_database_url as empty_database_url,
)
from test_postgres_integration import (
    migrated_database_url as migrated_database_url,
)

from market_data_center.quantity_unit_repair import (
    RepairScope,
    apply_plan,
    preview,
    rollback_plan,
)
from market_data_center.raw_store import LocalRawStore

pytestmark = pytest.mark.integration


def seed(engine: Engine, tmp_path: Path, *, days: int = 1, target_already: bool = False):
    store = LocalRawStore(tmp_path)
    run_id = uuid4()
    raw_id = uuid4()
    dates = [date(2026, 10, 20) + timedelta(days=offset) for offset in range(days)]
    rows = [
        {
            "date": day.strftime("%Y%m%d"),
            "price_scale": "1",
            "open": "10",
            "high": "11",
            "low": "9",
            "close": "10",
            "previous_close": "10",
            "volume": "37",
            "amount": "37000",
        }
        for day in dates
    ]
    stored = store.write_jsonl(
        provider="pytdx",
        dataset="daily_bar",
        partition_date=dates[0],
        ingestion_id=run_id,
        schema_version="pytdx.remote_daily_bar.v1",
        rows=rows,
    )
    with engine.begin() as connection:
        connection.execute(
            text("""
            insert into ingestion.ingestion_run
                (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at,
                 request_params,fetched_rows,accepted_rows)
            values (:id,'pytdx','daily_bar','succeeded',now(),now(),now(),
                    cast(:params as jsonb),:count,:count)
        """),
            {"id": run_id, "params": dumps({"source_symbol": "sh.600000"}), "count": days},
        )
        connection.execute(
            text("""
            insert into ingestion.raw_manifest
                (raw_id,ingestion_id,object_path,file_format,content_sha256,byte_size,row_count,schema_version)
            values (:raw,:id,:path,'jsonl',:hash,:size,:count,'pytdx.remote_daily_bar.v1')
        """),
            {
                "raw": raw_id,
                "id": run_id,
                "path": stored.object_path,
                "hash": stored.content_sha256,
                "size": stored.byte_size,
                "count": days,
            },
        )
        connection.execute(
            text("""
            insert into core.security
                (symbol,code,exchange,current_name,security_type,status,source_code,ingestion_id)
            values ('SSE:600000','600000','SSE','test stock','stock','listed','baostock',:id)
        """),
            {"id": run_id},
        )
        for day in dates:
            connection.execute(
                text("""
                insert into core.trading_calendar
                    (market,trade_date,is_trading_day,source_code,ingestion_id)
                values ('CN_A_SHARE',:day,true,'baostock',:id)
            """),
                {"day": day, "id": run_id},
            )
            connection.execute(
                text("""
                insert into core.daily_bar
                    (symbol,trade_date,market,open,high,low,close,previous_close,volume,amount,
                     trade_status,source_code,ingestion_id)
                values ('SSE:600000',:day,'CN_A_SHARE',10,11,9,10,10,:volume,37000,
                        'unknown','pytdx',:id)
            """),
                {"day": day, "id": run_id, "volume": 3700 if target_already else 37},
            )
    scope = RepairScope(dates[0], dates[-1], ("pytdx",))
    return store, scope, run_id


def test_preview_is_read_only_apply_is_atomic_idempotent_and_rollback_quarantines(
    database_engine, tmp_path
):
    engine = database_engine
    store, scope, original_id = seed(engine, tmp_path)
    plan = preview(engine, store, scope, page_size=1)
    assert plan["items"][0]["target_volume"] == 3700
    with engine.connect() as connection:
        assert connection.execute(text("select volume from core.daily_bar")).scalar_one() == 37
        assert (
            connection.execute(
                text("select count(*) from audit.quantity_unit_certification")
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("select count(*) from audit.quantity_unit_repair_run")
            ).scalar_one()
            == 0
        )
    assert apply_plan(engine, store, plan, plan["plan_hash"]) == {"applied": 1}
    assert apply_plan(engine, store, plan, plan["plan_hash"]) == {"applied": 1}
    with engine.connect() as connection:
        row = connection.execute(text("select volume,ingestion_id from core.daily_bar")).one()
        assert row.volume == 3700 and row.ingestion_id != original_id
        assert connection.execute(
            text("""
            select r.replayed_from_raw_id=m.raw_id and c.status='certified'
            from ingestion.ingestion_run r
            join audit.quantity_unit_certification c using(ingestion_id)
            join ingestion.raw_manifest m on m.raw_id=r.replayed_from_raw_id
            where r.ingestion_id=:id
                and c.field_group='daily_bar.volume'
        """),
            {"id": row.ingestion_id},
        ).scalar_one()
        assert (
            connection.execute(text("select count(*) from ingestion.raw_manifest")).scalar_one()
            == 1
        )
    assert rollback_plan(engine, plan, plan["plan_hash"]) == {"rolled_back": 1}
    assert rollback_plan(engine, plan, plan["plan_hash"]) == {"rolled_back": 1}
    with engine.connect() as connection:
        row = connection.execute(text("select volume,ingestion_id from core.daily_bar")).one()
        assert row.volume == 37 and row.ingestion_id == original_id
        assert (
            connection.execute(
                text("""
            select status from audit.quantity_unit_certification
            where ingestion_id=:id and field_group='daily_bar.volume'
        """),
                {"id": original_id},
            ).scalar_one()
            == "quarantined"
        )


def test_changed_before_and_after_are_conflicts(database_engine, tmp_path):
    engine = database_engine
    store, scope, original_id = seed(engine, tmp_path, days=2)
    plan = preview(engine, store, scope)
    with engine.begin() as connection:
        connection.execute(
            text("update core.daily_bar set volume=38 where trade_date=:day"),
            {"day": scope.start_date},
        )
    assert apply_plan(engine, store, plan, plan["plan_hash"]) == {"applied": 1, "conflict": 1}
    with engine.begin() as connection:
        connection.execute(
            text("update core.daily_bar set amount=37001 where trade_date=:day"),
            {"day": scope.end_date},
        )
    assert rollback_plan(engine, plan, plan["plan_hash"]) == {"conflict": 1, "rollback_conflict": 1}
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("select count(*) from core.daily_bar where ingestion_id=:id"),
                {"id": original_id},
            ).scalar_one()
            == 1
        )


def test_interrupted_batch_rolls_back_fact_certificate_and_ledger_and_resumes(
    database_engine, tmp_path, monkeypatch
):
    import market_data_center.quantity_unit_repair as repair

    engine = database_engine
    store, scope, _ = seed(engine, tmp_path, days=2)
    plan = preview(engine, store, scope)
    original_certificate = repair._certificate
    calls = 0

    def interrupt_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        original_certificate(*args, **kwargs)
        if calls == 2:
            raise RuntimeError("simulated interruption after second fact+certificate")

    monkeypatch.setattr(repair, "_certificate", interrupt_second)
    with pytest.raises(RuntimeError, match="simulated interruption"):
        apply_plan(engine, store, plan, plan["plan_hash"], batch_size=1)
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("select count(*) from core.daily_bar where volume=3700")
            ).scalar_one()
            == 1
        )
        assert (
            connection.execute(
                text("select count(*) from audit.quantity_unit_repair_item")
            ).scalar_one()
            == 1
        )
        assert (
            connection.execute(
                text("select count(*) from audit.quantity_unit_certification")
            ).scalar_one()
            == 2
        )
        assert (
            connection.execute(
                text("select next_item from audit.quantity_unit_repair_run")
            ).scalar_one()
            == 1
        )
    monkeypatch.setattr(repair, "_certificate", original_certificate)
    assert apply_plan(engine, store, plan, plan["plan_hash"], batch_size=1) == {"applied": 2}


def test_existing_correct_ingestion_needs_complete_scope_and_has_no_repair_items(
    database_engine, tmp_path
):
    engine = database_engine
    store, scope, original_id = seed(engine, tmp_path, days=2, target_already=True)
    narrow = RepairScope(scope.start_date, scope.start_date, scope.providers)
    plan = preview(engine, store, narrow)
    assert apply_plan(engine, store, plan, plan["plan_hash"]) == {}
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("select distinct status from audit.quantity_unit_certification")
            ).scalar_one()
            == "quarantined"
        )
    full = preview(engine, store, scope)
    assert apply_plan(engine, store, full, full["plan_hash"]) == {}
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("select count(*) from audit.quantity_unit_repair_item")
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text(
                    "select distinct status from audit.quantity_unit_certification "
                    "where ingestion_id=:id"
                ),
                {"id": original_id},
            ).scalar_one()
            == "certified"
        )
        prior_proof = connection.execute(
            text("""
            select field_group,evidence_hash,certified_at
            from audit.quantity_unit_certification order by field_group
        """)
        ).all()
    different_narrow_scope = RepairScope(
        narrow.start_date, narrow.end_date, narrow.providers, symbols=("SSE:600000",)
    )
    repeated_narrow = preview(engine, store, different_narrow_scope)
    assert repeated_narrow["plan_hash"] != plan["plan_hash"]
    assert apply_plan(engine, store, repeated_narrow, repeated_narrow["plan_hash"]) == {}
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("""
            select field_group,evidence_hash,certified_at
            from audit.quantity_unit_certification order by field_group
        """)
            ).all()
            == prior_proof
        )
        assert (
            connection.execute(
                text("""
            select distinct status from audit.quantity_unit_certification
        """)
            ).scalar_one()
            == "certified"
        )


def test_overlapping_approved_plans_record_already_applied_without_double_conversion(
    database_engine, tmp_path
):
    engine = database_engine
    store, scope, _ = seed(engine, tmp_path, days=2)
    full = preview(engine, store, scope)
    narrow = preview(
        engine, store, RepairScope(scope.start_date, scope.start_date, scope.providers)
    )
    assert apply_plan(engine, store, narrow, narrow["plan_hash"]) == {"applied": 1}
    assert apply_plan(engine, store, full, full["plan_hash"]) == {
        "already_applied": 1,
        "applied": 1,
    }
    with engine.connect() as connection:
        assert connection.execute(
            text("select array_agg(volume order by trade_date) from core.daily_bar")
        ).scalar_one() == [3700, 3700]
        assert (
            connection.execute(
                text(
                    "select count(*) from ingestion.ingestion_run "
                    "where replayed_from_raw_id is not null"
                )
            ).scalar_one()
            == 2
        )


def test_new_preview_after_rollback_can_repair_new_before_fingerprint(database_engine, tmp_path):
    engine = database_engine
    store, scope, _ = seed(engine, tmp_path)
    first = preview(engine, store, scope)
    assert apply_plan(engine, store, first, first["plan_hash"]) == {"applied": 1}
    assert rollback_plan(engine, first, first["plan_hash"]) == {"rolled_back": 1}
    second = preview(engine, store, scope)
    assert second["plan_hash"] != first["plan_hash"]
    assert apply_plan(engine, store, second, second["plan_hash"]) == {"applied": 1}


def test_501_row_correct_history_group_certifies_without_repair_items(database_engine, tmp_path):
    engine = database_engine
    store, scope, original_id = seed(engine, tmp_path, days=501, target_already=True)
    plan = preview(engine, store, scope)
    assert len(plan["items"]) == 501
    assert apply_plan(engine, store, plan, plan["plan_hash"]) == {}
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("select count(*) from audit.quantity_unit_repair_item")
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("""
            select count(*) from audit.quantity_unit_certification
            where ingestion_id=:id and status='certified'
        """),
                {"id": original_id},
            ).scalar_one()
            == 2
        )


def test_audit_tables_rls_and_least_privilege(database_engine):
    engine = database_engine
    with engine.connect() as connection:
        tables = (
            "quantity_unit_certification",
            "quantity_unit_repair_run",
            "quantity_unit_repair_item",
        )
        for table in tables:
            assert connection.execute(
                text("""
                select relrowsecurity from pg_class c join pg_namespace n on n.oid=c.relnamespace
                where n.nspname='audit' and c.relname=:name
            """),
                {"name": table},
            ).scalar_one()
            assert connection.execute(
                text(
                    "select has_table_privilege('market_data_worker',:table,'SELECT,INSERT,UPDATE')"
                ),
                {"table": "audit." + table},
            ).scalar_one()
            assert not connection.execute(
                text("select has_table_privilege('market_data_worker',:table,'DELETE')"),
                {"table": "audit." + table},
            ).scalar_one()
            assert not connection.execute(
                text("select has_table_privilege('market_data_api',:table,'SELECT')"),
                {"table": "audit." + table},
            ).scalar_one()
        connection.rollback()
        with connection.begin():
            connection.execute(text("set local role market_data_api"))
            with pytest.raises(DBAPIError):
                connection.execute(text("select * from audit.quantity_unit_certification"))
