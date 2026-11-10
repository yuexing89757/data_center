"""Direct deletion/RLS/lineage tests on disposable PostgreSQL, never production."""

from datetime import datetime, timedelta
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from test_postgres_integration import (  # noqa: F401
    database_engine,
    empty_database_url,
    migrated_database_url,
)
from test_regulation_query_postgres import regulation_database  # noqa: F401

from market_data_center.direct_retention_service import DirectRetentionService, retention_cutoff
from market_data_center.domain.regulation import MonitorState
from market_data_center.persistence.direct_retention_postgres import (
    PostgreSQLDirectRetentionPersistence,
)
from market_data_center.persistence.regulation_postgres import PostgreSQLRegulationPersistence
from market_data_center.quality_cleanup_service import CleanupBudget
from market_data_center.regulation_monitor_codec import encode_monitor

pytestmark = pytest.mark.integration


def worker_engine(engine):
    from sqlalchemy import create_engine, event

    worker = create_engine(engine.url)

    @event.listens_for(worker, "connect")
    def set_worker(connection, record):
        with connection.cursor() as cursor:
            cursor.execute("set role market_data_worker")
        connection.commit()

    return worker


def test_quality_all_rules_delete_only_before_fifteen_day_boundary(database_engine):  # noqa: F811
    with database_engine.begin() as c:
        today = c.scalar(text("select (current_timestamp at time zone 'Asia/Shanghai')::date"))
        boundary = datetime.combine(
            retention_cutoff(today), datetime.min.time(), ZoneInfo("Asia/Shanghai")
        )
        ingestion = uuid4()
        c.execute(
            text("""insert into ingestion.ingestion_run
          (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
          values (:id,'baostock','security','succeeded',now(),now(),now())"""),
            {"id": ingestion},
        )
        for rule, severity, created in [
            ("any.error", "error", boundary - timedelta(seconds=1)),
            ("any.info", "info", boundary - timedelta(days=2)),
            ("boundary", "warning", boundary),
            ("recent", "info", boundary + timedelta(seconds=1)),
        ]:
            c.execute(
                text("""insert into audit.quality_result
              (ingestion_id,dataset_code,rule_code,severity,status,message,created_at)
              values (:id,'security',:rule,:severity,'failed','fixture',:created)"""),
                {"id": ingestion, "rule": rule, "severity": severity, "created": created},
            )
    worker = worker_engine(database_engine)
    try:
        service = DirectRetentionService(PostgreSQLDirectRetentionPersistence(worker))
        assert service.preview(today)["quality"]["rows"] == 2
        result = service.run(today, CleanupBudget(1e20))
        assert result.deleted_rows == 2 and not result.partial
        assert service.run(today, CleanupBudget(1e20)).deleted_rows == 0
        # Even a caller supplying a later cutoff cannot bypass the RLS boundary.
        with worker.begin() as c:
            assert c.execute(text("delete from audit.quality_result")).rowcount == 0
        with database_engine.connect() as c:
            assert c.execute(
                text("select rule_code from audit.quality_result order by rule_code")
            ).scalars().all() == ["boundary", "recent"]
            assert c.scalar(text("select count(*) from audit.quality_archive")) == 0
    finally:
        worker.dispose()


@pytest.mark.parametrize("concurrent_checkpoint", [False, True])
def test_monitor_purge_preserves_events_and_marks_expired_snapshot(
    regulation_database,  # noqa: F811
    concurrent_checkpoint,
):
    engine = regulation_database
    ids = [uuid4(), uuid4()]
    with engine.begin() as c:
        today = c.scalar(text("select (current_timestamp at time zone 'Asia/Shanghai')::date"))
        before = retention_cutoff(today)
        old_day = before - timedelta(days=1)
        while old_day.weekday() > 4:
            old_day -= timedelta(days=1)
        next_day = old_day + timedelta(days=1)
        while next_day.weekday() > 4:
            next_day += timedelta(days=1)
        c.execute(
            text("""insert into core.trading_calendar
              (market,trade_date,is_trading_day,source_code,ingestion_id)
              select 'CN_A_SHARE',d::date,extract(isodow from d)<6,'baostock',s.ingestion_id
              from generate_series(cast(:start as date),cast(:end as date),interval '1 day') d
              cross join core.security s where s.symbol='SSE:600000'
              on conflict do nothing"""),
            {"start": old_day - timedelta(days=5), "end": today + timedelta(days=3)},
        )
        for id_, day in zip(ids, [old_day, before], strict=True):
            c.execute(
                text("""insert into regulation.calculation_run
              (calculation_id,trade_date,next_trade_date,status,algorithm_version,rule_set_version,
               rule_set_hash,scenario_config_version,input_hash,market_watermark,capital_watermark,
               event_watermark,expected_count,complete_count,incomplete_count,not_applicable_count,
               started_at,completed_at)
              select :id,:day,:next,'SUCCEEDED','regulation-monitor.v3',rule_set_version,
               rule_set_hash,scenario_config_version,:hash,market_watermark,capital_watermark,
               event_watermark,1,1,0,0,started_at,completed_at
              from regulation.calculation_run where calculation_id=:source"""),
                {
                    "id": id_,
                    "day": day,
                    "next": next_day if id_ == ids[0] else day + timedelta(days=1),
                    "hash": id_.hex * 2,
                    "source": UUID(int=2),
                },
            )
            c.execute(
                text(
                    "insert into regulation.monitor_context(calculation_id,source) "
                    "values (:id,jsonb_build_object('monitor_start_date',cast(:day as text)))"
                ),
                {"id": id_, "day": day},
            )
            c.execute(
                text("""insert into regulation.monitor_input
                    (calculation_id,symbol,candidate_reasons,payload)
              values (:id,'SSE:600000','{}',cast(:payload as jsonb))"""),
                {
                    "id": id_,
                    "payload": encode_monitor(
                        {"state": MonitorState("SSE:600000", day, True, None, None, (), (), ())}
                    ),
                },
            )
        c.execute(
            text("""insert into regulation.calculated_event
          (calculation_id,symbol,trade_date,level,direction,kind,rule_codes,windows)
          values (:id,'SSE:600000',:day,'ABNORMAL','UP',
                  'CUMULATIVE_DEVIATION','{fixture}','[]')"""),
            {"id": ids[0], "day": old_day},
        )
    worker = worker_engine(engine)
    try:
        repo = PostgreSQLDirectRetentionPersistence(worker)
        service = DirectRetentionService(repo)
        if concurrent_checkpoint:
            committed = []

            def purge_after_snapshot(conn, cursor, statement, params, context, many):
                if "inputs_expired_at" in statement and not committed:
                    committed.append(service.run(today, CleanupBudget(1e20)))

            event.listen(engine, "after_cursor_execute", purge_after_snapshot)
            try:
                parent, states = PostgreSQLRegulationPersistence(engine).load_monitor_checkpoint(
                    next_day
                )
                assert parent == ids[0]
                assert len(states) == 1 and states[0].symbol == "SSE:600000"
            finally:
                event.remove(engine, "after_cursor_execute", purge_after_snapshot)
            result = committed[0]
        else:
            result = service.run(today, CleanupBudget(1e20))
        assert result.monitor_input_deleted_rows == 1 and not result.partial
        with engine.connect() as c:
            assert c.scalar(text("select count(*) from regulation.calculated_event")) == 1
            assert c.scalar(
                text(
                    "select inputs_expired_at is not null from regulation.monitor_context "
                    "where calculation_id=:id"
                ),
                {"id": ids[0]},
            )
            assert (
                c.scalar(
                    text("select count(*) from regulation.monitor_input where calculation_id=:id"),
                    {"id": ids[1]},
                )
                == 1
            )
        with worker.begin() as c:
            assert c.execute(text("delete from regulation.monitor_input")).rowcount == 0
        with pytest.raises(DBAPIError) as expired, engine.connect() as c:
            c.execute(
                text("select api_v1.query_regulation_monitor_inputs(:day,:id,array['600000'])"),
                {"day": old_day, "id": ids[0]},
            )
        assert expired.value.orig.sqlstate == "P0002"
        with pytest.raises(ValueError, match="checkpoint inputs expired"):
            PostgreSQLRegulationPersistence(engine).load_monitor_checkpoint(next_day)
        with pytest.raises(DBAPIError), worker.begin() as c:
            c.execute(
                text("""insert into regulation.monitor_input
                    (calculation_id,symbol,candidate_reasons,payload)
                  values (:id,'SSE:600001','{}','{}')"""),
                {"id": ids[0]},
            )
    finally:
        worker.dispose()
