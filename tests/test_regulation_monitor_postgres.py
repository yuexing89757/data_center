"""Monitor RPC and privileges against a disposable database, never the configured production DB."""

import base64
import json
from dataclasses import replace
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from test_postgres_integration import (  # noqa: F401
    database_engine,
    empty_database_url,
    migrated_database_url,
)
from test_regulation_calculator import _candidate, _daily, _input, _rule
from test_regulation_persistence import _run
from test_regulation_query_postgres import regulation_database  # noqa: F401

from market_data_center.domain.regulation import RegulationDirection, RegulationRunStatus
from market_data_center.persistence.regulation_postgres import (
    PostgreSQLRegulationPersistence,
    _watermark,
)
from market_data_center.regulation_calculator import calculate_monitor_day, calculate_regulation
from market_data_center.regulation_monitor_codec import monitor_input_hash

pytestmark = pytest.mark.integration


def test_monitor_limits_are_rejected_with_parameter_error(database_engine):  # noqa: F811
    with database_engine.begin() as connection:
        with pytest.raises(DBAPIError) as error:
            connection.execute(
                text("""
                select api_v1.query_regulation_monitor_candidates(
                    date '2026-09-29', 101, null, null)
            """)
            )
        assert error.value.orig.sqlstate == "22023"


def test_api_role_cannot_access_or_write_monitor_tables(database_engine):  # noqa: F811
    with database_engine.connect() as connection:
        assert connection.execute(
            text("""
            select has_table_privilege('market_data_api', 'regulation.calculated_event', 'SELECT'),
                   has_table_privilege('market_data_api', 'regulation.monitor_input', 'INSERT'),
                   has_table_privilege('market_data_worker',
                       'regulation.calculated_event', 'INSERT'),
                   has_table_privilege('market_data_worker',
                       'regulation.calculated_event', 'UPDATE')
        """)
        ).one() == (False, False, True, False)


def test_publish_read_isolation_nulls_and_legacy_version_routing(regulation_database):  # noqa: F811
    engine = regulation_database
    persistence = PostgreSQLRegulationPersistence(engine)
    st_ingestion_ids = {}
    with engine.begin() as c:
        c.execute(
            text("""insert into core.trading_calendar
            (market,trade_date,is_trading_day,source_code,ingestion_id)
            select 'CN_A_SHARE',d::date,true,'baostock',
                (select ingestion_id from ingestion.ingestion_run limit 1)
            from generate_series(date '2026-07-06',date '2026-07-08',interval '1 day') d""")
        )
        for st_day in (date(2026, 7, 7), date(2026, 7, 8)):
            st_id = uuid4()
            st_ingestion_ids[st_day] = st_id
            c.execute(
                text("""insert into ingestion.ingestion_run
                    (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
                    values (:id,'tushare','regulation_st_snapshot','succeeded',
                        now(),now(),now())"""),
                {"id": st_id},
            )
            c.execute(
                text("""insert into regulation.st_day_snapshot
                    (trade_date,symbols,symbol_count,source_code,ingestion_id)
                    values (:day,'["SSE:600004"]'::jsonb,1,'tushare',:id)"""),
                {"day": st_day, "id": st_id},
            )
    day, next_day = date(2026, 7, 6), date(2026, 7, 7)
    candidate = _candidate((_daily(day, ".1", "-.1"),))
    candidate = replace(
        candidate, next_day_price_limit=replace(candidate.next_day_price_limit, trade_date=next_day)
    )
    legacy = replace(
        _input((_rule(),), candidate),
        trade_date=day,
        next_trade_date=next_day,
        trading_dates=(day,),
    )
    old_output = calculate_regulation(legacy)
    old_run = _run(
        calculation_id=uuid4(),
        trade_date=day,
        next_trade_date=next_day,
        input_hash="c" * 64,
        coverage=old_output.coverage,
    )
    persistence.start_calculation(
        replace(old_run, status=RegulationRunStatus.RUNNING, completed_at=None), ()
    )
    persistence.publish_calculation(old_run, old_output)
    second = replace(candidate, symbol="SSE:600001", next_day_price_limit=None)
    src = replace(legacy, algorithm_version="regulation-monitor.v2", candidates=(candidate, second))
    result = calculate_monitor_day(src, ())
    run = _run(
        calculation_id=uuid4(),
        input_hash=monitor_input_hash(src, (), None),
        algorithm_version=src.algorithm_version,
        trade_date=day,
        next_trade_date=next_day,
        coverage=result.output.coverage,
    )
    persistence.start_calculation(
        replace(run, status=RegulationRunStatus.RUNNING, completed_at=None), ()
    )
    with engine.connect() as c:
        official_before = c.execute(text("select count(*) from regulation.event")).scalar_one()
    persistence.publish_monitor_day(
        run, src, result, parent_calculation_id=None, previous_states=()
    )
    parent, states = persistence.load_monitor_checkpoint(next_day)
    assert parent == run.calculation_id
    assert states == result.states
    with engine.connect() as c:
        candidates = c.execute(
            text("select api_v1.query_regulation_monitor_candidates('2026-07-07',50,null,null)")
        ).scalar_one()
        assert candidates["calculation_id"] == str(run.calculation_id)
        assert candidates["items"][0]["code"] == "600000"
        assert candidates["official_coverage"] == "UNKNOWN"
        inputs = c.execute(
            text(
                "select api_v1.query_regulation_monitor_inputs("
                "'2026-07-07',:id,array['600000','999999'])"
            ),
            {"id": run.calculation_id},
        ).scalar_one()
        assert inputs["found_count"] == 1 and inputs["missing_count"] == 1
        assert inputs["items"][0]["payload"]["state"]["events"][0]["direction"] == "UP"
        assert inputs["items"][1]["payload"] is None
        assert inputs["items"][0]["next_day_reference_safe"] is True
        assert inputs["items"][0]["next_day_st_verified"] is True
        with c.begin_nested() as check:
            c.execute(text("delete from regulation.st_day_snapshot where trade_date='2026-07-08'"))
            conditional = c.execute(
                text("""select api_v1.query_regulation_monitor_inputs(
                    '2026-07-07',:id,array['600000'])"""),
                {"id": run.calculation_id},
            ).scalar_one()
            assert conditional["items"][0]["next_day_reference_safe"] is True
            assert conditional["items"][0]["next_day_st_verified"] is False
            check.rollback()
        with c.begin_nested() as check:
            c.execute(
                text("""update regulation.st_day_snapshot
                    set symbols='["SSE:600000"]'::jsonb where trade_date='2026-07-08'""")
            )
            known_st = c.execute(
                text("""select api_v1.query_regulation_monitor_inputs(
                    '2026-07-07',:id,array['600000'])"""),
                {"id": run.calculation_id},
            ).scalar_one()
            assert known_st["items"][0]["next_day_reference_safe"] is False
            assert known_st["items"][0]["next_day_st_verified"] is True
            check.rollback()
        assert inputs["items"][0]["target_applicability"] == "INSUFFICIENT_DATA"
        assert inputs["items"][0]["target_applicability_reason"] == "listing_calendar_unverified"
        with c.begin_nested() as check:
            c.execute(
                text(
                    "update core.security_name_history set name='ST测试' where symbol='SSE:600000'"
                )
            )
            changed = c.execute(
                text(
                    "select api_v1.query_regulation_monitor_inputs("
                    "'2026-07-07',:id,array['600000'])"
                ),
                {"id": run.calculation_id},
            ).scalar_one()
            assert (
                changed["items"][0]["target_applicability_reason"] == "listing_calendar_unverified"
            )
            c.execute(
                text("""update regulation.st_day_snapshot
                    set symbols='["SSE:600000"]'::jsonb where trade_date='2026-07-07'""")
            )
            changed = c.execute(
                text(
                    "select api_v1.query_regulation_monitor_inputs("
                    "'2026-07-07',:id,array['600000'])"
                ),
                {"id": run.calculation_id},
            ).scalar_one()
            assert changed["items"][0]["target_applicability"] == "NOT_APPLICABLE"
            assert changed["items"][0]["target_applicability_reason"] == "st_security_excluded"
            c.execute(text("delete from regulation.st_day_snapshot where trade_date='2026-07-07'"))
            missing_st = c.execute(
                text(
                    "select api_v1.query_regulation_monitor_inputs("
                    "'2026-07-07',:id,array['600000'])"
                ),
                {"id": run.calculation_id},
            ).scalar_one()
            assert missing_st["items"][0]["target_applicability_reason"] == (
                "missing_regulation_st_snapshot"
            )
            check.rollback()
        assert inputs["items"][1]["next_day_reference_safe"] is False
        assert inputs["reference_checked_at"] is not None
        old = c.execute(
            text("select regulation.query_context('2026-07-06',null,50,'triggers')")
        ).scalar_one()
        assert old["metadata"]["calculation_id"] == str(old_run.calculation_id)
        assert (
            c.execute(text("select count(*) from regulation.event")).scalar_one() == official_before
        )
    with pytest.raises((DBAPIError, ValueError)):
        persistence.publish_monitor_day(
            run, src, result, parent_calculation_id=None, previous_states=()
        )
    with engine.connect() as c:
        assert (
            c.execute(
                text("select count(*) from regulation.calculated_event where calculation_id=:id"),
                {"id": run.calculation_id},
            ).scalar_one()
            == 2
        )
    # Pagination is tied to a single date/batch/search and cannot be replayed elsewhere.
    with engine.connect() as c:
        page = c.execute(
            text("select api_v1.query_regulation_monitor_candidates('2026-07-07',1,null,null)")
        ).scalar_one()
        assert page["total"] == 2 and len(page["items"]) == 1
        page2 = c.execute(
            text("select api_v1.query_regulation_monitor_candidates('2026-07-07',1,:cursor,null)"),
            {"cursor": page["next_cursor"]},
        ).scalar_one()
        assert page2["items"][0]["code"] == "600001" and page2["next_cursor"] is None
        cursor = json.loads(base64.urlsafe_b64decode(page["next_cursor"] + "=="))
        for field, value in [
            ("symbol", None),
            ("calculation_id", None),
            ("trade_date", "2026-07-08"),
        ]:
            malformed = (
                base64.urlsafe_b64encode(json.dumps({**cursor, field: value}).encode())
                .decode()
                .rstrip("=")
            )
            with c.begin_nested():
                with pytest.raises(DBAPIError) as error:
                    c.execute(
                        text(
                            "select api_v1.query_regulation_monitor_candidates("
                            "'2026-07-07',1,:cursor,null)"
                        ),
                        {"cursor": malformed},
                    )
                assert error.value.orig.sqlstate == "22023"
                c.get_nested_transaction().rollback()
        for codes in [[], ["600000"] * 51, [None]]:
            with c.begin_nested():
                with pytest.raises(DBAPIError) as error:
                    c.execute(
                        text(
                            "select api_v1.query_regulation_monitor_inputs('2026-07-07',:id,:codes)"
                        ),
                        {"id": run.calculation_id, "codes": codes},
                    )
                assert error.value.orig.sqlstate == "22023"
                c.get_nested_transaction().rollback()

    # A constraint failure after snapshot insertion rolls back the entire publication.
    changed = replace(src, market_watermark="changed")
    bad_run = replace(run, calculation_id=uuid4(), input_hash=monitor_input_hash(changed, (), None))
    persistence.start_calculation(
        replace(bad_run, status=RegulationRunStatus.RUNNING, completed_at=None), ()
    )
    invalid = replace(
        result, events=(replace(result.events[0], direction=RegulationDirection.NONE),)
    )
    with pytest.raises(DBAPIError):
        persistence.publish_monitor_day(
            bad_run, changed, invalid, parent_calculation_id=None, previous_states=()
        )
    with engine.connect() as c:
        assert (
            c.execute(
                text("select count(*) from regulation.monitor_context where calculation_id=:id"),
                {"id": bad_run.calculation_id},
            ).scalar_one()
            == 0
        )

    # A corrected ancestor makes yesterday's old descendant unusable.
    day2 = replace(
        src,
        trade_date=next_day,
        next_trade_date=date(2026, 7, 8),
        st_watermark=_watermark(({"ingestion_id": st_ingestion_ids[next_day]},)),
        trading_dates=(day, next_day),
        candidates=tuple(
            replace(
                c,
                daily_returns=(*c.daily_returns, _daily(next_day, "0", "0")),
                next_day_price_limit=None,
            )
            for c in src.candidates
        ),
    )
    result2 = calculate_monitor_day(day2, result.states)
    run2 = replace(
        run,
        calculation_id=uuid4(),
        trade_date=next_day,
        next_trade_date=day2.next_trade_date,
        input_hash=monitor_input_hash(day2, result.states, run.calculation_id),
        coverage=result2.output.coverage,
    )
    persistence.start_calculation(
        replace(run2, status=RegulationRunStatus.RUNNING, completed_at=None), ()
    )
    persistence.publish_monitor_day(
        run2, day2, result2, parent_calculation_id=run.calculation_id, previous_states=result.states
    )
    assert persistence.load_monitor_checkpoint(day2.next_trade_date)[0] == run2.calculation_id
    correction = replace(bad_run, completed_at=run.completed_at + timedelta(minutes=1))
    persistence.publish_monitor_day(
        correction, changed, result, parent_calculation_id=None, previous_states=()
    )
    with pytest.raises(ValueError, match="superseded"):
        persistence.load_monitor_checkpoint(day2.next_trade_date)
    with engine.connect() as c, pytest.raises(DBAPIError) as error:
        c.execute(
            text("select api_v1.query_regulation_monitor_inputs('2026-07-07',:id,array['600000'])"),
            {"id": run2.calculation_id},
        )
    assert error.value.orig.sqlstate == "P0004"
    with engine.begin() as c:
        corrected_st_id = uuid4()
        c.execute(
            text("""insert into ingestion.ingestion_run
                (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
                values (:id,'tushare','regulation_st_snapshot','succeeded',now(),now(),now())"""),
            {"id": corrected_st_id},
        )
        c.execute(
            text("""insert into regulation.st_day_snapshot
                (trade_date,symbols,symbol_count,source_code,ingestion_id)
                values ('2026-07-06','["SSE:600004"]'::jsonb,1,'tushare',:id)"""),
            {"id": corrected_st_id},
        )
    with engine.connect() as c:
        assert (
            c.execute(
                text("select regulation.monitor_chain_current(:id)"), {"id": run.calculation_id}
            ).scalar_one()
            is False
        )
