"""Exercise public RPCs on an isolated migrated PostgreSQL database, never production."""

from base64 import urlsafe_b64decode, urlsafe_b64encode
from dataclasses import replace
from datetime import date, datetime
from json import dumps, loads
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError
from test_postgres_integration import (  # noqa: F401 - shared disposable database fixtures
    database_engine,
    empty_database_url,
    migrated_database_url,
)
from test_regulation_persistence import _run

from market_data_center.domain.regulation import RegulationRunStatus
from market_data_center.persistence.regulation_postgres import (
    PostgreSQLRegulationPersistence,
    _event_from_row,
)
from market_data_center.public_api.models import (
    RegulationRecentNextTriggerResponse,
    RegulationTriggerResponse,
)
from market_data_center.regulation_service import RegulationService

pytestmark = pytest.mark.integration
DAY = date(2026, 9, 18)
RUN = UUID("00000000-0000-0000-0000-000000000002")
RPCS = ("query_regulation_triggers", "query_regulation_recent_event_next_triggers")


def _query(connection, rpc=RPCS[0], *, day=DAY, cursor=None, limit=100):
    assert rpc in RPCS
    return connection.execute(
        text(f"select api_v1.{rpc}(:day, :cursor, :limit)"),
        {"day": day, "cursor": cursor, "limit": limit},
    ).scalar_one()


@pytest.fixture
def regulation_database(database_engine: Engine) -> Engine:  # noqa: F811
    ingestion = uuid4()
    with database_engine.begin() as c:
        c.execute(
            text("""
            insert into ingestion.ingestion_run
                (ingestion_id, provider_code, dataset_code, status,
                 requested_at, started_at, finished_at)
            values (:id, 'baostock', 'security', 'succeeded', now(), now(), now())
        """),
            {"id": ingestion},
        )
        c.execute(
            text("""
            insert into core.trading_calendar
                (market, trade_date, is_trading_day, source_code, ingestion_id)
            select 'CN_A_SHARE', d::date, extract(isodow from d) < 6, 'baostock', :id
            from generate_series(date '2026-08-07', date '2026-09-21', interval '1 day') d
        """),
            {"id": ingestion},
        )
        for index in range(5):
            symbol = f"SSE:{600000 + index}"
            c.execute(
                text("""
                insert into core.security
                    (symbol, code, exchange, current_name, security_type, status,
                     ipo_date, source_code, ingestion_id)
                values (:symbol, :code, 'SSE', 'Future Name', 'stock', 'listed',
                        '2000-01-01', 'baostock', :id)
            """),
                {"symbol": symbol, "code": symbol[4:], "id": ingestion},
            )
            c.execute(
                text("""
                insert into core.security_name_history
                    (symbol, name, effective_from, effective_to,
                     source_code, ingestion_id)
                values (:symbol, 'Historical Name', '2000-01-01', '2026-09-30',
                        'baostock', :id)
            """),
                {"symbol": symbol, "id": ingestion},
            )
        for index, status, completed in (
            (1, "SUCCEEDED", "2026-09-18 22:01:00+08"),
            (2, "PARTIAL", "2026-09-18 22:02:00+08"),
            (3, "FAILED", "2026-09-18 23:00:00+08"),
            (4, "RUNNING", None),
        ):
            c.execute(
                text("""
                insert into regulation.calculation_run
                    (calculation_id, trade_date, next_trade_date, status, algorithm_version,
                     rule_set_version, rule_set_hash, scenario_config_version, input_hash,
                     market_watermark, capital_watermark, event_watermark, expected_count,
                     complete_count, incomplete_count, not_applicable_count,
                     started_at, completed_at)
                values (:id, '2026-09-18', '2026-09-21', :status, 'regulation-core.v1',
                        'cn-a-share-regulation-2026-07-06.v1', :hash, 'regulation-scenarios.v1',
                        :hash, 'market-v1', 'capital-v1', '2026-09-18 21:00:00+08',
                        5, 3, 1, 1, '2026-09-18 22:00:00+08', :completed)
            """),
                {
                    "id": UUID(int=index),
                    "status": status,
                    "hash": str(index) * 64,
                    "completed": completed,
                },
            )
        for index, state, applicability, completeness in (
            (0, "SERIOUS_TRIGGERED", "APPLICABLE", "COMPLETE"),
            (1, "ABNORMAL_TRIGGERED", "APPLICABLE", "COMPLETE"),
            (2, "ABNORMAL_TRIGGERED", "INSUFFICIENT_DATA", "INCOMPLETE"),
            (3, "NORMAL", "NOT_APPLICABLE", "NOT_APPLICABLE"),
            (4, "NORMAL", "APPLICABLE", "COMPLETE"),
        ):
            c.execute(
                text("""
                insert into regulation.status
                    (calculation_id, trade_date, symbol, exchange, segment, applicability,
                     data_completeness, calculated_state, announced_state, close, benchmark_symbol,
                     abnormal_count_10d, abnormal_count_10d_up, abnormal_count_10d_down)
                values (:run, '2026-09-18', :symbol, 'SSE', 'SSE_MAIN', :applicability,
                        :completeness, :state, 'NONE', 10, 'SSE:000002', 0, 0, 0)
            """),
                {
                    "run": RUN,
                    "symbol": f"SSE:{600000 + index}",
                    "state": state,
                    "applicability": applicability,
                    "completeness": completeness,
                },
            )
        for symbol, rule, triggered in (
            ("SSE:600000", "SSE_MAIN_SERIOUS_10D_DEV_UP", True),
            ("SSE:600000", "SSE_MAIN_ABNORMAL_3D_DEV_UP", True),
            ("SSE:600001", "SSE_MAIN_ABNORMAL_3D_DEV_UP", True),
            ("SSE:600002", "SSE_MAIN_ABNORMAL_3D_DEV_UP", True),
            ("SSE:600004", "SSE_MAIN_ABNORMAL_3D_DEV_UP", False),
            ("SSE:600004", "SSE_MAIN_SERIOUS_10D_DEV_UP", False),
            ("SSE:600004", "SSE_MAIN_ABNORMAL_TURNOVER", False),
        ):
            c.execute(
                text("""
                insert into regulation.rule_result
                    (calculation_id, symbol, rule_id, evaluation_state, triggered,
                     window_start_date, window_end_date, observed_window_days,
                     current_value, threshold, data_completeness)
                select :run, :symbol, rule_id, :state, :triggered, '2026-09-16',
                       '2026-09-18', 3, 20.12345678, threshold_pct, 'COMPLETE'
                from regulation.rule where rule_code=:rule
            """),
                {
                    "run": RUN,
                    "symbol": symbol,
                    "rule": rule,
                    "triggered": triggered,
                    "state": "TRIGGERED_CALCULATED" if triggered else "NOT_TRIGGERED",
                },
            )
        for rule in ("SSE_MAIN_ABNORMAL_3D_DEV_UP", "SSE_MAIN_SERIOUS_10D_DEV_UP"):
            for scenario, pct in (("INDEX_DOWN_2", -2), ("INDEX_FLAT", 0), ("INDEX_UP_2", 2)):
                c.execute(
                    text("""
                    insert into regulation.warning
                        (calculation_id, trade_date, next_trade_date, symbol, rule_id,
                         warning_type, level, direction, scenario_code, scenario_index_pct,
                         next_day_reference_price, raw_trigger_price, next_day_trigger_price,
                         next_day_trigger_pct, price_limit_ratio, lower_limit_price,
                         upper_limit_price, reachability, window_start_date, window_end_date,
                         requires_official_event_confirmation, message_template_code, message)
                    select :run, '2026-09-18', '2026-09-21', 'SSE:600004', rule_id,
                           'REGULATION_CONDITION', level, direction, :scenario, :pct, 10,
                           10.648, 10.65, 6.5, 0.1, 9, 11, 'REACHABLE_NEXT_SESSION',
                           '2026-09-17', '2026-09-21', false, 'test', 'test'
                    from regulation.rule where rule_code=:rule
                """),
                    {"run": RUN, "rule": rule, "scenario": scenario, "pct": pct},
                )
        for symbol, rule, scenario, reachability in (
            ("SSE:600000", "SSE_MAIN_ABNORMAL_3D_DEV_UP", "CURRENT", "CURRENT"),
            ("SSE:600004", "SSE_MAIN_ABNORMAL_TURNOVER", "NONE", "NOT_PRICE_CALCULABLE"),
        ):
            c.execute(
                text("""
                insert into regulation.warning
                    (calculation_id, trade_date, next_trade_date, symbol, rule_id,
                     warning_type, level, direction, scenario_code, reachability,
                     requires_official_event_confirmation, message_template_code, message)
                select :run, '2026-09-18', '2026-09-21', :symbol, rule_id,
                       'REGULATION_CONDITION', level, direction, :scenario, :reachability,
                       false, 'test', 'test' from regulation.rule where rule_code=:rule
            """),
                {
                    "run": RUN,
                    "symbol": symbol,
                    "rule": rule,
                    "scenario": scenario,
                    "reachability": reachability,
                },
            )
        for index, symbol, end, observed in (
            (1, "SSE:600000", "2026-08-10", "2026-09-18 20:00:00+08"),
            (2, "SSE:600001", "2026-08-07", "2026-09-18 20:00:00+08"),
            (3, "SSE:600001", "2026-09-18", "2026-09-18 22:00:00+08"),
            (4, "SSE:600004", "2026-09-17", "2026-09-18 20:00:00+08"),
            (5, "SSE:600004", "2026-09-16", "2026-09-18 20:00:00+08"),
            (6, "SSE:600002", "2026-09-15", "2026-09-18 20:00:00+08"),
        ):
            c.execute(
                text("""
                insert into regulation.event
                    (symbol, exchange, segment, event_type, event_level, direction,
                     period_start_date, period_end_date, published_at, source_event_id,
                     source_title, source_url, source_content_hash, source_code,
                     observed_at, ingestion_id)
                values (:symbol, 'SSE', 'SSE_MAIN', 'ABNORMAL_VOLATILITY', 'ABNORMAL', null,
                        :end, :end, '2026-09-18 18:00:00+08', :event, 'Official event',
                        'https://www.sse.com.cn/example.html', :hash, 'sse_official',
                        :observed, :id)
            """),
                {
                    "symbol": symbol,
                    "end": date.fromisoformat(end),
                    "event": f"event-{index}",
                    "hash": str(index) * 64,
                    "observed": observed,
                    "id": ingestion,
                },
            )
        # Only the events actually present in the calculation input are captured.
        c.execute(
            text("""
            update regulation.calculation_run r set input_event_keys = (
                select coalesce(jsonb_agg(jsonb_build_object(
                    'source_code', source_code, 'source_event_id', source_event_id,
                    'source_content_hash', source_content_hash
                ) order by source_code, source_event_id), '[]'::jsonb)
                from regulation.event where observed_at <= r.event_watermark
            )
        """)
        )
    return database_engine


def test_regulation_trigger_rpc_aggregates_exact_latest_version_and_pins_pages(regulation_database):
    with regulation_database.begin() as c:
        first = _query(c, limit=1)
        assert first["calculation_id"] == str(RUN)
        assert first["calculation_status"] == "PARTIAL"
        assert first["coverage"] == {
            "expected_count": 5,
            "complete_count": 3,
            "incomplete_count": 1,
            "not_applicable_count": 1,
        }
        assert first["has_more"] is True
        [stock] = first["items"]
        assert stock["symbol"] == "SSE:600000"
        assert stock["name"] == "Historical Name"
        assert stock["announced_state"] == "NONE"
        assert len(stock["triggered_rules"]) == 2
        assert stock["triggered_rules"][0]["level"] == "SERIOUS_ABNORMAL"
        assert stock["triggered_rules"][0]["current_value"] == "20.12345678"
        RegulationTriggerResponse.model_validate(first)
        # A newly completed version must not replace a cursor's pinned calculation.
        c.execute(
            text("""
            update regulation.calculation_run set status='SUCCEEDED', completed_at=now()
            where calculation_id=:id
        """),
            {"id": UUID(int=4)},
        )
        second = _query(c, cursor=first["next_cursor"], limit=1)
        assert second["calculation_id"] == str(RUN)
        assert [item["symbol"] for item in second["items"]] == ["SSE:600001"]
        assert second["next_cursor"] is None
        assert second["has_more"] is False
        assert _query(c)["items"] == []  # new published empty version, never older fallback


def test_regulation_recent_rpc_thirty_sessions_watermark_and_scenario_mapping(regulation_database):
    with regulation_database.connect() as c:
        payload = _query(c, RPCS[1])
        assert payload["lookback_start_date"] == "2026-08-10"
        assert payload["next_trade_date"] == "2026-09-21"
        assert [item["symbol"] for item in payload["items"]] == [
            "SSE:600004",
            "SSE:600002",
            "SSE:600000",
        ]
        stock = payload["items"][0]
        assert stock["official_event_count_30d"] == 2
        assert stock["latest_source_event_id"] == "event-4"
        assert len(stock["next_triggers"]) == 7
        price_rows = [r for r in stock["next_triggers"] if r["scenario_code"] != "NONE"]
        assert {r["level"] for r in price_rows} == {"ABNORMAL", "SERIOUS_ABNORMAL"}
        assert {r["scenario_code"] for r in price_rows} == {
            "INDEX_DOWN_2",
            "INDEX_FLAT",
            "INDEX_UP_2",
        }
        assert price_rows[0]["trigger_change_pct"] == "6.50000000"
        assert payload["items"][1]["next_triggers"] == []  # incomplete inputs are not predictions
        assert payload["items"][2]["next_triggers"][0]["reachability"] == "CURRENTLY_TRIGGERED"
        assert payload["items"][2]["next_triggers"][0]["trigger_price"] is None
        RegulationRecentNextTriggerResponse.model_validate(payload)
        first = _query(c, RPCS[1], limit=2)
        second = _query(c, RPCS[1], cursor=first["next_cursor"], limit=2)
        assert [i["symbol"] for i in first["items"] + second["items"]] == [
            i["symbol"] for i in payload["items"]
        ]


def test_regulation_rpc_validation_and_no_date_fallback(regulation_database):
    with regulation_database.connect() as c:
        cursor = _query(c, limit=1)["next_cursor"]
        decoded = loads(urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        bad_version = urlsafe_b64encode(dumps({**decoded, "v": 9}).encode()).decode().rstrip("=")
        for rpc in RPCS:
            cases = [
                ({"day": None}, "22023"),
                ({"day": date(2026, 7, 5)}, "22023"),
                ({"day": date(2026, 9, 19)}, "22023"),
                ({"limit": 0}, "22023"),
                ({"limit": 501}, "22023"),
                ({"limit": None}, "22023"),
                ({"cursor": ""}, "22023"),
                ({"cursor": "bad-cursor"}, "22023"),
                ({"cursor": "x" * 2049}, "22023"),
                ({"cursor": bad_version}, "22023"),
                ({"day": date(2026, 9, 17)}, "P0002"),
                ({"day": date(2026, 9, 17), "cursor": cursor, "limit": 1}, "22023"),
                ({"cursor": cursor, "limit": 2}, "22023"),
            ]
            if rpc == RPCS[1]:
                cases.append(({"cursor": cursor, "limit": 1}, "22023"))
            for kwargs, state in cases:
                savepoint = c.begin_nested()
                with pytest.raises(DBAPIError) as error:
                    _query(c, rpc, **kwargs)
                assert error.value.orig.sqlstate == state, kwargs
                savepoint.rollback()


def test_regulation_recent_rpc_pages_do_not_change_after_late_raw_replay(regulation_database):
    with regulation_database.begin() as c:
        baseline = _query(c, RPCS[1])
        first = _query(c, RPCS[1], limit=2)
        # Raw replay preserves observed_at, even when persistence happens after publication.
        c.execute(
            text("""
            insert into regulation.event
                (symbol, exchange, segment, event_type, event_level, direction,
                 period_start_date, period_end_date, published_at, source_event_id,
                 source_title, source_url, source_content_hash, source_code,
                 observed_at, ingestion_id)
            select symbol, exchange, segment, event_type, event_level, direction,
                   '2026-09-18', '2026-09-18', published_at, 'late-replay-event',
                   source_title, source_url, repeat('a', 64), source_code,
                   '2026-09-18 20:30:00+08', ingestion_id
            from regulation.event where source_event_id = 'event-1'
        """)
        )
        second = _query(c, RPCS[1], cursor=first["next_cursor"], limit=2)
        assert second["calculation_id"] == first["calculation_id"] == str(RUN)
        assert first["items"] + second["items"] == baseline["items"]


def test_regulation_worker_captures_loaded_events_and_preserves_idempotence(regulation_database):
    persistence = PostgreSQLRegulationPersistence(regulation_database)
    service = RegulationService(
        persistence, clock=lambda: datetime.fromisoformat("2026-09-18T22:03:00+08:00")
    )
    summary = service.calculate(DAY)
    with regulation_database.connect() as c:
        keys = c.execute(
            text(
                "select input_event_keys from regulation.calculation_run where calculation_id=:id"
            ),
            {"id": summary.calculation_id},
        ).scalar_one()
    assert keys == [
        {
            "source_code": "sse_official",
            "source_event_id": f"event-{index}",
            "source_content_hash": str(index) * 64,
        }
        for index in range(1, 7)
    ]
    assert service.calculate(DAY).reused is True
    assert service.calculate(DAY).calculation_id == summary.calculation_id
    # Legacy batches are not silently repaired by the existing idempotent reuse path.
    with regulation_database.begin() as c:
        c.execute(
            text(
                "update regulation.calculation_run set input_event_keys=null "
                "where calculation_id=:id"
            ),
            {"id": summary.calculation_id},
        )
    assert service.calculate(DAY).reused is True
    with regulation_database.connect() as c:
        with pytest.raises(DBAPIError) as error:
            _query(c, RPCS[1])
        assert error.value.orig.sqlstate == "P0002"


def test_regulation_recent_legacy_membership_is_not_guessed_or_older_version_used(
    regulation_database,
):
    with regulation_database.begin() as c:
        c.execute(
            text(
                "update regulation.calculation_run set input_event_keys=null "
                "where calculation_id=:id"
            ),
            {"id": RUN},
        )
        savepoint = c.begin_nested()
        with pytest.raises(DBAPIError) as error:
            _query(c, RPCS[1])
        assert error.value.orig.sqlstate == "P0002"
        savepoint.rollback()
        # Triggered rules are already frozen in the old calculation and remain readable.
        assert _query(c)["returned_count"] == 2
        c.execute(
            text(
                "update regulation.calculation_run set input_event_keys='[]'::jsonb "
                "where calculation_id=:id"
            ),
            {"id": RUN},
        )
        assert _query(c, RPCS[1])["items"] == []


def test_regulation_worker_excludes_events_committed_between_load_and_start(regulation_database):
    clock_calls = 0

    def clock():
        nonlocal clock_calls
        clock_calls += 1
        if clock_calls == 1:
            with regulation_database.begin() as c:
                c.execute(
                    text("""
                    insert into regulation.event
                        (symbol, exchange, segment, event_type, event_level, direction,
                         period_start_date, period_end_date, published_at, source_event_id,
                         source_title, source_url, source_content_hash, source_code,
                         observed_at, ingestion_id)
                    select symbol, exchange, segment, event_type, event_level, direction,
                           '2026-09-18', '2026-09-18', published_at, 'between-load-and-start',
                           source_title, source_url, repeat('b',64), source_code,
                           '2026-09-18 20:30:00+08', ingestion_id
                    from regulation.event where source_event_id='event-1'
                """)
                )
        return datetime.fromisoformat("2026-09-18T22:03:00+08:00")

    service = RegulationService(PostgreSQLRegulationPersistence(regulation_database), clock=clock)
    summary = service.calculate(DAY)
    with regulation_database.connect() as c:
        keys = c.execute(
            text(
                "select input_event_keys from regulation.calculation_run where calculation_id=:id"
            ),
            {"id": summary.calculation_id},
        ).scalar_one()
        assert {key["source_event_id"] for key in keys} == {f"event-{i}" for i in range(1, 7)}
        assert _query(c, RPCS[1])["calculation_id"] == str(summary.calculation_id)
    # A subsequent genuine input change creates a new version with the new event.
    next_summary = service.calculate(DAY)
    assert next_summary.calculation_id != summary.calculation_id
    with regulation_database.connect() as c:
        keys = c.execute(
            text(
                "select input_event_keys from regulation.calculation_run where calculation_id=:id"
            ),
            {"id": next_summary.calculation_id},
        ).scalar_one()
        assert "between-load-and-start" in {key["source_event_id"] for key in keys}


def test_regulation_start_rejects_missing_facts_and_retries_failed_membership(regulation_database):
    persistence = PostgreSQLRegulationPersistence(regulation_database)
    with regulation_database.connect() as c:
        event = _event_from_row(
            c.execute(text("select * from regulation.event where source_event_id='event-1'"))
            .mappings()
            .one()
        )
    run = _run(
        status=RegulationRunStatus.RUNNING,
        completed_at=None,
        event_watermark=datetime.fromisoformat("2026-09-18T21:00:00+08:00"),
    )
    with pytest.raises(ValueError, match="input events are unavailable"):
        persistence.start_calculation(run, (replace(event, source_content_hash="f" * 64),))
    with pytest.raises(ValueError, match="unique natural keys"):
        persistence.start_calculation(run, (event, event))
    with regulation_database.connect() as c:
        assert (
            c.execute(
                text("select count(*) from regulation.calculation_run where calculation_id=:id"),
                {"id": run.calculation_id},
            ).scalar_one()
            == 0
        )
    run_id = persistence.start_calculation(run, (event,))
    persistence.mark_calculation_failed(run_id, run.started_at)
    assert persistence.start_calculation(replace(run, calculation_id=uuid4()), (event,)) == run_id
    with regulation_database.connect() as c:
        keys = c.execute(
            text(
                "select input_event_keys from regulation.calculation_run where calculation_id=:id"
            ),
            {"id": run_id},
        ).scalar_one()
        assert keys == [
            {
                "source_code": "sse_official",
                "source_event_id": "event-1",
                "source_content_hash": "1" * 64,
            }
        ]


def test_regulation_rpcs_are_read_only_and_private_tables_stay_private(regulation_database):
    with regulation_database.begin() as c:
        for rpc in RPCS:
            signature = f"api_v1.{rpc}(date,text,integer)"
            row = c.execute(
                text("""
                select provolatile, prosecdef, proconfig
                from pg_proc where oid=to_regprocedure(:rpc)
            """),
                {"rpc": signature},
            ).one()
            assert row.provolatile == "s" and row.prosecdef is True
            assert "statement_timeout=5s" in row.proconfig
            assert any(option.startswith("search_path=") for option in row.proconfig)
            for role in ("anon", "authenticated"):
                assert not c.execute(
                    text("select has_function_privilege(:role,:rpc,'execute')"),
                    {"role": role, "rpc": signature},
                ).scalar_one()
        c.execute(text("set local role market_data_api"))
        for rpc in RPCS:
            assert _query(c, rpc)["calculation_id"] == str(RUN)
        savepoint = c.begin_nested()
        with pytest.raises(DBAPIError) as error:
            c.execute(text("select * from regulation.event"))
        assert error.value.orig.sqlstate == "42501"
        savepoint.rollback()


def test_regulation_triggers_new_batch_with_stale_statistics_stays_bounded(regulation_database):
    # Reproduce a new 4k-stock batch absent from the last ANALYZE statistics.
    # Without a materialized triggered set, the planner rescans all rules per stock.
    with regulation_database.begin() as c:
        c.execute(
            text("""
            insert into core.security
                (symbol, code, exchange, current_name, security_type, status,
                 ipo_date, source_code, ingestion_id)
            select 'SSE:'||generated_code, generated_code::text, 'SSE',
                   'Test only', 'stock', 'listed',
                   '2000-01-01', source_code, ingestion_id
            from core.security cross join generate_series(600010,603999) generated_code
            where symbol='SSE:600000'
        """)
        )
        c.execute(
            text("""
            insert into regulation.status
            select (jsonb_populate_record(null::regulation.status, to_jsonb(s) ||
                    jsonb_build_object('symbol', 'SSE:'||code))).*
            from regulation.status s cross join generate_series(600010,603999) code
            where symbol='SSE:600004'
        """)
        )
        c.execute(
            text("""
            insert into regulation.rule_result
            select (jsonb_populate_record(null::regulation.rule_result, to_jsonb(rr) ||
                    jsonb_build_object('symbol', s.symbol, 'rule_id', r.rule_id))).*
            from regulation.rule_result rr cross join regulation.status s
            cross join regulation.rule r
            where rr.symbol='SSE:600004'
              and rr.rule_id=(select rule_id from regulation.rule
                             where rule_code='SSE_MAIN_ABNORMAL_3D_DEV_UP')
              and s.symbol>='SSE:600010' and r.segment='SSE_MAIN'
        """)
        )
        c.execute(text("analyze regulation.status"))
        c.execute(text("analyze regulation.rule_result"))
        c.execute(
            text("""
            insert into regulation.calculation_run
            select (jsonb_populate_record(null::regulation.calculation_run, to_jsonb(r) ||
                    jsonb_build_object('calculation_id', cast(:new as uuid),
                                       'input_hash', repeat('8',64),
                                       'completed_at','2026-09-18 23:10:00+08'))).*
            from regulation.calculation_run r where calculation_id=:old
        """),
            {"new": str(UUID(int=8)), "old": RUN},
        )
        for table in ("status", "rule_result"):
            c.execute(
                text(f"""
                insert into regulation.{table}
                select (jsonb_populate_record(null::regulation.{table}, to_jsonb(t) ||
                        jsonb_build_object('calculation_id', cast(:new as uuid)))).*
                from regulation.{table} t where calculation_id=:old
            """),
                {"new": str(UUID(int=8)), "old": RUN},
            )
        c.execute(text("set local statement_timeout='2s'"))
        result = _query(c, limit=3)
        assert result["calculation_id"] == str(UUID(int=8))
        assert [item["symbol"] for item in result["items"]] == ["SSE:600000", "SSE:600001"]
