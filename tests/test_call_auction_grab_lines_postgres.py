from datetime import date, timedelta
from decimal import Decimal
from time import perf_counter
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from test_postgres_integration import (
    _seed_one_price_pattern_session,
    database_engine,  # noqa: F401
    empty_database_url,  # noqa: F401
    migrated_database_url,  # noqa: F401
)

from market_data_center.domain.call_auction_market_series import series_slots

pytestmark = pytest.mark.integration


def test_call_auction_grab_lines_uses_exact_rounds_and_strict_threshold(
    database_engine: Engine,  # noqa: F811
) -> None:
    trade_date = date(2026, 9, 7)
    session_id = uuid4()
    ingestion_id = uuid4()
    slots = series_slots(trade_date)

    with database_engine.begin() as connection:
        _seed_one_price_pattern_session(
            connection, trade_date, session_id, include_preclose_round=True
        )
        connection.execute(
            text("""
update core.security
set status = 'delisted', delisting_date = :delisting_date
where symbol = 'SSE:600000'
"""),
            {"delisting_date": trade_date + timedelta(days=1)},
        )
        connection.execute(
            text("""
insert into ingestion.ingestion_run (
    ingestion_id, provider_code, dataset_code, status, requested_at,
    started_at, finished_at, fetched_rows, accepted_rows
) values (
    :ingestion_id, 'pytdx_hq', 'call_auction_market_series', 'succeeded',
    :scheduled_at, :scheduled_at, :finished_at, 11, 11
)
"""),
            {
                "ingestion_id": ingestion_id,
                "scheduled_at": slots[31],
                "finished_at": slots[31] + timedelta(seconds=2),
            },
        )
        connection.execute(
            text("""
update realtime.call_auction_market_series_round
set collected_at = :collected_at,
    status = 'succeeded',
    attempt_count = 1,
    successful_quotes = expected_quotes,
    failed_quotes = 0,
    selected_ingestion_id = :ingestion_id
where session_id = :session_id
  and sample_seq = 31
"""),
            {
                "collected_at": slots[31] + timedelta(seconds=2),
                "ingestion_id": ingestion_id,
                "session_id": session_id,
            },
        )
        connection.execute(
            text("""
insert into realtime.call_auction_market_series_snapshot (
    trade_date, ingestion_id, session_id, sample_seq, batch_code,
    scheduled_at, symbol, observed_at, last_price, previous_close,
    source_code, value_semantics
)
select
    snapshot.trade_date,
    :ingestion_id,
    snapshot.session_id,
    31,
    '092520',
    :scheduled_at,
    snapshot.symbol,
    :observed_at,
    case
        when snapshot.symbol = 'SSE:600000' then 10.50::numeric
        when snapshot.symbol = 'SSE:600004' then snapshot.last_price + 0.10::numeric
        else snapshot.last_price
    end,
    snapshot.previous_close,
    'pytdx_hq',
    'opening_trade'
from realtime.call_auction_market_series_snapshot snapshot
where snapshot.session_id = :session_id
  and snapshot.sample_seq = 30
"""),
            {
                "ingestion_id": ingestion_id,
                "session_id": session_id,
                "scheduled_at": slots[31],
                "observed_at": slots[31] + timedelta(seconds=1),
            },
        )

        first_ingestion_id = connection.scalar(
            text("""
select selected_ingestion_id
from realtime.call_auction_market_series_round
where session_id = :session_id and sample_seq = 30
"""),
            {"session_id": session_id},
        )
        security_ingestion_id = connection.scalar(
            text("select ingestion_id from core.security order by symbol limit 1")
        )
        connection.execute(
            text("""
insert into core.security (
    symbol, code, exchange, current_name, security_type, status,
    ipo_date, source_code, ingestion_id
)
select
    'SSE:' || (700000 + value)::text,
    (700000 + value)::text,
    'SSE',
    'PERF ' || value::text,
    'stock',
    'listed',
    :trade_date - 100,
    'baostock',
    :security_ingestion_id
from generate_series(0, 5199) value
"""),
            {
                "trade_date": trade_date,
                "security_ingestion_id": security_ingestion_id,
            },
        )
        connection.execute(
            text("""
insert into realtime.call_auction_market_series_snapshot (
    trade_date, ingestion_id, session_id, sample_seq, batch_code,
    scheduled_at, symbol, observed_at, last_price, previous_close,
    source_code, value_semantics
)
select
    :trade_date,
    source.ingestion_id,
    :session_id,
    source.sample_seq,
    source.batch_code,
    source.scheduled_at,
    security.symbol,
    source.observed_at,
    10.00,
    10.00,
    'pytdx_hq',
    source.value_semantics
from core.security security
cross join (values
    (:first_ingestion_id, 30, '092453', :first_scheduled_at,
        :first_observed_at, 'auction_indicative'),
    (:final_ingestion_id, 31, '092520', :final_scheduled_at,
        :final_observed_at, 'opening_trade')
) source(
    ingestion_id, sample_seq, batch_code, scheduled_at,
    observed_at, value_semantics
)
where security.code between '700000' and '705199'
"""),
            {
                "trade_date": trade_date,
                "session_id": session_id,
                "first_ingestion_id": first_ingestion_id,
                "first_scheduled_at": slots[30],
                "first_observed_at": slots[30] + timedelta(seconds=1),
                "final_ingestion_id": ingestion_id,
                "final_scheduled_at": slots[31],
                "final_observed_at": slots[31] + timedelta(seconds=1),
            },
        )
        connection.execute(
            text("""
update realtime.call_auction_market_series_round
set expected_quotes = expected_quotes + 5200,
    successful_quotes = successful_quotes + 5200
where session_id = :session_id and sample_seq in (30, 31)
"""),
            {"session_id": session_id},
        )
        connection.execute(
            text("""
update ingestion.ingestion_run
set fetched_rows = fetched_rows + 5200,
    accepted_rows = accepted_rows + 5200
where ingestion_id in (:first_ingestion_id, :final_ingestion_id)
"""),
            {
                "first_ingestion_id": first_ingestion_id,
                "final_ingestion_id": ingestion_id,
            },
        )

        connection.execute(text("set local role market_data_api"))
        query_started = perf_counter()
        payload = connection.scalar(
            text(
                "select api_v1.query_call_auction_grab_lines("
                ":day, :min_grab_line_pct, :max_grab_line_pct, "
                ":min_change_pct, :max_change_pct)"
            ),
            {
                "day": trade_date,
                "min_grab_line_pct": Decimal("1.00"),
                "max_grab_line_pct": Decimal("3.00"),
                "min_change_pct": Decimal("4.00"),
                "max_change_pct": Decimal("6.00"),
            },
        )
        query_elapsed_seconds = perf_counter() - query_started
        boundary_payload = connection.scalar(
            text(
                "select api_v1.query_call_auction_grab_lines("
                ":day, :min_grab_line_pct, :max_grab_line_pct, "
                ":min_change_pct, :max_change_pct)"
            ),
            {
                "day": trade_date,
                "min_grab_line_pct": Decimal("2.00"),
                "max_grab_line_pct": None,
                "min_change_pct": Decimal("4.00"),
                "max_change_pct": None,
            },
        )
        change_boundary_payload = connection.scalar(
            text(
                "select api_v1.query_call_auction_grab_lines("
                ":day, :min_grab_line_pct, :max_grab_line_pct, "
                ":min_change_pct, :max_change_pct)"
            ),
            {
                "day": trade_date,
                "min_grab_line_pct": Decimal("1.00"),
                "max_grab_line_pct": None,
                "min_change_pct": Decimal("5.00"),
                "max_change_pct": None,
            },
        )
        connection.execute(text("reset role"))

        assert connection.scalar(
            text("""
select has_function_privilege(
    'market_data_api',
    'api_v1.query_call_auction_grab_lines(date,numeric,numeric,numeric,numeric)',
    'execute'
)
""")
        )
        assert connection.scalar(
            text("""
select 'statement_timeout=10s' = any(proconfig)
from pg_proc
where oid = (
    'api_v1.query_call_auction_grab_lines(date,numeric,numeric,numeric,numeric)'
)::regprocedure
""")
        )

    assert payload["trade_date"] == "2026-09-07"
    assert payload["session_id"] == str(session_id)
    assert payload["session_status"] == "partial"
    assert Decimal(str(payload["min_grab_line_pct"])) == Decimal("1.00")
    assert Decimal(str(payload["max_grab_line_pct"])) == Decimal("3.00")
    assert Decimal(str(payload["min_change_pct"])) == Decimal("4.00")
    assert Decimal(str(payload["max_change_pct"])) == Decimal("6.00")
    assert payload["first_batch_code"] == "092453"
    assert payload["final_batch_code"] == "092520"
    assert payload["count"] == 1
    assert payload["items"][0]["code"] == "600000"
    assert payload["items"][0]["name"] == "浦发银行"
    assert Decimal(str(payload["items"][0]["grab_line_pct"])) == Decimal("2.0000000000")
    assert Decimal(str(payload["items"][0]["change_pct_092520"])) == Decimal("5.0000000000")
    assert payload["items"][0]["trade_date"] == "2026-09-07"
    assert query_elapsed_seconds < 2
    assert boundary_payload["count"] == 0
    assert boundary_payload["items"] == []
    assert change_boundary_payload["count"] == 0
    assert change_boundary_payload["items"] == []


def test_call_auction_grab_lines_requires_an_exact_complete_pair(
    database_engine: Engine,  # noqa: F811
) -> None:
    with database_engine.connect() as connection:
        connection.execute(text("set local role market_data_api"))
        with pytest.raises(DBAPIError) as raised:
            connection.scalar(
                text("select api_v1.query_call_auction_grab_lines(:day, 0, null, 0, null)"),
                {"day": date(2026, 9, 3)},
            )

    assert raised.value.orig.sqlstate == "P0002"
