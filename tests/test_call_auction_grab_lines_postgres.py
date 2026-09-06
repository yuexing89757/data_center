from datetime import date, timedelta
from decimal import Decimal
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
    trade_date = date(2026, 9, 4)
    session_id = uuid4()
    ingestion_id = uuid4()
    slots = series_slots(trade_date)

    with database_engine.begin() as connection:
        _seed_one_price_pattern_session(connection, trade_date, session_id)
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
  and snapshot.sample_seq = 29
"""),
            {
                "ingestion_id": ingestion_id,
                "session_id": session_id,
                "scheduled_at": slots[31],
                "observed_at": slots[31] + timedelta(seconds=1),
            },
        )

        connection.execute(text("set local role market_data_api"))
        payload = connection.scalar(
            text("select api_v1.query_call_auction_grab_lines(:day, :threshold_n)"),
            {"day": trade_date, "threshold_n": Decimal("1.00")},
        )
        boundary_payload = connection.scalar(
            text("select api_v1.query_call_auction_grab_lines(:day, :threshold_n)"),
            {"day": trade_date, "threshold_n": Decimal("3.00")},
        )
        connection.execute(text("reset role"))

        assert connection.scalar(
            text("""
select has_function_privilege(
    'market_data_api',
    'api_v1.query_call_auction_grab_lines(date,numeric)',
    'execute'
)
""")
        )
        assert connection.scalar(
            text("""
select 'statement_timeout=10s' = any(proconfig)
from pg_proc
where oid = 'api_v1.query_call_auction_grab_lines(date,numeric)'::regprocedure
""")
        )

    assert payload["trade_date"] == "2026-09-04"
    assert payload["session_id"] == str(session_id)
    assert payload["session_status"] == "partial"
    assert Decimal(str(payload["threshold_n"])) == Decimal("1.00")
    assert payload["first_batch_code"] == "092440"
    assert payload["final_batch_code"] == "092520"
    assert payload["count"] == 1
    assert payload["items"][0]["code"] == "600000"
    assert payload["items"][0]["name"] == "浦发银行"
    assert Decimal(str(payload["items"][0]["grab_line_pct"])) == Decimal("3.0000000000")
    assert payload["items"][0]["trade_date"] == "2026-09-04"
    assert boundary_payload["count"] == 0
    assert boundary_payload["items"] == []


def test_call_auction_grab_lines_requires_an_exact_complete_pair(
    database_engine: Engine,  # noqa: F811
) -> None:
    with database_engine.connect() as connection:
        connection.execute(text("set local role market_data_api"))
        with pytest.raises(DBAPIError) as raised:
            connection.scalar(
                text("select api_v1.query_call_auction_grab_lines(:day, 0)"),
                {"day": date(2026, 9, 3)},
            )

    assert raised.value.orig.sqlstate == "P0002"
