"""Snapshot rebuilds cannot consume unverified quantities through internal SQL."""

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from market_data_center.persistence.stock_pool_postgres import _SELECT_CANDIDATES
from market_data_center.persistence.today_limit_down_postgres import (
    PostgreSQLTodayLimitDownPersistence,
)
from market_data_center.persistence.today_limit_down_postgres import _member as down_member
from market_data_center.persistence.today_limit_down_postgres import _missing_core as down_missing
from market_data_center.persistence.today_limit_up_postgres import (
    PostgreSQLTodayLimitUpPersistence,
)
from market_data_center.persistence.today_limit_up_postgres import _member as up_member
from market_data_center.persistence.today_limit_up_postgres import _missing_core as up_missing


def test_stock_pool_quantity_input_is_gated_at_the_exact_basis_date():
    query = str(_SELECT_CANDIDATES)
    assert "left join audit.unit_safe_stock_daily_indicator i" in query
    assert "i.trade_date=:basis" in query
    assert "left join core.daily_bar b" in query  # This query reads prices only.
    assert "i.free_float_shares" in query and "i.circulating_market_value" in query
    assert "coalesce(i." not in query.lower()


@pytest.mark.parametrize(
    "persistence_type,quantity_daily_bar",
    [(PostgreSQLTodayLimitUpPersistence, True), (PostgreSQLTodayLimitDownPersistence, False)],
)
def test_rebuild_queries_gate_quantities_without_date_fallback(
    persistence_type, quantity_daily_bar
):
    connection, engine = MagicMock(), MagicMock()
    engine.begin.return_value.__enter__.return_value = connection
    pool = MagicMock()
    pool.mappings.return_value.one.return_value = {"snapshot_id": uuid4(), "member_count": 0}
    connection.execute.side_effect = [pool, LookupError("stop after capturing candidate query")]
    ingestion_id = uuid4()
    with pytest.raises(LookupError, match="stop after capturing"):
        persistence_type(engine).commit_snapshot(
            trade_date=date(2026, 10, 22),
            requested_status=None,
            run=SimpleNamespace(ingestion_id=ingestion_id, fetched_rows=0),
            manifest=SimpleNamespace(ingestion_id=ingestion_id, row_count=0),
            source_records=(),
            ingestion_quality=(),
        )
    query = str(connection.execute.call_args_list[1].args[0])
    assert "left join audit.unit_safe_stock_daily_indicator ind" in query
    assert "left join audit.unit_safe_eod_quote_snapshot e" in query
    assert "ind.trade_date=s.basis_trade_date" in query
    assert "e.trade_date=s.basis_trade_date" in query
    assert "where m.snapshot_id=:snapshot_id" in query
    daily_relation = "audit.unit_safe_daily_bar" if quantity_daily_bar else "core.daily_bar"
    assert f"left join {daily_relation} db" in query
    assert "db.trade_date=s.basis_trade_date" in query
    assert "coalesce(ind." not in query.lower() and "coalesce(e." not in query.lower()


def _row():
    return {
        "symbol": "SSE:600000",
        "code": "600000",
        "historical_name": "test",
        "name_ingestion_id": uuid4(),
        "daily_bar_ingestion_id": uuid4(),
        "pool_calculation_id": uuid4(),
        "indicator_ingestion_id": uuid4(),
        "previous_close": Decimal("10"),
        "close": Decimal("11"),
        "limit_price": Decimal("11"),
        "free_float_shares": 100,
        "daily_bar_volume": None,
        "daily_bar_amount": None,
        "indicator_turnover": None,
        "computed_consecutive_days": 0,
        **{
            f"{side}{level}_{field}": Decimal("11") if field == "price" else None
            for side in ("bid", "ask")
            for level in range(1, 6)
            for field in ("price", "volume")
        },
    }


def test_masked_quantities_do_not_become_zero_or_recover_from_older_inputs():
    row = _row()
    member = up_member(row, None)
    assert member.volume is None and member.amount_cny is None
    assert member.closing_bid1_volume_shares is None
    assert member.closing_bid1_sealing_amount_cny is None
    assert member.consecutive_limit_up_days is None
    member_down = down_member(row, None)
    assert member_down.closing_ask1_volume_shares is None
    assert member_down.closing_ask1_sealing_amount_cny is None
    row["free_float_shares"] = None
    assert up_missing(row) == down_missing(row) == "free_float_shares"
