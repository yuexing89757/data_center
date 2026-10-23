"""Paged inputs retain pre-window events, warmup, nulls and the exact input hash."""

from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from test_postgres_integration import (
    _seed_quantity_fixture_proof,
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
from test_quantity_unit_repair_integration import seed

from market_data_center.calculators import calculation_input_hash
from market_data_center.domain.derived import DerivedCalculationInput
from market_data_center.persistence import derived_postgres as module

pytestmark = pytest.mark.integration


def test_paged_source_preserves_events_warmup_order_and_quantity_gates(
    database_engine, tmp_path, monkeypatch
):
    _, scope, run_id = seed(database_engine, tmp_path, days=25, target_already=True)
    start = scope.end_date - timedelta(days=1)
    unverified_id = uuid4()
    with database_engine.begin() as c:
        c.execute(
            text("""
insert into ingestion.ingestion_run
 (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
values (:id,'pytdx','daily_bar','succeeded',now(),now(),now())
"""),
            {"id": unverified_id},
        )
        c.execute(
            text("""
insert into core.security
 (symbol,code,exchange,current_name,security_type,status,source_code,ingestion_id)
values ('SZSE:000001','000001','SZSE','synthetic second','stock','listed','baostock',:id)
"""),
            {"id": unverified_id},
        )
        c.execute(
            text("""
insert into core.daily_bar
 (symbol,trade_date,market,open,high,low,close,previous_close,volume,amount,
  trade_status,source_code,ingestion_id)
select 'SZSE:000001',trade_date,market,open,high,low,close,null,volume,amount,
       trade_status,source_code,:id from core.daily_bar
"""),
            {"id": unverified_id},
        )
        c.execute(
            text("""
insert into capital.distribution
 (symbol,report_period,ex_date,cash_dividend_per_share,status,source_code,ingestion_id)
values ('SSE:600000','2026-06-30',:day,0.1,'implemented','pytdx',:id)
"""),
            {"day": scope.start_date, "id": run_id},
        )
        c.execute(
            text("""
update core.daily_bar set volume=0,amount=null
where symbol='SSE:600000' and trade_date=:day
"""),
            {"day": start},
        )
        _seed_quantity_fixture_proof(c, run_id, ("daily_bar.volume", "daily_bar.amount"))
        # Restore the original target CTE to compare against the pre-paging query.
        original = text(
            str(module._SELECT_DAILY_BARS).replace(
                "select unnest(cast(:symbols as text[])) as symbol",
                "select distinct symbol from audit.unit_safe_daily_bar "
                "where trade_date between :start_date and :end_date",
            )
        )
        rows = c.execute(
            original,
            {
                "start_date": start,
                "end_date": scope.end_date,
            },
        ).mappings()
        reference = DerivedCalculationInput(
            daily_bars=tuple(module._daily_bar(row) for row in rows),
            distributions=tuple(
                module._distribution(row)
                for row in c.execute(
                    module._SELECT_DISTRIBUTIONS,
                    {"start_date": start, "end_date": scope.end_date},
                ).mappings()
            ),
            rights_issues=(),
            share_capital=(),
            memberships=(),
        )
        expected_watermark = module.PostgreSQLDerivedPersistence._load_watermark(
            c, start, scope.end_date
        )
    monkeypatch.setattr(module, "_DAILY_BAR_SYMBOL_PAGE_SIZE", 1)
    changed = False

    def change_next_page(connection, cursor, statement, parameters, context, executemany):
        nonlocal changed
        if (
            context.compiled is None
            or context.compiled.statement is not module._SELECT_DAILY_BARS
            or changed
        ):
            return
        changed = True
        # A concurrent writer changes the not-yet-read page and its certification.
        with database_engine.begin() as other:
            other.execute(
                text("""
update core.daily_bar set volume=999,updated_at='2099-01-01'
where symbol='SZSE:000001'
""")
            )
            _seed_quantity_fixture_proof(
                other, unverified_id, ("daily_bar.volume", "daily_bar.amount")
            )

    event.listen(database_engine, "after_cursor_execute", change_next_page)
    try:
        inputs, watermark = module.PostgreSQLDerivedPersistence(
            database_engine
        ).load_calculation_source(start, scope.end_date)
    finally:
        event.remove(database_engine, "after_cursor_execute", change_next_page)
    assert changed and watermark == expected_watermark
    assert calculation_input_hash(inputs) == calculation_input_hash(reference)
    assert len(inputs.daily_bars) == 45  # 22 per symbol plus the older event for SSE.
    assert inputs.daily_bars[0].trade_date == scope.start_date
    assert all(
        bar.volume is None and bar.amount is None
        for bar in inputs.daily_bars
        if bar.symbol == "SZSE:000001"
    )
    assert all(bar.previous_close == 10 for bar in inputs.daily_bars if bar.symbol == "SZSE:000001")
    zero = next(
        bar for bar in inputs.daily_bars if bar.symbol == "SSE:600000" and bar.trade_date == start
    )
    assert zero.volume == 0 and zero.amount is None
    refreshed, refreshed_watermark = module.PostgreSQLDerivedPersistence(
        database_engine
    ).load_calculation_source(start, scope.end_date)
    assert refreshed_watermark != watermark
    assert all(bar.volume == 999 for bar in refreshed.daily_bars if bar.symbol == "SZSE:000001")
