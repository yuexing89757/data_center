"""Historical auction reads remain bounded to one selected attempt and source."""

from datetime import UTC, date, datetime
from decimal import Decimal
from json import dumps
from time import monotonic
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text
from test_postgres_integration import (
    _insert_call_auction_security_universe,
    _insert_series_round,
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

from market_data_center.domain.call_auction_market_series import series_slots

pytestmark = pytest.mark.integration
DAY = date(2026, 10, 19)
SYMBOLS = ("SSE:600000", "SZSE:000001")


def seed(c, *, status="succeeded", attempt=1, sample_seq=31):
    ingestion_id = uuid4()
    session_id = _insert_series_round(
        c,
        trade_date=DAY,
        ingestion_id=ingestion_id,
        ingestion_status=status,
        symbols=SYMBOLS,
        started_at=datetime(2026, 10, 19, 1, 15, attempt, tzinfo=UTC),
        workflow_attempt=attempt,
        sample_seq=sample_seq,
    )
    c.execute(
        text("""
        update realtime.call_auction_market_series_session
        set status=:status,finished_at=:at where session_id=:sid
        """),
        {"status": status, "at": series_slots(DAY)[31], "sid": session_id},
    )
    c.execute(
        text("""
        insert into realtime.call_auction_market_series_snapshot (
            trade_date,ingestion_id,session_id,sample_seq,scheduled_at,batch_code,
            symbol,observed_at,last_price,previous_close,high_price,low_price,
            cumulative_volume,cumulative_amount,bid1_price,bid1_volume,bid2_volume,
            ask1_price,ask1_volume,
            source_code,value_semantics
        ) values (
            :day,:iid,:sid,:seq,:at,:batch,:symbol,:at,10,9,10,9,3700,37000,10,100,37,10.01,0,
            'pytdx_hq',:semantics
        )
        """),
        [
            {
                "day": DAY,
                "iid": ingestion_id,
                "sid": session_id,
                "seq": sample_seq,
                "at": series_slots(DAY)[sample_seq],
                "batch": series_slots(DAY)[sample_seq]
                .astimezone(ZoneInfo("Asia/Shanghai"))
                .strftime("%H%M%S"),
                "symbol": symbol,
                "semantics": "opening_trade" if sample_seq == 31 else "auction_indicative",
            }
            for symbol in SYMBOLS
        ],
    )
    return ingestion_id, session_id


def archive(c, ingestion_id, *, remove=True):
    c.execute(
        text("""
        insert into realtime.call_auction_market_series_snapshot_history
        select * from realtime.call_auction_market_series_snapshot where ingestion_id=:id
        """),
        {"id": ingestion_id},
    )
    if remove:
        c.execute(
            text("delete from realtime.call_auction_market_series_snapshot where ingestion_id=:id"),
            {"id": ingestion_id},
        )


def query(c, *, series=False, codes=None, day=DAY, batch="092520"):
    params = {"day": day, "codes": codes or ["600000", "000001"]}
    if series:
        params["batch"] = batch
        return c.scalar(
            text("select api_v1.query_call_auction_market_series_snapshots(:day,:codes,:batch)"),
            params,
        )
    return c.scalar(text("select api_v1.query_call_auction_market_snapshots(:day,:codes)"), params)


@pytest.mark.parametrize("series", [False, True])
@pytest.mark.parametrize("source", ["history", "hot", "overlap"])
def test_historical_snapshot_reads_one_complete_attempt(database_engine, source, series):
    with database_engine.begin() as c:
        _insert_call_auction_security_universe(c)
        iid, _ = seed(c)
        if source != "hot":
            archive(c, iid, remove=source == "history")
        c.execute(text("set local role market_data_api"))
        result = query(c, series=series)
        payload = result["rounds"][0] if series else result
        assert payload["returned_count"] == 2
        assert payload["missing_codes"] == []
        assert {item["symbol"] for item in payload["items"]} == set(SYMBOLS)
        assert all(item["cumulative_volume"] == 3700 for item in payload["items"])
        assert all(Decimal(str(item["cumulative_amount"])) == 37000 for item in payload["items"])
        assert all(item["quantity_quality"]["status"] == "certified" for item in payload["items"])
        assert all(
            item["bid2_volume"] == 37 and item["ask1_volume"] == 0 for item in payload["items"]
        )


@pytest.mark.parametrize("series", [False, True])
def test_history_does_not_fill_partial_hot_copy_per_requested_code(database_engine, series):
    with database_engine.begin() as c:
        _insert_call_auction_security_universe(c)
        iid, _ = seed(c)
        archive(c, iid, remove=False)
        c.execute(
            text(
                "delete from realtime.call_auction_market_series_snapshot "
                "where ingestion_id=:id and symbol='SZSE:000001'"
            ),
            {"id": iid},
        )
        for codes in (["600000", "000001"], ["000001"]):
            result = query(c, series=series, codes=codes)
            payload = result["rounds"][0] if series else result
            assert payload["missing_codes"] == ["000001"]
            assert payload["returned_count"] == (1 if "600000" in codes else 0)


def test_historical_units_remain_gated_and_missing_batches_remain_missing(database_engine):
    with database_engine.begin() as c:
        _insert_call_auction_security_universe(c)
        iid, _ = seed(c, status="partial")
        archive(c, iid)
        c.execute(
            text("delete from audit.quantity_unit_certification where ingestion_id=:id"),
            {"id": iid},
        )
        for series in (False, True):
            result = query(c, series=series)
            payload = result["rounds"][0] if series else result
            for item in payload["items"]:
                assert item["last_price"] == 10
                assert item["cumulative_volume"] is None
                assert item["cumulative_amount"] is None
                assert all(
                    item[f"{side}{level}_volume"] is None
                    for side in ("bid", "ask")
                    for level in range(1, 6)
                )
                assert item["quantity_quality"]["status"] == "unverified"
                if not series:
                    assert item["seal_amount"] is None
        assert query(c, series=True, batch="091500")["returned_rounds"] == 0


def test_newer_attempt_with_no_facts_keeps_existing_session_selection_rules(database_engine):
    with database_engine.begin() as c:
        _insert_call_auction_security_universe(c)
        old_iid, old_sid = seed(c)
        archive(c, old_iid)
        new_iid, new_sid = seed(c, attempt=2)
        c.execute(
            text("delete from realtime.call_auction_market_series_snapshot where ingestion_id=:id"),
            {"id": new_iid},
        )
        assert query(c)["ingestion_id"] == str(old_iid)
        series = query(c, series=True)
        assert series["session_id"] == str(new_sid)
        assert series["session_id"] != str(old_sid)
        assert series["rounds"][0]["returned_count"] == 0


def test_internal_history_source_is_private_and_rpc_bounds_stay_intact(database_engine):
    with database_engine.begin() as c:
        for role in ("anon", "authenticated", "market_data_api"):
            assert (
                c.scalar(
                    text(
                        "select has_function_privilege(:role,"
                        "'audit.read_auction_snapshot_attempt(date,uuid,integer,uuid,text[])',"
                        "'execute')"
                    ),
                    {"role": role},
                )
                is False
            )
            assert (
                c.scalar(
                    text(
                        "select has_table_privilege(:role,"
                        "'audit.unit_safe_auction_snapshot_read','select')"
                    ),
                    {"role": role},
                )
                is False
            )
        for name in (
            "query_call_auction_market_snapshots",
            "query_call_auction_market_series_snapshots",
        ):
            definition = c.scalar(
                text(
                    "select pg_get_functiondef(p.oid) from pg_proc p "
                    "join pg_namespace n on n.oid=p.pronamespace "
                    "where n.nspname='api_v1' and p.proname=:name"
                ),
                {"name": name},
            )
            assert "SECURITY DEFINER" in definition
            assert "statement_timeout TO '5s'" in definition
        c.execute(text("set local role market_data_api"))
        with pytest.raises(Exception, match="invalid call-auction market snapshot query boundary"):
            query(c, codes=[f"{i:06d}" for i in range(501)])


@pytest.mark.parametrize("certified", [True, False])
def test_historical_full_32_rounds_and_500_codes_fit_rpc_budget(database_engine, certified):
    with database_engine.begin() as c:
        _insert_call_auction_security_universe(c)
        c.execute(
            text("""
            insert into core.security
                (symbol,code,exchange,current_name,security_type,status,source_code,ingestion_id)
            select 'SSE:'||n::text,n::text,'SSE','budget fixture','stock','listed',
                   source_code,ingestion_id
            from core.security cross join generate_series(600001,600499) n
            where symbol='SSE:600000'
        """)
        )
        symbols = tuple(f"SSE:{code}" for code in range(600000, 600500))
        final_id = uuid4()
        sid = _insert_series_round(
            c,
            trade_date=DAY,
            ingestion_id=final_id,
            ingestion_status="succeeded",
            symbols=symbols,
            started_at=series_slots(DAY)[0],
            workflow_attempt=1,
        )
        c.execute(
            text(
                "update realtime.call_auction_market_series_session "
                "set status='succeeded',finished_at=:at where session_id=:sid"
            ),
            {"at": series_slots(DAY)[31], "sid": sid},
        )
        for seq in range(31):
            iid = uuid4()
            c.execute(
                text("""
                insert into ingestion.ingestion_run
                    (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
                values (:id,'pytdx_hq','call_auction_market_series','succeeded',:at,:at,:at)
            """),
                {"id": iid, "at": series_slots(DAY)[seq]},
            )
            c.execute(
                text("""
                insert into realtime.call_auction_market_series_round
                    (session_id,sample_seq,scheduled_at,collected_at,status,attempt_count,
                     expected_quotes,successful_quotes,failed_quotes,selected_ingestion_id)
                values (:sid,:seq,:at,:at,'succeeded',1,500,500,0,:id)
            """),
                {"sid": sid, "seq": seq, "at": series_slots(DAY)[seq], "id": iid},
            )
            _seed_quantity_fixture_proof(c, iid, ("call_auction_market_series.quantities",))
        c.execute(
            text("""
            insert into realtime.call_auction_market_series_snapshot_history
                (trade_date,ingestion_id,session_id,sample_seq,scheduled_at,batch_code,symbol,
                 observed_at,last_price,previous_close,cumulative_volume,cumulative_amount,
                 bid1_price,bid1_volume,source_code,value_semantics)
            select :day,r.selected_ingestion_id,r.session_id,r.sample_seq,r.scheduled_at,
                   to_char(r.scheduled_at at time zone 'Asia/Shanghai','HH24MISS'),s.symbol,
                   r.scheduled_at,10,9,3700,37000,10,3700,'pytdx_hq',
                   case when r.sample_seq=31 then 'opening_trade' else 'auction_indicative' end
            from realtime.call_auction_market_series_round r cross join core.security s
            where r.session_id=:sid and s.symbol=any(:symbols)
        """),
            {"day": DAY, "sid": sid, "symbols": list(symbols)},
        )
        if not certified:
            c.execute(
                text(
                    "delete from audit.quantity_unit_certification where ingestion_id in "
                    "(select selected_ingestion_id from realtime.call_auction_market_series_round "
                    "where session_id=:sid)"
                ),
                {"sid": sid},
            )
        c.execute(text("set local role market_data_api"))
        with c.begin_nested():
            c.execute(text("reset role"))
            plan = c.scalar(
                text(
                    "explain (format json) select symbol from "
                    "realtime.call_auction_market_series_snapshot_history "
                    "where trade_date=:day and ingestion_id=:iid and symbol=any(:symbols)"
                ),
                {"day": DAY, "iid": final_id, "symbols": list(symbols)},
            )
            serialized = dumps(plan)
            assert "call_auction_market_series_snapshot_history_202610" in serialized
            assert "_202609" not in serialized and "_202611" not in serialized
            assert "Index" in serialized
        c.execute(text("set local role market_data_api"))
        started = monotonic()
        payload = query(c, series=True, codes=[s.split(":")[1] for s in symbols], batch=None)
        assert monotonic() - started < 5
        assert payload["returned_rounds"] == 32
        assert sum(r["returned_count"] for r in payload["rounds"]) == 16000
        assert all(not r["missing_codes"] for r in payload["rounds"])
        for r in payload["rounds"]:
            assert all(i["cumulative_volume"] == (3700 if certified else None) for i in r["items"])


def test_unselected_attempt_and_other_dates_cannot_fill_history_gaps(database_engine):
    with database_engine.begin() as c:
        _insert_call_auction_security_universe(c)
        selected, sid = seed(c, status="partial")
        other, other_sid = seed(c, status="partial", attempt=2)
        # Move the second attempt's facts under the chosen round, without selecting it.
        c.execute(
            text(
                "update realtime.call_auction_market_series_snapshot "
                "set session_id=:sid where ingestion_id=:iid"
            ),
            {"sid": sid, "iid": other},
        )
        archive(c, selected)
        archive(c, other)
        c.execute(
            text(
                "delete from realtime.call_auction_market_series_snapshot_history "
                "where ingestion_id=:id and symbol='SZSE:000001'"
            ),
            {"id": selected},
        )
        # Keep the original selected session as the canonical successful candidate.
        c.execute(
            text(
                "update realtime.call_auction_market_series_session "
                "set status='failed' where session_id=:sid"
            ),
            {"sid": other_sid},
        )
        for series in (False, True):
            result = query(c, series=series)
            payload = result["rounds"][0] if series else result
            assert payload["returned_count"] == 1
            assert payload["missing_codes"] == ["000001"]
        with pytest.raises(Exception, match="not found"):
            query(c, day=date(2026, 10, 20))
