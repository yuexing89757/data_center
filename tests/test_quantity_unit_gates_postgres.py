"""Public quantity reads depend on current evidence, never on schema-name guesses."""

# Imported fixtures are registered and injected by pytest.
# ruff: noqa: F811

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError
from test_postgres_integration import (  # noqa: F401
    _insert_series_round,
    database_engine,
    empty_database_url,
    migrated_database_url,
)

pytestmark = pytest.mark.integration
DAY = date(2026, 10, 22)


def _facts(engine: Engine) -> UUID:
    ingestion_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("""
insert into ingestion.ingestion_run (
 ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at
) values (:id,'pytdx','daily_bar','succeeded',now(),now(),now())
"""),
            {"id": ingestion_id},
        )
        connection.execute(
            text("""
insert into ingestion.raw_manifest (
 ingestion_id,object_path,file_format,content_sha256,byte_size,row_count,schema_version
) values (:id,:path,'jsonl',:hash,100,1,'pytdx.remote_daily_bar.v2')
"""),
            {"id": ingestion_id, "path": f"test/{ingestion_id}.jsonl", "hash": "a" * 64},
        )
        connection.execute(
            text("""
insert into core.security (
 symbol,code,exchange,current_name,security_type,status,source_code,ingestion_id
) values ('SSE:600000','600000','SSE','浦发银行','stock','listed','baostock',:id)
"""),
            {"id": ingestion_id},
        )
        connection.execute(
            text("""
insert into core.trading_calendar (
 market,trade_date,is_trading_day,source_code,ingestion_id
) values ('CN_A_SHARE',:day,true,'baostock',:id)
"""),
            {"id": ingestion_id, "day": DAY},
        )
        connection.execute(
            text("""
insert into core.daily_bar (
 symbol,trade_date,market,open,high,low,close,previous_close,volume,amount,
 trade_status,is_st,source_code,ingestion_id
) values ('SSE:600000',:day,'CN_A_SHARE',10,10,10,10,9,100,1000,
 'trading',false,'pytdx',:id)
"""),
            {"id": ingestion_id, "day": DAY},
        )
    return ingestion_id


def _certify(engine: Engine, ingestion_id: UUID, group: str, raw_hash: str = "a" * 64) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("""
insert into audit.quantity_unit_certification (
 ingestion_id,field_group,status,raw_hashes,normalizer_version,evidence_hash
) values (:id,:group,'certified',jsonb_build_array(cast(:hash as text)),
 'quantity-normalizer.v1',:evidence)
"""),
            {"id": ingestion_id, "group": group, "hash": raw_hash, "evidence": "b" * 64},
        )


def test_ordinary_daily_reads_keep_prices_and_mask_unverified_quantities(database_engine: Engine):
    _facts(database_engine)
    with database_engine.connect() as connection:
        direct = connection.execute(text("select * from api_v1.daily_bars")).mappings().one()
        rpc = (
            connection.execute(
                text("""
select * from api_v1.query_daily_bars('SSE:600000',:day,:day,10)
"""),
                {"day": DAY},
            )
            .mappings()
            .one()
        )
        recent = connection.execute(
            text("""
select api_v1.query_recent_daily_bars('600000',:day,10)
"""),
            {"day": DAY},
        ).scalar_one()["items"][0]

    for row in (direct, rpc, recent):
        assert Decimal(str(row["close"])) == Decimal(10)
        assert row["volume"] is row["amount"] is None
        assert row["quantity_quality"]["status"] == "unverified"


def test_volume_and_amount_need_independent_matching_raw_certificates(database_engine: Engine):
    ingestion_id = _facts(database_engine)
    _certify(database_engine, ingestion_id, "daily_bar.volume")
    _certify(database_engine, ingestion_id, "daily_bar.amount", "c" * 64)
    with database_engine.connect() as connection:
        row = connection.execute(text("select * from api_v1.daily_bars")).mappings().one()
        admitted = connection.execute(
            text("""
select audit.quantity_unit_is_certified(:id,'daily_bar.amount')
"""),
            {"id": ingestion_id},
        ).scalar_one()

    assert row["volume"] == 100
    assert row["amount"] is None
    assert admitted is False


def test_current_raw_changes_invalidate_previously_matching_evidence(database_engine: Engine):
    ingestion_id = _facts(database_engine)
    _certify(database_engine, ingestion_id, "daily_bar.volume")
    with database_engine.begin() as connection:
        before = connection.execute(text("select volume from api_v1.daily_bars")).scalar_one()
        connection.execute(
            text("""
update ingestion.raw_manifest set content_sha256=:hash where ingestion_id=:id
"""),
            {"id": ingestion_id, "hash": "c" * 64},
        )
        after = connection.execute(text("select volume from api_v1.daily_bars")).scalar_one()

    assert before == 100
    assert after is None


def test_certified_zero_is_public_without_rescaling(database_engine: Engine):
    ingestion_id = _facts(database_engine)
    _certify(database_engine, ingestion_id, "daily_bar.volume")
    _certify(database_engine, ingestion_id, "daily_bar.amount")
    with database_engine.begin() as connection:
        connection.execute(text("update core.daily_bar set volume=0,amount=0"))
        row = connection.execute(text("select * from api_v1.daily_bars")).mappings().one()

    assert row["volume"] == 0
    assert row["amount"] == Decimal(0)
    assert row["quantity_quality"]["status"] == "certified"


def test_public_reader_cannot_read_evidence_or_write_safe_views(database_engine: Engine):
    _facts(database_engine)
    with pytest.raises(DBAPIError), database_engine.begin() as connection:
        connection.execute(text("set local role market_data_api"))
        connection.execute(text("select * from audit.quantity_unit_certification"))
    with pytest.raises(DBAPIError), database_engine.begin() as connection:
        connection.execute(text("set local role market_data_api"))
        connection.execute(text("update audit.unit_safe_daily_bar set volume=10000"))
    with database_engine.begin() as connection:
        connection.execute(text("set local role market_data_api"))
        row = connection.execute(text("select * from api_v1.daily_bars")).mappings().one()
    assert row["volume"] is None


def test_daily_bounds_survive_unit_gate(database_engine: Engine):
    _facts(database_engine)
    with pytest.raises(DBAPIError), database_engine.begin() as connection:
        connection.execute(
            text("""
select * from api_v1.query_daily_bars('SSE:600000',:day,:day,5001)
"""),
            {"day": DAY},
        )


def test_stock_certificate_cannot_admit_convertible_bond_quantities(database_engine: Engine):
    ingestion_id = _facts(database_engine)
    _certify(database_engine, ingestion_id, "daily_bar.volume")
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into core.security (
 symbol,code,exchange,current_name,security_type,status,source_code,ingestion_id
) values ('SSE:110001','110001','SSE','test bond','convertible_bond','listed','baostock',:id)
"""),
            {"id": ingestion_id},
        )
        connection.execute(
            text("""
insert into convertible_bond.daily_bar (
 symbol,trade_date,market,open,high,low,close,previous_close,volume,amount,
 remain_size,trade_status,source_code,ingestion_id
) values ('SSE:110001',:day,'CN_A_SHARE',100,100,100,100,99,37,3700,
 5000000,'trading','tushare',:id)
"""),
            {"id": ingestion_id, "day": DAY},
        )
        row = (
            connection.execute(text("select * from api_v1.convertible_bond_daily_bars"))
            .mappings()
            .one()
        )

    assert row["close"] == Decimal(100)
    assert row["volume"] is row["amount"] is row["remain_size"] is None
    assert row["quantity_quality"]["status"] == "unverified"
    _certify(database_engine, ingestion_id, "convertible_bond_daily_bar.volume")
    with database_engine.connect() as connection:
        admitted = (
            connection.execute(text("select * from api_v1.convertible_bond_daily_bars"))
            .mappings()
            .one()
        )
    assert admitted["volume"] == 37
    assert admitted["amount"] is admitted["remain_size"] is None
    assert (
        admitted["quantity_quality"]["field_groups"]["convertible_bond_daily_bar.volume"]["status"]
        == "certified"
    )


def test_rights_base_share_proof_does_not_certify_proceeds_or_change_price_events(
    database_engine: Engine,
):
    ingestion_id = _facts(database_engine)
    _certify(database_engine, ingestion_id, "capital.rights_issue.quantities")
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into capital.rights_issue (
 symbol,record_date,ex_date,rights_ratio,rights_price,base_shares,proceeds,source_code,ingestion_id
) values ('SSE:600000',:day,:day,0.1,5,100,50,'akshare',:id)
"""),
            {"id": ingestion_id, "day": DAY},
        )
        row = connection.execute(text("select * from api_v1.rights_issues")).mappings().one()
    assert row["base_shares"] == 100
    assert row["proceeds"] is None
    assert row["rights_ratio"] == Decimal("0.1")
    assert row["rights_price"] == Decimal(5)


def test_public_contract_preflight_is_exact_and_has_no_write_privilege(database_engine: Engine):
    with database_engine.begin() as connection:
        evidence_oid = connection.execute(
            text("select 'audit.quantity_unit_certification'::regclass::oid")
        ).scalar_one()
        connection.execute(text("set local role market_data_api"))
        assert (
            connection.execute(text("select api_v1.quantity_unit_contract_version()")).scalar_one()
            == "stock-quantity.v1"
        )
        privileges = connection.execute(
            text("""
select has_table_privilege(current_user,cast(:oid as oid),'insert'),
       has_function_privilege('anon','api_v1.quantity_unit_contract_version()','execute')
"""),
            {"oid": evidence_oid},
        ).one()
    assert tuple(privileges) == (False, False)


def test_indicator_rpc_and_private_quote_view_mask_quantities(database_engine: Engine):
    ingestion_id = _facts(database_engine)
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into core.stock_daily_indicator (
 symbol,trade_date,market,close,total_shares,circulating_shares,free_float_shares,
 total_market_value,circulating_market_value,price_limit_status,source_code,ingestion_id
) values ('SSE:600000',:day,'CN_A_SHARE',10,1000,900,800,10000,9000,'flat','tushare',:id)
"""),
            {"id": ingestion_id, "day": DAY},
        )
        connection.execute(
            text("""
insert into realtime.stock_quote_snapshot (
 ingestion_id,raw_id,symbol,observed_at,source_timestamp,quote_status,last_price,
 cumulative_volume,cumulative_amount,bid1_price,bid1_volume,source_code
) select :id,raw_id,'SSE:600000',now(),now(),'trading',10,100,1000,10,37,'tencent_quote'
from ingestion.raw_manifest where ingestion_id=:id
"""),
            {"id": ingestion_id},
        )
        indicator = connection.execute(
            text("""
select api_v1.query_latest_stock_daily_indicators(array['600000'])
""")
        ).scalar_one()["items"][0]
        quote = (
            connection.execute(text("select * from audit.unit_safe_stock_quote_snapshot"))
            .mappings()
            .one()
        )

    assert Decimal(str(indicator["close"])) == Decimal(10)
    assert indicator["total_shares"] is indicator["free_float_shares"] is None
    assert indicator["quantity_quality"]["status"] == "unverified"
    assert Decimal(str(quote["last_price"])) == Decimal(10)
    assert quote["cumulative_volume"] is None
    assert quote["bid1_volume"] is None
    assert quote["quantity_quality"]["status"] == "unverified"


def test_factor_quantity_leaves_are_null_under_a_new_response_catalog(database_engine: Engine):
    ingestion_id = _facts(database_engine)
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into core.stock_technical_factor_snapshot (
 symbol,trade_date,market,source_code,factor_catalog_version,
 unadjusted,forward_adjusted,backward_adjusted,common,ingestion_id
) values ('SSE:600000',:day,'CN_A_SHARE','tushare','stock-technical-factor.v1',
 '{"obv": 100,"emv": 20,"maemv": 30,"macd": 12.5}',
 '{"obv": 100,"emv": 20,"maemv": 30,"macd": 12.5}',
 '{"obv": 100,"emv": 20,"maemv": 30,"macd": 12.5}','{}',:id)
"""),
            {"id": ingestion_id, "day": DAY},
        )
        result = connection.execute(
            text("""
select api_v1.query_stock_technical_factors(array['SSE:600000'],:day)
"""),
            {"day": DAY},
        ).scalar_one()["items"][0]
        audit = connection.execute(
            text("select unadjusted from core.stock_technical_factor_snapshot")
        ).scalar_one()

    assert result["factor_catalog_version"] == "stock-technical-factor.v2"
    assert result["source_factor_catalog_version"] == "stock-technical-factor.v1"
    for group in ("unadjusted", "forward_adjusted", "backward_adjusted"):
        assert result[group]["obv"] is result[group]["emv"] is result[group]["maemv"] is None
        assert result[group]["macd"] == "12.5"
    assert audit["obv"] == 100
    assert result["quantity_quality"]["status"] == "unverified"


def test_immutable_snapshot_default_is_deferred_and_explicit_old_version_is_409(
    database_engine: Engine,
):
    ingestion_id = _facts(database_engine)
    snapshot_id, calculation_id = uuid4(), uuid4()
    params = {
        "id": ingestion_id,
        "snapshot": snapshot_id,
        "calculation": calculation_id,
        "day": DAY,
        "hash": "a" * 64,
        "input": "b" * 64,
    }
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into derived.calculation_run (
 calculation_id,calculation_code,algorithm_version,mode,start_date,end_date,status,
 input_watermark,input_hash,requested_at,calculated_at,finished_at
) values (:calculation,'cn_a_share_daily_derived','1.1.0','incremental',:day,:day,
 'succeeded','{}',:hash,now(),now(),now())
"""),
            params,
        )
        connection.execute(
            text("""
insert into today_limit_up.snapshot (
 snapshot_id,calculation_id,trade_date,version,status,member_count,candidate_count,
 rejected_count,content_hash,input_hash,rule_version,algorithm_version,generated_at
) values (:snapshot,:calculation,:day,1,'ready',1,1,0,:hash,:input,'1','1',now())
"""),
            params,
        )
        connection.execute(
            text("""
insert into today_limit_up.member (
 snapshot_id,symbol,code,historical_name,previous_close,close,limit_price,change_percent,
 free_float_shares,free_float_market_cap_cny,daily_bar_ingestion_id,indicator_ingestion_id,
 name_ingestion_id,pool_calculation_id,volume,amount_cny
) values (:snapshot,'SSE:600000','600000','浦发银行',10,11,11,10,100,1100,
 :id,:id,:id,:calculation,100,1000)
"""),
            params,
        )
        public = connection.execute(
            text("""
select api_v1.query_daily_limit_up_list(:day,null,0,500)
"""),
            params,
        ).scalar_one()
        audit = connection.execute(text("select volume from today_limit_up.member")).scalar_one()

    assert public["status"] == "deferred"
    assert public["items"] == []
    assert public["quantity_quality"]["status"] == "unverified"
    assert audit == 100
    with pytest.raises(DBAPIError) as error, database_engine.begin() as connection:
        connection.execute(text("select api_v1.query_daily_limit_up_list(:day,1,0,500)"), params)
    assert error.value.orig.sqlstate == "PT409"

    for group in ("daily_bar.volume", "daily_bar.amount", "stock_daily_indicator.shares"):
        _certify(database_engine, ingestion_id, group)
    with database_engine.begin() as connection:
        admitted = connection.execute(
            text("select api_v1.query_daily_limit_up_list(:day,null,0,500)"), params
        ).scalar_one()
        assert admitted["status"] == "ready"
        repair_id, replacement_id = uuid4(), uuid4()
        connection.execute(
            text("""
insert into ingestion.ingestion_run (
 ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at
) values (:replacement,'pytdx','daily_bar','succeeded',now(),now(),now())
"""),
            {"replacement": replacement_id},
        )
        connection.execute(
            text("""
insert into audit.quantity_unit_repair_run(repair_run_id,plan_hash,rule_version,plan,status)
values (:repair,:hash,'test-rule.v1','{}','completed')
"""),
            {"repair": repair_id, "hash": "c" * 64},
        )
        connection.execute(
            text("""
insert into audit.quantity_unit_repair_item (
 repair_run_id,item_key,rule_version,symbol,trade_date,original_ingestion_id,raw_id,
 raw_hash,before_state,before_hash,after_state,after_hash,new_ingestion_id,status
) select :repair,:key,'test-rule.v1','SSE:600001',:day,:id,raw_id,content_sha256,
 '{"volume": 1}',:before,'{"volume": 100}',:after,:replacement,'applied'
from ingestion.raw_manifest where ingestion_id=:id
"""),
            {
                "repair": repair_id,
                "key": "d" * 64,
                "before": "e" * 64,
                "after": "f" * 64,
                "replacement": replacement_id,
                "id": ingestion_id,
                "day": DAY,
            },
        )
        quarantined = connection.execute(
            text("select api_v1.query_daily_limit_up_list(:day,null,0,500)"), params
        ).scalar_one()

    assert quarantined["status"] == "deferred"
    assert quarantined["items"] == []
    with pytest.raises(DBAPIError) as error, database_engine.begin() as connection:
        connection.execute(text("select api_v1.query_daily_limit_up_list(:day,1,0,500)"), params)
    assert error.value.orig.sqlstate == "PT409"


@pytest.mark.parametrize(
    "relation",
    ["call_auction_market_series_snapshot", "call_auction_market_series_snapshot_history"],
)
def test_series_hot_and_history_use_identical_quantity_admission(
    database_engine: Engine, relation: str
):
    _facts(database_engine)
    ingestion_id = uuid4()
    with database_engine.begin() as connection:
        session_id = _insert_series_round(
            connection,
            trade_date=DAY,
            ingestion_id=ingestion_id,
            ingestion_status="succeeded",
            symbols=("SSE:600000",),
            started_at=datetime(2026, 10, 22, 1, 15, tzinfo=UTC),
            workflow_attempt=1,
        )
        # The shared positive fixture issues a synthetic certificate. This case
        # specifically models an archived batch restored without any unit proof.
        connection.execute(
            text("delete from audit.quantity_unit_certification where ingestion_id=:id"),
            {"id": ingestion_id},
        )
        connection.execute(
            text(f"""
insert into realtime.{relation} (
 ingestion_id,session_id,sample_seq,batch_code,scheduled_at,symbol,trade_date,
 observed_at,last_price,previous_close,high_price,low_price,cumulative_volume,
 cumulative_amount,bid1_price,bid1_volume,source_code,value_semantics
) values (:id,:session,31,'092520','2026-10-22 01:25:20+00','SSE:600000',:day,
 '2026-10-22 01:25:21+00',10,9,10,9,100,1000,10,37,'pytdx_hq','opening_trade')
"""),
            {"id": ingestion_id, "session": session_id, "day": DAY},
        )
        row = connection.execute(text(f"select * from audit.unit_safe_{relation}")).mappings().one()

    assert row["cumulative_volume"] is row["cumulative_amount"] is row["bid1_volume"] is None
    assert row["last_price"] == Decimal(10)
    assert row["quantity_quality"]["status"] == "unverified"

    if relation == "call_auction_market_series_snapshot":
        with database_engine.begin() as connection:
            connection.execute(
                text("""
update realtime.call_auction_market_series_session
set status='partial',finished_at=now() where session_id=:session
"""),
                {"session": session_id},
            )
            series = connection.execute(
                text("""
select api_v1.query_call_auction_market_series_snapshots(:day,array['600000'],null)
"""),
                {"day": DAY},
            ).scalar_one()
            final = connection.execute(
                text("""
select api_v1.query_call_auction_market_snapshots(:day,array['600000'])
"""),
                {"day": DAY},
            ).scalar_one()
        for item in (series["rounds"][0]["items"][0], final["items"][0]):
            assert item["cumulative_volume"] is item["cumulative_amount"] is None
            assert item["quantity_quality"]["status"] == "unverified"
