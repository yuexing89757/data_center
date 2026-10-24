"""Tushare down-source publication under the real Worker and API roles."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from test_postgres_integration import (  # noqa: F401
    TRADE_DATE,
    _completed_run,
    _running_run,
    _seed_quantity_fixture_proof,
    _sqlalchemy_url,
    database_engine,
    empty_database_url,
    migrated_database_url,
)

from market_data_center.domain.ingestion import DatasetCode, IngestionStatus, ProviderCode
from market_data_center.persistence.postgres import UPDATE_INGESTION_RUN, PostgreSQLPersistence
from market_data_center.persistence.today_limit_down_postgres import (
    PostgreSQLTodayLimitDownPersistence,
)
from market_data_center.providers.tushare_limit_down import (
    FIELDS,
    TushareCurrentDayLimitDownProvider,
)
from market_data_center.raw_store import LocalRawStore
from market_data_center.today_limit_down_service import TodayLimitDownFillService

pytestmark = pytest.mark.integration


@pytest.fixture
def down_facts(database_engine):  # noqa: F811
    persistence = PostgreSQLPersistence(database_engine)
    market = _running_run(DatasetCode.DAILY_BAR)
    indicator = _running_run(DatasetCode.STOCK_DAILY_INDICATOR, ProviderCode.TUSHARE)
    for run in (market, indicator):
        persistence.create_ingestion_run(run)
    calculation, pool = uuid4(), uuid4()
    with database_engine.begin() as c:
        for run in (market, indicator):
            c.execute(
                UPDATE_INGESTION_RUN,
                persistence._run_update_parameters(_completed_run(run, row_count=2)),
            )
        _seed_quantity_fixture_proof(
            c, market.ingestion_id, ("daily_bar.volume", "daily_bar.amount")
        )
        _seed_quantity_fixture_proof(c, indicator.ingestion_id, ("stock_daily_indicator.shares",))
        c.execute(
            text("""insert into core.trading_calendar
            (market,trade_date,is_trading_day,source_code,ingestion_id)
            values ('CN_A_SHARE',:day,true,'baostock',:id)"""),
            {"day": TRADE_DATE, "id": market.ingestion_id},
        )
        for symbol in ("SSE:600000", "SSE:600001"):
            p = {
                "symbol": symbol,
                "code": symbol.split(":")[1],
                "day": TRADE_DATE,
                "id": market.ingestion_id,
                "indicator": indicator.ingestion_id,
            }
            c.execute(
                text("""insert into core.security
(symbol,code,exchange,current_name,security_type,status,ipo_date,source_code,ingestion_id)
values
(:symbol,:code,'SSE','Test','stock','listed','2000-01-01','baostock',:id)"""),
                p,
            )
            c.execute(
                text("""insert into core.security_name_history
                (symbol,name,effective_from,source_code,ingestion_id)
                values (:symbol,'Test','2000-01-01','baostock',:id)"""),
                p,
            )
            c.execute(
                text("""insert into core.daily_bar
(symbol,trade_date,market,open,high,low,close,previous_close,volume,amount,trade_status,is_st,source_code,ingestion_id)
values
(:symbol,:day,'CN_A_SHARE',9,9,9,9,10,37,333,'trading',false,'baostock',:id)"""),
                p,
            )
            c.execute(
                text("""insert into core.stock_daily_indicator
(symbol,trade_date,market,close,free_float_shares,free_float_turnover_rate_pct,price_limit_status,source_code,ingestion_id)
values (:symbol,:day,'CN_A_SHARE',9,100,1,'unknown','tushare',:indicator)"""),
                p,
            )
        c.execute(
            text("""insert into derived.calculation_run
(calculation_id,calculation_code,algorithm_version,mode,start_date,end_date,status,input_watermark,input_hash,requested_at,calculated_at,finished_at,output_rows)
values
(:id,'cn_a_mainboard_price_limit_pools','1.0.0','incremental',:day,:day,'succeeded','{}',:hash,now(),now(),now(),2)"""),
            {"id": calculation, "day": TRADE_DATE, "hash": "1" * 64},
        )
        c.execute(
            text("""insert into stock_pool.snapshot
(snapshot_id,calculation_id,pool_code,basis_trade_date,effective_trade_date,version,status,member_count,candidate_count,rejected_count,content_hash,input_hash,rule_version,algorithm_version,generated_at)
values
(:id,:calc,'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN',:day,:next,1,'ready',2,2,0,:hash,:hash,'CN_MAINBOARD_2026_07_06','1.0.0',now())"""),
            {
                "id": pool,
                "calc": calculation,
                "day": TRADE_DATE,
                "next": TRADE_DATE + timedelta(days=1),
                "hash": "2" * 64,
            },
        )
        for symbol in ("SSE:600000", "SSE:600001"):
            p = {"pool": pool, "calc": calculation, "symbol": symbol, "day": TRADE_DATE}
            c.execute(
                text(
                    """insert into stock_pool.member(snapshot_id,symbol,direction)
values(:pool,:symbol,'down')"""
                ),
                p,
            )
            c.execute(
                text("""insert into derived.daily_price_limit
(calculation_id,symbol,trade_date,previous_close,upper_limit,lower_limit,limit_ratio,price_tick,is_st,rule_version,algorithm_version)
values
(:calc,:symbol,:day,10,11,9,0.1,0.01,false,'CN_MAINBOARD_2026_07_06','1.0.0')"""),
                p,
            )
            c.execute(
                text("""insert into derived.price_limit_event
(calculation_id,symbol,trade_date,direction,close,limit_price,rule_version,algorithm_version)
values (:calc,:symbol,:day,'down',9,9,'CN_MAINBOARD_2026_07_06','1.0.0')"""),
                p,
            )
        for code in ("daily_market", "stock_daily_indicator"):
            c.execute(
                text("""insert into operations.workflow_run
(workflow_run_id,workflow_code,scheduled_for,trigger_source,attempt,status,started_at,finished_at)
values (:id,:code,:at,'manual',1,'succeeded',:at,:at)"""),
                {
                    "id": uuid4(),
                    "code": code,
                    "at": datetime.combine(TRADE_DATE, datetime.min.time(), tzinfo=UTC),
                },
            )


def test_tushare_down_source_worker_snapshot_versions_and_api(
    database_engine,  # noqa: F811
    migrated_database_url,  # noqa: F811
    down_facts,
    tmp_path,
):
    worker = create_engine(
        _sqlalchemy_url(migrated_database_url),
        connect_args={"options": "-c role=market_data_worker"},
    )
    try:
        persistence = PostgreSQLTodayLimitDownPersistence(worker)
        old = _running_run(DatasetCode.TODAY_LIMIT_DOWN_SOURCE, ProviderCode.AKSHARE)
        persistence.create_ingestion_run(old)
        failed = replace(_completed_run(old, row_count=0), status=IngestionStatus.FAILED)
        old_summary = persistence.commit_failed(
            TRADE_DATE, failed, "source_normalization_failed_ProviderError"
        )
        with database_engine.connect() as c:
            old_fact = c.execute(
                text(
                    """select status,content_hash,input_hash
from today_limit_down.snapshot where
snapshot_id=:id"""
                ),
                {"id": old_summary.snapshot_id},
            ).one()
        raw = dict.fromkeys(FIELDS, "")
        raw.update(
            ts_code="600000.SH",
            trade_date=TRADE_DATE.strftime("%Y%m%d"),
            name="source name",
            fd_amount=Decimal("123.45"),
            last_time="145959",
            open_times=0,
            limit_times=1,
            limit="D",
        )
        client = Mock()
        client.query.return_value = [raw]
        service = TodayLimitDownFillService(
            persistence=persistence,
            raw_store=LocalRawStore(tmp_path),
            provider_factory=lambda: TushareCurrentDayLimitDownProvider(client),
            provider_code=ProviderCode.TUSHARE,
        )
        result = service.fill(TRADE_DATE)
        assert result.version == 2 and result.status == "partial" and result.member_count == 2
        assert service.fill(TRADE_DATE).snapshot_id == result.snapshot_id
        with database_engine.connect() as c:
            assert c.scalar(
                text("select audit.snapshot_quantity_is_certified('today_limit_down',:id)"),
                {"id": result.snapshot_id},
            )
            assert c.execute(
                text(
                    """select source_code,source_reported_sealed_funds_cny from
today_limit_down.source_observation limit 1"""
                )
            ).one() == ("tushare", Decimal("123.45"))
            assert c.scalar(text("select count(*) from today_limit_down.snapshot")) == 2
            assert (
                c.execute(
                    text(
                        """select status,content_hash,input_hash
from today_limit_down.snapshot where
snapshot_id=:id"""
                    ),
                    {"id": old_summary.snapshot_id},
                ).one()
                == old_fact
            )
            c.execute(text("set local role market_data_api"))
            payload = c.scalar(
                text("select api_v1.query_daily_limit_down_list(:day,null,0,100)"),
                {"day": TRADE_DATE},
            )
            assert payload["status"] == "partial"
            items = payload["items"]
            assert len(items) == 2
            assert Decimal(str(items[0]["source_reported_sealed_funds_cny"])) == Decimal("123.45")
            assert items[1]["source_reported_sealed_funds_cny"] is None
            assert items[0]["name"] == "Test"
            assert items[0]["first_limit_down_at"] is None
            assert items[0]["closing_ask1_sealing_amount_cny"] is None
        with database_engine.connect() as c:
            metadata = c.scalar(
                text(
                    """select request_params from ingestion.ingestion_run where
provider_code='tushare' and dataset_code='today_limit_down_source' limit 1"""
                )
            )
            assert metadata["api_name"] == "limit_list_d" and metadata["limit_type"] == "D"
    finally:
        worker.dispose()
