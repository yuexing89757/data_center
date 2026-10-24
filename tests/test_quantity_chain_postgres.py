"""Synthetic adapter/Raw to native PostgreSQL RPC and FastAPI chain, without PostgREST."""

from datetime import date
from decimal import Decimal
from struct import pack
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine, create_engine, text
from test_postgres_integration import database_engine as database_engine
from test_postgres_integration import empty_database_url as empty_database_url
from test_postgres_integration import migrated_database_url as migrated_database_url
from test_quantity_unit_repair_integration import seed

from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.pipeline import IngestionPipeline
from market_data_center.providers.pytdx import PytdxProvider
from market_data_center.providers.tushare import TushareProvider
from market_data_center.public_api.app import create_app
from market_data_center.public_api.queries import PostgreSQLPublicQueryService
from market_data_center.quantity_unit_repair import apply_plan, preview
from market_data_center.raw_store import LocalRawStore
from market_data_center.reliability import RawReplayService
from market_data_center.settings import ApiSettings, PytdxDailyBarSettings, PytdxPoolSettings

pytestmark = pytest.mark.integration
KEY = "synthetic-quantity-chain-api-key-32-characters"


@pytest.fixture
def reader(database_engine):
    """The HTTP service uses the API role and read-only native RPC connections."""
    engine = create_engine(
        database_engine.url,
        connect_args={"options": "-c default_transaction_read_only=on -c role=market_data_api"},
    )
    service = PostgreSQLPublicQueryService(engine)
    app = create_app(
        settings=ApiSettings(
            fastapi_database_url=SecretStr("unused"),
            fastapi_api_key=SecretStr(KEY),
            _env_file=None,
        ),
        query_service=service,
        auction_indicative_service=Mock(),
        tencent_quote_live_service=Mock(),
    )
    try:
        with TestClient(app) as client:
            yield client
    finally:
        engine.dispose()


def _assert_public(engine, client, day, volume, amount, status="certified"):
    with engine.begin() as connection:
        connection.execute(text("set local role market_data_api"))
        row = (
            connection.execute(
                text("select * from api_v1.query_daily_bars('SSE:600000',:day,:day,1)"),
                {"day": day},
            )
            .mappings()
            .one()
        )
    assert row["volume"] == volume
    assert row["amount"] == amount
    assert row["quantity_quality"]["status"] == status
    response = client.get(
        "/api/v1/daily-bars/600000",
        params={"trade_date": day.isoformat(), "limit": 1},
        headers={"X-API-Key": KEY},
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["volume"] == volume
    assert (None if item["amount"] is None else Decimal(item["amount"])) == amount
    assert item["quantity_quality"]["status"] == status


def _references(engine: Engine, day: date) -> None:
    ingestion_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("""
insert into ingestion.ingestion_run
 (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
values (:id,'baostock','security','succeeded',now(),now(),now())
"""),
            {"id": ingestion_id},
        )
        connection.execute(
            text("""
insert into core.security
 (symbol,code,exchange,current_name,security_type,status,source_code,ingestion_id)
values ('SSE:600000','600000','SSE','synthetic stock','stock','listed','baostock',:id)
"""),
            {"id": ingestion_id},
        )
        connection.execute(
            text("""
insert into core.trading_calendar (market,trade_date,is_trading_day,source_code,ingestion_id)
values ('CN_A_SHARE',:day,true,'baostock',:id)
"""),
            {"id": ingestion_id, "day": day},
        )


def test_local_share_adapter_pipeline_replay_and_http_preserve_odd_shares(
    database_engine,
    reader,
    tmp_path,
):
    day = date(2026, 10, 22)
    _references(database_engine, day)
    local = tmp_path / "vipdoc" / "sh" / "lday"
    local.mkdir(parents=True)
    day_bytes = pack("<IIIIIfII", 20261022, 1000, 1000, 1000, 1000, 370.25, 37, 0)
    day_file = local / "sh600000.day"
    day_file.write_bytes(day_bytes)
    provider = PytdxProvider(
        PytdxDailyBarSettings(pytdx_vipdoc_path=str(tmp_path / "vipdoc"), _env_file=None),
        pool_settings=PytdxPoolSettings(
            pytdx_pool_path=tmp_path / "absent-pool.json", _env_file=None
        ),
        client_factory=lambda: pytest.fail("local fixture must not connect to a remote provider"),
    )
    store = LocalRawStore(tmp_path / "raw")
    persistence = PostgreSQLPersistence(database_engine)
    with provider:
        run = IngestionPipeline(
            provider=provider, raw_store=store, persistence=persistence
        ).ingest_daily_bars(
            "sh.600000",
            day,
            day,
        )
    assert run.accepted_rows == 1
    source = persistence.replay_source(run.ingestion_id)
    assert source.manifest.schema_version == "pytdx.local_daily_bar.v3"
    raw_path = store.root / source.manifest.object_path
    raw_bytes = raw_path.read_bytes()
    assert store.read_jsonl(source.manifest)[0]["volume"] == "37"
    _assert_public(database_engine, reader, day, 37, Decimal("370.25"))
    with database_engine.connect() as connection:
        groups = (
            connection.execute(
                text("""
select field_group from audit.quantity_unit_certification
where ingestion_id=:id and status='certified'
"""),
                {"id": run.ingestion_id},
            )
            .scalars()
            .all()
        )
    assert set(groups) == {"daily_bar.volume", "daily_bar.amount"}
    for _ in range(2):
        summary = RawReplayService(raw_store=store, persistence=persistence).replay(
            run.ingestion_id
        )
        assert summary.accepted_rows == 1
        _assert_public(database_engine, reader, day, 37, Decimal("370.25"))
    assert raw_path.read_bytes() == raw_bytes
    assert day_file.read_bytes() == day_bytes
    assert len(list((tmp_path / "raw").rglob("*.jsonl"))) == 1


def test_legacy_remote_raw_repair_replay_and_same_http_client_never_restore_lots(
    database_engine,
    reader,
    tmp_path,
):
    store, scope, original_id = seed(database_engine, tmp_path)
    persistence = PostgreSQLPersistence(database_engine)
    source = persistence.replay_source(original_id)
    raw_path = tmp_path / source.manifest.object_path
    original_bytes = raw_path.read_bytes()
    assert store.read_jsonl(source.manifest)[0]["volume"] == "37"  # Source lots.
    _assert_public(database_engine, reader, scope.start_date, None, None, "unverified")
    plan = preview(database_engine, store, scope)
    assert plan["items"][0]["target_volume"] == 3700
    assert apply_plan(database_engine, store, plan, plan["plan_hash"]) == {"applied": 1}
    _assert_public(database_engine, reader, scope.start_date, 3700, Decimal("37000"))
    for _ in range(2):
        summary = RawReplayService(raw_store=store, persistence=persistence).replay(original_id)
        assert summary.accepted_rows == 1
        _assert_public(database_engine, reader, scope.start_date, 3700, Decimal("37000"))
    with database_engine.connect() as connection:
        row = connection.execute(text("select volume,amount from core.daily_bar")).one()
        assert row.volume == 3700 and row.amount == Decimal("37000")
        assert (
            connection.execute(text("select count(*) from ingestion.raw_manifest")).scalar_one()
            == 1
        )
    assert raw_path.read_bytes() == original_bytes
    assert source.manifest.schema_version == "pytdx.remote_daily_bar.v1"
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1


def test_regulation_index_pipeline_replay_certificates_and_api_rpc(database_engine, tmp_path):
    day = date(2026, 10, 23)
    _references(database_engine, day)
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into core.security
 (symbol,code,exchange,current_name,security_type,status,source_code,ingestion_id)
select 'SSE:000688','000688','SSE','synthetic benchmark','index','listed','baostock',ingestion_id
from core.security where symbol='SSE:600000'
""")
        )

    class IndexClient:
        def query(self, api_name, *, params, fields):
            assert api_name == "index_daily"
            return [
                {
                    "ts_code": "000688.SH",
                    "trade_date": "20261023",
                    "open": "100",
                    "high": "102",
                    "low": "99",
                    "close": "101",
                    "pre_close": "100",
                    "vol": "0.37",
                    "amount": "56.789",
                }
            ]

    store = LocalRawStore(tmp_path)
    persistence = PostgreSQLPersistence(database_engine)
    run = IngestionPipeline(
        provider=TushareProvider(IndexClient()), raw_store=store, persistence=persistence
    ).ingest_daily_bars("SSE:000688", day, day)
    assert run.accepted_rows == 1
    source = persistence.replay_source(run.ingestion_id)
    raw_path = store.root / source.manifest.object_path
    original = raw_path.read_bytes()
    for attempt in range(3):
        if attempt:
            replay = RawReplayService(raw_store=store, persistence=persistence).replay(
                run.ingestion_id
            )
            assert replay.accepted_rows == 1
        with database_engine.begin() as connection:
            record = connection.execute(
                text(
                    "select volume,amount,ingestion_id from core.daily_bar "
                    "where symbol='SSE:000688'"
                )
            ).one()
            assert (record.volume, record.amount) == (37, Decimal("56789"))
            assert (
                connection.execute(
                    text("""
select count(*) from audit.quantity_unit_certification
where ingestion_id=:id and status='certified'
"""),
                    {"id": record.ingestion_id},
                ).scalar_one()
                == 2
            )
            connection.execute(text("set local role market_data_api"))
            public = (
                connection.execute(
                    text("select * from api_v1.query_daily_bars('SSE:000688',:day,:day,1)"),
                    {"day": day},
                )
                .mappings()
                .one()
            )
            assert public["volume"] == 37 and public["amount"] == Decimal("56789")
            assert public["quantity_quality"]["status"] == "certified"
    assert raw_path.read_bytes() == original
    assert store.read_jsonl(source.manifest)[0]["vol"] == "0.37"
