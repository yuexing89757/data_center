"""Exact JSON writes and bounded, private technical-factor reads."""

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from json import loads
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from test_postgres_integration import (  # noqa: F401
    database_engine,
    empty_database_url,
    migrated_database_url,
)

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    RawFileFormat,
    RawManifest,
)
from market_data_center.domain.records import IngestionEnvelope, Market
from market_data_center.domain.stock_technical_factor import (
    COMMON_FACTOR_NAMES,
    FACTOR_CATALOG_VERSION,
    TECHNICAL_FACTOR_NAMES,
    StockTechnicalFactorSnapshotRecord,
)
from market_data_center.persistence.technical_factor_postgres import (
    UPSERT_TECHNICAL_FACTOR,
    PostgreSQLTechnicalFactorPersistence,
    _numeric_json,
)

DAY = date(2026, 10, 9)
NOW = datetime(2026, 10, 9, 13, tzinfo=UTC)


def _record(symbol="SSE:600000", value=Decimal("12345678901234567890.123456789")):
    factors = dict.fromkeys(TECHNICAL_FACTOR_NAMES)
    factors.update(macd=value, cci=Decimal("-12.345"), atr=Decimal(0))
    return StockTechnicalFactorSnapshotRecord(
        symbol=symbol,
        trade_date=DAY,
        market=Market.CN_A_SHARE,
        source_code="tushare",
        factor_catalog_version=FACTOR_CATALOG_VERSION,
        unadjusted=factors,
        forward_adjusted=factors,
        backward_adjusted=factors,
        common={name: 3 if name == "up_days" else None for name in COMMON_FACTOR_NAMES},
    )


def _batch(persistence, record):
    run = IngestionRun(
        ingestion_id=uuid4(),
        provider_code=ProviderCode.TUSHARE,
        dataset_code=DatasetCode.STOCK_TECHNICAL_FACTOR,
        status=IngestionStatus.RUNNING,
        requested_at=NOW,
        started_at=NOW,
        request_params={"trade_date": DAY.isoformat()},
    )
    persistence.create_ingestion_run(run)
    manifest = RawManifest(
        raw_id=uuid4(),
        ingestion_id=run.ingestion_id,
        object_path=f"factor/{run.ingestion_id}.jsonl",
        file_format=RawFileFormat.JSONL,
        content_sha256="a" * 64,
        byte_size=100,
        row_count=1,
        schema_version="tushare.stock_technical_factor.v1",
    )
    return (
        replace(
            run, status=IngestionStatus.SUCCEEDED, finished_at=NOW, fetched_rows=1, accepted_rows=1
        ),
        manifest,
        [IngestionEnvelope(run.ingestion_id, record)],
    )


@pytest.fixture
def factor_seed(database_engine):  # noqa: F811
    ingestion_id = uuid4()
    with database_engine.begin() as connection:
        connection.execute(
            text("""insert into ingestion.ingestion_run
            (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
            values (:id,'baostock','security','succeeded',now(),now(),now())"""),
            {"id": ingestion_id},
        )
        connection.execute(
            text("""insert into core.security
            (symbol,code,exchange,current_name,security_type,status,ipo_date,source_code,ingestion_id)
            values ('SSE:600000','600000','SSE','浦发银行','stock','listed',
                    '2000-01-01','baostock',:id)"""),
            {"id": ingestion_id},
        )
        connection.execute(
            text("""insert into core.trading_calendar
            (market,trade_date,is_trading_day,source_code,ingestion_id)
            values ('CN_A_SHARE',:day,true,'baostock',:id)"""),
            {"day": DAY, "id": ingestion_id},
        )


@pytest.mark.integration
def test_rpc_precision_exact_date_upsert_missing_and_grants(database_engine, factor_seed):  # noqa: F811
    persistence = PostgreSQLTechnicalFactorPersistence(database_engine)
    run, manifest, records = _batch(persistence, _record())
    persistence.commit_technical_factor_batch(run, manifest, records, [])
    with database_engine.connect() as connection:
        payload = connection.execute(
            text("select api_v1.query_stock_technical_factors(:symbols,:day)"),
            {"symbols": ["SSE:600000", "SZSE:000001", "SSE:600000"], "day": DAY},
        ).scalar_one()
        assert payload["missing_symbols"] == ["SZSE:000001"]
        assert len(payload["items"]) == 1
        item = payload["items"][0]
        assert item["unadjusted"]["macd"] == "12345678901234567890.123456789"
        assert item["unadjusted"]["cci"] == "-12.345"
        assert item["unadjusted"]["atr"] == "0"
        assert item["unadjusted"]["bbi"] is None
        assert item["common"]["up_days"] == 3
        assert "ingestion_id" not in item
        assert connection.execute(
            text(
                "select jsonb_typeof(unadjusted->'macd'),unadjusted->>'macd' "
                "from core.stock_technical_factor_snapshot"
            )
        ).one() == ("number", "12345678901234567890.123456789")
        missing = connection.execute(
            text("select api_v1.query_stock_technical_factors_by_codes(:codes,:day)"),
            {"codes": ["600000", "000001"], "day": date(2026, 10, 8)},
        ).scalar_one()
        assert missing["items"] == [] and missing["missing_codes"] == ["600000", "000001"]
        for role in ("anon", "authenticated", "market_data_api"):
            assert not connection.execute(
                text(
                    "select has_table_privilege(:role,"
                    "'core.stock_technical_factor_snapshot','SELECT')"
                ),
                {"role": role},
            ).scalar_one()
        assert connection.execute(
            text(
                "select has_function_privilege('market_data_api',"
                "'api_v1.query_stock_technical_factors_by_codes(text[],date)','EXECUTE')"
            )
        ).scalar_one()
    revised, manifest, records = _batch(persistence, _record(value=None))
    persistence.commit_technical_factor_batch(revised, manifest, records, [])
    with database_engine.connect() as connection:
        row = connection.execute(
            text("select unadjusted->'macd',ingestion_id from core.stock_technical_factor_snapshot")
        ).one()
        assert row[0] is None and row[1] == revised.ingestion_id
    assert persistence.technical_factor_securities(DAY) == {"SSE:600000"}


@pytest.mark.integration
def test_atomic_rollback_on_invalid_security(database_engine, factor_seed):  # noqa: F811
    persistence = PostgreSQLTechnicalFactorPersistence(database_engine)
    run, manifest, records = _batch(persistence, _record("SZSE:000001"))
    with pytest.raises(IntegrityError):
        persistence.commit_technical_factor_batch(run, manifest, records, [])
    with database_engine.connect() as connection:
        assert (
            connection.execute(
                text("select count(*) from ingestion.raw_manifest where raw_id=:id"),
                {"id": manifest.raw_id},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("select status from ingestion.ingestion_run where ingestion_id=:id"),
                {"id": run.ingestion_id},
            ).scalar_one()
            == "running"
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("symbols", "day"),
    [
        ([], DAY),
        (["SSE:600000"] * 101, DAY),
        ([None], DAY),
        (["600000"], DAY),
        (["SSE:600000"], None),
    ],
)
def test_rpc_rejects_unbounded_or_invalid_inputs(database_engine, symbols, day):  # noqa: F811
    with database_engine.connect() as connection, pytest.raises(DBAPIError):
        connection.execute(
            text("select api_v1.query_stock_technical_factors(:symbols,:day)"),
            {"symbols": symbols, "day": day},
        )


@pytest.mark.integration
@pytest.mark.parametrize("codes", [[], ["600000"] * 101, [None], ["SSE:600000"]])
def test_code_rpc_rejects_invalid_inputs(database_engine, codes):  # noqa: F811
    with database_engine.connect() as connection, pytest.raises(DBAPIError):
        connection.execute(
            text("select api_v1.query_stock_technical_factors_by_codes(:codes,:day)"),
            {"codes": codes, "day": DAY},
        )


@pytest.mark.integration
def test_codes_are_security_resolved_and_ambiguity_rejected(database_engine, factor_seed):  # noqa: F811
    with database_engine.begin() as connection:
        connection.execute(
            text("""insert into core.security
            (symbol,code,exchange,current_name,security_type,status,source_code,ingestion_id)
            select 'SZSE:600000',code,'SZSE',current_name,security_type,status,
                   source_code,ingestion_id from core.security where symbol='SSE:600000'""")
        )
    with database_engine.connect() as connection, pytest.raises(DBAPIError) as error:
        connection.execute(
            text("select api_v1.query_stock_technical_factors_by_codes(:codes,:day)"),
            {"codes": ["600000"], "day": DAY},
        )
    assert error.value.orig.sqlstate == "P0003"


@pytest.mark.parametrize("mismatch", ["envelope", "manifest", "source", "date"])
def test_lineage_mismatch_rejected_before_transaction(mismatch):
    engine = MagicMock()
    persistence = PostgreSQLTechnicalFactorPersistence(engine)
    run, manifest, records = _batch(persistence, _record())
    engine.reset_mock()
    if mismatch == "envelope":
        records = [replace(records[0], ingestion_id=uuid4())]
    elif mismatch == "manifest":
        manifest = replace(manifest, ingestion_id=uuid4())
    elif mismatch == "source":
        records = [replace(records[0], record=replace(records[0].record, source_code="other"))]
    else:
        run = replace(run, request_params={"trade_date": "2026-10-08"})
    with pytest.raises(ValueError):
        persistence.commit_technical_factor_batch(run, manifest, records, [])
    engine.begin.assert_not_called()


def test_publish_removes_omitted_same_day_symbols_before_upsert():
    engine = MagicMock()
    persistence = PostgreSQLTechnicalFactorPersistence(engine)
    run, manifest, records = _batch(persistence, _record())
    engine.reset_mock()
    persistence.commit_technical_factor_batch(run, manifest, records, [])
    statements = engine.begin.return_value.__enter__.return_value.execute.call_args_list
    deletion = [call for call in statements if str(call.args[0]).lstrip().startswith("delete")]
    assert len(deletion) == 1
    assert deletion[0].args[1] == {"trade_date": DAY, "symbols": ["SSE:600000"]}
    assert "trade_date = :trade_date" in str(deletion[0].args[0])
    assert "not (symbol = any(:symbols))" in str(deletion[0].args[0])
    assert statements.index(deletion[0]) < next(
        index for index, call in enumerate(statements) if call.args[0] is UPSERT_TECHNICAL_FACTOR
    )


@pytest.mark.integration
def test_daily_replacement_gaps_and_rollback(database_engine, factor_seed):  # noqa: F811
    persistence = PostgreSQLTechnicalFactorPersistence(database_engine)
    with database_engine.begin() as connection:
        connection.execute(
            text("""insert into core.security
            (symbol,code,exchange,current_name,security_type,status,source_code,ingestion_id)
            select 'SZSE:000001','000001','SZSE',current_name,security_type,status,
                   source_code,ingestion_id from core.security where symbol='SSE:600000'""")
        )
    initial, manifest, records = _batch(persistence, _record())
    initial = replace(initial, fetched_rows=2, accepted_rows=2)
    manifest = replace(manifest, row_count=2)
    records.append(IngestionEnvelope(initial.ingestion_id, _record("SZSE:000001")))
    persistence.commit_technical_factor_batch(initial, manifest, records, [])
    failed, manifest, records = _batch(persistence, _record("SZSE:999999"))
    with pytest.raises(IntegrityError):
        persistence.commit_technical_factor_batch(failed, manifest, records, [])
    with database_engine.connect() as connection:
        assert connection.execute(
            text("""select symbol,ingestion_id
            from core.stock_technical_factor_snapshot order by symbol""")
        ).all() == [("SSE:600000", initial.ingestion_id), ("SZSE:000001", initial.ingestion_id)]
    revised, manifest, records = _batch(persistence, _record(value=None))
    persistence.commit_technical_factor_batch(revised, manifest, records, [])
    with database_engine.connect() as connection:
        payload = connection.execute(
            text("select api_v1.query_stock_technical_factors_by_codes(:codes,:day)"),
            {"codes": ["600000", "000001"], "day": DAY},
        ).scalar_one()
        assert payload["missing_codes"] == ["000001"]
        assert [item["symbol"] for item in payload["items"]] == ["SSE:600000"]
        assert connection.execute(
            text("""select symbol,ingestion_id
            from core.stock_technical_factor_snapshot""")
        ).one() == ("SSE:600000", revised.ingestion_id)


@pytest.mark.integration
def test_live_raw_and_replay_publish_same_facts_with_new_lineage(
    database_engine,  # noqa: F811
    factor_seed,
    tmp_path,
):
    from unittest.mock import Mock

    from market_data_center.providers.tushare_technical_factor import (
        TECHNICAL_FACTOR_FIELDS,
        TushareTechnicalFactorProvider,
    )
    from market_data_center.raw_store import LocalRawStore
    from market_data_center.reliability import RawReplayService
    from market_data_center.technical_factor_service import TechnicalFactorIngestionService

    row = dict.fromkeys(TECHNICAL_FACTOR_FIELDS, "")
    row.update(ts_code="600000.SH", trade_date="20261009", macd_qfq="-0.123456789123456789")
    client = Mock()
    client.query.return_value = [row, row]
    persistence = PostgreSQLTechnicalFactorPersistence(database_engine)
    store = LocalRawStore(tmp_path)
    live = TechnicalFactorIngestionService(
        provider=TushareTechnicalFactorProvider(client), persistence=persistence, raw_store=store
    ).ingest_snapshot(DAY)
    assert live.status is IngestionStatus.SUCCEEDED
    assert (live.fetched_rows, live.accepted_rows) == (2, 1)
    replay = RawReplayService(raw_store=store, persistence=persistence).replay(live.ingestion_id)
    assert (replay.fetched_rows, replay.accepted_rows) == (2, 1)
    assert replay.status == "succeeded"
    client.query.assert_called_once()
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1
    with database_engine.connect() as connection:
        fact = connection.execute(
            text(
                "select forward_adjusted->>'macd',ingestion_id "
                "from core.stock_technical_factor_snapshot"
            )
        ).one()
        assert fact == ("-0.123456789123456789", replay.replay_ingestion_id)
        assert (
            connection.execute(
                text("select count(*) from ingestion.raw_manifest where ingestion_id = :original"),
                {"original": live.ingestion_id},
            ).scalar_one()
            == 1
        )


def test_numeric_json_preserves_decimal_zero_negative_and_null():
    encoded = _numeric_json(
        {
            "precise": Decimal("12345678901234567890.123456789"),
            "negative": Decimal("-0.0123"),
            "zero": Decimal(0),
            "missing": None,
            "days": 12,
        }
    )
    assert loads(encoded, parse_float=Decimal)["precise"] == Decimal(
        "12345678901234567890.123456789"
    )
    assert '"12345678901234567890.123456789"' not in encoded
    assert loads(encoded)["missing"] is None


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), 1.2, True])
def test_numeric_json_rejects_non_domain_numbers(value):
    with pytest.raises(ValueError):
        _numeric_json({"bad": value})


def test_upsert_replaces_all_maps_and_lineage():
    sql = str(UPSERT_TECHNICAL_FACTOR)
    for field in ("unadjusted", "forward_adjusted", "backward_adjusted", "common", "ingestion_id"):
        assert f"{field} = excluded.{field}" in sql
    assert "||" not in sql


@pytest.mark.parametrize(
    ("previous", "baseline", "expected"), [(11, 5000, 10), (None, 11, 10), (None, 0, 1)]
)
def test_coverage_floor_rounds_up_and_uses_first_active_baseline(previous, baseline, expected):
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar_one_or_none.return_value = previous
    connection.execute.return_value.scalar_one.return_value = baseline
    assert (
        PostgreSQLTechnicalFactorPersistence(engine).technical_factor_coverage_floor(
            date(2026, 10, 9)
        )
        == expected
    )
