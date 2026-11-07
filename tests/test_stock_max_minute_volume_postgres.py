from dataclasses import replace
from datetime import UTC, date, datetime
from unittest.mock import MagicMock
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import event, text
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
from market_data_center.domain.stock_max_minute_volume import MinuteVolumePeak
from market_data_center.persistence.stock_max_minute_volume_postgres import (
    PostgreSQLStockMaxMinuteVolumePersistence,
)
from market_data_center.providers.quantity_units import NORMALIZER_VERSION, UNIT_CONTRACT_VERSION
from market_data_center.providers.stock_minute_volume import (
    SOURCE_SCHEMA_VERSION,
    UNIT_EVIDENCE_VERSION,
)
from market_data_center.quantity_certification import build_quantity_certificates

DAY = date(2026, 11, 3)
SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.fixture
def minute_seed(database_engine):  # noqa: F811
    evidence = {"coverage_scope": "requested_symbols", "stocks": {}}
    with database_engine.begin() as c:
        for symbol in ("SSE:600000", "SZSE:000001"):
            ingestion_id, raw_id = uuid4(), uuid4()
            c.execute(
                text("""insert into ingestion.ingestion_run
            (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at,
             request_params,fetched_rows,accepted_rows)
            values (:id,'pytdx','stock_minute_volume','succeeded',now(),now(),now(),
                jsonb_build_object('symbol',cast(:symbol as text),
                    'trade_date','2026-11-03'),240,240)
            """),
                {"id": ingestion_id, "symbol": symbol},
            )
            c.execute(
                text("""insert into core.security
            (symbol,code,exchange,current_name,security_type,status,ipo_date,source_code,ingestion_id)
            values (:symbol,:code,:exchange,'synthetic','stock','listed','1999-01-01','pytdx',:id)
            """),
                {
                    "symbol": symbol,
                    "code": symbol.split(":")[1],
                    "exchange": symbol.split(":")[0],
                    "id": ingestion_id,
                },
            )
            c.execute(
                text("""insert into ingestion.raw_manifest
            (raw_id,ingestion_id,storage_backend,object_path,file_format,content_sha256,byte_size,row_count,schema_version)
            values (:raw,:id,'local',:path,'jsonl',:hash,1,240,'pytdx.stock_minute_volume.v1')"""),
                {
                    "raw": raw_id,
                    "id": ingestion_id,
                    "path": f"synthetic/{raw_id}.jsonl",
                    "hash": "a" * 64,
                },
            )
            c.execute(
                text("""insert into audit.quantity_unit_certification
            (ingestion_id,unit_contract_version,field_group,status,raw_hashes,normalizer_version,evidence_hash)
            values (:id,'stock-quantity.v1','stock_minute_volume.volume',
                'certified',cast(:hashes as jsonb),'test.v1',:hash)
            """),
                {"id": ingestion_id, "hashes": '["' + "a" * 64 + '"]', "hash": "b" * 64},
            )
            evidence["stocks"][symbol] = {
                "ingestion_id": str(ingestion_id),
                "raw_id": str(raw_id),
                "raw_hash": "a" * 64,
                "evidence_hash": "b" * 64,
            }
    return evidence


def peak(symbol="SSE:600000", volume=100):
    return MinuteVolumePeak(
        symbol,
        DAY,
        volume,
        datetime(2026, 11, 3, 9, 58, tzinfo=SHANGHAI) if volume is not None else None,
        240,
        240 if volume is not None else 239,
        () if volume is not None else ("missing_minute_bar",),
    )


@pytest.mark.parametrize("volume", [True, 1.5, -1, 2**63])
def test_minute_persistence_rejects_non_integral_or_out_of_range_peak_before_sql(volume):
    engine = MagicMock()
    with pytest.raises(ValueError, match="integral"):
        PostgreSQLStockMaxMinuteVolumePersistence(engine).publish(
            DAY,
            (peak(volume=volume),),
            {"coverage_scope": "requested_symbols", "stocks": {"SSE:600000": {}}},
        )
    engine.begin.assert_not_called()


def test_minute_persistence_requires_aware_peak_time_and_complete_counts():
    engine = MagicMock()
    storage = PostgreSQLStockMaxMinuteVolumePersistence(engine)
    for value in (
        replace(peak(), peak_at=datetime(2026, 11, 3, 9, 58)),
        replace(peak(), observed_count=239),
        replace(peak(), quality_reasons=("missing_minute_bar",)),
    ):
        with pytest.raises(ValueError):
            storage.publish(
                DAY, (value,), {"coverage_scope": "requested_symbols", "stocks": {"SSE:600000": {}}}
            )
    engine.begin.assert_not_called()


@pytest.mark.integration
def test_minute_publication_is_immutable_idempotent_and_preserves_old_coverage(
    database_engine,  # noqa: F811
    minute_seed,
):
    storage = PostgreSQLStockMaxMinuteVolumePersistence(database_engine)
    first = storage.publish(DAY, (peak(),), minute_seed)
    assert first.version == 1
    assert storage.publish(DAY, (peak(),), minute_seed).status == "unchanged"
    changed_evidence = {**minute_seed, "attempt": "new-evidence"}
    second = storage.publish(DAY, (peak(),), changed_evidence)
    assert second.version == 2 and second.calculation_id == first.calculation_id
    rejected = storage.publish(DAY, (peak(volume=None), peak("SZSE:000001")), minute_seed)
    assert rejected.status == "partial" and rejected.version == 2
    with database_engine.connect() as c:
        payload = c.execute(
            text("select api_v1.query_stock_max_minute_volume(:symbols,:day)"),
            {"symbols": ["SSE:600000", "SZSE:000001"], "day": DAY},
        ).scalar_one()
        assert payload["version"] == 2
        assert payload["items"][0]["max_minute_volume_shares"] == 100
        assert payload["items"][1]["max_minute_volume_shares"] is None
        missing = c.execute(
            text("select api_v1.query_stock_max_minute_volume(array['SSE:600000'],'2026-11-02')")
        ).scalar_one()
        assert missing["status"] == "not_ready" and missing["trade_date"] == "2026-11-02"


@pytest.mark.integration
def test_minute_zero_valid_does_not_publish_and_missing_evidence_rolls_back(
    database_engine,  # noqa: F811
    minute_seed,
):
    storage = PostgreSQLStockMaxMinuteVolumePersistence(database_engine)
    assert storage.publish(DAY, (peak(volume=None),), minute_seed).status == "not_ready"
    with pytest.raises(ValueError, match="certif"):
        storage.publish(
            DAY, (peak(),), {"coverage_scope": "requested_symbols", "stocks": {"SSE:600000": {}}}
        )
    with database_engine.connect() as c:
        assert (
            c.execute(
                text("select count(*) from derived.stock_max_minute_volume_snapshot")
            ).scalar_one()
            == 0
        )
        assert c.execute(text("select count(*) from derived.calculation_run")).scalar_one() == 1


@pytest.mark.integration
def test_minute_candidates_are_exact_date_ordinary_stocks_with_known_lifecycle(
    database_engine,  # noqa: F811
    minute_seed,
):
    storage = PostgreSQLStockMaxMinuteVolumePersistence(database_engine)
    assert storage.candidates(DAY) == ("SSE:600000", "SZSE:000001")
    assert storage.candidates(DAY, ("SZSE:000001", "SSE:600000")) == ("SZSE:000001", "SSE:600000")
    with database_engine.begin() as c:
        c.execute(text("update core.security set ipo_date='2026-11-04' where symbol='SSE:600000'"))
    assert storage.candidates(DAY) == ("SZSE:000001",)
    with pytest.raises(ValueError, match=r"stock universe|active"):
        storage.candidates(DAY, ("SSE:600000",))
    with database_engine.begin() as c:
        c.execute(text("update core.security set ipo_date=null where symbol='SZSE:000001'"))
    with pytest.raises(ValueError, match="lifecycle"):
        storage.candidates(DAY)


@pytest.mark.integration
def test_minute_rpc_bounds_and_api_permissions(database_engine, minute_seed):  # noqa: F811
    storage = PostgreSQLStockMaxMinuteVolumePersistence(database_engine)
    storage.publish(DAY, (peak(volume=0),), minute_seed)
    with database_engine.begin() as c:
        c.execute(text("set local role market_data_api"))
        payload = c.execute(
            text(
                "select api_v1.query_stock_max_minute_volume(array['SSE:600000','SSE:600000'],:day)"
            ),
            {"day": DAY},
        ).scalar_one()
        assert len(payload["items"]) == 1 and payload["items"][0]["max_minute_volume_shares"] == 0
    for sql in (
        "select * from derived.stock_max_minute_volume",
        "delete from derived.stock_max_minute_volume",
    ):
        with pytest.raises(DBAPIError), database_engine.begin() as c:
            c.execute(text("set local role market_data_api"))
            c.execute(text(sql))
    for role in ("anon", "authenticated"):
        with pytest.raises(DBAPIError), database_engine.begin() as c:
            c.execute(text(f"set local role {role}"))
            c.execute(text("select * from derived.stock_max_minute_volume"))
    with database_engine.begin() as c:
        c.execute(text("set local role market_data_worker"))
        assert (
            c.execute(text("select count(*) from derived.stock_max_minute_volume")).scalar_one()
            == 1
        )
    for symbols in ([], ["bad"], ["SSE:600000"] * 501):
        with pytest.raises(DBAPIError), database_engine.begin() as c:
            c.execute(
                text("select api_v1.query_stock_max_minute_volume(:symbols,:day)"),
                {"symbols": symbols, "day": DAY},
            )
    with pytest.raises(IntegrityError), database_engine.begin() as c:
        c.execute(
            text("""insert into derived.stock_max_minute_volume
        select * from derived.stock_max_minute_volume""")
        )


@pytest.mark.integration
def test_minute_rpc_hides_revoked_certification_without_deleting_old_publication(
    database_engine,  # noqa: F811
    minute_seed,
):
    storage = PostgreSQLStockMaxMinuteVolumePersistence(database_engine)
    first = storage.publish(DAY, (peak(),), minute_seed)
    with database_engine.begin() as c:
        c.execute(text("update audit.quantity_unit_certification set status='quarantined'"))
    with database_engine.connect() as c:
        payload = c.execute(
            text("select api_v1.query_stock_max_minute_volume(array['SSE:600000'],:day)"),
            {"day": DAY},
        ).scalar_one()
        assert payload["version"] == first.version
        assert payload["items"][0]["max_minute_volume_shares"] is None
        assert payload["items"][0]["peak_at"] is None
        assert payload["items"][0]["quality_reasons"] == ["quantity_units_unverified"]


@pytest.mark.integration
def test_minute_publication_failure_rolls_back_new_calculation_and_keeps_current(
    database_engine,  # noqa: F811
    minute_seed,
):
    storage = PostgreSQLStockMaxMinuteVolumePersistence(database_engine)
    first = storage.publish(DAY, (peak(),), minute_seed)

    def reject_snapshot(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().startswith("insert into derived.stock_max_minute_volume_snapshot"):
            raise RuntimeError("synthetic rollback")

    event.listen(database_engine, "before_cursor_execute", reject_snapshot)
    try:
        with pytest.raises(RuntimeError, match="rollback"):
            storage.publish(DAY, (peak(volume=200),), minute_seed)
    finally:
        event.remove(database_engine, "before_cursor_execute", reject_snapshot)
    with database_engine.connect() as c:
        assert c.execute(text("select count(*) from derived.calculation_run")).scalar_one() == 1
        assert (
            c.execute(text("select count(*) from derived.stock_max_minute_volume")).scalar_one()
            == 1
        )
        assert (
            c.execute(
                text("select calculation_id from derived.stock_max_minute_volume_snapshot")
            ).scalar_one()
            == first.calculation_id
        )


@pytest.mark.integration
def test_minute_reused_calculation_reads_current_publication_certification(
    database_engine,  # noqa: F811
    minute_seed,
):
    storage = PostgreSQLStockMaxMinuteVolumePersistence(database_engine)
    first = storage.publish(DAY, (peak(),), minute_seed)
    old = minute_seed["stocks"]["SSE:600000"]
    ingestion_id, raw_id = uuid4(), uuid4()
    with database_engine.begin() as c:
        c.execute(
            text("""insert into ingestion.ingestion_run
            (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at,
             request_params,fetched_rows,accepted_rows)
            select :new,provider_code,dataset_code,status,requested_at,started_at,finished_at,
                request_params,fetched_rows,accepted_rows from ingestion.ingestion_run
                where ingestion_id=:old"""),
            {"new": ingestion_id, "old": old["ingestion_id"]},
        )
        c.execute(
            text("""insert into ingestion.raw_manifest
            (raw_id,ingestion_id,storage_backend,object_path,file_format,content_sha256,
             byte_size,row_count,schema_version)
            select :raw,:new,storage_backend,:path,file_format,content_sha256,byte_size,row_count,
                schema_version from ingestion.raw_manifest where raw_id=:old"""),
            {
                "raw": raw_id,
                "new": ingestion_id,
                "path": f"synthetic/{raw_id}.jsonl",
                "old": old["raw_id"],
            },
        )
        c.execute(
            text("""insert into audit.quantity_unit_certification
            (ingestion_id,unit_contract_version,field_group,status,raw_hashes,normalizer_version,evidence_hash)
            select :new,unit_contract_version,field_group,status,raw_hashes,normalizer_version,:hash
                from audit.quantity_unit_certification where ingestion_id=:old"""),
            {"new": ingestion_id, "hash": "c" * 64, "old": old["ingestion_id"]},
        )
    proof = {
        **old,
        "ingestion_id": str(ingestion_id),
        "raw_id": str(raw_id),
        "evidence_hash": "c" * 64,
    }
    current_evidence = {**minute_seed, "stocks": {**minute_seed["stocks"], "SSE:600000": proof}}
    current = storage.publish(DAY, (peak(),), current_evidence)
    assert current.calculation_id == first.calculation_id and current.version == 2
    with database_engine.begin() as c:
        c.execute(
            text(
                "update audit.quantity_unit_certification set status='quarantined' "
                "where ingestion_id=:old"
            ),
            {"old": old["ingestion_id"]},
        )
    with database_engine.connect() as c:
        payload = c.execute(
            text("select api_v1.query_stock_max_minute_volume(array['SSE:600000'],:day)"),
            {"day": DAY},
        ).scalar_one()
        assert payload["items"][0]["max_minute_volume_shares"] == 100


@pytest.mark.integration
def test_minute_source_manifest_and_certificate_commit_atomically_with_worker_role(
    database_engine,  # noqa: F811
):
    now = datetime.now(UTC)
    params = {
        "provider": "pytdx",
        "endpoint": "get_security_bars",
        "schema_version": SOURCE_SCHEMA_VERSION,
        "provider_schema_version": SOURCE_SCHEMA_VERSION,
        "instrument_type": "stock",
        "field": "volume",
        "category": 8,
        "market": 1,
        "exchange": "SSE",
        "peer": "103.251.85.94:7709",
        "unit_evidence_version": UNIT_EVIDENCE_VERSION,
        "source_quantity_unit": "share",
        "standard_quantity_unit": "share",
        "symbol": "SSE:600000",
        "trade_date": DAY.isoformat(),
        "quantity_unit_contract_version": UNIT_CONTRACT_VERSION,
        "quantity_normalizer_version": NORMALIZER_VERSION,
    }

    def batch():
        ingestion_id = uuid4()
        run = IngestionRun(
            ingestion_id,
            ProviderCode.PYTDX,
            DatasetCode.STOCK_MINUTE_VOLUME,
            IngestionStatus.SUCCEEDED,
            now,
            now,
            now,
            params,
            240,
            240,
        )
        manifest = RawManifest(
            uuid4(),
            ingestion_id,
            f"synthetic/{ingestion_id}.jsonl",
            RawFileFormat.JSONL,
            "c" * 64,
            1,
            240,
            SOURCE_SCHEMA_VERSION,
        )
        certificate = build_quantity_certificates(
            ingestion_id=ingestion_id,
            provider="pytdx",
            dataset="stock_minute_volume",
            manifest=manifest,
            request_params=params,
        )[0]
        return run, manifest, certificate

    storage = PostgreSQLStockMaxMinuteVolumePersistence(database_engine)
    bad_run, bad_manifest, bad_certificate = batch()
    with pytest.raises(ValueError, match="certificate"):
        storage.record_source(bad_run, bad_manifest, {**bad_certificate, "evidence_hash": "d" * 64})
    run, manifest, certificate = batch()
    storage.record_source(run, manifest, certificate)
    failed = IngestionRun(
        uuid4(),
        ProviderCode.PYTDX,
        DatasetCode.STOCK_MINUTE_VOLUME,
        IngestionStatus.FAILED,
        now,
        now,
        now,
        error_summary="synthetic network error",
    )
    storage.record_source(failed, None, None)
    with database_engine.connect() as c:
        assert c.execute(text("select count(*) from ingestion.ingestion_run")).scalar_one() == 2
        assert c.execute(text("select count(*) from ingestion.raw_manifest")).scalar_one() == 1
        assert (
            c.execute(text("select count(*) from audit.quantity_unit_certification")).scalar_one()
            == 1
        )
    for sql in (
        "update derived.stock_max_minute_volume set observed_count=0",
        "delete from derived.stock_max_minute_volume_snapshot",
    ):
        with pytest.raises(DBAPIError), database_engine.begin() as c:
            c.execute(text("set local role market_data_worker"))
            c.execute(text(sql))
