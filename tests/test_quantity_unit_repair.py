from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal, localcontext
from pathlib import Path
from subprocess import run
from sys import executable
from uuid import UUID, uuid4

import pytest

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.quantity_unit_repair import (
    RepairScope,
    apply_plan,
    certification_eligible,
    evaluate_fact,
    fingerprint,
    seal_plan,
    verify_plan,
)
from market_data_center.raw_store import LocalRawStore


def raw_fixture(tmp_path: Path, schema: str = "pytdx.remote_daily_bar.v1"):
    store = LocalRawStore(tmp_path)
    ingestion_id = uuid4()
    stored = store.write_jsonl(
        provider="pytdx",
        dataset="daily_bar",
        partition_date=date(2026, 10, 21),
        ingestion_id=ingestion_id,
        schema_version=schema,
        rows=[
            {
                "date": "20261021",
                "price_scale": "1",
                "open": "10",
                "high": "11",
                "low": "9",
                "close": "10",
                "previous_close": "10",
                "volume": "37",
                "amount": "37000",
            }
        ],
    )
    manifest = RawManifest(
        uuid4(),
        ingestion_id,
        stored.object_path,
        RawFileFormat.JSONL,
        stored.content_sha256,
        stored.byte_size,
        stored.row_count,
        schema,
    )
    fact = dict(
        symbol="SSE:600000",
        trade_date=date(2026, 10, 21),
        market="CN_A_SHARE",
        open=Decimal("10.0000"),
        high=Decimal("11.0000"),
        low=Decimal("9.0000"),
        close=Decimal("10.0000"),
        previous_close=Decimal("10.0000"),
        volume=37,
        amount=Decimal("37000.00"),
        trade_status="unknown",
        is_st=None,
        source_code="pytdx",
        ingestion_id=ingestion_id,
        created_at=datetime(2026, 10, 21, tzinfo=UTC),
        updated_at=datetime(2026, 10, 21, tzinfo=UTC),
    )
    source = dict(
        provider_code="pytdx",
        dataset_code="daily_bar",
        source_ingestion_id=ingestion_id,
        request_params={"source_symbol": "sh.600000"},
    )
    return store, manifest, fact, source


def test_remote_legacy_target_is_reconstructed_from_raw_not_current_value(tmp_path):
    store, manifest, fact, source = raw_fixture(tmp_path)
    item = evaluate_fact(fact, "stock", source, [manifest], store)
    assert item["status"] == "repair"
    assert item["target_volume"] == 3700
    fact["volume"] = 38
    item = evaluate_fact(fact, "stock", source, [manifest], store)
    assert item["status"] == "quarantined"
    assert item["reason"] == "current_fact_does_not_match_raw"


def test_preview_rejects_amount_metadata_conflict_and_allows_declared_share_scale(tmp_path):
    store, manifest, fact, source = raw_fixture(tmp_path)
    source["request_params"]["source_amount_unit"] = "thousand_cny"
    assert evaluate_fact(fact, "stock", source, [manifest], store)["status"] == "quarantined"
    store, manifest, fact, source = raw_fixture(tmp_path, "pytdx.local_daily_bar.v2")
    source["request_params"].update(source_quantity_unit="share", volume_lot_size=1)
    assert evaluate_fact(fact, "stock", source, [manifest], store)["status"] == "already_correct"


def test_missing_and_ambiguous_raw_never_guess(tmp_path):
    store, manifest, fact, source = raw_fixture(tmp_path)
    assert evaluate_fact(fact, "stock", source, [], store)["status"] == "quarantined"
    duplicate = replace(manifest, raw_id=uuid4())
    assert (
        evaluate_fact(fact, "stock", source, [manifest, duplicate], store)["reason"]
        == "ambiguous_raw"
    )
    (tmp_path / manifest.object_path).unlink()
    assert (
        evaluate_fact(fact, "stock", source, [manifest], store)["reason"] == "raw_integrity_failed"
    )


def test_correct_values_are_verified_and_nonstock_is_out_of_scope(tmp_path):
    store, manifest, fact, source = raw_fixture(tmp_path)
    fact["volume"] = 3700
    assert evaluate_fact(fact, "stock", source, [manifest], store)["status"] == "already_correct"
    assert evaluate_fact(fact, "index", source, [manifest], store)["reason"] == "not_stock"


def test_manifest_integrity_and_nonquantity_fact_mismatch_block_repair(tmp_path):
    store, manifest, fact, source = raw_fixture(tmp_path)
    assert (
        evaluate_fact(fact, "stock", source, [replace(manifest, row_count=2)], store)["reason"]
        == "raw_integrity_failed"
    )
    fact["amount"] = Decimal("37001")
    assert evaluate_fact(fact, "stock", source, [manifest], store)["status"] == "quarantined"


def test_plan_hash_binds_exact_keys_before_and_target_values():
    scope = RepairScope(date(2026, 10, 1), date(2026, 10, 21), ("pytdx",))
    plan = seal_plan(scope, [{"symbol": "SSE:600000", "target_volume": 3700}], [])
    assert verify_plan(plan, plan["plan_hash"]) is None
    assert seal_plan(scope, [{"symbol": "SSE:600000", "target_volume": 3700}], []) == plan
    plan["items"][0]["target_volume"] = 370000
    with pytest.raises(ValueError, match="hash"):
        verify_plan(plan, plan["plan_hash"])


def test_fingerprint_uses_exact_decimals_and_lineage():
    assert fingerprint({"volume": 100, "amount": Decimal("1.00")}) == fingerprint(
        {"amount": Decimal("1"), "volume": 100}
    )
    assert fingerprint({"ingestion_id": UUID(int=1)}) != fingerprint({"ingestion_id": UUID(int=2)})


@pytest.mark.parametrize(
    "schema,current,status",
    [
        ("pytdx.local_daily_bar.v2", 37, "already_correct"),
        ("pytdx.remote_daily_bar.v2", 3700, "already_correct"),
        ("pytdx.remote_daily_bar.v2", 37, "quarantined"),
    ],
)
def test_correct_versions_do_not_apply_legacy_rule(tmp_path, schema, current, status):
    store, manifest, fact, source = raw_fixture(tmp_path, schema)
    fact["volume"] = current
    assert evaluate_fact(fact, "stock", source, [manifest], store)["status"] == status


def test_contradictory_unit_metadata_and_unknown_schema_are_quarantined(tmp_path):
    store, manifest, fact, source = raw_fixture(tmp_path)
    source["request_params"]["source_quantity_unit"] = "share"
    item = evaluate_fact(fact, "stock", source, [manifest], store)
    assert item["reason"] == "contradictory_unit_evidence"
    assert item["raw_candidates"][0]["content_sha256"] == manifest.content_sha256
    item = evaluate_fact(
        fact, "stock", source, [replace(manifest, schema_version="pytdx.unknown.v1")], store
    )
    assert item["reason"] == "unregistered_unit_evidence"


def test_raw_duplicate_natural_keys_are_explicitly_quarantined(tmp_path):
    store, manifest, fact, source = raw_fixture(tmp_path)
    original = store.read_jsonl(manifest)[0]
    stored = store.write_jsonl(
        provider="pytdx",
        dataset="daily_bar",
        partition_date=date(2026, 10, 21),
        ingestion_id=uuid4(),
        schema_version=manifest.schema_version,
        rows=[original, original],
    )
    duplicate_manifest = replace(
        manifest,
        object_path=stored.object_path,
        content_sha256=stored.content_sha256,
        byte_size=stored.byte_size,
        row_count=2,
    )
    assert (
        evaluate_fact(fact, "stock", source, [duplicate_manifest], store)["reason"]
        == "duplicate_raw_natural_key"
    )


def test_bad_approval_hash_fails_before_any_database_access(tmp_path):
    scope = RepairScope(date(2026, 10, 1), date(2026, 10, 21), ("pytdx",))
    plan = seal_plan(scope, [], [])
    with pytest.raises(ValueError, match="hash"):
        apply_plan(None, LocalRawStore(tmp_path), plan, "0" * 64)


def test_fingerprint_does_not_round_under_small_decimal_context():
    exact = Decimal("12345678901234567890.1234")
    expected = fingerprint({"amount": exact})
    with localcontext() as context:
        context.prec = 3
        assert fingerprint({"amount": exact}) == expected


@pytest.mark.parametrize(
    "raw_volume,current,target,status",
    [
        ("37", 37, 3700, "repair"),
        ("0", 0, 0, "already_correct"),
        ("", None, None, "already_correct"),
        ("0.37", 37, 37, "already_correct"),
    ],
)
def test_akshare_legacy_raw_preserves_zero_missing_and_exact_share_targets(
    tmp_path, raw_volume, current, target, status
):
    store, manifest, fact, source = raw_fixture(tmp_path)
    stored = store.write_jsonl(
        provider="akshare",
        dataset="daily_bar",
        partition_date=date(2026, 10, 21),
        ingestion_id=uuid4(),
        schema_version="akshare.daily_bar.v1",
        rows=[
            {
                "日期": "2026-10-21",
                "股票代码": "600000",
                "开盘": "10",
                "最高": "11",
                "最低": "9",
                "收盘": "10",
                "涨跌额": "0",
                "成交量": raw_volume,
                "成交额": "37000",
            }
        ],
    )
    manifest = replace(
        manifest,
        object_path=stored.object_path,
        byte_size=stored.byte_size,
        row_count=stored.row_count,
        content_sha256=stored.content_sha256,
        schema_version=stored.schema_version,
    )
    fact.update(volume=current, trade_status="trading", source_code="akshare")
    source.update(provider_code="akshare", request_params={"source_symbol": "600000"})
    item = evaluate_fact(fact, "stock", source, [manifest], store)
    assert item["status"] == status
    assert item["target_volume"] == target


def test_cli_requires_explicit_approval_and_keeps_evidence_outside_repository():
    repository = Path(__file__).resolve().parents[1]
    script = repository / "scripts" / "repair_stock_quantity_units.py"
    missing_hash = run([executable, str(script), "--apply"], capture_output=True, text=True)
    assert missing_hash.returncode == 2
    assert "require --plan and --expected-hash" in missing_hash.stderr
    unsafe_destination = run(
        [
            executable,
            str(script),
            "--start",
            "2026-10-20",
            "--end",
            "2026-10-20",
            "--provider",
            "pytdx",
            "--output",
            str(repository / "protected-test-preview.json"),
        ],
        capture_output=True,
        text=True,
    )
    assert unsafe_destination.returncode == 2
    assert "outside the repository" in unsafe_destination.stderr
    assert not (repository / "protected-test-preview.json").exists()


def test_historical_certification_accepts_501_complete_rows_and_bounds_5000():
    def group(count, actual_count=None):
        return [
            {"status": "already_correct", "ingestion_current_row_count": actual_count or count}
            for _ in range(count)
        ]

    assert certification_eligible(group(501))
    assert certification_eligible(group(5000))
    assert not certification_eligible(group(5001))
    assert not certification_eligible(group(501, 502))
    items = group(501)
    items[250]["status"] = "quarantined"
    assert not certification_eligible(items)


def test_incomplete_scope_never_blindly_overwrites_existing_certified_proof(tmp_path):
    from market_data_center.quantity_unit_repair import _certify_existing_group

    class CaptureConnection:
        def __init__(self):
            self.writes = []

        def execute(self, statement, parameters):
            self.writes.append((str(statement), parameters))

    connection = CaptureConnection()
    item = {
        "original_ingestion_id": str(uuid4()),
        "status": "already_correct",
        "ingestion_current_row_count": 2,
        "raw_hash": "a" * 64,
        "before_hash": "b" * 64,
    }
    _certify_existing_group(connection, LocalRawStore(tmp_path), "c" * 64, [item])
    assert len(connection.writes) == 2
    for sql, parameters in connection.writes:
        assert parameters["preserve_certified"] is True
        assert parameters["status"] == "quarantined"
        assert "where not :preserve_certified" in sql
        assert "status <> 'certified'" in sql
