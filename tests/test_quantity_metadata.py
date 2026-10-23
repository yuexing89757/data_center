from unittest.mock import Mock

import pytest
from test_quantity_certification import manifest
from test_reliability import NOW, SOURCE_RUN_ID, StubReliabilityPersistence, _source

from market_data_center.domain import DatasetCode, IngestionRun, IngestionStatus, ProviderCode
from market_data_center.pipeline import IngestionPipeline
from market_data_center.providers import quantity_units
from market_data_center.providers.contracts import ProviderBatch, ProviderError
from market_data_center.quantity_certification import quantity_certificates
from market_data_center.raw_store import LocalRawStore
from market_data_center.reliability import RawReplayService


@pytest.mark.parametrize(
    "metadata",
    [
        {"source_quantity_unit": "share"},
        {"quantity_unit_contract_version": "unknown"},
        {"quantity_normalizer_version": "legacy"},
    ],
)
def test_new_ingestion_checks_original_metadata_before_normalization(tmp_path, metadata):
    persistence = Mock()
    provider = Mock(source_code="pytdx")
    factory = Mock(return_value=())
    pipeline = IngestionPipeline(
        provider=provider, raw_store=LocalRawStore(tmp_path), persistence=persistence
    )
    batch = ProviderBatch(
        raw_rows=[{"volume": "37"}],
        request_params=metadata,
        schema_version="pytdx.remote_daily_bar.v2",
        record_factory=factory,
    )
    run = IngestionRun(
        ingestion_id=SOURCE_RUN_ID,
        provider_code=ProviderCode.PYTDX,
        dataset_code=DatasetCode.DAILY_BAR,
        status=IngestionStatus.RUNNING,
        requested_at=NOW,
        started_at=NOW,
    )
    with pytest.raises(ProviderError, match=r"quantity.*metadata"):
        pipeline._stage_batch(run, batch)
    factory.assert_not_called()
    assert persistence.commit_rejected_batch.call_count == 1
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1


@pytest.mark.parametrize(
    "metadata",
    [
        {"source_quantity_unit": "share"},
        {"volume_unit": "share"},
        {"standard_quantity_unit": "bond"},
        {"volume_lot_size": 10},
        {"volume_lot_size": True},
        {"provider_schema_version": "pytdx.remote_daily_bar.v1"},
        {"quantity_unit_contract_version": "unknown"},
    ],
)
def test_replay_rejects_conflicting_original_metadata(tmp_path, metadata):
    store = LocalRawStore(tmp_path)
    source = _source(
        store,
        provider=ProviderCode.PYTDX,
        dataset=DatasetCode.DAILY_BAR,
        schema_version="pytdx.remote_daily_bar.v2",
        rows=[
            {
                "date": "20260729",
                "price_scale": "1",
                "open": "10",
                "high": "10",
                "low": "10",
                "close": "10",
                "volume": "37",
                "amount": "370",
            }
        ],
        request_params={"source_symbol": "sh.600000", **metadata},
    )
    persistence = StubReliabilityPersistence(source)
    with pytest.raises(ProviderError, match=r"quantity.*metadata"):
        RawReplayService(raw_store=store, persistence=persistence).replay(SOURCE_RUN_ID)
    assert not persistence.daily_commits
    assert persistence.rejected_commits
    assert persistence.rejected_commits[0][2][0].details["quantity_reason"] == (
        "quantity_metadata_conflict"
    )
    assert source.request_params == {"source_symbol": "sh.600000", **metadata}


def test_quantity_audit_reason_does_not_copy_untrusted_error_details():
    details = quantity_units.quantity_failure_details(
        ProviderError("quantity unit contract unverified: secret fixture path and payload")
    )
    assert details["quantity_reason"] == "quantity_units_unverified"
    assert "secret" not in str(details)
    assert quantity_units.quantity_failure_details(RuntimeError("private fixture")) == {}


@pytest.mark.parametrize(
    "metadata",
    [{"source_quantity_unit": "share"}, {"quantity_normalizer_version": "legacy"}],
)
def test_new_batch_cannot_certify_conflicting_metadata(metadata):
    raw = manifest()
    run = IngestionRun(
        ingestion_id=raw.ingestion_id,
        provider_code=ProviderCode.PYTDX,
        dataset_code=DatasetCode.DAILY_BAR,
        status=IngestionStatus.SUCCEEDED,
        requested_at=NOW,
        started_at=NOW,
        finished_at=NOW,
        fetched_rows=1,
        accepted_rows=1,
        request_params=metadata,
    )
    with pytest.raises(ProviderError, match=r"quantity.*metadata"):
        quantity_certificates(Mock(), run, raw)


@pytest.mark.parametrize(
    "provider,schema,metadata",
    [
        ("pytdx", "pytdx.local_daily_bar.v2", {}),
        ("pytdx", "pytdx.remote_daily_bar.v1", {"quantity_normalizer_version": "legacy"}),
        (
            "pytdx",
            "pytdx.remote_daily_bar.v2",
            {"source_quantity_unit": "lot", "volume_lot_size": 100},
        ),
        (
            "pytdx_hq",
            "market_data_center.call_auction_market_series.raw.v2",
            {
                "source_quantity_unit": "lot",
                "standard_quantity_unit": "share",
                "volume_lot_size": 100,
            },
        ),
        (
            "tushare",
            "tushare.convertible_bond_daily_bar.v1",
            {
                "source_quantity_unit": "bond_lot",
                "standard_quantity_unit": "bond",
                "volume_lot_size": 10,
            },
        ),
    ],
)
def test_legacy_and_independent_registered_metadata_allowed(provider, schema, metadata):
    quantity_units.validate_quantity_metadata(
        provider=provider, schema_version=schema, request_params=metadata, replay=True
    )


def test_share_archive_cannot_claim_live_lots():
    with pytest.raises(ProviderError, match=r"quantity.*metadata"):
        quantity_units.validate_quantity_metadata(
            provider="pytdx_hq",
            schema_version="market_data_center.call_auction_market_series.raw.v1",
            request_params={"source_quantity_unit": "lot"},
            replay=True,
        )


def test_top10_certificate_covers_signed_holding_change():
    rules = quantity_units.quantity_group_rules(
        provider="tushare",
        schema_version="tushare.top10_float_holder.v1",
        field_group="top10_float_holder.hold_amount",
    )
    assert {identity[4] for identity, _ in rules} == {"hold_amount", "hold_change"}


@pytest.mark.parametrize("key", ["source_amount_unit", "amount_unit"])
def test_quantity_proof_cannot_override_conflicting_amount_metadata(key):
    with pytest.raises(ProviderError, match=r"quantity.*metadata"):
        quantity_units.validate_quantity_metadata(
            provider="tushare",
            schema_version="tushare.daily_bar.v1",
            request_params={"source_quantity_unit": "lot", key: "cny"},
            replay=True,
        )


@pytest.mark.parametrize(
    "provider,schema,unit",
    [
        ("tushare", "tushare.daily_bar.v1", "thousand_cny"),
        ("pytdx", "pytdx.remote_daily_bar.v2", "cny"),
    ],
)
def test_matching_independent_amount_metadata_is_allowed(provider, schema, unit):
    quantity_units.validate_quantity_metadata(
        provider=provider,
        schema_version=schema,
        request_params={
            "source_amount_unit": unit,
            "amount_unit": unit,
            "standard_amount_unit": "cny",
        },
        replay=True,
    )
