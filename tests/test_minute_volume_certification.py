from datetime import date
from uuid import uuid4

import pytest

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.quantity_units import quantity_unit_rule
from market_data_center.quantity_certification import build_quantity_certificates


def metadata():
    return {
        "schema_version": "pytdx.stock_minute_volume.v1",
        "provider": "pytdx",
        "endpoint": "get_security_bars",
        "category": 8,
        "market": 1,
        "peer": "103.251.85.94:7709",
        "exchange": "SSE",
        "field": "volume",
        "instrument_type": "stock",
        "unit_evidence_version": "pytdx-minute-volume.2026-11-08.v1",
        "source_quantity_unit": "share",
        "standard_quantity_unit": "share",
        "symbol": "SSE:600327",
        "trade_date": date(2026, 11, 3).isoformat(),
        "pages": [],
        "history_boundary_reached": True,
    }


def manifest():
    return RawManifest(
        uuid4(),
        uuid4(),
        "pytdx/minute/sample.jsonl",
        RawFileFormat.JSONL,
        "a" * 64,
        0,
        240,
        "pytdx.stock_minute_volume.v1",
    )


def test_minute_generic_rule_cannot_certify_without_actual_source_identity():
    with pytest.raises(ProviderError):
        quantity_unit_rule(
            provider="pytdx",
            endpoint="get_security_bars",
            schema_version="pytdx.stock_minute_volume.v1",
            instrument_type="stock",
            field="volume",
        )


def test_minute_certificate_requires_context_and_binds_request_identity():
    raw = manifest()
    kwargs = dict(
        ingestion_id=raw.ingestion_id, provider="pytdx", dataset="stock_minute_volume", manifest=raw
    )
    with pytest.raises(ProviderError):
        build_quantity_certificates(**kwargs)
    first = build_quantity_certificates(**kwargs, request_params=metadata())
    second = build_quantity_certificates(
        **kwargs, request_params={**metadata(), "symbol": "SSE:600000"}
    )
    assert len(first) == 1
    assert first[0]["field_group"] == "stock_minute_volume.volume"
    assert first[0]["evidence_hash"] != second[0]["evidence_hash"]


@pytest.mark.parametrize(
    "changes",
    [
        {"peer": "192.0.2.1:7709"},
        {"category": 9},
        {"market": 0},
        {"exchange": "BSE", "market": 0, "symbol": "BSE:920000"},
        {"unit_evidence_version": "unknown"},
        {"field": "amount"},
    ],
)
def test_minute_certificate_rejects_unverified_source_identity(changes):
    raw = manifest()
    with pytest.raises(ProviderError):
        build_quantity_certificates(
            ingestion_id=raw.ingestion_id,
            provider="pytdx",
            dataset="stock_minute_volume",
            manifest=raw,
            request_params={**metadata(), **changes},
        )
