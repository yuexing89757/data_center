from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import Mock
from uuid import uuid4

import pytest

from market_data_center.domain import IngestionStatus, Market
from market_data_center.domain.stock_technical_factor import (
    COMMON_FACTOR_NAMES,
    FACTOR_CATALOG_VERSION,
    TECHNICAL_FACTOR_NAMES,
    StockTechnicalFactorSnapshotRecord,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError
from market_data_center.providers.tushare_technical_factor import (
    TECHNICAL_FACTOR_FIELDS,
    TushareTechnicalFactorProvider,
)
from market_data_center.raw_store import LocalRawStore
from market_data_center.technical_factor_service import TechnicalFactorIngestionService

DAY = date(2026, 10, 9)


def record():
    group = dict.fromkeys(TECHNICAL_FACTOR_NAMES, Decimal("-0.123456789123456789"))
    return StockTechnicalFactorSnapshotRecord(
        "SZSE:000001",
        DAY,
        Market.CN_A_SHARE,
        "tushare",
        FACTOR_CATALOG_VERSION,
        group,
        group,
        group,
        dict.fromkeys(COMMON_FACTOR_NAMES, 0),
    )


def setup_service(tmp_path, records=None):
    provider = Mock(source_code="tushare")
    provider.fetch_snapshot.return_value = ProviderBatch(
        raw_rows=[{"ts_code": "000001.SZ", "trade_date": "20261009"}],
        request_params={"trade_date": "20261009"},
        schema_version="tushare.stock_technical_factor.v1",
        records=records or [record()],
    )
    persistence = Mock()
    persistence.task_lock.return_value = nullcontext()
    persistence.technical_factor_securities.return_value = {"SZSE:000001"}
    persistence.known_trading_dates.return_value = {DAY}
    persistence.technical_factor_coverage_floor.return_value = 1
    service = TechnicalFactorIngestionService(
        provider=provider,
        persistence=persistence,
        raw_store=LocalRawStore(tmp_path),
        clock=lambda: datetime(2026, 10, 9, 14, tzinfo=UTC),
        uuid_factory=uuid4,
    )
    return service, provider, persistence


def test_factor_pipeline_stores_raw_and_exact_atomic_snapshot(tmp_path):
    service, provider, persistence = setup_service(tmp_path)
    result = service.ingest_snapshot(DAY)
    assert result.status is IngestionStatus.SUCCEEDED
    provider.fetch_snapshot.assert_called_once_with(DAY)
    run, manifest, envelopes, quality = persistence.commit_technical_factor_batch.call_args.args
    assert run == result
    assert manifest.ingestion_id == run.ingestion_id
    assert manifest.schema_version == "tushare.stock_technical_factor.v1"
    assert envelopes[0].ingestion_id == run.ingestion_id
    assert envelopes[0].record.forward_adjusted["macd"] == Decimal("-0.123456789123456789")
    assert not quality
    assert list(tmp_path.rglob("*.jsonl"))


@pytest.mark.parametrize("failure", ["date", "symbol", "coverage", "conflict", "calendar"])
def test_factor_pipeline_hard_failure_keeps_raw_and_publishes_nothing(tmp_path, failure):
    service, provider, persistence = setup_service(tmp_path)
    if failure == "date":
        provider.fetch_snapshot.return_value._records = [
            replace(record(), trade_date=date(2026, 10, 8))
        ]
    elif failure == "symbol":
        persistence.technical_factor_securities.return_value = set()
    elif failure == "coverage":
        persistence.technical_factor_coverage_floor.return_value = 2
    elif failure == "calendar":
        persistence.known_trading_dates.return_value = set()
    else:
        other = dict(record().forward_adjusted)
        other["macd"] = Decimal("1")
        provider.fetch_snapshot.return_value._records = [
            record(),
            replace(record(), forward_adjusted=other),
        ]
    result = service.ingest_snapshot(DAY)
    assert result.status is IngestionStatus.FAILED
    assert result.accepted_rows == 0
    persistence.commit_technical_factor_batch.assert_not_called()
    failed, manifest, quality = persistence.commit_rejected_batch.call_args.args
    assert manifest is not None
    assert failed == result
    assert quality[0].dataset_code.value == "stock_technical_factor"


def test_factor_provider_error_records_failed_run(tmp_path):
    service, provider, persistence = setup_service(tmp_path)
    provider.fetch_snapshot.side_effect = ProviderError("unavailable")
    with pytest.raises(ProviderError):
        service.ingest_snapshot(DAY)
    persistence.commit_technical_factor_batch.assert_not_called()
    assert persistence.fail_ingestion_run.call_args.args[0].status is IngestionStatus.FAILED


def test_malformed_factor_response_keeps_raw_manifest_and_failure(tmp_path):
    service, _, persistence = setup_service(tmp_path)
    row = dict.fromkeys(TECHNICAL_FACTOR_FIELDS, "")
    row.update(ts_code="000001.SZ", trade_date="20261009")
    del row["macd_qfq"]
    client = Mock()
    client.query.return_value = [row]
    service._factor_provider = TushareTechnicalFactorProvider(client)
    with pytest.raises(ProviderError, match="missing fields"):
        service.ingest_snapshot(DAY)
    failed, manifest, quality = persistence.commit_rejected_batch.call_args.args
    assert failed.fetched_rows == failed.rejected_rows == 1
    assert failed.status is IngestionStatus.FAILED
    assert manifest.row_count == 1
    assert quality[0].rule_code.endswith("provider_normalization")
    persistence.fail_ingestion_run.assert_not_called()
    persistence.commit_technical_factor_batch.assert_not_called()
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1
