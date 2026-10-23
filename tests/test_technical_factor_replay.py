from contextlib import nullcontext
from datetime import UTC, date, datetime
from unittest.mock import Mock
from uuid import uuid4

import pytest

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionStatus,
    ProviderCode,
    RawFileFormat,
    RawManifest,
    ReplaySource,
)
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.tushare_technical_factor import TECHNICAL_FACTOR_FIELDS
from market_data_center.raw_store import LocalRawStore
from market_data_center.reliability import RawReplayService


@pytest.mark.parametrize("dry_run", [True, False])
def test_technical_factor_raw_replay_rejects_unknown_units_and_retains_raw(tmp_path, dry_run):
    raw_store = LocalRawStore(tmp_path)
    original_id = uuid4()
    day = date(2026, 10, 9)
    row = dict.fromkeys(TECHNICAL_FACTOR_FIELDS, "")
    row.update(ts_code="000001.SZ", trade_date="20261009", macd_qfq="-0.123456789123456789")
    stored = raw_store.write_jsonl(
        provider="tushare",
        dataset="stock_technical_factor",
        partition_date=day,
        ingestion_id=original_id,
        rows=[row, row],
        schema_version="tushare.stock_technical_factor.v1",
    )
    manifest = RawManifest(
        uuid4(),
        original_id,
        stored.object_path,
        RawFileFormat.JSONL,
        stored.content_sha256,
        stored.byte_size,
        stored.row_count,
        stored.schema_version,
    )
    source = ReplaySource(
        original_id,
        ProviderCode.TUSHARE,
        DatasetCode.STOCK_TECHNICAL_FACTOR,
        datetime(2026, 10, 9, tzinfo=UTC),
        {"trade_date": "2026-10-09", "minimum_accepted_rows": 1},
        manifest,
    )
    persistence = Mock()
    persistence.task_lock.return_value = nullcontext()
    persistence.replay_source.return_value = source
    persistence.technical_factor_securities.return_value = {"SZSE:000001"}
    persistence.known_trading_dates.return_value = {day}
    original_bytes = (tmp_path / stored.object_path).read_bytes()
    with pytest.raises(ProviderError, match="quantity unit contract unverified"):
        RawReplayService(raw_store=raw_store, persistence=persistence).replay(
            original_id, dry_run=dry_run
        )
    persistence.commit_technical_factor_batch.assert_not_called()
    if dry_run:
        persistence.create_ingestion_run.assert_not_called()
        persistence.commit_rejected_batch.assert_not_called()
    else:
        persistence.create_ingestion_run.assert_called_once()
        persistence.commit_rejected_batch.assert_called_once()
        run, copied_manifest, quality = persistence.commit_rejected_batch.call_args.args
        assert copied_manifest is None
        assert run.status is IngestionStatus.FAILED
        assert run.accepted_rows == 0
        assert run.replayed_from_raw_id == manifest.raw_id
        assert run.request_params["provider_schema_version"] == "tushare.stock_technical_factor.v1"
        assert quality[0].rule_code == "stock_technical_factor.raw_replay"
        assert quality[0].blocks_core_write
    assert raw_store.read_jsonl(manifest) == (row, row)
    assert (tmp_path / stored.object_path).read_bytes() == original_bytes
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1
