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
from market_data_center.providers.tushare_technical_factor import TECHNICAL_FACTOR_FIELDS
from market_data_center.raw_store import LocalRawStore
from market_data_center.reliability import RawReplayService


@pytest.mark.parametrize("request_date", ["2026-10-09", "20261009"])
@pytest.mark.parametrize("dry_run", [True, False])
def test_technical_factor_raw_replay_preserves_native_scale_and_retains_raw(
    tmp_path, dry_run, request_date
):
    raw_store = LocalRawStore(tmp_path)
    original_id = uuid4()
    day = date(2026, 10, 9)
    row = dict.fromkeys(TECHNICAL_FACTOR_FIELDS, "")
    row.update(
        ts_code="000001.SZ",
        trade_date="20261009",
        macd_qfq="-0.123456789123456789",
        obv_bfq="123456",
    )
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
        {"trade_date": request_date, "minimum_accepted_rows": 1},
        manifest,
    )
    persistence = Mock()
    persistence.task_lock.return_value = nullcontext()
    persistence.replay_source.return_value = source
    persistence.technical_factor_securities.return_value = {"SZSE:000001"}
    persistence.known_trading_dates.return_value = {day}
    original_bytes = (tmp_path / stored.object_path).read_bytes()
    RawReplayService(raw_store=raw_store, persistence=persistence).replay(
        original_id, dry_run=dry_run
    )
    persistence.commit_rejected_batch.assert_not_called()
    if dry_run:
        persistence.create_ingestion_run.assert_not_called()
        persistence.commit_technical_factor_batch.assert_not_called()
    else:
        persistence.create_ingestion_run.assert_called_once()
        run, copied_manifest, envelopes, quality = (
            persistence.commit_technical_factor_batch.call_args.args
        )
        assert copied_manifest is None
        assert quality == ()
        assert run.status is IngestionStatus.SUCCEEDED
        assert run.accepted_rows == 1
        assert run.request_params["trade_date"] == "2026-10-09"
        assert run.replayed_from_raw_id == manifest.raw_id
        assert envelopes[0].record.unadjusted["obv"] == 123456
    assert raw_store.read_jsonl(manifest) == (row, row)
    assert (tmp_path / stored.object_path).read_bytes() == original_bytes
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1
