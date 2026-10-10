from contextlib import nullcontext
from datetime import UTC, date, datetime
from unittest.mock import Mock
from uuid import uuid4

import pytest

from market_data_center.domain.ingestion import (
    DatasetCode,
    ProviderCode,
    RawFileFormat,
    RawManifest,
    ReplaySource,
)
from market_data_center.providers.tushare_technical_factor import TECHNICAL_FACTOR_FIELDS
from market_data_center.raw_store import LocalRawStore
from market_data_center.reliability import RawReplayService


@pytest.mark.parametrize("dry_run", [True, False])
def test_technical_factor_raw_replay_uses_same_mapping_and_exact_date(tmp_path, dry_run):
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
    summary = RawReplayService(raw_store=raw_store, persistence=persistence).replay(
        original_id, dry_run=dry_run
    )
    assert summary.accepted_rows == 1
    assert summary.fetched_rows == 2
    persistence.task_lock.assert_called_once_with("tushare:stock_technical_factor:2026-10-09")
    if dry_run:
        persistence.create_ingestion_run.assert_not_called()
        persistence.commit_technical_factor_batch.assert_not_called()
    else:
        run, copied_manifest, records, quality = (
            persistence.commit_technical_factor_batch.call_args.args
        )
        assert copied_manifest is None
        assert run.replayed_from_raw_id == manifest.raw_id
        assert str(records[0].record.forward_adjusted["macd"]) == "-0.123456789123456789"
        assert not quality
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1
