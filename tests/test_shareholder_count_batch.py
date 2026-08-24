from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Any, cast
from uuid import uuid4

import pytest
from sqlalchemy import Engine

from market_data_center.domain import (
    DatasetCode,
    IngestionEnvelope,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    RawFileFormat,
    RawManifest,
    ShareholderCountRecord,
    shareholder_count_revision_key,
)
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.shareholder_count_batch import (
    PreparedShareholderCountBatch,
    ShareholderCountSyncSummary,
)

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)


class RecordingConnection:
    def __init__(self) -> None:
        self.executions: list[tuple[object, object]] = []

    def execute(self, statement: object, parameters: object) -> None:
        self.executions.append((statement, parameters))


class RecordingEngine:
    def __init__(self) -> None:
        self.begin_count = 0
        self.connection = RecordingConnection()

    @contextmanager
    def begin(self):
        self.begin_count += 1
        yield self.connection


def _prepared(
    symbol: str = "SSE:600000",
    *,
    statistics_date: date = date(2026, 6, 30),
    manifest: bool = True,
) -> PreparedShareholderCountBatch:
    ingestion_id = uuid4()
    replay_raw_id = None if manifest else uuid4()
    run = IngestionRun(
        ingestion_id=ingestion_id,
        provider_code=ProviderCode.TUSHARE,
        dataset_code=DatasetCode.SHAREHOLDER_COUNT,
        status=IngestionStatus.SUCCEEDED,
        requested_at=NOW,
        started_at=NOW,
        finished_at=NOW,
        fetched_rows=1,
        accepted_rows=1,
        request_params={"source_symbol": symbol},
        replayed_from_raw_id=replay_raw_id,
    )
    raw_manifest = (
        RawManifest(
            raw_id=uuid4(),
            ingestion_id=ingestion_id,
            object_path=f"tushare/shareholder_count/2026-08-24/{ingestion_id}.jsonl",
            file_format=RawFileFormat.JSONL,
            content_sha256="0" * 64,
            byte_size=100,
            row_count=1,
            schema_version="tushare.shareholder_count.v1",
        )
        if manifest
        else None
    )
    announcement_date = date(2026, 8, 20)
    count = 12_001
    record = ShareholderCountRecord(
        symbol=symbol,
        statistics_date=statistics_date,
        announcement_date=announcement_date,
        shareholder_count=count,
        revision_key=shareholder_count_revision_key(
            symbol=symbol,
            statistics_date=statistics_date,
            announcement_date=announcement_date,
            shareholder_count=count,
        ),
        source_code="tushare",
    )
    return PreparedShareholderCountBatch(
        run=run,
        manifest=raw_manifest,
        records=(IngestionEnvelope(ingestion_id=ingestion_id, record=record),),
    )


def test_shareholder_count_summary_validates_counts() -> None:
    summary = ShareholderCountSyncSummary(2, 10, 8, 1)

    assert summary.request_count == 2
    with pytest.raises(ValueError):
        ShareholderCountSyncSummary(1, 1, 2, 0)
    with pytest.raises(ValueError):
        ShareholderCountSyncSummary(1, 1, 1, 2)


def test_multi_request_commit_uses_one_transaction() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))
    batches = (_prepared(), _prepared("SZSE:000001"))

    persistence.commit_shareholder_count_batches(batches)

    assert engine.begin_count == 1
    assert len(engine.connection.executions) == 3
    assert len(cast(list[object], engine.connection.executions[0][1])) == 2
    assert len(cast(list[object], engine.connection.executions[1][1])) == 2
    assert len(cast(list[object], engine.connection.executions[2][1])) == 2


def test_commit_accepts_replay_without_copying_manifest() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))

    persistence.commit_shareholder_count_batches((_prepared(manifest=False),))

    assert engine.begin_count == 1
    assert len(engine.connection.executions) == 2


def test_commit_rejects_duplicate_natural_keys_before_transaction() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))

    with pytest.raises(ValueError, match="duplicate natural keys"):
        persistence.commit_shareholder_count_batches((_prepared(), _prepared()))

    assert engine.begin_count == 0


def test_commit_rejects_mismatched_manifest_and_envelope_before_transaction() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))
    batch = _prepared()
    assert batch.manifest is not None
    mismatched_manifest = replace(batch.manifest, ingestion_id=uuid4())

    with pytest.raises(ValueError, match="manifest"):
        persistence.commit_shareholder_count_batches(
            (replace(batch, manifest=mismatched_manifest),)
        )

    with pytest.raises(ValueError, match="envelope"):
        persistence.commit_shareholder_count_batches(
            (
                replace(
                    batch,
                    records=(replace(batch.records[0], ingestion_id=uuid4()),),
                ),
            )
        )

    assert engine.begin_count == 0


def test_abort_persists_raw_and_quality_without_inserting_facts() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))

    persistence.abort_shareholder_count_batches((_prepared(),), error_type="ProviderError")

    assert engine.begin_count == 1
    assert len(engine.connection.executions) == 3
    quality_parameters = cast(list[dict[str, object]], engine.connection.executions[1][1])
    run_parameters = cast(list[dict[str, object]], engine.connection.executions[2][1])
    assert quality_parameters[0]["rule_code"] == "shareholder_count.batch_aborted"
    assert run_parameters[0]["status"] == "failed"
    assert run_parameters[0]["accepted_rows"] == 0
