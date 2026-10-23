from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
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
    Top10FloatHolderRecord,
)
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.top10_float_holder_batch import (
    PreparedTop10FloatHolderBatch,
    Top10FloatHolderSyncSummary,
)

NOW = datetime(2026, 10, 5, 12, tzinfo=UTC)


class RecordingConnection:
    def __init__(self) -> None:
        self.executions: list[tuple[object, object]] = []

    def execute(self, statement: object, parameters: object) -> Any:
        self.executions.append((statement, parameters))
        if "from ingestion.raw_manifest where raw_id" in str(statement):
            row = {
                "raw_id": parameters["raw_id"],
                "ingestion_id": uuid4(),
                "object_path": "synthetic-replay.jsonl",
                "file_format": "jsonl",
                "content_sha256": "0" * 64,
                "byte_size": 100,
                "row_count": 1,
                "schema_version": "tushare.top10_float_holder.v1",
                "storage_backend": "local",
            }
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: row))
        return None


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
    announcement_date: date = date(2026, 8, 28),
    manifest: bool = True,
) -> PreparedTop10FloatHolderBatch:
    ingestion_id = uuid4()
    replay_raw_id = None if manifest else uuid4()
    run = IngestionRun(
        ingestion_id=ingestion_id,
        provider_code=ProviderCode.TUSHARE,
        dataset_code=DatasetCode.TOP10_FLOAT_HOLDER,
        status=IngestionStatus.SUCCEEDED,
        requested_at=NOW,
        started_at=NOW,
        finished_at=NOW,
        fetched_rows=1,
        accepted_rows=1,
        request_params={"ann_date": "20260828", "period": None},
        replayed_from_raw_id=replay_raw_id,
    )
    raw_manifest = (
        RawManifest(
            raw_id=uuid4(),
            ingestion_id=ingestion_id,
            object_path=f"tushare/top10_float_holder/2026-08-28/{ingestion_id}.jsonl",
            file_format=RawFileFormat.JSONL,
            content_sha256="0" * 64,
            byte_size=100,
            row_count=1,
            schema_version="tushare.top10_float_holder.v1",
        )
        if manifest
        else None
    )
    record = Top10FloatHolderRecord(
        symbol=symbol,
        end_date=date(2026, 6, 30),
        announcement_date=announcement_date,
        holder_rank=1,
        holder_name="上海国际集团有限公司",
        hold_amount=7_086_834_641,
        hold_ratio=Decimal("21.2781"),
        hold_float_ratio=Decimal("21.2781"),
        hold_change=None,
        holder_type="一般企业",
        source_code="tushare",
    )
    return PreparedTop10FloatHolderBatch(
        run=run,
        manifest=raw_manifest,
        records=(IngestionEnvelope(ingestion_id=ingestion_id, record=record),),
    )


def test_top10_float_holder_summary_validates_counts() -> None:
    summary = Top10FloatHolderSyncSummary(2, 10, 8, 1)

    assert summary.request_count == 2
    with pytest.raises(ValueError):
        Top10FloatHolderSyncSummary(1, 1, 2, 0)
    with pytest.raises(ValueError):
        Top10FloatHolderSyncSummary(1, 1, 1, 2)


def test_multi_request_commit_uses_one_transaction() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))
    batches = (_prepared(), _prepared("SZSE:000001"))

    persistence.commit_top10_float_holder_batches(batches)

    assert engine.begin_count == 1
    assert len(engine.connection.executions) == 4
    insert_parameters = cast(list[dict[str, object]], engine.connection.executions[2][1])
    assert insert_parameters[0]["symbol"] == "SSE:600000"
    assert insert_parameters[0]["hold_amount"] == 7_086_834_641
    assert insert_parameters[1]["symbol"] == "SZSE:000001"


def test_commit_accepts_replay_without_copying_manifest() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))

    persistence.commit_top10_float_holder_batches((_prepared(manifest=False),))

    assert engine.begin_count == 1
    assert len(engine.connection.executions) == 4
    assert not any(
        "insert into ingestion.raw_manifest" in str(sql).lower()
        for sql, _ in engine.connection.executions
    )


def test_commit_rejects_duplicate_natural_keys_before_transaction() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))

    with pytest.raises(ValueError, match="duplicate natural keys"):
        persistence.commit_top10_float_holder_batches((_prepared(), _prepared()))

    assert engine.begin_count == 0


def test_commit_rejects_mismatched_manifest_and_envelope_before_transaction() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))
    batch = _prepared()
    assert batch.manifest is not None
    mismatched_manifest = replace(batch.manifest, ingestion_id=uuid4())

    with pytest.raises(ValueError, match="manifest"):
        persistence.commit_top10_float_holder_batches(
            (replace(batch, manifest=mismatched_manifest),)
        )

    with pytest.raises(ValueError, match="envelope"):
        persistence.commit_top10_float_holder_batches(
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

    persistence.abort_top10_float_holder_batches((_prepared(),), error_type="ProviderError")

    assert engine.begin_count == 1
    assert len(engine.connection.executions) == 3
    quality_parameters = cast(list[dict[str, object]], engine.connection.executions[1][1])
    run_parameters = cast(list[dict[str, object]], engine.connection.executions[2][1])
    assert quality_parameters[0]["rule_code"] == "top10_float_holder.batch_aborted"
    assert run_parameters[0]["status"] == "failed"
    assert run_parameters[0]["accepted_rows"] == 0
