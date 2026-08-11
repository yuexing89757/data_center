from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast
from uuid import uuid4

import pytest
from sqlalchemy import Engine

from market_data_center.daily_bar_batch import PreparedDailyBarBatch
from market_data_center.domain import (
    DailyBarRecord,
    DatasetCode,
    IngestionEnvelope,
    IngestionRun,
    IngestionStatus,
    Market,
    ProviderCode,
    RawFileFormat,
    RawManifest,
    TradeStatus,
)
from market_data_center.persistence import PostgreSQLPersistence

NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)


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


def _prepared(symbol: str, code: str) -> PreparedDailyBarBatch:
    ingestion_id = uuid4()
    run = IngestionRun(
        ingestion_id=ingestion_id,
        provider_code=ProviderCode.PYTDX,
        dataset_code=DatasetCode.DAILY_BAR,
        status=IngestionStatus.SUCCEEDED,
        requested_at=NOW,
        started_at=NOW,
        finished_at=NOW,
        fetched_rows=1,
        accepted_rows=1,
        request_params={"source_symbol": code},
    )
    manifest = RawManifest(
        raw_id=uuid4(),
        ingestion_id=ingestion_id,
        object_path=f"pytdx/daily_bar/2026-08-11/{ingestion_id}.jsonl",
        file_format=RawFileFormat.JSONL,
        content_sha256="0" * 64,
        byte_size=100,
        row_count=1,
        schema_version="pytdx.daily_bar.v1",
    )
    record = DailyBarRecord(
        symbol=symbol,
        trade_date=date(2026, 8, 11),
        market=Market.CN_A_SHARE,
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        previous_close=Decimal("10"),
        volume=100,
        amount=Decimal("1050"),
        trade_status=TradeStatus.TRADING,
        is_st=False,
        source_code="pytdx",
    )
    return PreparedDailyBarBatch(
        run,
        manifest,
        (IngestionEnvelope(ingestion_id=ingestion_id, record=record),),
        (),
    )


def test_multi_run_commit_uses_one_transaction_and_bounded_executemany_calls() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))
    batches = [_prepared(f"SSE:{600000 + index:06d}", str(600000 + index)) for index in range(100)]

    persistence.commit_daily_bar_batches(batches)

    assert engine.begin_count == 1
    assert len(engine.connection.executions) == 3
    assert len(cast(list[object], engine.connection.executions[0][1])) == 100
    assert len(cast(list[object], engine.connection.executions[1][1])) == 100
    assert len(cast(list[object], engine.connection.executions[2][1])) == 100


def test_multi_run_commit_rejects_duplicate_natural_keys_before_transaction() -> None:
    engine = RecordingEngine()
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))
    first = _prepared("SSE:600000", "600000")
    second = _prepared("SSE:600000", "600000")

    with pytest.raises(ValueError, match="duplicate natural keys"):
        persistence.commit_daily_bar_batches([first, second])

    assert engine.begin_count == 0
