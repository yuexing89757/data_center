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
from market_data_center.providers import ProviderError
from market_data_center.shareholder_count_batch import PreparedShareholderCountBatch
from market_data_center.shareholder_count_service import (
    ShareholderCountBackfillTarget,
    ShareholderCountService,
)

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)


def _batch(
    source_symbol: str | None,
    start_date: date,
    end_date: date,
    fetched_rows: int,
) -> PreparedShareholderCountBatch:
    ingestion_id = uuid4()
    run = IngestionRun(
        ingestion_id=ingestion_id,
        provider_code=ProviderCode.TUSHARE,
        dataset_code=DatasetCode.SHAREHOLDER_COUNT,
        status=IngestionStatus.SUCCEEDED,
        requested_at=NOW,
        started_at=NOW,
        finished_at=NOW,
        request_params={
            "source_symbol": source_symbol,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        fetched_rows=fetched_rows,
        accepted_rows=fetched_rows,
    )
    manifest = RawManifest(
        raw_id=uuid4(),
        ingestion_id=ingestion_id,
        object_path=f"tushare/shareholder_count/2026-08-24/{ingestion_id}.jsonl",
        file_format=RawFileFormat.JSONL,
        content_sha256="0" * 64,
        byte_size=fetched_rows,
        row_count=fetched_rows,
        schema_version="tushare.shareholder_count.v1",
    )
    records: tuple[IngestionEnvelope[ShareholderCountRecord], ...] = ()
    if fetched_rows and fetched_rows < 3_000:
        symbol = source_symbol or "SSE:600000"
        count = 12_000 + fetched_rows
        record = ShareholderCountRecord(
            symbol=symbol,
            statistics_date=start_date,
            announcement_date=end_date,
            shareholder_count=count,
            revision_key=shareholder_count_revision_key(
                symbol=symbol,
                statistics_date=start_date,
                announcement_date=end_date,
                shareholder_count=count,
            ),
            source_code="tushare",
        )
        records = (IngestionEnvelope(ingestion_id, record),)
    return PreparedShareholderCountBatch(run, manifest, records)


class FakePipeline:
    def __init__(self, outcomes: dict[tuple[str | None, date, date], int | Exception]) -> None:
        self.outcomes = outcomes
        self.calls: list[tuple[str | None, date, date]] = []

    def prepare_shareholder_count_request(
        self, source_symbol: str | None, start_date: date, end_date: date
    ) -> PreparedShareholderCountBatch:
        key = (source_symbol, start_date, end_date)
        self.calls.append(key)
        outcome = self.outcomes.get(key, 1)
        if isinstance(outcome, Exception):
            raise outcome
        return _batch(source_symbol, start_date, end_date, outcome)


class FakePersistence:
    def __init__(self, symbols: tuple[str, ...] = ("SSE:600000", "SZSE:000001")) -> None:
        self.targets = tuple(
            ShareholderCountBackfillTarget(symbol, date(1990, 12, 19)) for symbol in symbols
        )
        self.commits: list[tuple[PreparedShareholderCountBatch, ...]] = []
        self.aborts: list[tuple[tuple[PreparedShareholderCountBatch, ...], str]] = []

    def shareholder_count_backfill_targets(
        self, symbols: set[str] | None, resume_after_symbol: str | None
    ) -> tuple[ShareholderCountBackfillTarget, ...]:
        targets = self.targets
        if symbols is not None:
            targets = tuple(target for target in targets if target.symbol in symbols)
        if resume_after_symbol is not None:
            targets = tuple(target for target in targets if target.symbol > resume_after_symbol)
        return targets

    def commit_shareholder_count_batches(
        self, batches: tuple[PreparedShareholderCountBatch, ...]
    ) -> None:
        self.commits.append(batches)

    def abort_shareholder_count_batches(
        self,
        batches: tuple[PreparedShareholderCountBatch, ...],
        *,
        error_type: str,
    ) -> None:
        self.aborts.append((batches, error_type))


def test_daily_sync_uses_inclusive_thirty_day_window() -> None:
    pipeline = FakePipeline({})
    persistence = FakePersistence()

    summary = ShareholderCountService(pipeline, persistence).sync_daily(date(2026, 8, 24))

    assert pipeline.calls == [(None, date(2026, 7, 26), date(2026, 8, 24))]
    assert summary.request_count == 1
    assert len(persistence.commits) == 1


def test_limit_sized_multi_day_response_splits_into_nonoverlapping_halves() -> None:
    start = date(2026, 8, 1)
    end = date(2026, 8, 4)
    pipeline = FakePipeline({(None, start, end): 3_000})
    persistence = FakePersistence()

    summary = ShareholderCountService(pipeline, persistence).sync_range(None, start, end)

    assert pipeline.calls == [
        (None, start, end),
        (None, date(2026, 8, 1), date(2026, 8, 2)),
        (None, date(2026, 8, 3), date(2026, 8, 4)),
    ]
    assert summary.superseded_request_count == 1
    committed = persistence.commits[0]
    assert committed[0].records == ()
    assert committed[0].quality_results[0].rule_code == "shareholder_count.response_split"
    assert sum(len(batch.records) for batch in committed) == 2


def test_superseded_probe_preserves_rejected_source_row_accounting() -> None:
    start = date(2026, 8, 1)
    end = date(2026, 8, 2)
    probe = _batch(None, start, end, 3_000)
    probe = replace(
        probe,
        run=replace(
            probe.run,
            status=IngestionStatus.PARTIAL,
            accepted_rows=2_999,
            rejected_rows=1,
        ),
    )

    class PartialProbePipeline(FakePipeline):
        def prepare_shareholder_count_request(
            self, source_symbol: str | None, start_date: date, end_date: date
        ) -> PreparedShareholderCountBatch:
            if (source_symbol, start_date, end_date) == (None, start, end):
                self.calls.append((source_symbol, start_date, end_date))
                return probe
            return super().prepare_shareholder_count_request(source_symbol, start_date, end_date)

    persistence = FakePersistence()
    summary = ShareholderCountService(
        PartialProbePipeline({}),
        persistence,  # type: ignore[arg-type]  # existing fake narrows sequences
    ).sync_range(None, start, end)

    superseded = persistence.commits[0][0]
    assert superseded.records == ()
    assert superseded.run.accepted_rows == 0
    assert superseded.run.rejected_rows == 1
    assert summary.rejected_rows == 1


def test_limit_sized_single_day_global_response_falls_back_to_sorted_symbols() -> None:
    day = date(2026, 8, 24)
    pipeline = FakePipeline({(None, day, day): 3_000})
    persistence = FakePersistence(("SZSE:000001", "SSE:600000"))

    ShareholderCountService(pipeline, persistence).sync_range(None, day, day)

    assert pipeline.calls == [
        (None, day, day),
        ("SSE:600000", day, day),
        ("SZSE:000001", day, day),
    ]


def test_limit_sized_single_symbol_day_aborts_and_fails() -> None:
    day = date(2026, 8, 24)
    pipeline = FakePipeline({("SSE:600000", day, day): 3_000})
    persistence = FakePersistence()

    with pytest.raises(
        ProviderError,
        match="response remains truncated for one symbol-day",
    ):
        ShareholderCountService(pipeline, persistence).sync_range("SSE:600000", day, day)

    assert len(persistence.aborts) == 1
    assert persistence.commits == []


def test_provider_failure_after_prepared_child_aborts_every_prepared_request() -> None:
    start = date(2026, 8, 1)
    end = date(2026, 8, 2)
    pipeline = FakePipeline(
        {
            (None, start, end): 3_000,
            (None, end, end): ProviderError("unavailable"),
        }
    )
    persistence = FakePersistence()

    with pytest.raises(ProviderError, match="unavailable"):
        ShareholderCountService(pipeline, persistence).sync_range(None, start, end)

    aborted, error_type = persistence.aborts[0]
    assert len(aborted) == 2
    assert aborted[0].records == ()
    assert error_type == "ProviderError"
    assert persistence.commits == []


def test_backfill_commits_each_symbol_tree_before_advancing() -> None:
    cutoff = date(2026, 8, 24)
    persistence = FakePersistence(("SSE:600000", "SZSE:000001"))
    pipeline = FakePipeline({})

    summary = ShareholderCountService(pipeline, persistence).backfill(cutoff)

    assert [call[0] for call in pipeline.calls] == ["SSE:600000", "SZSE:000001"]
    assert len(persistence.commits) == 2
    assert summary.request_count == 2


class TargetRowsResult:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def mappings(self) -> "TargetRowsResult":
        return self

    def all(self) -> list[dict[str, object]]:
        return self._rows


class TargetRowsConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.executions: list[tuple[object, object]] = []

    def execute(self, statement: object, parameters: object) -> TargetRowsResult:
        self.executions.append((statement, parameters))
        return TargetRowsResult(self.rows)


class TargetRowsEngine:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.connection = TargetRowsConnection(rows)

    @contextmanager
    def connect(self):
        yield self.connection


def test_backfill_targets_are_deterministic_and_validate_requested_symbols() -> None:
    engine = TargetRowsEngine(
        [
            {"symbol": "SSE:600000", "start_date": date(1999, 11, 10)},
            {"symbol": "SZSE:000001", "start_date": date(1991, 4, 3)},
        ]
    )
    persistence = PostgreSQLPersistence(cast(Engine, cast(Any, engine)))

    targets = persistence.shareholder_count_backfill_targets(
        {"SSE:600000", "SZSE:000001"}, "SSE:600000"
    )

    assert targets == (ShareholderCountBackfillTarget("SZSE:000001", date(1991, 4, 3)),)
    statement = str(engine.connection.executions[0][0])
    assert "security_type = 'stock'" in statement
    assert "exchange in ('SSE', 'SZSE', 'BSE')" in statement

    engine.connection.rows = []
    with pytest.raises(ValueError, match="unknown shareholder-count backfill symbols"):
        persistence.shareholder_count_backfill_targets({"SSE:999999"}, None)
