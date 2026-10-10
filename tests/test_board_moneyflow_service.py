from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from market_data_center.board_moneyflow_batch import (
    BoardMoneyflowSyncSummary,
    PreparedBoardMoneyflowBatch,
)
from market_data_center.board_moneyflow_service import BoardMoneyflowService
from market_data_center.domain import (
    BoardMoneyflowRecord,
    BoardMoneyflowType,
    DatasetCode,
    IngestionEnvelope,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    RawFileFormat,
    RawManifest,
)
from market_data_center.providers import ProviderError

NOW = datetime(2026, 10, 6, 12, tzinfo=UTC)


def _batch(trade_date: date, fetched_rows: int) -> PreparedBoardMoneyflowBatch:
    ingestion_id = uuid4()
    run = IngestionRun(
        ingestion_id=ingestion_id,
        provider_code=ProviderCode.TUSHARE,
        dataset_code=DatasetCode.BOARD_MONEYFLOW,
        status=IngestionStatus.SUCCEEDED,
        requested_at=NOW,
        started_at=NOW,
        finished_at=NOW,
        request_params={"trade_date": trade_date.isoformat()},
        fetched_rows=fetched_rows,
        accepted_rows=fetched_rows,
    )
    manifest = RawManifest(
        raw_id=uuid4(),
        ingestion_id=ingestion_id,
        object_path=f"tushare/board_moneyflow/{trade_date}/{ingestion_id}.jsonl",
        file_format=RawFileFormat.JSONL,
        content_sha256="0" * 64,
        byte_size=fetched_rows,
        row_count=fetched_rows,
        schema_version="tushare.board_moneyflow.v1",
    )
    record = BoardMoneyflowRecord(
        board_code="DC:BK0145",
        trade_date=trade_date,
        board_type=BoardMoneyflowType.REGION,
        name="上海板块",
        pct_change=Decimal("0.23"),
        close=None,
        net_amount=Decimal("-726583552.00"),
        net_amount_rate=None,
        extra_large_amount=None,
        extra_large_rate=None,
        large_amount=None,
        large_rate=None,
        medium_amount=None,
        medium_rate=None,
        small_amount=None,
        small_rate=None,
        top_small_buy_stock=None,
        rank=None,
        source_code="tushare",
    )
    records: tuple[IngestionEnvelope[BoardMoneyflowRecord], ...] = (
        (IngestionEnvelope(ingestion_id, record),) if fetched_rows else ()
    )
    return PreparedBoardMoneyflowBatch(run, manifest, records)


class FakePipeline:
    def __init__(
        self,
        outcomes: dict[date, int | Exception] | None = None,
    ) -> None:
        self.outcomes = outcomes or {}
        self.calls: list[date] = []

    def prepare_board_moneyflow_request(self, trade_date: date) -> PreparedBoardMoneyflowBatch:
        self.calls.append(trade_date)
        outcome = self.outcomes.get(trade_date, 1)
        if isinstance(outcome, Exception):
            raise outcome
        return _batch(trade_date, outcome)


class FakePersistence:
    def __init__(self, completed_days: set[date] | None = None) -> None:
        self.completed_days = completed_days or set()
        self.commits: list[tuple[PreparedBoardMoneyflowBatch, ...]] = []
        self.aborts: list[tuple[tuple[PreparedBoardMoneyflowBatch, ...], str]] = []

    def commit_board_moneyflow_batches(
        self, batches: tuple[PreparedBoardMoneyflowBatch, ...]
    ) -> None:
        self.commits.append(batches)

    def abort_board_moneyflow_batches(
        self,
        batches: tuple[PreparedBoardMoneyflowBatch, ...],
        *,
        error_type: str,
    ) -> None:
        self.aborts.append((batches, error_type))

    def board_moneyflow_completed_days(self, start_date: date, end_date: date) -> set[date]:
        return {day for day in self.completed_days if start_date <= day <= end_date}


def test_daily_sync_covers_five_day_rolling_window() -> None:
    pipeline = FakePipeline()
    persistence = FakePersistence()

    summary = BoardMoneyflowService(pipeline, persistence).sync_daily(date(2026, 10, 6))

    assert pipeline.calls == [date(2026, 10, 2) + timedelta(days=offset) for offset in range(5)]
    assert summary.request_count == 5
    assert summary.trade_days == 5
    # Each trade day is published in its own transaction.
    assert len(persistence.commits) == 5
    assert persistence.aborts == []


def test_range_sync_commits_each_day_separately() -> None:
    pipeline = FakePipeline()
    persistence = FakePersistence()

    summary = BoardMoneyflowService(pipeline, persistence).sync_range(
        date(2026, 9, 28), date(2026, 9, 30)
    )

    assert pipeline.calls == [date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30)]
    assert summary.request_count == 3
    assert summary.fetched_rows == 3
    assert summary.accepted_rows == 3
    assert len(persistence.commits) == 3
    assert [batches[0].run.request_params["trade_date"] for batches in persistence.commits] == [
        "2026-09-28",
        "2026-09-29",
        "2026-09-30",
    ]


def test_provider_failure_mid_range_keeps_committed_days() -> None:
    pipeline = FakePipeline({date(2026, 9, 30): ProviderError("unavailable")})
    persistence = FakePersistence()

    with pytest.raises(ProviderError, match="unavailable"):
        BoardMoneyflowService(pipeline, persistence).sync_range(
            date(2026, 9, 28), date(2026, 9, 30)
        )

    # Earlier days were already committed; the failing day prepared nothing.
    assert [batches[0].run.request_params["trade_date"] for batches in persistence.commits] == [
        "2026-09-28",
        "2026-09-29",
    ]
    assert persistence.aborts == []


def test_sync_range_skips_already_published_days() -> None:
    pipeline = FakePipeline()
    persistence = FakePersistence(completed_days={date(2026, 9, 28), date(2026, 9, 29)})

    summary = BoardMoneyflowService(pipeline, persistence).sync_range(
        date(2026, 9, 28), date(2026, 9, 30)
    )

    assert pipeline.calls == [date(2026, 9, 30)]
    assert summary.request_count == 1
    assert summary.trade_days == 1
    assert len(persistence.commits) == 1


def test_reversed_range_is_rejected_before_requests() -> None:
    pipeline = FakePipeline()
    persistence = FakePersistence()

    with pytest.raises(ValueError, match="end_date"):
        BoardMoneyflowService(pipeline, persistence).sync_range(
            date(2026, 9, 30), date(2026, 9, 28)
        )

    assert pipeline.calls == []
    assert persistence.commits == []


def test_summary_counts_are_cross_checked() -> None:
    assert BoardMoneyflowSyncSummary(2, 10, 8, 2).rejected_rows == 2
    with pytest.raises(ValueError):
        BoardMoneyflowSyncSummary(1, 1, 2, 0)
    with pytest.raises(ValueError):
        BoardMoneyflowSyncSummary(-1, 0, 0, 0)


def test_superseded_rejected_rows_flow_into_summary() -> None:
    first = _batch(date(2026, 9, 30), 1)
    partial = replace(
        first.run,
        status=IngestionStatus.PARTIAL,
        accepted_rows=0,
        rejected_rows=1,
    )
    batch = replace(first, run=partial, records=())

    class PartialPipeline(FakePipeline):
        def prepare_board_moneyflow_request(self, trade_date: date) -> PreparedBoardMoneyflowBatch:
            self.calls.append(trade_date)
            return batch

    persistence = FakePersistence()
    summary = BoardMoneyflowService(PartialPipeline(), persistence).sync_range(
        date(2026, 9, 30), date(2026, 9, 30)
    )

    assert summary.rejected_rows == 1
    assert summary.accepted_rows == 0
