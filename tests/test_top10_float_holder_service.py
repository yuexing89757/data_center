from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

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
from market_data_center.providers import ProviderError
from market_data_center.top10_float_holder_batch import PreparedTop10FloatHolderBatch
from market_data_center.top10_float_holder_service import (
    Top10FloatHolderService,
    recent_report_periods,
)

NOW = datetime(2026, 10, 5, 12, tzinfo=UTC)


def _batch(
    announcement_date: date,
    fetched_rows: int,
    *,
    period: str | None = None,
    symbol: str | None = None,
) -> PreparedTop10FloatHolderBatch:
    ingestion_id = uuid4()
    run = IngestionRun(
        ingestion_id=ingestion_id,
        provider_code=ProviderCode.TUSHARE,
        dataset_code=DatasetCode.TOP10_FLOAT_HOLDER,
        status=IngestionStatus.SUCCEEDED,
        requested_at=NOW,
        started_at=NOW,
        finished_at=NOW,
        request_params={
            "ann_date": announcement_date.strftime("%Y%m%d"),
            "period": period,
            "source_symbol": symbol,
        },
        fetched_rows=fetched_rows,
        accepted_rows=fetched_rows,
    )
    manifest = RawManifest(
        raw_id=uuid4(),
        ingestion_id=ingestion_id,
        object_path=(f"tushare/top10_float_holder/{announcement_date}/{ingestion_id}.jsonl"),
        file_format=RawFileFormat.JSONL,
        content_sha256="0" * 64,
        byte_size=fetched_rows,
        row_count=fetched_rows,
        schema_version="tushare.top10_float_holder.v1",
    )
    records: tuple[IngestionEnvelope[Top10FloatHolderRecord], ...] = ()
    if fetched_rows and fetched_rows < 6_000:
        record = Top10FloatHolderRecord(
            symbol=symbol or "SSE:600000",
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
        records = (IngestionEnvelope(ingestion_id, record),)
    return PreparedTop10FloatHolderBatch(run, manifest, records)


class FakePipeline:
    def __init__(
        self,
        outcomes: dict[tuple[date, str | None, str | None], int | Exception] | None = None,
    ) -> None:
        self.outcomes = outcomes or {}
        self.calls: list[tuple[date, str | None, str | None]] = []

    def prepare_top10_float_holder_request(
        self,
        announcement_date: date,
        *,
        period: str | None = None,
        symbol: str | None = None,
    ) -> PreparedTop10FloatHolderBatch:
        key = (announcement_date, period, symbol)
        self.calls.append(key)
        outcome = self.outcomes.get(key, 1)
        if isinstance(outcome, Exception):
            raise outcome
        return _batch(announcement_date, outcome, period=period, symbol=symbol)


class FakePersistence:
    def __init__(
        self,
        split_symbols: tuple[str, ...] = ("SSE:600000", "SZSE:000001"),
        completed_days: set[date] | None = None,
    ) -> None:
        self.split_symbols = split_symbols
        self.completed_days = completed_days or set()
        self.commits: list[tuple[PreparedTop10FloatHolderBatch, ...]] = []
        self.aborts: list[tuple[tuple[PreparedTop10FloatHolderBatch, ...], str]] = []

    def commit_top10_float_holder_batches(
        self, batches: tuple[PreparedTop10FloatHolderBatch, ...]
    ) -> None:
        self.commits.append(batches)

    def abort_top10_float_holder_batches(
        self,
        batches: tuple[PreparedTop10FloatHolderBatch, ...],
        *,
        error_type: str,
    ) -> None:
        self.aborts.append((batches, error_type))

    def top10_float_holder_split_symbols(self) -> tuple[str, ...]:
        return self.split_symbols

    def top10_float_holder_completed_days(self, start_date: date, end_date: date) -> set[date]:
        return {day for day in self.completed_days if start_date <= day <= end_date}


def _service(pipeline: FakePipeline, persistence: FakePersistence) -> Top10FloatHolderService:
    return Top10FloatHolderService(pipeline, persistence)


def test_recent_report_periods_are_quarter_ends_strictly_before_as_of() -> None:
    assert recent_report_periods(date(2026, 10, 5), count=4) == (
        "20260930",
        "20260630",
        "20260331",
        "20251231",
    )
    # A quarter-end announcement day must not include its own period.
    assert recent_report_periods(date(2026, 9, 30), count=2) == (
        "20260630",
        "20260331",
    )


def test_weekly_sync_covers_nine_day_rolling_window() -> None:
    pipeline = FakePipeline()
    persistence = FakePersistence()

    summary = _service(pipeline, persistence).sync_weekly(date(2026, 10, 4))

    assert [call[0] for call in pipeline.calls] == [
        date(2026, 9, 26) + timedelta(days=offset) for offset in range(9)
    ]
    assert summary.request_count == 9
    assert summary.announcement_days == 9
    assert summary.superseded_request_count == 0
    # Each announcement day is published in its own transaction.
    assert len(persistence.commits) == 9
    assert persistence.aborts == []


def test_limit_sized_day_supersedes_full_day_request_and_splits_by_period() -> None:
    day = date(2026, 10, 5)
    pipeline = FakePipeline({(day, None, None): 6_000})
    persistence = FakePersistence()

    summary = _service(pipeline, persistence).sync_range(day, day)

    full_day, *splits = pipeline.calls
    assert full_day == (day, None, None)
    assert [call[1] for call in splits] == list(recent_report_periods(day))
    assert summary.superseded_request_count == 1

    committed = persistence.commits[0]
    superseded = committed[0]
    assert superseded.records == ()
    assert superseded.run.accepted_rows == 0
    assert superseded.quality_results[-1].rule_code == "top10_float_holder.response_split"
    assert superseded.quality_results[-1].severity.value == "info"
    assert sum(len(batch.records) for batch in committed) == len(recent_report_periods(day))


def test_limit_sized_period_split_supersedes_and_re_reads_per_symbol() -> None:
    # Peak disclosure days stay truncated even within one report period, so the
    # period request is superseded and the day is re-read symbol by symbol.
    day = date(2026, 8, 28)
    truncated_period = recent_report_periods(day)[0]
    pipeline = FakePipeline(
        {
            (day, None, None): 6_000,
            (day, truncated_period, None): 6_000,
        }
    )
    persistence = FakePersistence(("SSE:600000", "SZSE:000001"))

    summary = _service(pipeline, persistence).sync_range(day, day)

    calls = pipeline.calls
    assert calls[0] == (day, None, None)
    assert calls[1] == (day, truncated_period, None)
    assert calls[2:4] == [
        (day, truncated_period, "SSE:600000"),
        (day, truncated_period, "SZSE:000001"),
    ]
    assert calls[4:] == [(day, period, None) for period in recent_report_periods(day)[1:]]
    assert summary.superseded_request_count == 2

    committed = persistence.commits[0]
    superseded = [batch for batch in committed if batch.records == ()]
    assert all(
        batch.quality_results[-1].rule_code == "top10_float_holder.response_split"
        for batch in superseded
    )
    per_symbol_records = sum(len(batch.records) for batch in committed[2:4])
    assert per_symbol_records == 2


def test_provider_failure_mid_window_keeps_committed_days() -> None:
    start = date(2026, 10, 3)
    end = date(2026, 10, 5)
    pipeline = FakePipeline({(end, None, None): ProviderError("unavailable")})
    persistence = FakePersistence()

    with pytest.raises(ProviderError, match="unavailable"):
        _service(pipeline, persistence).sync_range(start, end)

    # Earlier days were already committed; the failing day prepared nothing.
    assert [batches[0].run.request_params["ann_date"] for batches in persistence.commits] == [
        "20261003",
        "20261004",
    ]
    assert persistence.aborts == []


def test_partially_prepared_day_aborts_only_that_day() -> None:
    day = date(2026, 10, 5)
    truncated_period = recent_report_periods(day)[0]
    pipeline = FakePipeline(
        {
            (day, None, None): 6_000,
            (day, truncated_period, None): ProviderError("unavailable"),
        }
    )
    persistence = FakePersistence()

    with pytest.raises(ProviderError, match="unavailable"):
        _service(pipeline, persistence).sync_range(day, day)

    # The superseded full-day probe was prepared before the failure and is aborted.
    aborted, error_type = persistence.aborts[0]
    assert len(aborted) == 1
    assert aborted[0].run.request_params["ann_date"] == "20261005"
    assert error_type == "ProviderError"
    assert persistence.commits == []


def test_sync_range_skips_already_published_days() -> None:
    start = date(2026, 10, 3)
    end = date(2026, 10, 5)
    pipeline = FakePipeline()
    persistence = FakePersistence(completed_days={date(2026, 10, 3), date(2026, 10, 4)})

    summary = _service(pipeline, persistence).sync_range(start, end)

    assert [call[0] for call in pipeline.calls] == [end]
    assert summary.request_count == 1
    assert summary.announcement_days == 1
    assert len(persistence.commits) == 1


def test_superseded_probe_preserves_rejected_source_row_accounting() -> None:
    day = date(2026, 10, 5)
    probe = replace(
        _batch(day, 6_000),
        run=replace(
            _batch(day, 6_000).run,
            status=IngestionStatus.PARTIAL,
            accepted_rows=5_999,
            rejected_rows=1,
        ),
    )

    class PartialProbePipeline(FakePipeline):
        def prepare_top10_float_holder_request(
            self,
            announcement_date: date,
            *,
            period: str | None = None,
            symbol: str | None = None,
        ) -> PreparedTop10FloatHolderBatch:
            if (announcement_date, period, symbol) == (day, None, None):
                self.calls.append((announcement_date, period, symbol))
                return probe
            return super().prepare_top10_float_holder_request(
                announcement_date, period=period, symbol=symbol
            )

    persistence = FakePersistence()
    summary = Top10FloatHolderService(PartialProbePipeline(), persistence).sync_range(day, day)

    superseded = persistence.commits[0][0]
    assert superseded.records == ()
    assert superseded.run.rejected_rows == 1
    assert summary.rejected_rows == 1
