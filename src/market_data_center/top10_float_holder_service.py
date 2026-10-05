"""Controlled weekly synchronization for top-10 float-holder facts."""

from collections.abc import Sequence
from dataclasses import replace
from datetime import date, timedelta
from typing import Protocol
from uuid import uuid4

from market_data_center.providers.tushare import TOP10_FLOAT_HOLDERS_RESPONSE_LIMIT
from market_data_center.top10_float_holder_batch import (
    PreparedTop10FloatHolderBatch,
    Top10FloatHolderSyncSummary,
    top10_float_holder_split_quality_result,
)

WEEKLY_WINDOW_DAYS = 9
_QUARTER_ENDS = ((3, 31), (6, 30), (9, 30), (12, 31))


def recent_report_periods(as_of: date, count: int = 8) -> tuple[str, ...]:
    """Quarter-end report periods strictly before ``as_of``, newest first."""
    periods: list[str] = []
    year = as_of.year
    while len(periods) < count:
        for month, day in reversed(_QUARTER_ENDS):
            candidate = date(year, month, day)
            if candidate < as_of:
                periods.append(candidate.strftime("%Y%m%d"))
            if len(periods) >= count:
                break
        year -= 1
    return tuple(periods)


class Top10FloatHolderPipeline(Protocol):
    def prepare_top10_float_holder_request(
        self,
        announcement_date: date,
        *,
        period: str | None = None,
        symbol: str | None = None,
    ) -> PreparedTop10FloatHolderBatch: ...


class Top10FloatHolderPersistence(Protocol):
    def commit_top10_float_holder_batches(
        self, batches: Sequence[PreparedTop10FloatHolderBatch]
    ) -> None: ...

    def abort_top10_float_holder_batches(
        self,
        batches: Sequence[PreparedTop10FloatHolderBatch],
        *,
        error_type: str,
    ) -> None: ...

    def top10_float_holder_split_symbols(self) -> tuple[str, ...]: ...


class Top10FloatHolderService:
    def __init__(
        self,
        pipeline: Top10FloatHolderPipeline,
        persistence: Top10FloatHolderPersistence,
    ) -> None:
        self._pipeline = pipeline
        self._persistence = persistence

    def sync_weekly(self, as_of: date) -> Top10FloatHolderSyncSummary:
        """Rolling announcement window covering the seven prior days plus margins."""
        return self.sync_range(as_of - timedelta(days=WEEKLY_WINDOW_DAYS - 1), as_of)

    def sync_range(self, start_date: date, end_date: date) -> Top10FloatHolderSyncSummary:
        if end_date < start_date:
            raise ValueError("end_date must not precede start_date")
        batches: list[PreparedTop10FloatHolderBatch] = []
        superseded_request_count = 0
        try:
            day = start_date
            while day <= end_date:
                superseded_request_count += self._collect_day(day, batches)
                day += timedelta(days=1)
        except Exception as error:
            if batches:
                self._persistence.abort_top10_float_holder_batches(
                    tuple(batches), error_type=type(error).__name__
                )
            raise
        self._persistence.commit_top10_float_holder_batches(tuple(batches))
        return self._summary(start_date, end_date, batches, superseded_request_count)

    def _collect_day(self, day: date, batches: list[PreparedTop10FloatHolderBatch]) -> int:
        batch = self._pipeline.prepare_top10_float_holder_request(day)
        batches.append(batch)
        if batch.run.fetched_rows < TOP10_FLOAT_HOLDERS_RESPONSE_LIMIT:
            return 0

        batches[-1] = self._superseded(batch)
        superseded = 1
        for period in recent_report_periods(day):
            split = self._pipeline.prepare_top10_float_holder_request(day, period=period)
            batches.append(split)
            if split.run.fetched_rows < TOP10_FLOAT_HOLDERS_RESPONSE_LIMIT:
                continue
            # Peak disclosure days exceed the limit even within one report
            # period: supersede the period request and re-read it per symbol.
            batches[-1] = self._superseded(split)
            superseded += 1
            for symbol in self._persistence.top10_float_holder_split_symbols():
                batches.append(
                    self._pipeline.prepare_top10_float_holder_request(
                        day, period=period, symbol=symbol
                    )
                )
        return superseded

    @staticmethod
    def _superseded(batch: PreparedTop10FloatHolderBatch) -> PreparedTop10FloatHolderBatch:
        quality = top10_float_holder_split_quality_result(
            quality_result_id=uuid4(),
            ingestion_id=batch.run.ingestion_id,
            natural_key=dict(batch.run.request_params),
        )
        return replace(
            batch,
            run=replace(batch.run, accepted_rows=0),
            records=(),
            quality_results=(*batch.quality_results, quality),
        )

    @staticmethod
    def _summary(
        start_date: date,
        end_date: date,
        batches: Sequence[PreparedTop10FloatHolderBatch],
        superseded_request_count: int,
    ) -> Top10FloatHolderSyncSummary:
        return Top10FloatHolderSyncSummary(
            request_count=len(batches),
            fetched_rows=sum(batch.run.fetched_rows for batch in batches),
            accepted_rows=sum(len(batch.records) for batch in batches),
            superseded_request_count=superseded_request_count,
            rejected_rows=sum(batch.run.rejected_rows for batch in batches),
            announcement_days=(end_date - start_date).days + 1,
        )
