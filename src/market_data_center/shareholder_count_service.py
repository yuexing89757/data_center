"""Controlled daily and historical synchronization for shareholder-count facts."""

from collections.abc import Collection, Sequence
from dataclasses import dataclass, replace
from datetime import date, timedelta
from typing import Protocol
from uuid import uuid4

from market_data_center.domain import (
    DatasetCode,
    QualityResult,
    QualitySeverity,
    QualityStatus,
)
from market_data_center.providers import ProviderError
from market_data_center.providers.tushare import SHAREHOLDER_COUNT_RESPONSE_LIMIT
from market_data_center.shareholder_count_batch import (
    PreparedShareholderCountBatch,
    ShareholderCountSyncSummary,
)


@dataclass(frozen=True, slots=True)
class ShareholderCountBackfillTarget:
    symbol: str
    start_date: date


class ShareholderCountPipeline(Protocol):
    def prepare_shareholder_count_request(
        self, source_symbol: str | None, start_date: date, end_date: date
    ) -> PreparedShareholderCountBatch: ...


class ShareholderCountPersistence(Protocol):
    def shareholder_count_backfill_targets(
        self,
        symbols: Collection[str] | None,
        resume_after_symbol: str | None,
    ) -> tuple[ShareholderCountBackfillTarget, ...]: ...

    def commit_shareholder_count_batches(
        self, batches: Sequence[PreparedShareholderCountBatch]
    ) -> None: ...

    def abort_shareholder_count_batches(
        self,
        batches: Sequence[PreparedShareholderCountBatch],
        *,
        error_type: str,
    ) -> None: ...


class ShareholderCountService:
    def __init__(
        self,
        pipeline: ShareholderCountPipeline,
        persistence: ShareholderCountPersistence,
    ) -> None:
        self._pipeline = pipeline
        self._persistence = persistence

    def sync_daily(self, as_of_date: date) -> ShareholderCountSyncSummary:
        return self.sync_range(None, as_of_date - timedelta(days=29), as_of_date)

    def sync_range(
        self,
        source_symbol: str | None,
        start_date: date,
        end_date: date,
    ) -> ShareholderCountSyncSummary:
        if end_date < start_date:
            raise ValueError("end_date must not precede start_date")
        batches: list[PreparedShareholderCountBatch] = []
        superseded_request_count = 0
        try:
            superseded_request_count = self._collect_request_tree(
                source_symbol,
                start_date,
                end_date,
                batches,
            )
        except Exception as error:
            if batches:
                self._persistence.abort_shareholder_count_batches(
                    tuple(batches), error_type=type(error).__name__
                )
            raise
        self._persistence.commit_shareholder_count_batches(tuple(batches))
        return self._summary(batches, superseded_request_count)

    def backfill(
        self,
        cutoff_date: date,
        *,
        symbols: Collection[str] | None = None,
        resume_after_symbol: str | None = None,
    ) -> ShareholderCountSyncSummary:
        targets = self._persistence.shareholder_count_backfill_targets(symbols, resume_after_symbol)
        return self.backfill_targets(cutoff_date, targets)

    def backfill_targets(
        self,
        cutoff_date: date,
        targets: Sequence[ShareholderCountBackfillTarget],
    ) -> ShareholderCountSyncSummary:
        summaries = [
            self.sync_range(target.symbol, target.start_date, cutoff_date)
            for target in targets
            if target.start_date <= cutoff_date
        ]
        return ShareholderCountSyncSummary(
            request_count=sum(summary.request_count for summary in summaries),
            fetched_rows=sum(summary.fetched_rows for summary in summaries),
            accepted_rows=sum(summary.accepted_rows for summary in summaries),
            superseded_request_count=sum(summary.superseded_request_count for summary in summaries),
        )

    def _collect_request_tree(
        self,
        source_symbol: str | None,
        start_date: date,
        end_date: date,
        batches: list[PreparedShareholderCountBatch],
    ) -> int:
        batch = self._pipeline.prepare_shareholder_count_request(
            source_symbol, start_date, end_date
        )
        batches.append(batch)
        if batch.run.fetched_rows < SHAREHOLDER_COUNT_RESPONSE_LIMIT:
            return 0

        batches[-1] = self._superseded_probe(batch)
        if start_date < end_date:
            midpoint = start_date + timedelta(days=(end_date - start_date).days // 2)
            left = self._collect_request_tree(source_symbol, start_date, midpoint, batches)
            right = self._collect_request_tree(
                source_symbol, midpoint + timedelta(days=1), end_date, batches
            )
            return 1 + left + right

        if source_symbol is None:
            targets = sorted(
                self._persistence.shareholder_count_backfill_targets(None, None),
                key=lambda target: target.symbol,
            )
            nested = sum(
                self._collect_request_tree(
                    target.symbol,
                    start_date,
                    end_date,
                    batches,
                )
                for target in targets
            )
            return 1 + nested

        raise ProviderError(
            "Tushare shareholder count response remains truncated for one symbol-day"
        )

    @staticmethod
    def _superseded_probe(
        batch: PreparedShareholderCountBatch,
    ) -> PreparedShareholderCountBatch:
        quality = QualityResult(
            quality_result_id=uuid4(),
            ingestion_id=batch.run.ingestion_id,
            dataset_code=DatasetCode.SHAREHOLDER_COUNT,
            rule_code="shareholder_count.response_split",
            severity=QualitySeverity.INFO,
            status=QualityStatus.PASSED,
            message="Tushare response reached its row limit and was superseded by split requests",
            natural_key={
                "source_symbol": batch.run.request_params.get("source_symbol"),
                "start_date": batch.run.request_params.get("start_date"),
                "end_date": batch.run.request_params.get("end_date"),
            },
        )
        return replace(
            batch,
            run=replace(batch.run, accepted_rows=0, rejected_rows=0),
            records=(),
            quality_results=(*batch.quality_results, quality),
        )

    @staticmethod
    def _summary(
        batches: Sequence[PreparedShareholderCountBatch],
        superseded_request_count: int,
    ) -> ShareholderCountSyncSummary:
        return ShareholderCountSyncSummary(
            request_count=len(batches),
            fetched_rows=sum(batch.run.fetched_rows for batch in batches),
            accepted_rows=sum(len(batch.records) for batch in batches),
            superseded_request_count=superseded_request_count,
        )
