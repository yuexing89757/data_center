"""Exact-date, all-or-nothing ingestion of provider technical-factor facts."""

from collections.abc import Callable, Collection, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Protocol, cast
from uuid import UUID, uuid4

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    QualityResult,
    QualitySeverity,
    QualityStatus,
    RawManifest,
)
from market_data_center.domain.records import IngestionEnvelope
from market_data_center.domain.stock_technical_factor import (
    StockTechnicalFactorSnapshotRecord,
    validate_technical_factor_snapshot,
)
from market_data_center.pipeline import (
    IngestionPipeline,
    PipelinePersistence,
    _RecordedProviderError,
)
from market_data_center.providers.contracts import MarketDataProvider, ProviderBatch
from market_data_center.raw_store import LocalRawStore


class TechnicalFactorProvider(Protocol):
    source_code: str

    def fetch_snapshot(
        self, trade_date: date
    ) -> ProviderBatch[StockTechnicalFactorSnapshotRecord]: ...


class TechnicalFactorPersistence(PipelinePersistence, Protocol):
    def technical_factor_coverage_floor(self, trade_date: date) -> int: ...

    def technical_factor_securities(self, trade_date: date) -> Collection[str]: ...

    def commit_technical_factor_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[StockTechnicalFactorSnapshotRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None: ...


class TechnicalFactorIngestionService(IngestionPipeline):
    """Reuse the pipeline's audited Raw staging without changing ordinary providers."""

    def __init__(
        self,
        *,
        provider: TechnicalFactorProvider,
        persistence: TechnicalFactorPersistence,
        raw_store: LocalRawStore,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        super().__init__(
            provider=cast(MarketDataProvider, provider),
            persistence=persistence,
            raw_store=raw_store,
            clock=clock,
            uuid_factory=uuid_factory,
        )
        self._factor_provider = provider
        self._factor_persistence = persistence

    def ingest_snapshot(self, trade_date: date) -> IngestionRun:
        with self._persistence.task_lock(
            f"tushare:stock_technical_factor:{trade_date.isoformat()}"
        ):
            floor = self._factor_persistence.technical_factor_coverage_floor(trade_date)
            run = self._start_run(
                DatasetCode.STOCK_TECHNICAL_FACTOR,
                {
                    "api_name": "stk_factor_pro",
                    "trade_date": trade_date.isoformat(),
                    "minimum_accepted_rows": floor,
                },
            )
            try:
                batch = self._factor_provider.fetch_snapshot(trade_date)
                manifest, records = self._stage_batch(run, batch)
                try:
                    accepted = validate_technical_factor_snapshot(
                        records,
                        trade_date=trade_date,
                        known_symbols=self._factor_persistence.technical_factor_securities(
                            trade_date
                        ),
                        known_trading_dates=self._persistence.known_trading_dates({trade_date}),
                        minimum_accepted_rows=floor,
                    )
                except ValueError as error:
                    failed = replace(
                        self._completed_run(run, len(batch.raw_rows), 0, len(batch.raw_rows)),
                        status=IngestionStatus.FAILED,
                        error_summary="ValueError: technical factor validation failed",
                    )
                    quality = QualityResult(
                        self._uuid_factory(),
                        run.ingestion_id,
                        run.dataset_code,
                        "stock_technical_factor.invalid_snapshot",
                        QualitySeverity.ERROR,
                        QualityStatus.FAILED,
                        str(error),
                        {"trade_date": trade_date.isoformat()},
                    )
                    self._persistence.commit_rejected_batch(failed, manifest, [quality])
                    return failed
                completed = self._completed_run(run, len(batch.raw_rows), len(accepted), 0)
                self._factor_persistence.commit_technical_factor_batch(
                    completed,
                    manifest,
                    tuple(IngestionEnvelope(run.ingestion_id, record) for record in accepted),
                    (),
                )
                return completed
            except _RecordedProviderError:
                raise
            except Exception as error:
                self._record_failure(run, error)
                raise
