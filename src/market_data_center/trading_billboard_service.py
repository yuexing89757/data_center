"""Exact-day and bounded-range trading billboard collection orchestration."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid4

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    QualitySeverity,
    QualityStatus,
    RawFileFormat,
    RawManifest,
)
from market_data_center.domain.trading_billboard import (
    TradingBillboardRecord,
    TradingBillboardValidationResult,
    validate_trading_billboards,
)
from market_data_center.providers.contracts import (
    ProviderBatch,
    TradingBillboardProvider,
)
from market_data_center.raw_store import StoredRawObject


class TradingBillboardValidationError(RuntimeError):
    """Raised after a hard aggregate finding has been durably recorded."""


@dataclass(frozen=True, slots=True)
class TradingBillboardCollectionSummary:
    status: str
    ingestion_id: UUID
    trade_date: date
    fetched_rows: int
    accepted_entries: int
    accepted_seats: int
    filtered_rows: int
    unchanged_entries: int = 0


@dataclass(frozen=True, slots=True)
class TradingBillboardBackfillSummary:
    completed_dates: tuple[date, ...]
    skipped_dates: tuple[date, ...]
    failed_date: date | None
    results: tuple[TradingBillboardCollectionSummary, ...]


class TradingBillboardPersistence(Protocol):
    def is_trading_day(self, trade_date: date) -> bool: ...

    def known_stock_symbols(self, trade_date: date) -> frozenset[str]: ...

    def commit_success(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: Sequence[QualityResult],
        records: Sequence[TradingBillboardRecord],
    ) -> TradingBillboardCollectionSummary: ...

    def commit_failure(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality: Sequence[QualityResult],
    ) -> None: ...


class TradingBillboardRawStore(Protocol):
    def write_jsonl(
        self,
        *,
        provider: str,
        dataset: str,
        partition_date: date,
        ingestion_id: UUID,
        rows: Sequence[Mapping[str, str]],
        schema_version: str,
    ) -> StoredRawObject: ...


class TradingBillboardService:
    def __init__(
        self,
        *,
        persistence: TradingBillboardPersistence,
        raw_store: TradingBillboardRawStore,
        provider: TradingBillboardProvider,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._persistence = persistence
        self._raw_store = raw_store
        self._provider = provider
        self._clock = clock
        self._uuid_factory = uuid_factory

    def collect(self, trade_date: date) -> TradingBillboardCollectionSummary:
        if not self._persistence.is_trading_day(trade_date):
            raise ValueError(f"{trade_date.isoformat()} is not a CN_A_SHARE trading day")
        started = self._aware_now()
        run = IngestionRun(
            ingestion_id=self._uuid_factory(),
            provider_code=ProviderCode.EASTMONEY,
            dataset_code=DatasetCode.TRADING_BILLBOARD,
            status=IngestionStatus.RUNNING,
            requested_at=started,
            started_at=started,
            request_params={"trade_date": trade_date.isoformat()},
        )
        manifest: RawManifest | None = None
        stored: StoredRawObject | None = None
        try:
            batch = self._provider.fetch_trading_billboard(trade_date)
            run = replace(run, request_params=dict(batch.request_params))
            stored = self._write_raw(batch, trade_date, run.ingestion_id)
            manifest = self._manifest(run.ingestion_id, stored)
            records = tuple(batch.records)
        except Exception as error:
            self._record_external_failure(run, manifest, stored, error)
            raise

        validation = validate_trading_billboards(
            records,
            known_symbols=self._persistence.known_stock_symbols(trade_date),
            known_trading_dates={trade_date},
        )
        if validation.findings:
            quality = self._quality(run.ingestion_id, validation)
            failed = replace(
                run,
                status=IngestionStatus.FAILED,
                finished_at=self._aware_now(),
                fetched_rows=stored.row_count,
                accepted_rows=0,
                rejected_rows=stored.row_count,
                error_summary="TradingBillboardValidationError: hard validation failed",
            )
            self._persistence.commit_failure(failed, manifest, quality)
            raise TradingBillboardValidationError(
                f"trading billboard validation failed for {trade_date.isoformat()}"
            )

        accepted_seats = sum(
            len(record.buy_seats) + len(record.sell_seats) for record in validation.accepted
        )
        accepted_fact_rows = len(validation.accepted) + accepted_seats
        filtered_rows = max(stored.row_count - accepted_fact_rows, 0)
        completed = replace(
            run,
            status=IngestionStatus.SUCCEEDED,
            finished_at=self._aware_now(),
            fetched_rows=stored.row_count,
            accepted_rows=accepted_fact_rows,
            rejected_rows=filtered_rows,
        )
        return self._persistence.commit_success(completed, manifest, (), validation.accepted)

    def backfill(self, start_date: date, end_date: date) -> TradingBillboardBackfillSummary:
        if start_date > end_date:
            raise ValueError("start_date must not follow end_date")
        if (end_date - start_date).days > 365:
            raise ValueError("trading billboard backfill is bounded to 366 calendar days")
        completed: list[date] = []
        skipped: list[date] = []
        results: list[TradingBillboardCollectionSummary] = []
        current = start_date
        while current <= end_date:
            if not self._persistence.is_trading_day(current):
                skipped.append(current)
                current += timedelta(days=1)
                continue
            try:
                result = self.collect(current)
            except Exception:
                return TradingBillboardBackfillSummary(
                    tuple(completed), tuple(skipped), current, tuple(results)
                )
            completed.append(current)
            results.append(result)
            current += timedelta(days=1)
        return TradingBillboardBackfillSummary(
            tuple(completed), tuple(skipped), None, tuple(results)
        )

    def _write_raw(
        self,
        batch: ProviderBatch[TradingBillboardRecord],
        trade_date: date,
        ingestion_id: UUID,
    ) -> StoredRawObject:
        return self._raw_store.write_jsonl(
            provider=ProviderCode.EASTMONEY.value,
            dataset=DatasetCode.TRADING_BILLBOARD.value,
            partition_date=trade_date,
            ingestion_id=ingestion_id,
            rows=tuple(batch.raw_rows),
            schema_version=batch.schema_version,
        )

    def _manifest(self, ingestion_id: UUID, stored: StoredRawObject) -> RawManifest:
        return RawManifest(
            raw_id=self._uuid_factory(),
            ingestion_id=ingestion_id,
            object_path=stored.object_path,
            file_format=RawFileFormat.JSONL,
            content_sha256=stored.content_sha256,
            byte_size=stored.byte_size,
            row_count=stored.row_count,
            schema_version=stored.schema_version,
        )

    def _record_external_failure(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        stored: StoredRawObject | None,
        error: Exception,
    ) -> None:
        fetched = stored.row_count if stored is not None else 0
        failed = replace(
            run,
            status=IngestionStatus.FAILED,
            finished_at=self._aware_now(),
            fetched_rows=fetched,
            rejected_rows=fetched,
            error_summary=f"{type(error).__name__}: collection failed",
        )
        quality = (
            QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=run.ingestion_id,
                dataset_code=DatasetCode.TRADING_BILLBOARD,
                rule_code="trading_billboard.collection_error",
                severity=QualitySeverity.ERROR,
                status=QualityStatus.FAILED,
                message="trading billboard collection or normalization failed",
                natural_key={"trade_date": run.request_params["trade_date"]},
                details={"error_type": type(error).__name__},
            ),
        )
        self._persistence.commit_failure(failed, manifest, quality)

    def _quality(
        self, ingestion_id: UUID, validation: TradingBillboardValidationResult
    ) -> tuple[QualityResult, ...]:
        return tuple(
            QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=ingestion_id,
                dataset_code=DatasetCode.TRADING_BILLBOARD,
                rule_code=finding.rule_code,
                severity=QualitySeverity.ERROR,
                status=QualityStatus.FAILED,
                message=finding.message,
                natural_key=finding.natural_key,
            )
            for finding in validation.findings
        )

    def _aware_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("trading billboard clock must be timezone-aware")
        return value.astimezone(UTC)
