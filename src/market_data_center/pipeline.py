"""Phase-one ingestion orchestration."""

from collections.abc import Callable, Collection, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Protocol
from uuid import UUID, uuid4

from market_data_center.domain.calendar import calculate_trading_day_links
from market_data_center.domain.entities import CalculatedTradingDay
from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    QualityStatus,
    RawFileFormat,
    RawManifest,
)
from market_data_center.domain.records import DailyBarRecord, SecurityRecord
from market_data_center.domain.validation import validate_daily_bars
from market_data_center.providers.contracts import (
    MarketDataProvider,
    ProviderBatch,
    ProviderError,
    ProviderRecord,
)
from market_data_center.raw_store import LocalRawStore, StoredRawObject


class PipelinePersistence(Protocol):
    def task_lock(self, task_key: str) -> AbstractContextManager[None]: ...

    def create_ingestion_run(self, run: IngestionRun) -> None: ...

    def fail_ingestion_run(self, run: IngestionRun) -> None: ...

    def commit_security_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[SecurityRecord],
    ) -> None: ...

    def commit_trading_calendar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[CalculatedTradingDay],
    ) -> None: ...

    def known_symbols(self, symbols: Collection[str]) -> set[str]: ...

    def known_trading_dates(self, dates: Collection[date]) -> set[date]: ...

    def commit_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[DailyBarRecord],
        quality_results: Sequence[QualityResult],
    ) -> None: ...


class IngestionPipeline:
    def __init__(
        self,
        *,
        provider: MarketDataProvider,
        raw_store: LocalRawStore,
        persistence: PipelinePersistence,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._provider = provider
        self._raw_store = raw_store
        self._persistence = persistence
        self._clock = clock
        self._uuid_factory = uuid_factory

    def ingest_securities(self) -> IngestionRun:
        with self._persistence.task_lock(f"{self._provider.source_code}:security"):
            run = self._start_run(DatasetCode.SECURITY, {})
            try:
                batch = self._provider.fetch_securities()
                self._ensure_batch_source(batch)
                manifest = self._store_raw(run, batch)
                completed = self._completed_run(run, len(batch.raw_rows), len(batch.records), 0)
                self._persistence.commit_security_batch(completed, manifest, batch.records)
                return completed
            except Exception as error:
                self._record_failure(run, error)
                raise

    def ingest_trading_calendar(self, start_date: date, end_date: date) -> IngestionRun:
        params = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}
        with self._persistence.task_lock(f"{self._provider.source_code}:trading_calendar"):
            run = self._start_run(DatasetCode.TRADING_CALENDAR, params)
            try:
                batch = self._provider.fetch_trading_calendar(start_date, end_date)
                self._ensure_batch_source(batch)
                manifest = self._store_raw(run, batch)
                calculated = calculate_trading_day_links(batch.records)
                completed = self._completed_run(run, len(batch.raw_rows), len(calculated), 0)
                self._persistence.commit_trading_calendar_batch(completed, manifest, calculated)
                return completed
            except Exception as error:
                self._record_failure(run, error)
                raise

    def ingest_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> IngestionRun:
        params = {
            "source_symbol": source_symbol,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        with self._persistence.task_lock(f"{self._provider.source_code}:daily_bar:{source_symbol}"):
            run = self._start_run(DatasetCode.DAILY_BAR, params)
            try:
                batch = self._provider.fetch_daily_bars(source_symbol, start_date, end_date)
                self._ensure_batch_source(batch)
                manifest = self._store_raw(run, batch)
                records = list(batch.records)
                known_symbols = self._persistence.known_symbols(
                    {record.symbol for record in records}
                )
                known_dates = self._persistence.known_trading_dates(
                    {record.trade_date for record in records}
                )
                findings = validate_daily_bars(
                    records,
                    known_symbols=known_symbols,
                    known_trading_dates=known_dates,
                )
                blocked_keys = {
                    (finding.symbol, finding.trade_date)
                    for finding in findings
                    if finding.blocks_core_write
                }
                accepted = [
                    record
                    for record in records
                    if (record.symbol, record.trade_date) not in blocked_keys
                ]
                quality_results = [
                    QualityResult(
                        quality_result_id=self._uuid_factory(),
                        ingestion_id=run.ingestion_id,
                        dataset_code=DatasetCode.DAILY_BAR,
                        rule_code=finding.rule_code.value,
                        severity=finding.severity,
                        status=QualityStatus.FAILED,
                        message=finding.message,
                        natural_key={
                            "symbol": finding.symbol,
                            "trade_date": finding.trade_date.isoformat(),
                        },
                    )
                    for finding in findings
                ]
                rejected_rows = sum(
                    (record.symbol, record.trade_date) in blocked_keys for record in records
                )
                completed = self._completed_run(
                    run, len(batch.raw_rows), len(accepted), rejected_rows
                )
                self._persistence.commit_daily_bar_batch(
                    completed, manifest, accepted, quality_results
                )
                return completed
            except Exception as error:
                self._record_failure(run, error)
                raise

    def _start_run(
        self, dataset_code: DatasetCode, request_params: Mapping[str, object]
    ) -> IngestionRun:
        now = self._clock()
        run = IngestionRun(
            ingestion_id=self._uuid_factory(),
            provider_code=ProviderCode(self._provider.source_code),
            dataset_code=dataset_code,
            status=IngestionStatus.RUNNING,
            requested_at=now,
            started_at=now,
            request_params=request_params,
        )
        self._persistence.create_ingestion_run(run)
        return run

    def _store_raw[RecordT: ProviderRecord](
        self, run: IngestionRun, batch: ProviderBatch[RecordT]
    ) -> RawManifest:
        stored = self._raw_store.write_jsonl(
            provider=run.provider_code.value,
            dataset=run.dataset_code.value,
            partition_date=self._clock().date(),
            ingestion_id=run.ingestion_id,
            rows=batch.raw_rows,
            schema_version=batch.schema_version,
        )
        return self._manifest(run.ingestion_id, stored)

    def _ensure_batch_source[RecordT: ProviderRecord](self, batch: ProviderBatch[RecordT]) -> None:
        mismatched = sum(
            record.source_code != self._provider.source_code for record in batch.records
        )
        if mismatched:
            raise ProviderError(
                f"provider batch contains {mismatched} record(s) with a mismatched source_code"
            )

    def _manifest(self, ingestion_id: UUID, stored: StoredRawObject) -> RawManifest:
        return RawManifest(
            raw_id=self._uuid_factory(),
            ingestion_id=ingestion_id,
            object_path=stored.object_path,
            file_format=RawFileFormat(stored.file_format),
            content_sha256=stored.content_sha256,
            byte_size=stored.byte_size,
            row_count=stored.row_count,
            schema_version=stored.schema_version,
        )

    def _completed_run(
        self, run: IngestionRun, fetched_rows: int, accepted_rows: int, rejected_rows: int
    ) -> IngestionRun:
        if rejected_rows and accepted_rows:
            status = IngestionStatus.PARTIAL
        elif rejected_rows:
            status = IngestionStatus.FAILED
        else:
            status = IngestionStatus.SUCCEEDED
        return replace(
            run,
            status=status,
            finished_at=self._clock(),
            fetched_rows=fetched_rows,
            accepted_rows=accepted_rows,
            rejected_rows=rejected_rows,
        )

    def _record_failure(self, run: IngestionRun, error: Exception) -> None:
        failed = replace(
            run,
            status=IngestionStatus.FAILED,
            finished_at=self._clock(),
            error_summary=f"{type(error).__name__}: ingestion failed",
        )
        self._persistence.fail_ingestion_run(failed)
