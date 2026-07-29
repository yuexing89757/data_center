"""Phase-one ingestion orchestration."""

from collections.abc import Callable, Collection, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Protocol, cast
from uuid import UUID, uuid4

from market_data_center.domain.board_index import (
    BoardIndexConstituentSnapshotRecord,
    BoardIndexDailyBarRecord,
    BoardIndexFinding,
    BoardIndexProviderRecord,
    BoardIndexRecord,
    validate_board_index_constituent_snapshot,
    validate_board_index_daily_bars,
)
from market_data_center.domain.calendar import calculate_trading_day_links
from market_data_center.domain.capital import validate_capital
from market_data_center.domain.classification import (
    ClassificationCatalogSnapshotRecord,
    ClassificationFinding,
    ClassificationMemberSnapshotRecord,
    ClassificationRecord,
    ClassificationType,
    validate_catalog,
    validate_member_snapshot,
)
from market_data_center.domain.entities import CalculatedTradingDay
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
from market_data_center.domain.records import (
    CapitalRecord,
    DailyBarRecord,
    IngestionEnvelope,
    SecurityRecord,
)
from market_data_center.domain.validation import validate_daily_bars
from market_data_center.providers.contracts import (
    BoardIndexProvider,
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
        records: Sequence[IngestionEnvelope[SecurityRecord]],
    ) -> None: ...

    def commit_trading_calendar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[IngestionEnvelope[CalculatedTradingDay]],
    ) -> None: ...

    def known_symbols(self, symbols: Collection[str]) -> set[str]: ...

    def known_trading_dates(self, dates: Collection[date]) -> set[date]: ...

    def known_board_ids(self, board_ids: Collection[str]) -> set[str]: ...

    def trading_day_boundaries(
        self, start_date: date, end_date: date
    ) -> tuple[date | None, date | None]: ...

    def commit_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[IngestionEnvelope[DailyBarRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_capital_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[IngestionEnvelope[CapitalRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def known_classification_snapshots(
        self, keys: Collection[tuple[str, ClassificationType, str, date]]
    ) -> set[tuple[str, ClassificationType, str, date]]: ...

    def commit_classification_catalog_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        record: IngestionEnvelope[ClassificationCatalogSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_classification_members_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        record: IngestionEnvelope[ClassificationMemberSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_board_index_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[IngestionEnvelope[BoardIndexRecord]],
    ) -> None: ...

    def commit_board_index_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[IngestionEnvelope[BoardIndexDailyBarRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_board_index_constituents_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        record: IngestionEnvelope[BoardIndexConstituentSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_rejected_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality_results: Sequence[QualityResult],
    ) -> None: ...


class _RecordedProviderError(ProviderError):
    """Provider failure whose Raw manifest and quality result are already committed."""


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
                manifest, records = self._stage_batch(run, batch)
                completed = self._completed_run(run, len(batch.raw_rows), len(records), 0)
                self._persistence.commit_security_batch(
                    completed, manifest, self._envelopes(run.ingestion_id, records)
                )
                return completed
            except _RecordedProviderError:
                raise
            except Exception as error:
                self._record_failure(run, error)
                raise

    def ingest_trading_calendar(self, start_date: date, end_date: date) -> IngestionRun:
        params = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}
        with self._persistence.task_lock(f"{self._provider.source_code}:trading_calendar"):
            run = self._start_run(DatasetCode.TRADING_CALENDAR, params)
            try:
                batch = self._provider.fetch_trading_calendar(start_date, end_date)
                manifest, records = self._stage_batch(run, batch)
                boundaries = self._persistence.trading_day_boundaries(start_date, end_date)
                calculated = calculate_trading_day_links(
                    records,
                    previous_trading_day=boundaries[0],
                    next_trading_day=boundaries[1],
                )
                completed = self._completed_run(run, len(batch.raw_rows), len(calculated), 0)
                self._persistence.commit_trading_calendar_batch(
                    completed, manifest, self._envelopes(run.ingestion_id, calculated)
                )
                return completed
            except _RecordedProviderError:
                raise
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
                manifest, normalized = self._stage_batch(run, batch)
                records = list(normalized)
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
                    completed,
                    manifest,
                    self._envelopes(run.ingestion_id, accepted),
                    quality_results,
                )
                return completed
            except _RecordedProviderError:
                raise
            except Exception as error:
                self._record_failure(run, error)
                raise

    def ingest_capital(self, source_symbol: str, *, mode: str = "incremental") -> IngestionRun:
        if mode not in {"backfill", "incremental"}:
            raise ValueError("Capital mode must be backfill or incremental")
        params = {"source_symbol": source_symbol, "mode": mode}
        with self._persistence.task_lock(f"{self._provider.source_code}:capital:{source_symbol}"):
            run = self._start_run(DatasetCode.CAPITAL, params)
            try:
                batch = self._provider.fetch_capital(source_symbol)
                manifest, normalized = self._stage_batch(run, batch)
                records = list(normalized)
                known_symbols = self._persistence.known_symbols(
                    {record.symbol for record in records}
                )
                validation = validate_capital(records, known_symbols=known_symbols)
                quality_results = [
                    QualityResult(
                        quality_result_id=self._uuid_factory(),
                        ingestion_id=run.ingestion_id,
                        dataset_code=DatasetCode.CAPITAL,
                        rule_code=finding.rule_code,
                        severity=QualitySeverity.ERROR,
                        status=QualityStatus.FAILED,
                        message=finding.message,
                        natural_key=finding.natural_key,
                    )
                    for finding in validation.findings
                ]
                completed = self._completed_run(
                    run,
                    len(batch.raw_rows),
                    len(validation.accepted),
                    validation.rejected_rows,
                )
                self._persistence.commit_capital_batch(
                    completed,
                    manifest,
                    self._envelopes(run.ingestion_id, validation.accepted),
                    quality_results,
                )
                return completed
            except _RecordedProviderError:
                raise
            except Exception as error:
                self._record_failure(run, error)
                raise

    def ingest_classification_catalog(
        self, classification_type: str, *, snapshot_date: date | None = None
    ) -> IngestionRun:
        snapshot_date = snapshot_date or self._clock().date()
        params = {
            "classification_type": classification_type,
            "snapshot_date": snapshot_date.isoformat(),
        }
        task_key = f"{self._provider.source_code}:classification_catalog:{classification_type}"
        with self._persistence.task_lock(task_key):
            run = self._start_run(DatasetCode.CLASSIFICATION_CATALOG, params)
            try:
                batch = self._provider.fetch_classification_catalog(
                    classification_type, snapshot_date
                )
                manifest, normalized = self._stage_batch(run, batch)
                if len(normalized) != 1 or not isinstance(
                    normalized[0], ClassificationCatalogSnapshotRecord
                ):
                    raise ProviderError("classification catalog must normalize to one snapshot")
                record = normalized[0]
                findings = validate_catalog(record)
                quality = self._classification_quality_results(run, findings)
                fetched = max(len(batch.raw_rows), 1)
                accepted = 0 if findings else len(record.definitions)
                completed = self._completed_run(
                    run,
                    fetched,
                    accepted,
                    fetched if findings else 0,
                )
                self._persistence.commit_classification_catalog_batch(
                    completed,
                    manifest,
                    IngestionEnvelope(run.ingestion_id, record),
                    quality,
                )
                return completed
            except _RecordedProviderError:
                raise
            except Exception as error:
                self._record_failure(run, error)
                raise

    def ingest_classification_members(
        self,
        classification_type: str,
        classification_code: str,
        *,
        snapshot_date: date | None = None,
    ) -> IngestionRun:
        snapshot_date = snapshot_date or self._clock().date()
        params = {
            "classification_type": classification_type,
            "classification_code": classification_code,
            "snapshot_date": snapshot_date.isoformat(),
        }
        task_key = (
            f"{self._provider.source_code}:classification_members:"
            f"{classification_type}:{classification_code}"
        )
        with self._persistence.task_lock(task_key):
            run = self._start_run(DatasetCode.CLASSIFICATION_MEMBERS, params)
            try:
                batch = self._provider.fetch_classification_members(
                    classification_type, classification_code, snapshot_date
                )
                manifest, normalized = self._stage_batch(run, batch)
                if len(normalized) != 1 or not isinstance(
                    normalized[0], ClassificationMemberSnapshotRecord
                ):
                    raise ProviderError("classification members must normalize to one snapshot")
                record = normalized[0]
                key = (
                    record.namespace,
                    record.classification_type,
                    record.classification_code,
                    record.snapshot_date,
                )
                findings = validate_member_snapshot(
                    record,
                    known_classifications=self._persistence.known_classification_snapshots({key}),
                    known_symbols=self._persistence.known_symbols(set(record.members)),
                )
                quality = self._classification_quality_results(run, findings)
                fetched = max(len(batch.raw_rows), 1)
                accepted = 0 if findings else len(record.members)
                completed = self._completed_run(
                    run,
                    fetched,
                    accepted,
                    fetched if findings else 0,
                )
                self._persistence.commit_classification_members_batch(
                    completed,
                    manifest,
                    IngestionEnvelope(run.ingestion_id, record),
                    quality,
                )
                return completed
            except _RecordedProviderError:
                raise
            except Exception as error:
                self._record_failure(run, error)
                raise

    def _classification_quality_results(
        self, run: IngestionRun, findings: Sequence[ClassificationFinding]
    ) -> list[QualityResult]:
        return [
            QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=run.ingestion_id,
                dataset_code=run.dataset_code,
                rule_code=finding.rule_code,
                severity=QualitySeverity.ERROR,
                status=QualityStatus.FAILED,
                message=finding.message,
                natural_key=finding.natural_key,
            )
            for finding in findings
        ]

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

    def _stage_batch[RecordT: ProviderRecord](
        self, run: IngestionRun, batch: ProviderBatch[RecordT]
    ) -> tuple[RawManifest, tuple[RecordT, ...]]:
        manifest = self._store_raw(run, batch)
        try:
            records = tuple(batch.records)
            self._ensure_record_source(records)
        except ProviderError as error:
            self._record_normalization_failure(run, manifest, len(batch.raw_rows), error)
            raise _RecordedProviderError(str(error)) from error
        return manifest, records

    def _ensure_record_source[RecordT: ProviderRecord](self, records: Sequence[RecordT]) -> None:
        mismatched = sum(record.source_code != self._provider.source_code for record in records)
        if mismatched:
            raise ProviderError(
                f"provider batch contains {mismatched} record(s) with a mismatched source_code"
            )

    def _record_normalization_failure(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        fetched_rows: int,
        error: ProviderError,
    ) -> None:
        failed = replace(
            run,
            status=IngestionStatus.FAILED,
            finished_at=self._clock(),
            fetched_rows=fetched_rows,
            rejected_rows=fetched_rows,
            error_summary=f"{type(error).__name__}: provider normalization failed",
        )
        result = QualityResult(
            quality_result_id=self._uuid_factory(),
            ingestion_id=run.ingestion_id,
            dataset_code=run.dataset_code,
            rule_code=f"{run.dataset_code.value}.provider_normalization",
            severity=QualitySeverity.ERROR,
            status=QualityStatus.FAILED,
            message="provider response normalization failed",
            details={"error_type": type(error).__name__},
        )
        self._persistence.commit_rejected_batch(failed, manifest, [result])

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

    @staticmethod
    def _envelopes[
        RecordT: SecurityRecord
        | CalculatedTradingDay
        | DailyBarRecord
        | CapitalRecord
        | ClassificationRecord
    ](ingestion_id: UUID, records: Sequence[RecordT]) -> tuple[IngestionEnvelope[RecordT], ...]:
        return tuple(IngestionEnvelope(ingestion_id, record) for record in records)

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


class BoardIndexIngestionPipeline:
    """Ingestion orchestration for the isolated BoardIndex capability."""

    def __init__(
        self,
        *,
        provider: BoardIndexProvider,
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

    def ingest_board_indexes(self) -> IngestionRun:
        with self._persistence.task_lock(f"{self._provider.source_code}:board_index"):
            run = self._start_run(DatasetCode.BOARD_INDEX, {})
            try:
                batch = self._provider.fetch_board_indexes()
                manifest, normalized = self._stage_batch(run, batch)
                if not normalized or any(
                    not isinstance(record, BoardIndexRecord) for record in normalized
                ):
                    raise ProviderError("board-index catalog must normalize to BoardIndex records")
                records = cast(tuple[BoardIndexRecord, ...], normalized)
                completed = self._completed_run(run, len(batch.raw_rows), len(records), 0)
                self._persistence.commit_board_index_batch(
                    completed,
                    manifest,
                    self._envelopes(run.ingestion_id, records),
                )
                return completed
            except _RecordedProviderError:
                raise
            except Exception as error:
                self._record_failure(run, error)
                raise

    def ingest_board_index_daily_bars(
        self, board_id: str, start_date: date, end_date: date
    ) -> IngestionRun:
        params = {
            "board_id": board_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        task_key = f"{self._provider.source_code}:board_index_daily_bar:{board_id}"
        with self._persistence.task_lock(task_key):
            run = self._start_run(DatasetCode.BOARD_INDEX_DAILY_BAR, params)
            try:
                batch = self._provider.fetch_board_index_daily_bars(board_id, start_date, end_date)
                manifest, normalized = self._stage_batch(run, batch)
                if any(not isinstance(record, BoardIndexDailyBarRecord) for record in normalized):
                    raise ProviderError("board-index daily bars normalized to an unexpected record")
                records = cast(tuple[BoardIndexDailyBarRecord, ...], normalized)
                validation = validate_board_index_daily_bars(
                    records,
                    known_board_ids=self._persistence.known_board_ids(
                        {record.board_id for record in records}
                    ),
                    known_trading_dates=self._persistence.known_trading_dates(
                        {record.trade_date for record in records}
                    ),
                )
                quality = self._quality_results(run, validation.findings)
                completed = self._completed_run(
                    run,
                    len(batch.raw_rows),
                    len(validation.accepted),
                    validation.rejected_rows,
                )
                self._persistence.commit_board_index_daily_bar_batch(
                    completed,
                    manifest,
                    self._envelopes(run.ingestion_id, validation.accepted),
                    quality,
                )
                return completed
            except _RecordedProviderError:
                raise
            except Exception as error:
                self._record_failure(run, error)
                raise

    def ingest_board_index_constituents(self, board_id: str, snapshot_date: date) -> IngestionRun:
        params = {
            "board_id": board_id,
            "snapshot_date": snapshot_date.isoformat(),
        }
        task_key = (
            f"{self._provider.source_code}:board_index_constituent_snapshot:"
            f"{board_id}:{snapshot_date.isoformat()}"
        )
        with self._persistence.task_lock(task_key):
            run = self._start_run(DatasetCode.BOARD_INDEX_CONSTITUENT_SNAPSHOT, params)
            try:
                batch = self._provider.fetch_board_index_constituents(board_id, snapshot_date)
                manifest, normalized = self._stage_batch(run, batch)
                if len(normalized) != 1 or not isinstance(
                    normalized[0], BoardIndexConstituentSnapshotRecord
                ):
                    raise ProviderError("board-index constituents must normalize to one snapshot")
                record = normalized[0]
                findings = validate_board_index_constituent_snapshot(
                    record,
                    known_board_ids=self._persistence.known_board_ids({record.board_id}),
                    known_symbols=self._persistence.known_symbols(set(record.members)),
                    known_trading_dates=self._persistence.known_trading_dates({record.trade_date}),
                )
                quality = self._quality_results(run, findings)
                fetched = max(len(batch.raw_rows), 1)
                completed = self._completed_run(
                    run,
                    fetched,
                    0 if findings else len(record.members),
                    fetched if findings else 0,
                )
                self._persistence.commit_board_index_constituents_batch(
                    completed,
                    manifest,
                    IngestionEnvelope(run.ingestion_id, record),
                    quality,
                )
                return completed
            except _RecordedProviderError:
                raise
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

    def _stage_batch(
        self,
        run: IngestionRun,
        batch: ProviderBatch[BoardIndexProviderRecord],
    ) -> tuple[RawManifest, tuple[BoardIndexProviderRecord, ...]]:
        stored = self._raw_store.write_jsonl(
            provider=run.provider_code.value,
            dataset=run.dataset_code.value,
            partition_date=self._clock().date(),
            ingestion_id=run.ingestion_id,
            rows=batch.raw_rows,
            schema_version=batch.schema_version,
        )
        manifest = RawManifest(
            raw_id=self._uuid_factory(),
            ingestion_id=run.ingestion_id,
            object_path=stored.object_path,
            file_format=RawFileFormat(stored.file_format),
            content_sha256=stored.content_sha256,
            byte_size=stored.byte_size,
            row_count=stored.row_count,
            schema_version=stored.schema_version,
        )
        try:
            records = tuple(batch.records)
            mismatched = sum(record.source_code != self._provider.source_code for record in records)
            if mismatched:
                raise ProviderError(
                    f"provider batch contains {mismatched} record(s) with a mismatched source_code"
                )
        except ProviderError as error:
            self._record_normalization_failure(run, manifest, len(batch.raw_rows), error)
            raise _RecordedProviderError(str(error)) from error
        except Exception as error:
            provider_error = ProviderError("provider response normalization failed")
            self._record_normalization_failure(run, manifest, len(batch.raw_rows), provider_error)
            raise _RecordedProviderError(str(provider_error)) from error
        return manifest, records

    def _record_normalization_failure(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        fetched_rows: int,
        error: ProviderError,
    ) -> None:
        failed = replace(
            run,
            status=IngestionStatus.FAILED,
            finished_at=self._clock(),
            fetched_rows=fetched_rows,
            rejected_rows=fetched_rows,
            error_summary=f"{type(error).__name__}: provider normalization failed",
        )
        result = QualityResult(
            quality_result_id=self._uuid_factory(),
            ingestion_id=run.ingestion_id,
            dataset_code=run.dataset_code,
            rule_code=f"{run.dataset_code.value}.provider_normalization",
            severity=QualitySeverity.ERROR,
            status=QualityStatus.FAILED,
            message="provider response normalization failed",
            details={"error_type": type(error).__name__},
        )
        self._persistence.commit_rejected_batch(failed, manifest, [result])

    def _quality_results(
        self, run: IngestionRun, findings: Sequence[BoardIndexFinding]
    ) -> list[QualityResult]:
        return [
            QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=run.ingestion_id,
                dataset_code=run.dataset_code,
                rule_code=finding.rule_code,
                severity=QualitySeverity.ERROR,
                status=QualityStatus.FAILED,
                message=finding.message,
                natural_key=finding.natural_key,
            )
            for finding in findings
        ]

    @staticmethod
    def _envelopes[RecordT: BoardIndexProviderRecord](
        ingestion_id: UUID, records: Sequence[RecordT]
    ) -> tuple[IngestionEnvelope[RecordT], ...]:
        return tuple(IngestionEnvelope(ingestion_id, record) for record in records)

    def _completed_run(
        self,
        run: IngestionRun,
        fetched_rows: int,
        accepted_rows: int,
        rejected_rows: int,
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
