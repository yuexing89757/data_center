"""Staged DragonTiger collection with durable Raw lineage."""

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid4

from market_data_center.domain.dragon_tiger import (
    DragonTigerAmountPeriodBasis,
    DragonTigerEventDraft,
    DragonTigerEventRecord,
    DragonTigerFinding,
    DragonTigerNormalizationResult,
    DragonTigerSourceFinding,
    DragonTigerValidationResult,
    DragonTigerWindowBasis,
    validate_dragon_tiger_events,
)
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
from market_data_center.providers.contracts import DragonTigerProvider, DragonTigerProviderBatch
from market_data_center.raw_store import StoredRawObject


class DragonTigerCollectionError(RuntimeError):
    """A collection failure safe to persist and report."""

    def __init__(self, code: str, phase: str) -> None:
        self.code = code
        self.phase = phase
        super().__init__(f"{code}:{phase}")


class DragonTigerValidationError(DragonTigerCollectionError):
    """A hard domain validation failure."""


@dataclass(frozen=True, slots=True)
class DragonTigerCollectionSummary:
    status: str
    ingestion_id: UUID
    trade_date: date
    fetched_rows: int
    accepted_events: int
    accepted_seat_trades: int
    filtered_rows: int
    unchanged_events: int = 0


@dataclass(frozen=True, slots=True)
class DragonTigerBackfillFailure:
    trade_date: date
    error_code: str
    phase: str


@dataclass(frozen=True, slots=True)
class DragonTigerBackfillSummary:
    completed_dates: tuple[date, ...]
    skipped_non_trading_dates: tuple[date, ...]
    skipped_succeeded_dates: tuple[date, ...]
    failed_dates: tuple[DragonTigerBackfillFailure, ...]
    results: tuple[DragonTigerCollectionSummary, ...]
    quality_code_counts: Mapping[str, int]


class DragonTigerPersistence(Protocol):
    def is_trading_day(self, trade_date: date) -> bool: ...

    def period_start_date(self, trade_date: date, session_count: int) -> date: ...

    def security_traded_period_start_date(
        self, symbol: str, trade_date: date, session_count: int
    ) -> date | None: ...

    def known_stock_symbols(self, trade_date: date) -> frozenset[str]: ...

    def known_trading_dates(self, start_date: date, end_date: date) -> frozenset[date]: ...

    def succeeded_dates(self, start_date: date, end_date: date) -> frozenset[date]: ...

    def begin_ingestion(self, run: IngestionRun) -> None: ...

    def attach_raw_manifest(self, run: IngestionRun, manifest: RawManifest) -> None: ...

    def publish_success(
        self,
        run: IngestionRun,
        quality: Sequence[QualityResult],
        records: Sequence[DragonTigerEventRecord],
    ) -> DragonTigerCollectionSummary: ...

    def complete_failure(self, run: IngestionRun, quality: Sequence[QualityResult]) -> None: ...


class DragonTigerRawStore(Protocol):
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


class DragonTigerService:
    def __init__(
        self,
        *,
        persistence: DragonTigerPersistence,
        raw_store: DragonTigerRawStore,
        provider: DragonTigerProvider,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._persistence = persistence
        self._raw_store = raw_store
        self._provider = provider
        self._clock = clock
        self._uuid_factory = uuid_factory

    def collect(self, trade_date: date) -> DragonTigerCollectionSummary:
        if not self._persistence.is_trading_day(trade_date):
            raise ValueError(f"{trade_date.isoformat()} is not a CN_A_SHARE trading day")
        started = self._aware_now()
        run = IngestionRun(
            ingestion_id=self._uuid_factory(),
            provider_code=ProviderCode(self._provider.source_code),
            dataset_code=DatasetCode.DRAGON_TIGER,
            status=IngestionStatus.RUNNING,
            requested_at=started,
            started_at=started,
            request_params={"trade_date": trade_date.isoformat()},
        )
        try:
            self._persistence.begin_ingestion(run)
        except Exception as error:
            raise DragonTigerCollectionError("DT_INGESTION_BEGIN_FAILED", "begin") from error

        stored: StoredRawObject | None = None
        quality: tuple[QualityResult, ...] = ()
        try:
            batch = self._fetch(trade_date)
            run = replace(run, request_params=dict(batch.request_params))
            stored = self._write_raw_phase(batch, trade_date, run.ingestion_id)
            try:
                manifest = self._manifest(run.ingestion_id, stored)
            except Exception as error:
                raise DragonTigerCollectionError("DT_MANIFEST_ATTACH_FAILED", "manifest") from error
            self._attach_manifest(run, manifest)
            normalization = self._normalize(batch)
            records = self._resolve_windows(normalization.events, trade_date)
            quality = self._source_quality(
                run.ingestion_id, normalization.findings
            ) + self._trigger_start_quality(run.ingestion_id, records)
            validation = self._validate(records, trade_date)
            if validation.findings:
                quality += self._domain_quality(run.ingestion_id, validation.findings)
                blocking_findings = tuple(
                    item
                    for item in validation.findings
                    if item.rule_code != "dragon_tiger.unknown_security"
                )
                if blocking_findings:
                    raise DragonTigerValidationError("DT_DOMAIN_VALIDATION_FAILED", "validation")
            rejected_source_ids = {
                str(item.natural_key["source_record_id"])
                for item in validation.findings
                if item.rule_code == "dragon_tiger.unknown_security"
            }
            domain_filtered_rows = sum(
                1 + len(record.seat_trades)
                for record in records
                if record.source_record_id in rejected_source_ids
            )
            filtered_rows = (
                sum(item.filtered_count for item in normalization.findings) + domain_filtered_rows
            )
            if filtered_rows > stored.row_count:
                raise DragonTigerValidationError("DT_SOURCE_FINDING_COUNT_INVALID", "validation")
            completed = replace(
                run,
                status=IngestionStatus.SUCCEEDED,
                finished_at=self._aware_now(),
                fetched_rows=stored.row_count,
                accepted_rows=stored.row_count - filtered_rows,
                rejected_rows=filtered_rows,
            )
            try:
                return self._persistence.publish_success(completed, quality, validation.accepted)
            except Exception as error:
                raise DragonTigerCollectionError("DT_FACT_PUBLISH_FAILED", "publish") from error
        except DragonTigerCollectionError as error:
            self._complete_failure(run, stored, error, quality)
            raise
        except Exception as error:
            wrapped = DragonTigerCollectionError("DT_UNEXPECTED_COLLECTION_FAILED", "collect")
            self._complete_failure(run, stored, wrapped, quality)
            raise wrapped from error

    def backfill(self, start_date: date, end_date: date) -> DragonTigerBackfillSummary:
        if start_date > end_date:
            raise ValueError("start_date must not follow end_date")
        if (end_date - start_date).days > 729:
            raise ValueError("DragonTiger backfill is bounded to 730 calendar days")
        succeeded = self._persistence.succeeded_dates(start_date, end_date)
        completed: list[date] = []
        non_trading: list[date] = []
        skipped_succeeded: list[date] = []
        failed: list[DragonTigerBackfillFailure] = []
        results: list[DragonTigerCollectionSummary] = []
        quality_codes: Counter[str] = Counter()
        current = start_date
        while current <= end_date:
            if not self._persistence.is_trading_day(current):
                non_trading.append(current)
            elif current in succeeded:
                skipped_succeeded.append(current)
            else:
                try:
                    result = self.collect(current)
                except DragonTigerCollectionError as error:
                    failed.append(DragonTigerBackfillFailure(current, error.code, error.phase))
                    quality_codes[error.code] += 1
                except Exception:
                    failed.append(
                        DragonTigerBackfillFailure(
                            current, "DT_UNEXPECTED_COLLECTION_FAILED", "collect"
                        )
                    )
                    quality_codes["DT_UNEXPECTED_COLLECTION_FAILED"] += 1
                else:
                    completed.append(current)
                    results.append(result)
            current += timedelta(days=1)
        return DragonTigerBackfillSummary(
            completed_dates=tuple(completed),
            skipped_non_trading_dates=tuple(non_trading),
            skipped_succeeded_dates=tuple(skipped_succeeded),
            failed_dates=tuple(failed),
            results=tuple(results),
            quality_code_counts=dict(sorted(quality_codes.items())),
        )

    def _fetch(self, trade_date: date) -> DragonTigerProviderBatch:
        try:
            return self._provider.fetch_dragon_tiger(trade_date)
        except Exception as error:
            raise DragonTigerCollectionError("DT_PROVIDER_FETCH_FAILED", "fetch") from error

    def _write_raw_phase(
        self, batch: DragonTigerProviderBatch, trade_date: date, ingestion_id: UUID
    ) -> StoredRawObject:
        try:
            return self._raw_store.write_jsonl(
                provider=self._provider.source_code,
                dataset=DatasetCode.DRAGON_TIGER.value,
                partition_date=trade_date,
                ingestion_id=ingestion_id,
                rows=tuple(batch.raw_rows),
                schema_version=batch.schema_version,
            )
        except Exception as error:
            raise DragonTigerCollectionError("DT_RAW_WRITE_FAILED", "raw") from error

    def _attach_manifest(self, run: IngestionRun, manifest: RawManifest) -> None:
        try:
            self._persistence.attach_raw_manifest(run, manifest)
        except Exception as error:
            raise DragonTigerCollectionError("DT_MANIFEST_ATTACH_FAILED", "manifest") from error

    @staticmethod
    def _normalize(batch: DragonTigerProviderBatch) -> DragonTigerNormalizationResult:
        try:
            return batch.normalization
        except Exception as error:
            raise DragonTigerCollectionError("DT_NORMALIZATION_FAILED", "normalize") from error

    def _resolve_windows(
        self, drafts: Sequence[DragonTigerEventDraft], requested_date: date
    ) -> tuple[DragonTigerEventRecord, ...]:
        if not drafts:
            raise DragonTigerCollectionError("DT_NO_EVENTS", "validation")
        resolved: list[DragonTigerEventRecord] = []
        try:
            for draft in drafts:
                if draft.source_code != self._provider.source_code:
                    raise ValueError("source lineage mismatch")
                if draft.trade_date != requested_date:
                    raise ValueError("date lineage mismatch")
                trigger_start = draft.trigger_window.start_date
                if trigger_start is None:
                    if draft.trigger_window.basis is DragonTigerWindowBasis.MARKET_SESSIONS:
                        trigger_start = self._persistence.period_start_date(
                            draft.trade_date, draft.trigger_window.session_count
                        )
                    else:
                        trigger_start = self._persistence.security_traded_period_start_date(
                            draft.symbol,
                            draft.trade_date,
                            draft.trigger_window.session_count,
                        )
                amount_start = draft.amount_period.start_date
                if (
                    amount_start is None
                    and draft.amount_period.basis
                    is not DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED
                ):
                    sessions = draft.amount_period.session_count
                    if sessions is None:
                        raise ValueError("verified amount period has no sessions")
                    if draft.amount_period.basis is DragonTigerAmountPeriodBasis.MARKET_SESSIONS:
                        amount_start = self._persistence.period_start_date(
                            draft.trade_date, sessions
                        )
                    else:
                        amount_start = self._persistence.security_traded_period_start_date(
                            draft.symbol, draft.trade_date, sessions
                        )
                resolved.append(draft.resolve_windows(trigger_start, amount_start))
        except Exception as error:
            raise DragonTigerCollectionError("DT_WINDOW_RESOLUTION_FAILED", "validation") from error
        return tuple(resolved)

    def _validate(
        self, records: Sequence[DragonTigerEventRecord], trade_date: date
    ) -> DragonTigerValidationResult:
        try:
            period_start = min(
                (
                    record.trigger_window.start_date
                    for record in records
                    if record.trigger_window.start_date is not None
                ),
                default=trade_date,
            )
            return validate_dragon_tiger_events(
                records,
                known_symbols=self._persistence.known_stock_symbols(trade_date),
                known_trading_dates=self._persistence.known_trading_dates(period_start, trade_date),
            )
        except DragonTigerCollectionError:
            raise
        except Exception as error:
            raise DragonTigerCollectionError("DT_VALIDATION_FAILED", "validation") from error

    def _complete_failure(
        self,
        run: IngestionRun,
        stored: StoredRawObject | None,
        error: DragonTigerCollectionError,
        quality: Sequence[QualityResult],
    ) -> None:
        fetched = stored.row_count if stored is not None else 0
        failed = replace(
            run,
            status=IngestionStatus.FAILED,
            finished_at=self._aware_now(),
            fetched_rows=fetched,
            accepted_rows=0,
            rejected_rows=fetched,
            error_summary=f"{error.code}:{error.phase}",
        )
        failure_quality = QualityResult(
            quality_result_id=self._uuid_factory(),
            ingestion_id=run.ingestion_id,
            dataset_code=DatasetCode.DRAGON_TIGER,
            rule_code=error.code,
            severity=QualitySeverity.ERROR,
            status=QualityStatus.FAILED,
            message="DragonTiger collection phase failed",
            natural_key={"trade_date": run.request_params["trade_date"]},
            details={"phase": error.phase},
        )
        self._persistence.complete_failure(failed, (*quality, failure_quality))

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

    def _source_quality(
        self, ingestion_id: UUID, findings: Sequence[DragonTigerSourceFinding]
    ) -> tuple[QualityResult, ...]:
        return tuple(
            QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=ingestion_id,
                dataset_code=DatasetCode.DRAGON_TIGER,
                rule_code=finding.rule_code,
                severity=finding.severity,
                status=QualityStatus.FAILED,
                message="DragonTiger source rows were normalized with a known limitation",
                natural_key={"source_event_id": finding.source_event_id},
                details={
                    "report_kind": finding.report_kind,
                    "occurrence_count": finding.occurrence_count,
                    "filtered_count": finding.filtered_count,
                },
            )
            for finding in findings
        )

    def _trigger_start_quality(
        self, ingestion_id: UUID, records: Sequence[DragonTigerEventRecord]
    ) -> tuple[QualityResult, ...]:
        return tuple(
            QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=ingestion_id,
                dataset_code=DatasetCode.DRAGON_TIGER,
                rule_code="DT_TRIGGER_START_UNAVAILABLE",
                severity=QualitySeverity.WARNING,
                status=QualityStatus.FAILED,
                message="Security-traded trigger start is unavailable from confirmed Daily Bars",
                natural_key={"source_event_id": record.source_record_id},
                details={
                    "trigger_window_basis": record.trigger_window.basis.value,
                    "trigger_window_sessions": record.trigger_window.session_count,
                },
            )
            for record in records
            if record.trigger_window.start_date is None
        )

    def _domain_quality(
        self, ingestion_id: UUID, findings: Sequence[DragonTigerFinding]
    ) -> tuple[QualityResult, ...]:
        return tuple(
            QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=ingestion_id,
                dataset_code=DatasetCode.DRAGON_TIGER,
                rule_code=finding.rule_code,
                severity=(
                    QualitySeverity.WARNING
                    if finding.rule_code == "dragon_tiger.unknown_security"
                    else QualitySeverity.ERROR
                ),
                status=QualityStatus.FAILED,
                message=finding.message,
                natural_key=finding.natural_key,
            )
            for finding in findings
        )

    def _aware_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("DragonTiger clock must be timezone-aware")
        return value.astimezone(UTC)
