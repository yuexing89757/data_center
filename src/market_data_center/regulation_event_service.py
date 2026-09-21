"""Staged collection of traceable official regulation events."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

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
from market_data_center.domain.regulation import RegulationEventRecord
from market_data_center.persistence.regulation_event_postgres import (
    RegulationEventContentConflict,
    RegulationEventSemanticConflict,
)
from market_data_center.providers.contracts import ProviderBatch, RegulationEventProvider
from market_data_center.raw_store import StoredRawObject

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class RegulationEventCollectionError(RuntimeError):
    def __init__(self, code: str, phase: str, quality: tuple[QualityResult, ...] = ()) -> None:
        self.code = code
        self.phase = phase
        self.quality = quality
        super().__init__(f"{code}:{phase}")


@dataclass(frozen=True, slots=True)
class RegulationEventCollectionSummary:
    provider_code: str
    ingestion_id: UUID
    status: IngestionStatus
    observed_from: datetime
    observed_to: datetime
    fetched_rows: int
    accepted_events: int
    unchanged_events: int


class RegulationEventPersistence(Protocol):
    def begin_ingestion(self, run: IngestionRun) -> None: ...

    def attach_raw_manifest(self, run: IngestionRun, manifest: RawManifest) -> None: ...

    def known_stock_symbols(self, symbols: frozenset[str]) -> frozenset[str]: ...

    def publish_success(
        self,
        run: IngestionRun,
        quality: Sequence[QualityResult],
        records: Sequence[RegulationEventRecord],
    ) -> RegulationEventCollectionSummary: ...

    def complete_failure(self, run: IngestionRun, quality: Sequence[QualityResult]) -> None: ...


class RegulationEventRawStore(Protocol):
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


class RegulationEventCollectionService:
    def __init__(
        self,
        *,
        persistence: RegulationEventPersistence,
        raw_store: RegulationEventRawStore,
        provider: RegulationEventProvider,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._persistence = persistence
        self._raw_store = raw_store
        self._provider = provider
        self._clock = clock
        self._uuid_factory = uuid_factory

    def collect(
        self, observed_from: datetime, observed_to: datetime
    ) -> RegulationEventCollectionSummary:
        _validate_utc_bounds(observed_from, observed_to)
        started = self._aware_utc_now()
        run = IngestionRun(
            ingestion_id=self._uuid_factory(),
            provider_code=ProviderCode(self._provider.source_code),
            dataset_code=DatasetCode.REGULATION_EVENT,
            status=IngestionStatus.RUNNING,
            requested_at=started,
            started_at=started,
            request_params={
                "observed_from": observed_from.isoformat(),
                "observed_to": observed_to.isoformat(),
            },
        )
        try:
            self._persistence.begin_ingestion(run)
        except Exception as error:
            raise RegulationEventCollectionError(
                "REG_EVENT_INGESTION_BEGIN_FAILED", "begin"
            ) from error

        stored: StoredRawObject | None = None
        quality: tuple[QualityResult, ...] = ()
        try:
            batch = self._fetch(observed_from, observed_to)
            request_params = dict(batch.request_params)
            request_params["observed_from"] = observed_from.isoformat()
            request_params["observed_to"] = observed_to.isoformat()
            run = replace(run, request_params=request_params)
            stored = self._write_raw(batch, observed_from, run.ingestion_id)
            manifest = self._manifest(run.ingestion_id, stored)
            self._attach_manifest(run, manifest)
            records = self._normalize(batch)
            quality = self._validate(records, observed_from, observed_to, run.ingestion_id)
            completed = replace(
                run,
                status=IngestionStatus.SUCCEEDED,
                finished_at=self._aware_utc_now(),
                fetched_rows=stored.row_count,
                accepted_rows=len(records),
                rejected_rows=stored.row_count - len(records),
            )
            try:
                return self._persistence.publish_success(completed, quality, records)
            except RegulationEventContentConflict as error:
                raise RegulationEventCollectionError(
                    "REG_EVENT_CONTENT_CONFLICT", "publish"
                ) from error
            except RegulationEventSemanticConflict as error:
                raise RegulationEventCollectionError(
                    "REG_EVENT_SEMANTIC_CONFLICT", "publish"
                ) from error
            except Exception as error:
                raise RegulationEventCollectionError(
                    "REG_EVENT_FACT_PUBLISH_FAILED", "publish"
                ) from error
        except RegulationEventCollectionError as error:
            self._complete_failure(run, stored, error, quality)
            raise
        except Exception as error:
            wrapped = RegulationEventCollectionError(
                "REG_EVENT_UNEXPECTED_COLLECTION_FAILED", "collect"
            )
            self._complete_failure(run, stored, wrapped, quality)
            raise wrapped from error

    def _fetch(
        self, observed_from: datetime, observed_to: datetime
    ) -> ProviderBatch[RegulationEventRecord]:
        try:
            return self._provider.fetch_events(observed_from, observed_to)
        except Exception as error:
            raise RegulationEventCollectionError(
                "REG_EVENT_PROVIDER_FETCH_FAILED", "fetch"
            ) from error

    def _write_raw(
        self,
        batch: ProviderBatch[RegulationEventRecord],
        observed_from: datetime,
        ingestion_id: UUID,
    ) -> StoredRawObject:
        try:
            return self._raw_store.write_jsonl(
                provider=self._provider.source_code,
                dataset=DatasetCode.REGULATION_EVENT.value,
                partition_date=observed_from.astimezone(_SHANGHAI).date(),
                ingestion_id=ingestion_id,
                rows=tuple(batch.raw_rows),
                schema_version=batch.schema_version,
            )
        except Exception as error:
            raise RegulationEventCollectionError("REG_EVENT_RAW_WRITE_FAILED", "raw") from error

    def _attach_manifest(self, run: IngestionRun, manifest: RawManifest) -> None:
        try:
            self._persistence.attach_raw_manifest(run, manifest)
        except Exception as error:
            raise RegulationEventCollectionError(
                "REG_EVENT_MANIFEST_ATTACH_FAILED", "manifest"
            ) from error

    @staticmethod
    def _normalize(
        batch: ProviderBatch[RegulationEventRecord],
    ) -> tuple[RegulationEventRecord, ...]:
        try:
            return tuple(batch.records)
        except Exception as error:
            raise RegulationEventCollectionError(
                "REG_EVENT_NORMALIZATION_FAILED", "normalize"
            ) from error

    def _validate(
        self,
        records: tuple[RegulationEventRecord, ...],
        observed_from: datetime,
        observed_to: datetime,
        ingestion_id: UUID,
    ) -> tuple[QualityResult, ...]:
        local_start = observed_from.astimezone(_SHANGHAI).date()
        local_end = (observed_to - timedelta(microseconds=1)).astimezone(_SHANGHAI).date()
        if any(
            record.source_code != self._provider.source_code
            or not local_start <= record.period_end_date <= local_end
            or not observed_from <= record.observed_at.astimezone(UTC) < observed_to
            for record in records
        ):
            raise RegulationEventCollectionError("REG_EVENT_LINEAGE_MISMATCH", "validation")
        symbols = frozenset(record.symbol for record in records)
        known = self._persistence.known_stock_symbols(symbols)
        unknown = tuple(sorted(symbols - known))
        if not unknown:
            return ()
        quality = tuple(
            QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=ingestion_id,
                dataset_code=DatasetCode.REGULATION_EVENT,
                rule_code="REG_EVENT_UNKNOWN_SECURITY",
                severity=QualitySeverity.ERROR,
                status=QualityStatus.FAILED,
                message="Official regulation event references an unknown stock security",
                natural_key={"symbol": symbol},
            )
            for symbol in unknown
        )
        raise RegulationEventCollectionError("REG_EVENT_UNKNOWN_SECURITY", "validation", quality)

    def _complete_failure(
        self,
        run: IngestionRun,
        stored: StoredRawObject | None,
        error: RegulationEventCollectionError,
        quality: Sequence[QualityResult],
    ) -> None:
        failure_quality = QualityResult(
            quality_result_id=self._uuid_factory(),
            ingestion_id=run.ingestion_id,
            dataset_code=DatasetCode.REGULATION_EVENT,
            rule_code=error.code,
            severity=QualitySeverity.ERROR,
            status=QualityStatus.FAILED,
            message="Official regulation-event collection phase failed",
            details={"phase": error.phase},
        )
        fetched = stored.row_count if stored is not None else 0
        failed = replace(
            run,
            status=IngestionStatus.FAILED,
            finished_at=self._aware_utc_now(),
            fetched_rows=fetched,
            accepted_rows=0,
            rejected_rows=fetched,
            error_summary=f"{error.code}:{error.phase}",
        )
        self._persistence.complete_failure(failed, (*quality, *error.quality, failure_quality))

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

    def _aware_utc_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("regulation-event clock must be timezone-aware")
        return value.astimezone(UTC)


def _validate_utc_bounds(observed_from: datetime, observed_to: datetime) -> None:
    bounds = (observed_from, observed_to)
    if any(value.tzinfo is None or value.utcoffset() != timedelta(0) for value in bounds):
        raise ValueError("observation bounds must be aware UTC datetimes")
    if observed_to <= observed_from:
        raise ValueError("observation start must precede end")
