"""Atomic PostgreSQL persistence for immutable official regulation events."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from json import dumps
from typing import TYPE_CHECKING

from sqlalchemy import Connection, Engine, bindparam, text

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    RawManifest,
)
from market_data_center.domain.regulation import RegulationEventRecord

if TYPE_CHECKING:
    from market_data_center.regulation_event_service import RegulationEventCollectionSummary


class RegulationEventContentConflict(RuntimeError):
    """An official event identifier was observed with changed immutable content."""


class RegulationEventSemanticConflict(RuntimeError):
    """A second identifier asserted the same official event identity."""


class PostgreSQLRegulationEventPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def begin_ingestion(self, run: IngestionRun) -> None:
        _require_run(run, IngestionStatus.RUNNING)
        with self._engine.begin() as connection:
            _insert_run(connection, run)

    def attach_raw_manifest(self, run: IngestionRun, manifest: RawManifest) -> None:
        _require_run(run, IngestionStatus.RUNNING)
        if manifest.ingestion_id != run.ingestion_id:
            raise ValueError("regulation-event manifest does not match its run")
        with self._engine.begin() as connection:
            _update_running_run(connection, run, manifest.row_count)
            _insert_manifest(connection, manifest)

    def known_stock_symbols(self, symbols: frozenset[str]) -> frozenset[str]:
        if not symbols:
            return frozenset()
        statement = text("""
            select symbol from core.security
            where security_type='stock' and symbol in :symbols
        """).bindparams(bindparam("symbols", expanding=True))
        with self._engine.connect() as connection:
            values = connection.scalars(statement, {"symbols": sorted(symbols)}).all()
        return frozenset(values)

    def publish_success(
        self,
        run: IngestionRun,
        quality: Sequence[QualityResult],
        records: Sequence[RegulationEventRecord],
    ) -> RegulationEventCollectionSummary:
        return self._commit(run, quality, records)

    def publish_replay(
        self,
        run: IngestionRun,
        quality: Sequence[QualityResult],
        records: Sequence[RegulationEventRecord],
    ) -> RegulationEventCollectionSummary:
        return self._commit(run, quality, records)

    def complete_failure(self, run: IngestionRun, quality: Sequence[QualityResult]) -> None:
        _require_run(run, IngestionStatus.FAILED)
        with self._engine.begin() as connection:
            _update_run(connection, run)
            _insert_quality(connection, quality)

    def _commit(
        self,
        run: IngestionRun,
        quality: Sequence[QualityResult],
        records: Sequence[RegulationEventRecord],
    ) -> RegulationEventCollectionSummary:
        _require_run(run, IngestionStatus.SUCCEEDED)
        unchanged = 0
        with self._engine.begin() as connection:
            _insert_quality(connection, quality)
            for record in records:
                connection.execute(
                    text("select pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                    {"key": f"{record.source_code}:{record.source_event_id}"},
                )
                current = connection.execute(
                    text("""
                        select source_content_hash from regulation.event
                        where source_code=:source_code and source_event_id=:source_event_id
                        for update
                    """),
                    {
                        "source_code": record.source_code,
                        "source_event_id": record.source_event_id,
                    },
                ).one_or_none()
                if current is not None:
                    if current.source_content_hash != record.source_content_hash:
                        raise RegulationEventContentConflict
                    unchanged += 1
                    continue
                semantic = connection.execute(
                    text("""
                        select source_event_id from regulation.event
                        where source_code=:source_code and symbol=:symbol
                          and period_start_date=:period_start_date
                          and period_end_date=:period_end_date
                          and event_level=:event_level
                          and direction is not distinct from :direction
                        for update
                    """),
                    _event_params(record, run),
                ).one_or_none()
                if semantic is not None:
                    raise RegulationEventSemanticConflict
                connection.execute(
                    text("""
                        insert into regulation.event (
                            symbol, exchange, segment, event_type, event_level, direction,
                            period_start_date, period_end_date, published_at,
                            effective_reset_date, source_event_id, source_title, source_url,
                            source_content_hash, source_code, explicit_rule_codes,
                            observed_at, ingestion_id
                        ) values (
                            :symbol, :exchange, :segment, :event_type, :event_level, :direction,
                            :period_start_date, :period_end_date, :published_at,
                            :effective_reset_date, :source_event_id, :source_title, :source_url,
                            :source_content_hash, :source_code, :explicit_rule_codes,
                            :observed_at, :ingestion_id
                        )
                    """),
                    _event_params(record, run),
                )
            _update_run(connection, run)
        from market_data_center.regulation_event_service import RegulationEventCollectionSummary

        return RegulationEventCollectionSummary(
            provider_code=run.provider_code.value,
            ingestion_id=run.ingestion_id,
            status=run.status,
            observed_from=_request_datetime(run, "observed_from"),
            observed_to=_request_datetime(run, "observed_to"),
            fetched_rows=run.fetched_rows,
            accepted_events=len(records),
            unchanged_events=unchanged,
        )


def _event_params(record: RegulationEventRecord, run: IngestionRun) -> dict[str, object]:
    return {
        "symbol": record.symbol,
        "exchange": record.exchange.value,
        "segment": record.segment.value,
        "event_type": record.event_type.value,
        "event_level": record.event_level.value,
        "direction": record.direction.value if record.direction is not None else None,
        "period_start_date": record.period_start_date,
        "period_end_date": record.period_end_date,
        "published_at": record.published_at,
        "effective_reset_date": record.effective_reset_date,
        "source_event_id": record.source_event_id,
        "source_title": record.source_title,
        "source_url": record.source_url,
        "source_content_hash": record.source_content_hash,
        "source_code": record.source_code,
        "explicit_rule_codes": list(record.explicit_rule_codes),
        "observed_at": record.observed_at,
        "ingestion_id": run.ingestion_id,
    }


def _insert_run(connection: Connection, run: IngestionRun) -> None:
    connection.execute(
        text("""
            insert into ingestion.ingestion_run (
                ingestion_id, provider_code, dataset_code, status, requested_at,
                started_at, finished_at, request_params, fetched_rows, accepted_rows,
                rejected_rows, error_summary, replayed_from_raw_id
            ) values (
                :ingestion_id, :provider_code, :dataset_code, :status, :requested_at,
                :started_at, :finished_at, cast(:request_params as jsonb), :fetched_rows,
                :accepted_rows, :rejected_rows, :error_summary, :replayed_from_raw_id
            )
        """),
        _run_params(run),
    )


def _update_run(connection: Connection, run: IngestionRun) -> None:
    result = connection.execute(
        text("""
            update ingestion.ingestion_run set status=:status, finished_at=:finished_at,
                fetched_rows=:fetched_rows, accepted_rows=:accepted_rows,
                rejected_rows=:rejected_rows, error_summary=:error_summary
            where ingestion_id=:ingestion_id and status='running'
        """),
        _run_params(run),
    )
    if result.rowcount != 1:
        raise RuntimeError("REG_EVENT_INGESTION_STATE_CONFLICT")


def _update_running_run(connection: Connection, run: IngestionRun, fetched_rows: int) -> None:
    result = connection.execute(
        text("""
            update ingestion.ingestion_run set
                request_params=cast(:request_params as jsonb), fetched_rows=:fetched_rows
            where ingestion_id=:ingestion_id and status='running'
        """),
        {
            "ingestion_id": run.ingestion_id,
            "request_params": _json(run.request_params),
            "fetched_rows": fetched_rows,
        },
    )
    if result.rowcount != 1:
        raise RuntimeError("REG_EVENT_INGESTION_STATE_CONFLICT")


def _insert_manifest(connection: Connection, manifest: RawManifest) -> None:
    connection.execute(
        text("""
            insert into ingestion.raw_manifest (
                raw_id, ingestion_id, storage_backend, object_path, file_format,
                content_sha256, byte_size, row_count, schema_version
            ) values (
                :raw_id, :ingestion_id, :storage_backend, :object_path, :file_format,
                :content_sha256, :byte_size, :row_count, :schema_version
            )
        """),
        {
            "raw_id": manifest.raw_id,
            "ingestion_id": manifest.ingestion_id,
            "storage_backend": manifest.storage_backend,
            "object_path": manifest.object_path,
            "file_format": manifest.file_format.value,
            "content_sha256": manifest.content_sha256,
            "byte_size": manifest.byte_size,
            "row_count": manifest.row_count,
            "schema_version": manifest.schema_version,
        },
    )


def _insert_quality(connection: Connection, quality: Sequence[QualityResult]) -> None:
    if not quality:
        return
    connection.execute(
        text("""
            insert into audit.quality_result (
                quality_result_id, ingestion_id, dataset_code, rule_code,
                severity, status, message, natural_key, details
            ) values (
                :quality_result_id, :ingestion_id, :dataset_code, :rule_code,
                :severity, :status, :message,
                cast(:natural_key as jsonb), cast(:details as jsonb)
            )
        """),
        [
            {
                "quality_result_id": item.quality_result_id,
                "ingestion_id": item.ingestion_id,
                "dataset_code": item.dataset_code.value,
                "rule_code": item.rule_code,
                "severity": item.severity.value,
                "status": item.status.value,
                "message": item.message,
                "natural_key": _json(item.natural_key),
                "details": _json(item.details),
            }
            for item in quality
        ],
    )


def _run_params(run: IngestionRun) -> dict[str, object]:
    return {
        "ingestion_id": run.ingestion_id,
        "provider_code": run.provider_code.value,
        "dataset_code": run.dataset_code.value,
        "status": run.status.value,
        "requested_at": run.requested_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "request_params": _json(run.request_params),
        "fetched_rows": run.fetched_rows,
        "accepted_rows": run.accepted_rows,
        "rejected_rows": run.rejected_rows,
        "error_summary": run.error_summary,
        "replayed_from_raw_id": run.replayed_from_raw_id,
    }


def _request_datetime(run: IngestionRun, key: str) -> datetime:
    value = run.request_params.get(key)
    if not isinstance(value, str):
        raise ValueError(f"regulation-event request {key} is missing")
    return datetime.fromisoformat(value)


def _require_run(run: IngestionRun, status: IngestionStatus) -> None:
    if run.provider_code not in {ProviderCode.SSE_OFFICIAL, ProviderCode.SZSE_OFFICIAL}:
        raise ValueError("regulation-event run provider is invalid")
    if run.dataset_code is not DatasetCode.REGULATION_EVENT:
        raise ValueError("regulation-event run dataset is invalid")
    if run.status is not status:
        raise ValueError(f"regulation-event run must be {status.value}")


def _json(value: object) -> str:
    return dumps(value, default=str, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
