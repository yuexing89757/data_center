"""Explicit bounded Tencent quote collection with Raw and ingestion lineage."""

from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime
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
from market_data_center.domain.realtime_quote import (
    RealtimeQuoteFinding,
    validate_realtime_quotes,
)
from market_data_center.persistence.realtime_quote_postgres import (
    PostgreSQLRealtimeQuotePersistence,
)
from market_data_center.providers.contracts import RealtimeQuoteFetch, RealtimeQuoteProvider
from market_data_center.providers.tencent_quote import TencentQuoteProvider
from market_data_center.raw_store import LocalRawStore

SHANGHAI = ZoneInfo("Asia/Shanghai")


def collect_tencent_realtime_quotes(
    persistence: PostgreSQLRealtimeQuotePersistence,
    raw_store: LocalRawStore,
    symbols: Sequence[str],
    *,
    provider: RealtimeQuoteProvider | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    uuid_factory: Callable[[], UUID] = uuid4,
) -> IngestionRun:
    requested = tuple(symbols)
    if not 1 <= len(requested) <= 500 or len(set(requested)) != len(requested):
        raise ValueError("realtime quote collection requires 1 to 500 unique symbols")
    started = clock().astimezone(UTC)
    run = IngestionRun(
        ingestion_id=uuid_factory(),
        provider_code=ProviderCode.TENCENT_QUOTE,
        dataset_code=DatasetCode.FIVE_LEVEL_QUOTE,
        status=IngestionStatus.RUNNING,
        requested_at=started,
        started_at=started,
        request_params={"symbols": list(requested), "symbol_count": len(requested)},
    )
    persistence.create_run(run)
    try:
        known = persistence.known_stock_symbols(requested)
        if known != set(requested):
            unknown = sorted(set(requested) - known)
            raise ValueError(f"unknown or non-stock symbols: {','.join(unknown)}")
        fetched = (provider or TencentQuoteProvider()).fetch_five_level_quotes(requested)
        stored = raw_store.write_jsonl(
            provider=ProviderCode.TENCENT_QUOTE.value,
            dataset=DatasetCode.FIVE_LEVEL_QUOTE.value,
            partition_date=started.astimezone(SHANGHAI).date(),
            ingestion_id=run.ingestion_id,
            rows=fetched.raw_rows,
            schema_version=fetched.schema_version,
        )
        validation = validate_realtime_quotes(
            fetched.records,
            known_symbols=known,
            known_stock_symbols=known,
            now=clock().astimezone(UTC),
        )
        accepted = validation.accepted
        accepted_symbols = {record.symbol for record in accepted}
        missing = tuple(symbol for symbol in requested if symbol not in accepted_symbols)
        quality = _quality(run.ingestion_id, missing, fetched, validation.findings, uuid_factory)
        status = (
            IngestionStatus.SUCCEEDED
            if not missing
            else IngestionStatus.PARTIAL
            if accepted
            else IngestionStatus.FAILED
        )
        terminal = replace(
            run,
            status=status,
            finished_at=clock().astimezone(UTC),
            fetched_rows=len(requested),
            accepted_rows=len(accepted),
            rejected_rows=len(missing),
            error_summary=None if not missing else "Tencent quote batch incomplete",
        )
        manifest = RawManifest(
            raw_id=uuid_factory(),
            ingestion_id=run.ingestion_id,
            object_path=stored.object_path,
            file_format=RawFileFormat.JSONL,
            content_sha256=stored.content_sha256,
            byte_size=stored.byte_size,
            row_count=stored.row_count,
            schema_version=stored.schema_version,
        )
        persistence.commit(terminal, manifest, quality, accepted)
        return terminal
    except Exception as error:
        failed = replace(
            run,
            status=IngestionStatus.FAILED,
            finished_at=clock().astimezone(UTC),
            error_summary=f"{type(error).__name__}: realtime quote collection failed",
        )
        persistence.fail_run(failed)
        raise


def _quality(
    ingestion_id: UUID,
    missing: Sequence[str],
    fetched: RealtimeQuoteFetch,
    findings: Sequence[RealtimeQuoteFinding],
    uuid_factory: Callable[[], UUID],
) -> tuple[QualityResult, ...]:
    result = [
        QualityResult(
            uuid_factory(),
            ingestion_id,
            DatasetCode.FIVE_LEVEL_QUOTE,
            "realtime_quote.missing_symbol",
            QualitySeverity.ERROR,
            QualityStatus.FAILED,
            "Tencent did not produce an accepted quote for a requested symbol",
            {"symbol": symbol},
        )
        for symbol in missing
    ]
    for error in fetched.normalization_errors:
        result.append(
            QualityResult(
                uuid_factory(),
                ingestion_id,
                DatasetCode.FIVE_LEVEL_QUOTE,
                f"realtime_quote.{error.reason}",
                QualitySeverity.ERROR,
                QualityStatus.FAILED,
                "Tencent quote row normalization failed",
                {"symbol": error.symbol},
                {"raw_row_index": error.raw_row_index},
            )
        )
    for item in findings:
        result.append(
            QualityResult(
                uuid_factory(),
                ingestion_id,
                DatasetCode.FIVE_LEVEL_QUOTE,
                item.rule_code,
                item.severity,
                QualityStatus.FAILED,
                item.message,
                item.natural_key,
            )
        )
    return tuple(result)
