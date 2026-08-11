"""Dependency policy and end-to-end same-day limit-up snapshot fill."""

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from types import TracebackType
from typing import Protocol, Self
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
from market_data_center.domain.today_limit_up import (
    LimitUpSourceRecord,
    TodayLimitUpDependencies,
    TodayLimitUpSnapshotStatus,
    UpstreamState,
)
from market_data_center.persistence.today_limit_up_postgres import TodayLimitUpFillSummary
from market_data_center.providers.contracts import CurrentDayLimitUpPoolProvider
from market_data_center.raw_store import LocalRawStore, StoredRawObject

if False:  # pragma: no cover - typing only
    from sqlalchemy import Engine


@dataclass(frozen=True, slots=True)
class FillDecision:
    status: TodayLimitUpSnapshotStatus
    reasons: tuple[str, ...]
    may_collect_source: bool


def decide_fill(dependencies: TodayLimitUpDependencies) -> FillDecision:
    """Fail closed before provider I/O; partial inputs remain visible and non-ready."""
    reasons: list[str] = []
    if not dependencies.is_trading_day:
        reasons.append("not_trading_day")
    if dependencies.daily_market is UpstreamState.MISSING:
        reasons.append("missing_daily_market")
    elif dependencies.daily_market is UpstreamState.FAILED:
        reasons.append("failed_daily_market")
    if dependencies.stock_daily_indicator is UpstreamState.MISSING:
        reasons.append("missing_stock_daily_indicator")
    elif dependencies.stock_daily_indicator is UpstreamState.FAILED:
        reasons.append("failed_stock_daily_indicator")
    if not dependencies.exact_ready_limit_up_pool:
        reasons.append("missing_exact_ready_limit_up_pool")
    if reasons:
        return FillDecision(TodayLimitUpSnapshotStatus.DEFERRED, tuple(reasons), False)
    partial_reasons = []
    if dependencies.daily_market is UpstreamState.PARTIAL:
        partial_reasons.append("partial_daily_market")
    if dependencies.stock_daily_indicator is UpstreamState.PARTIAL:
        partial_reasons.append("partial_stock_daily_indicator")
    return FillDecision(
        TodayLimitUpSnapshotStatus.PARTIAL if partial_reasons else TodayLimitUpSnapshotStatus.READY,
        tuple(partial_reasons),
        True,
    )


class TodayLimitUpPersistence(Protocol):
    def dependencies(self, trade_date: date) -> TodayLimitUpDependencies: ...

    def create_ingestion_run(self, run: IngestionRun) -> None: ...

    def fail_ingestion_run(self, run: IngestionRun) -> None: ...

    def commit_deferred(
        self, trade_date: date, reasons: tuple[str, ...]
    ) -> TodayLimitUpFillSummary: ...

    def commit_snapshot(
        self,
        *,
        trade_date: date,
        requested_status: TodayLimitUpSnapshotStatus,
        run: IngestionRun,
        manifest: RawManifest,
        source_records: tuple[LimitUpSourceRecord, ...],
        ingestion_quality: tuple[QualityResult, ...],
    ) -> TodayLimitUpFillSummary: ...

    def commit_failed_source(
        self, run: IngestionRun, manifest: RawManifest, quality: tuple[QualityResult, ...]
    ) -> None: ...


class ManagedLimitUpProvider(CurrentDayLimitUpPoolProvider, Protocol):
    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class TodayLimitUpFillService:
    def __init__(
        self,
        *,
        persistence: TodayLimitUpPersistence,
        raw_store: LocalRawStore,
        provider_factory: Callable[[], ManagedLimitUpProvider],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._persistence = persistence
        self._raw_store = raw_store
        self._provider_factory = provider_factory
        self._clock = clock

    def fill(self, trade_date: date) -> TodayLimitUpFillSummary:
        decision = decide_fill(self._persistence.dependencies(trade_date))
        if not decision.may_collect_source:
            return self._persistence.commit_deferred(trade_date, decision.reasons)
        now = self._aware_now()
        run = IngestionRun(
            uuid4(),
            ProviderCode.AKSHARE,
            DatasetCode.TODAY_LIMIT_UP_SOURCE,
            IngestionStatus.RUNNING,
            now,
            now,
            request_params={"trade_date": trade_date.isoformat()},
        )
        self._persistence.create_ingestion_run(run)
        try:
            with self._provider_factory() as provider:
                batch = provider.fetch_limit_up_pool(trade_date)
            stored = self._raw_store.write_jsonl(
                provider=ProviderCode.AKSHARE.value,
                dataset=DatasetCode.TODAY_LIMIT_UP_SOURCE.value,
                partition_date=trade_date,
                ingestion_id=run.ingestion_id,
                rows=batch.raw_rows,
                schema_version=batch.schema_version,
            )
        except Exception as error:
            self._persistence.fail_ingestion_run(
                _finish_run(
                    run,
                    self._aware_now(),
                    0,
                    0,
                    0,
                    IngestionStatus.FAILED,
                    type(error).__name__,
                )
            )
            raise
        manifest = _manifest(run.ingestion_id, stored)
        try:
            normalized = tuple(batch.records)
        except Exception as error:
            failed = _finish_run(
                run,
                self._aware_now(),
                stored.row_count,
                0,
                stored.row_count,
                IngestionStatus.FAILED,
                type(error).__name__,
            )
            failed_quality = (
                _quality(
                    run.ingestion_id,
                    "normalization_error",
                    QualitySeverity.ERROR,
                    "provider response normalization failed",
                ),
            )
            self._persistence.commit_failed_source(failed, manifest, failed_quality)
            raise
        counts = Counter(record.symbol for record in normalized)
        duplicates = {symbol for symbol, count in counts.items() if count > 1}
        records = tuple(record for record in normalized if record.symbol not in duplicates)
        quality: tuple[QualityResult, ...] = tuple(
            _quality(
                run.ingestion_id,
                "duplicate_symbol",
                QualitySeverity.ERROR,
                "duplicate provider symbol was omitted",
                symbol,
            )
            for symbol in sorted(duplicates)
        )
        status = IngestionStatus.PARTIAL if duplicates else IngestionStatus.SUCCEEDED
        completed = _finish_run(
            run,
            self._aware_now(),
            stored.row_count,
            len(records),
            len(normalized) - len(records),
            status,
            "duplicate source symbols" if duplicates else None,
        )
        requested = (
            TodayLimitUpSnapshotStatus.PARTIAL
            if duplicates or decision.status is TodayLimitUpSnapshotStatus.PARTIAL
            else TodayLimitUpSnapshotStatus.READY
        )
        return self._persistence.commit_snapshot(
            trade_date=trade_date,
            requested_status=requested,
            run=completed,
            manifest=manifest,
            source_records=records,
            ingestion_quality=quality,
        )

    def _aware_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("today-limit-up clock must be timezone-aware")
        return value.astimezone(UTC)


def _manifest(ingestion_id: UUID, stored: StoredRawObject) -> RawManifest:
    return RawManifest(
        uuid4(),
        ingestion_id,
        stored.object_path,
        RawFileFormat.JSONL,
        stored.content_sha256,
        stored.byte_size,
        stored.row_count,
        stored.schema_version,
    )


def _quality(
    ingestion_id: UUID,
    suffix: str,
    severity: QualitySeverity,
    message: str,
    symbol: str | None = None,
) -> QualityResult:
    return QualityResult(
        uuid4(),
        ingestion_id,
        DatasetCode.TODAY_LIMIT_UP_SOURCE,
        f"today_limit_up_source.{suffix}",
        severity,
        QualityStatus.FAILED,
        message,
        {"symbol": symbol} if symbol else None,
    )


def _finish_run(
    run: IngestionRun,
    finished_at: datetime,
    fetched: int,
    accepted: int,
    rejected: int,
    status: IngestionStatus,
    error: str | None,
) -> IngestionRun:
    return IngestionRun(
        run.ingestion_id,
        run.provider_code,
        run.dataset_code,
        status,
        run.requested_at,
        run.started_at,
        finished_at,
        run.request_params,
        fetched,
        accepted,
        rejected,
        error,
    )


def fill_today_limit_up_snapshot(
    engine: "Engine", raw_store: LocalRawStore, trade_date: date
) -> TodayLimitUpFillSummary:
    """Composition root shared by the explicit CLI and the controlled Worker job."""
    from market_data_center.persistence.today_limit_up_postgres import (
        PostgreSQLTodayLimitUpPersistence,
    )
    from market_data_center.providers.akshare_limit_up import (
        AkshareCurrentDayLimitUpProvider,
        BoundedAkshareLimitUpClient,
    )
    from market_data_center.settings import TodayLimitUpProviderSettings

    settings = TodayLimitUpProviderSettings()

    return TodayLimitUpFillService(
        persistence=PostgreSQLTodayLimitUpPersistence(engine),
        raw_store=raw_store,
        provider_factory=lambda: AkshareCurrentDayLimitUpProvider(
            BoundedAkshareLimitUpClient(
                timeout_seconds=settings.today_limit_up_timeout_seconds,
                max_attempts=settings.today_limit_up_max_attempts,
            )
        ),
    ).fill(trade_date)
