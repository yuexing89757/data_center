"""Dependency policy and end-to-end same-day limit-down snapshot fill."""

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
from market_data_center.domain.today_limit_down import (
    LimitDownSourceRecord,
    TodayLimitDownDependencies,
    TodayLimitDownSnapshotStatus,
    UpstreamState,
)
from market_data_center.persistence.today_limit_down_postgres import TodayLimitDownFillSummary
from market_data_center.providers.contracts import CurrentDayLimitDownPoolProvider
from market_data_center.raw_store import LocalRawStore, StoredRawObject

if False:  # pragma: no cover - typing only
    from sqlalchemy import Engine


@dataclass(frozen=True, slots=True)
class FillDecision:
    status: TodayLimitDownSnapshotStatus
    reasons: tuple[str, ...]
    may_collect_source: bool


def decide_fill(dependencies: TodayLimitDownDependencies) -> FillDecision:
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
    if not dependencies.exact_ready_limit_down_pool:
        reasons.append("missing_exact_ready_limit_down_pool")
    if reasons:
        return FillDecision(TodayLimitDownSnapshotStatus.DEFERRED, tuple(reasons), False)
    partial_reasons = []
    if dependencies.daily_market is UpstreamState.PARTIAL:
        partial_reasons.append("partial_daily_market")
    if dependencies.stock_daily_indicator is UpstreamState.PARTIAL:
        partial_reasons.append("partial_stock_daily_indicator")
    return FillDecision(
        TodayLimitDownSnapshotStatus.PARTIAL
        if partial_reasons
        else TodayLimitDownSnapshotStatus.READY,
        tuple(partial_reasons),
        True,
    )


class TodayLimitDownPersistence(Protocol):
    def dependencies(self, trade_date: date) -> TodayLimitDownDependencies: ...

    def create_ingestion_run(self, run: IngestionRun) -> None: ...

    def commit_deferred(
        self, trade_date: date, reasons: tuple[str, ...]
    ) -> TodayLimitDownFillSummary: ...

    def commit_failed(
        self,
        trade_date: date,
        run: IngestionRun,
        reason: str,
        manifest: RawManifest | None = None,
        quality: tuple[QualityResult, ...] = (),
    ) -> TodayLimitDownFillSummary: ...

    def commit_snapshot(
        self,
        *,
        trade_date: date,
        requested_status: TodayLimitDownSnapshotStatus,
        run: IngestionRun,
        manifest: RawManifest,
        source_records: tuple[LimitDownSourceRecord, ...],
        ingestion_quality: tuple[QualityResult, ...],
    ) -> TodayLimitDownFillSummary: ...


class ManagedLimitDownProvider(CurrentDayLimitDownPoolProvider, Protocol):
    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class TodayLimitDownFillService:
    def __init__(
        self,
        *,
        persistence: TodayLimitDownPersistence,
        raw_store: LocalRawStore,
        provider_factory: Callable[[], ManagedLimitDownProvider],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._persistence = persistence
        self._raw_store = raw_store
        self._provider_factory = provider_factory
        self._clock = clock

    def fill(self, trade_date: date) -> TodayLimitDownFillSummary:
        decision = decide_fill(self._persistence.dependencies(trade_date))
        if not decision.may_collect_source:
            return self._persistence.commit_deferred(trade_date, decision.reasons)
        now = self._aware_now()
        run = IngestionRun(
            uuid4(),
            ProviderCode.AKSHARE,
            DatasetCode.TODAY_LIMIT_DOWN_SOURCE,
            IngestionStatus.RUNNING,
            now,
            now,
            request_params={"trade_date": trade_date.isoformat()},
        )
        self._persistence.create_ingestion_run(run)
        try:
            with self._provider_factory() as provider:
                batch = provider.fetch_limit_down_pool(trade_date)
        except Exception as error:
            failed = _finish_run(
                run, self._aware_now(), 0, 0, 0, IngestionStatus.FAILED, type(error).__name__
            )
            return self._persistence.commit_failed(
                trade_date, failed, f"source_request_failed_{type(error).__name__}"
            )
        try:
            stored = self._raw_store.write_jsonl(
                provider=ProviderCode.AKSHARE.value,
                dataset=DatasetCode.TODAY_LIMIT_DOWN_SOURCE.value,
                partition_date=trade_date,
                ingestion_id=run.ingestion_id,
                rows=batch.raw_rows,
                schema_version=batch.schema_version,
            )
        except Exception as error:
            failed = _finish_run(
                run, self._aware_now(), 0, 0, 0, IngestionStatus.FAILED, type(error).__name__
            )
            return self._persistence.commit_failed(
                trade_date, failed, f"raw_write_failed_{type(error).__name__}"
            )
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
            reason = f"source_normalization_failed_{type(error).__name__}"
            quality: tuple[QualityResult, ...] = (
                _quality(run.ingestion_id, reason, QualitySeverity.ERROR),
            )
            return self._persistence.commit_failed(trade_date, failed, reason, manifest, quality)
        counts = Counter(record.symbol for record in normalized)
        duplicates = {symbol for symbol, count in counts.items() if count > 1}
        records = tuple(record for record in normalized if record.symbol not in duplicates)
        quality = tuple(
            _quality(run.ingestion_id, "duplicate_symbol", QualitySeverity.ERROR, symbol)
            for symbol in sorted(duplicates)
        )
        completed = _finish_run(
            run,
            self._aware_now(),
            stored.row_count,
            len(records),
            len(normalized) - len(records),
            IngestionStatus.PARTIAL if duplicates else IngestionStatus.SUCCEEDED,
            "duplicate source symbols" if duplicates else None,
        )
        requested = (
            TodayLimitDownSnapshotStatus.PARTIAL
            if duplicates or decision.status is TodayLimitDownSnapshotStatus.PARTIAL
            else TodayLimitDownSnapshotStatus.READY
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
            raise ValueError("today-limit-down clock must be timezone-aware")
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
    ingestion_id: UUID, suffix: str, severity: QualitySeverity, symbol: str | None = None
) -> QualityResult:
    return QualityResult(
        uuid4(),
        ingestion_id,
        DatasetCode.TODAY_LIMIT_DOWN_SOURCE,
        f"today_limit_down_source.{suffix}",
        severity,
        QualityStatus.FAILED,
        suffix.replace("_", " "),
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


def fill_today_limit_down_snapshot(
    engine: "Engine", raw_store: LocalRawStore, trade_date: date
) -> TodayLimitDownFillSummary:
    from market_data_center.persistence.today_limit_down_postgres import (
        PostgreSQLTodayLimitDownPersistence,
    )
    from market_data_center.providers.akshare_limit_down import (
        AkshareCurrentDayLimitDownProvider,
        BoundedAkshareLimitDownClient,
    )
    from market_data_center.settings import TodayLimitDownProviderSettings

    settings = TodayLimitDownProviderSettings()
    return TodayLimitDownFillService(
        persistence=PostgreSQLTodayLimitDownPersistence(engine),
        raw_store=raw_store,
        provider_factory=lambda: AkshareCurrentDayLimitDownProvider(
            BoundedAkshareLimitDownClient(
                timeout_seconds=settings.today_limit_down_timeout_seconds,
                max_attempts=settings.today_limit_down_max_attempts,
            )
        ),
    ).fill(trade_date)
