"""Raw replay, stale-run recovery and cross-provider comparison services."""

from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Protocol, cast
from uuid import UUID, uuid4

from market_data_center.domain.calendar import calculate_trading_day_links
from market_data_center.domain.entities import CalculatedTradingDay
from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    QualitySeverity,
    QualityStatus,
    RawManifest,
    ReplaySource,
)
from market_data_center.domain.records import (
    DailyBarRecord,
    IngestionEnvelope,
    SecurityRecord,
    TradingDayRecord,
)
from market_data_center.domain.validation import validate_daily_bars
from market_data_center.providers.akshare import normalize_akshare_raw
from market_data_center.providers.baostock import normalize_baostock_raw
from market_data_center.providers.contracts import ProviderError, ProviderRecord
from market_data_center.providers.pytdx import normalize_pytdx_raw
from market_data_center.raw_store import LocalRawStore, RawIntegrityError


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    source_ingestion_id: UUID
    replay_ingestion_id: UUID | None
    provider_code: str
    dataset_code: str
    dry_run: bool
    status: str
    fetched_rows: int
    accepted_rows: int
    rejected_rows: int

    def as_json(self) -> dict[str, object]:
        result = asdict(self)
        result["source_ingestion_id"] = str(self.source_ingestion_id)
        result["replay_ingestion_id"] = (
            str(self.replay_ingestion_id) if self.replay_ingestion_id else None
        )
        return result


@dataclass(frozen=True, slots=True)
class DailyBarComparisonReport:
    symbol: str
    start_date: date
    end_date: date
    providers: tuple[str, ...]
    provider_rows: Mapping[str, int]
    comparable_dates: int
    mismatched_dates: int
    differences: tuple[Mapping[str, object], ...]

    def as_json(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "providers": list(self.providers),
            "provider_rows": dict(self.provider_rows),
            "comparable_dates": self.comparable_dates,
            "mismatched_dates": self.mismatched_dates,
            "differences": list(self.differences),
        }


class ReliabilityPersistence(Protocol):
    def create_ingestion_run(self, run: IngestionRun) -> None: ...

    def replay_source(self, ingestion_id: UUID) -> ReplaySource: ...

    def daily_bar_replay_sources(
        self, symbol: str, start_date: date, end_date: date
    ) -> Sequence[ReplaySource]: ...

    def known_symbols(self, symbols: Collection[str]) -> set[str]: ...

    def known_trading_dates(self, dates: Collection[date]) -> set[date]: ...

    def trading_day_boundaries(
        self, start_date: date, end_date: date
    ) -> tuple[date | None, date | None]: ...

    def commit_security_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[SecurityRecord]],
    ) -> None: ...

    def commit_trading_calendar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[CalculatedTradingDay]],
    ) -> None: ...

    def commit_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[DailyBarRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_rejected_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def stale_ingestion_run_ids(self, stale_before: datetime) -> Sequence[UUID]: ...

    def recover_stale_ingestion_runs(
        self, stale_before: datetime, finished_at: datetime, reason: str
    ) -> Sequence[UUID]: ...


Normalizer = Callable[
    [DatasetCode, str, Sequence[Mapping[str, str]], Mapping[str, object]],
    tuple[ProviderRecord, ...],
]

_NORMALIZERS: Mapping[ProviderCode, Normalizer] = {
    ProviderCode.AKSHARE: normalize_akshare_raw,
    ProviderCode.BAOSTOCK: normalize_baostock_raw,
    ProviderCode.PYTDX: normalize_pytdx_raw,
}


class RawReplayService:
    def __init__(
        self,
        *,
        raw_store: LocalRawStore,
        persistence: ReliabilityPersistence,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._raw_store = raw_store
        self._persistence = persistence
        self._clock = clock
        self._uuid_factory = uuid_factory

    def replay(self, source_ingestion_id: UUID, *, dry_run: bool = False) -> ReplaySummary:
        source = self._persistence.replay_source(source_ingestion_id)
        run = self._new_replay_run(source) if not dry_run else None
        if run is not None:
            self._persistence.create_ingestion_run(run)
        try:
            records = self._normalize(source)
            return self._validate_and_commit(source, records, run, dry_run=dry_run)
        except Exception as error:
            if run is not None:
                self._commit_failure(run, error)
            raise

    def _normalize(self, source: ReplaySource) -> tuple[ProviderRecord, ...]:
        if source.manifest is None:
            raise RawIntegrityError("ingestion run has no Raw manifest")
        rows = self._raw_store.read_jsonl(source.manifest)
        normalizer = _NORMALIZERS[source.provider_code]
        records = normalizer(
            source.dataset_code,
            source.manifest.schema_version,
            rows,
            source.request_params,
        )
        mismatched = sum(record.source_code != source.provider_code.value for record in records)
        if mismatched:
            raise ProviderError(
                f"replayed batch contains {mismatched} record(s) with mismatched source_code"
            )
        return records

    def _validate_and_commit(
        self,
        source: ReplaySource,
        records: tuple[ProviderRecord, ...],
        run: IngestionRun | None,
        *,
        dry_run: bool,
    ) -> ReplaySummary:
        if source.dataset_code is DatasetCode.SECURITY:
            security_records = cast(tuple[SecurityRecord, ...], records)
            completed = self._completed(run, len(records), len(records), 0)
            if completed is not None:
                self._persistence.commit_security_batch(
                    completed, None, self._envelopes(completed.ingestion_id, security_records)
                )
            return self._summary(source, completed, dry_run, len(records), len(records), 0)

        if source.dataset_code is DatasetCode.TRADING_CALENDAR:
            calendar_records = cast(tuple[TradingDayRecord, ...], records)
            if calendar_records:
                start_date = min(record.trade_date for record in calendar_records)
                end_date = max(record.trade_date for record in calendar_records)
                boundaries = self._persistence.trading_day_boundaries(start_date, end_date)
            else:
                boundaries = (None, None)
            calculated = tuple(
                calculate_trading_day_links(
                    calendar_records,
                    previous_trading_day=boundaries[0],
                    next_trading_day=boundaries[1],
                )
            )
            completed = self._completed(run, len(records), len(calculated), 0)
            if completed is not None:
                self._persistence.commit_trading_calendar_batch(
                    completed, None, self._envelopes(completed.ingestion_id, calculated)
                )
            return self._summary(source, completed, dry_run, len(records), len(calculated), 0)

        daily_records = cast(tuple[DailyBarRecord, ...], records)
        known_symbols = self._persistence.known_symbols({record.symbol for record in daily_records})
        known_dates = self._persistence.known_trading_dates(
            {record.trade_date for record in daily_records}
        )
        findings = validate_daily_bars(
            daily_records,
            known_symbols=known_symbols,
            known_trading_dates=known_dates,
        )
        blocked_keys = {
            (finding.symbol, finding.trade_date)
            for finding in findings
            if finding.blocks_core_write
        }
        accepted = tuple(
            record
            for record in daily_records
            if (record.symbol, record.trade_date) not in blocked_keys
        )
        rejected_rows = sum(
            (record.symbol, record.trade_date) in blocked_keys for record in daily_records
        )
        completed = self._completed(run, len(records), len(accepted), rejected_rows)
        if completed is not None:
            quality_results = tuple(
                QualityResult(
                    quality_result_id=self._uuid_factory(),
                    ingestion_id=completed.ingestion_id,
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
            )
            self._persistence.commit_daily_bar_batch(
                completed,
                None,
                self._envelopes(completed.ingestion_id, accepted),
                quality_results,
            )
        return self._summary(source, completed, dry_run, len(records), len(accepted), rejected_rows)

    def _new_replay_run(self, source: ReplaySource) -> IngestionRun:
        now = self._clock()
        request_params = dict(source.request_params)
        request_params["replay_source_ingestion_id"] = str(source.source_ingestion_id)
        return IngestionRun(
            ingestion_id=self._uuid_factory(),
            provider_code=source.provider_code,
            dataset_code=source.dataset_code,
            status=IngestionStatus.RUNNING,
            requested_at=now,
            started_at=now,
            request_params=request_params,
            replayed_from_raw_id=source.manifest.raw_id if source.manifest else None,
        )

    def _completed(
        self,
        run: IngestionRun | None,
        fetched_rows: int,
        accepted_rows: int,
        rejected_rows: int,
    ) -> IngestionRun | None:
        if run is None:
            return None
        status = (
            IngestionStatus.PARTIAL
            if accepted_rows and rejected_rows
            else IngestionStatus.FAILED
            if rejected_rows
            else IngestionStatus.SUCCEEDED
        )
        return replace(
            run,
            status=status,
            finished_at=self._clock(),
            fetched_rows=fetched_rows,
            accepted_rows=accepted_rows,
            rejected_rows=rejected_rows,
        )

    def _commit_failure(self, run: IngestionRun, error: Exception) -> None:
        failed = replace(
            run,
            status=IngestionStatus.FAILED,
            finished_at=self._clock(),
            error_summary=f"{type(error).__name__}: raw replay failed",
        )
        quality = QualityResult(
            quality_result_id=self._uuid_factory(),
            ingestion_id=run.ingestion_id,
            dataset_code=run.dataset_code,
            rule_code=f"{run.dataset_code.value}.raw_replay",
            severity=QualitySeverity.ERROR,
            status=QualityStatus.FAILED,
            message="Raw replay failed integrity or normalization checks",
            details={"error_type": type(error).__name__},
        )
        self._persistence.commit_rejected_batch(failed, None, [quality])

    @staticmethod
    def _envelopes[RecordT: SecurityRecord | CalculatedTradingDay | DailyBarRecord](
        ingestion_id: UUID, records: Sequence[RecordT]
    ) -> tuple[IngestionEnvelope[RecordT], ...]:
        return tuple(IngestionEnvelope(ingestion_id, record) for record in records)

    @staticmethod
    def _summary(
        source: ReplaySource,
        run: IngestionRun | None,
        dry_run: bool,
        fetched_rows: int,
        accepted_rows: int,
        rejected_rows: int,
    ) -> ReplaySummary:
        return ReplaySummary(
            source_ingestion_id=source.source_ingestion_id,
            replay_ingestion_id=run.ingestion_id if run else None,
            provider_code=source.provider_code.value,
            dataset_code=source.dataset_code.value,
            dry_run=dry_run,
            status=run.status.value if run else "valid",
            fetched_rows=fetched_rows,
            accepted_rows=accepted_rows,
            rejected_rows=rejected_rows,
        )


def recover_stale_runs(
    persistence: ReliabilityPersistence,
    *,
    older_than: timedelta,
    dry_run: bool,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[UUID, ...]:
    if older_than <= timedelta(0):
        raise ValueError("older_than must be positive")
    now = clock()
    stale_before = now - older_than
    if dry_run:
        return tuple(persistence.stale_ingestion_run_ids(stale_before))
    return tuple(
        persistence.recover_stale_ingestion_runs(
            stale_before,
            now,
            "StaleRunRecovery: running ingestion exceeded the configured age",
        )
    )


def compare_daily_bar_sources(
    persistence: ReliabilityPersistence,
    raw_store: LocalRawStore,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> DailyBarComparisonReport:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    sources = persistence.daily_bar_replay_sources(symbol, start_date, end_date)
    latest: dict[tuple[str, date], DailyBarRecord] = {}
    for source in sorted(sources, key=lambda item: item.requested_at):
        if source.manifest is None:
            raise RawIntegrityError("comparison source has no Raw manifest")
        rows = raw_store.read_jsonl(source.manifest)
        records = _NORMALIZERS[source.provider_code](
            source.dataset_code,
            source.manifest.schema_version,
            rows,
            source.request_params,
        )
        for record in cast(tuple[DailyBarRecord, ...], records):
            if record.symbol == symbol and start_date <= record.trade_date <= end_date:
                latest[(source.provider_code.value, record.trade_date)] = record

    by_date: dict[date, dict[str, DailyBarRecord]] = defaultdict(dict)
    provider_rows: dict[str, int] = defaultdict(int)
    for (provider, trade_date), record in latest.items():
        by_date[trade_date][provider] = record
        provider_rows[provider] += 1

    differences: list[Mapping[str, object]] = []
    comparable_dates = 0
    for trade_date in sorted(by_date):
        provider_records = by_date[trade_date]
        if len(provider_records) < 2:
            continue
        comparable_dates += 1
        changed_fields: dict[str, Mapping[str, object]] = {}
        for field in (
            "open",
            "high",
            "low",
            "close",
            "previous_close",
            "volume",
            "amount",
            "trade_status",
            "is_st",
        ):
            values = {
                provider: _json_value(getattr(record, field))
                for provider, record in sorted(provider_records.items())
            }
            if len({repr(value) for value in values.values()}) > 1:
                changed_fields[field] = values
        if changed_fields:
            differences.append({"trade_date": trade_date.isoformat(), "fields": changed_fields})

    providers = tuple(sorted(provider_rows))
    return DailyBarComparisonReport(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        providers=providers,
        provider_rows=dict(sorted(provider_rows.items())),
        comparable_dates=comparable_dates,
        mismatched_dates=len(differences),
        differences=tuple(differences),
    )


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "value"):
        return cast(object, value.value)
    return value
