"""Raw replay, stale-run recovery and cross-provider comparison services."""

from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
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
from market_data_center.domain.deducted_profit import (
    DeductedProfitRecord,
    validate_deducted_profits,
)
from market_data_center.domain.dragon_tiger import (
    DragonTigerEventDraft,
    DragonTigerEventRecord,
    DragonTigerFinding,
    DragonTigerPeriodType,
    validate_dragon_tiger_events,
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
    RawManifest,
    ReplaySource,
)
from market_data_center.domain.records import (
    CapitalRecord,
    DailyBarRecord,
    IngestionEnvelope,
    SecurityRecord,
    TradingDayRecord,
)
from market_data_center.domain.shareholder_count import (
    ShareholderCountRecord,
    validate_shareholder_counts,
)
from market_data_center.domain.stock_daily_indicator import (
    StockDailyIndicatorSnapshotRecord,
    validate_stock_daily_indicators,
)
from market_data_center.domain.validation import validate_daily_bars
from market_data_center.providers.akshare import normalize_akshare_raw
from market_data_center.providers.akshare_ths import normalize_akshare_ths_raw
from market_data_center.providers.baostock import normalize_baostock_raw
from market_data_center.providers.contracts import ProviderError, ProviderRecord
from market_data_center.providers.eastmoney_dragon_tiger import (
    normalize_eastmoney_dragon_tiger_raw,
)
from market_data_center.providers.pytdx import normalize_pytdx_raw
from market_data_center.providers.tushare import normalize_tushare_raw
from market_data_center.providers.tushare_dragon_tiger import (
    normalize_tushare_dragon_tiger_raw,
)
from market_data_center.raw_store import LocalRawStore, RawIntegrityError
from market_data_center.shareholder_count_batch import (
    PreparedShareholderCountBatch,
    shareholder_count_missing_source_quality_result,
    shareholder_count_unsupported_exchange_quality_result,
)


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

    def known_stock_symbols_for_date(
        self, symbols: Collection[str], trade_date: date
    ) -> set[str]: ...

    def dragon_tiger_period_start_date(self, trade_date: date, session_count: int) -> date: ...

    def known_trading_dates(self, dates: Collection[date]) -> set[date]: ...

    def known_board_ids(self, board_ids: Collection[str]) -> set[str]: ...

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

    def commit_stock_daily_indicator_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[StockDailyIndicatorSnapshotRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_deducted_profit_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[DeductedProfitRecord]],
    ) -> None: ...

    def commit_shareholder_count_batches(
        self, batches: Sequence[PreparedShareholderCountBatch]
    ) -> None: ...

    def commit_capital_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[CapitalRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def known_classification_snapshots(
        self, keys: Collection[tuple[str, ClassificationType, str, date]]
    ) -> set[tuple[str, ClassificationType, str, date]]: ...

    def commit_classification_catalog_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        record: IngestionEnvelope[ClassificationCatalogSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_classification_members_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        record: IngestionEnvelope[ClassificationMemberSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_board_index_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[BoardIndexRecord]],
    ) -> None: ...

    def commit_board_index_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[BoardIndexDailyBarRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_board_index_constituents_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        record: IngestionEnvelope[BoardIndexConstituentSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_rejected_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality_results: Sequence[QualityResult],
    ) -> None: ...

    def commit_dragon_tiger_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[DragonTigerEventRecord],
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
    ProviderCode.AKSHARE_THS: normalize_akshare_ths_raw,
    ProviderCode.BAOSTOCK: normalize_baostock_raw,
    ProviderCode.PYTDX: normalize_pytdx_raw,
    ProviderCode.TUSHARE: lambda dataset, schema, rows, params: (
        _normalize_tushare_dragon_tiger_replay(schema, rows, params)
        if dataset is DatasetCode.DRAGON_TIGER
        else normalize_tushare_raw(dataset, schema, rows, params)
    ),
    ProviderCode.EASTMONEY: lambda dataset, schema, rows, params: (
        _normalize_eastmoney_dragon_tiger_replay(schema, rows, params)
        if dataset in {DatasetCode.TRADING_BILLBOARD, DatasetCode.DRAGON_TIGER}
        else _unsupported_eastmoney_replay(dataset)
    ),
}


def _unsupported_eastmoney_replay(dataset: DatasetCode) -> tuple[ProviderRecord, ...]:
    raise ProviderError(f"Eastmoney Raw replay is unsupported for {dataset.value}")


def _normalize_eastmoney_dragon_tiger_replay(
    schema: str,
    rows: Sequence[Mapping[str, str]],
    request_params: Mapping[str, object],
) -> tuple[ProviderRecord, ...]:
    requested_value = request_params.get("trade_date")
    if not isinstance(requested_value, str):
        raise ProviderError("Eastmoney DragonTiger replay request trade_date is missing")
    try:
        requested_date = date.fromisoformat(requested_value)
    except ValueError as error:
        raise ProviderError("Eastmoney DragonTiger replay request trade_date is invalid") from error
    records = normalize_eastmoney_dragon_tiger_raw(rows, schema)
    if any(record.trade_date != requested_date for record in records):
        raise ProviderError("Eastmoney DragonTiger Raw date does not match request trade_date")
    return records


def _normalize_tushare_dragon_tiger_replay(
    schema: str,
    rows: Sequence[Mapping[str, str]],
    request_params: Mapping[str, object],
) -> tuple[ProviderRecord, ...]:
    requested_value = request_params.get("trade_date")
    if not isinstance(requested_value, str):
        raise ProviderError("Tushare DragonTiger replay request trade_date is missing")
    try:
        requested_date = date.fromisoformat(requested_value)
    except ValueError as error:
        raise ProviderError("Tushare DragonTiger replay request trade_date is invalid") from error
    records = normalize_tushare_dragon_tiger_raw(rows, schema)
    if any(record.trade_date != requested_date for record in records):
        raise ProviderError("Tushare DragonTiger Raw date does not match request trade_date")
    return records


CALL_AUCTION_MARKET_REPLAY_DISABLED = (
    "Raw replay is disabled for call_auction_market_snapshot until exact "
    "frozen-universe identity can be proven"
)
CALL_AUCTION_MARKET_SERIES_REPLAY_DISABLED = (
    "Raw replay is disabled for call_auction_market_series until exact "
    "session, round, attempt, and frozen-universe lineage is implemented"
)


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
        if source.dataset_code is DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT:
            raise ProviderError(CALL_AUCTION_MARKET_REPLAY_DISABLED)
        if source.dataset_code is DatasetCode.CALL_AUCTION_MARKET_SERIES:
            raise ProviderError(CALL_AUCTION_MARKET_SERIES_REPLAY_DISABLED)
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
        if source.dataset_code in {DatasetCode.TRADING_BILLBOARD, DatasetCode.DRAGON_TIGER}:
            drafts = cast(tuple[DragonTigerEventDraft, ...], records)
            dragon_tiger_records = tuple(
                draft.resolve_period(
                    draft.trade_date
                    if draft.period_type is DragonTigerPeriodType.DAY
                    else self._persistence.dragon_tiger_period_start_date(draft.trade_date, 3)
                )
                for draft in drafts
            )
            trade_date = (
                dragon_tiger_records[0].trade_date
                if dragon_tiger_records
                else date.fromisoformat(cast(str, source.request_params["trade_date"]))
            )
            validation = validate_dragon_tiger_events(
                dragon_tiger_records,
                known_symbols=self._persistence.known_stock_symbols_for_date(
                    {record.symbol for record in dragon_tiger_records},
                    trade_date,
                ),
                known_trading_dates=self._persistence.known_trading_dates(
                    {
                        candidate
                        for record in dragon_tiger_records
                        for candidate in (record.trade_date, record.period_start_date)
                    }
                ),
            )
            rejected_count = len(dragon_tiger_records) if validation.findings else 0
            accepted_count = 0 if validation.findings else len(validation.accepted)
            completed = self._completed(
                run,
                len(dragon_tiger_records),
                accepted_count,
                rejected_count,
            )
            quality = self._dragon_tiger_quality(completed, validation.findings)
            if completed is not None:
                if validation.findings:
                    self._persistence.commit_rejected_batch(completed, None, quality)
                else:
                    self._persistence.commit_dragon_tiger_batch(
                        completed,
                        None,
                        validation.accepted,
                        quality,
                    )
            return self._summary(
                source,
                completed,
                dry_run,
                len(dragon_tiger_records),
                accepted_count,
                rejected_count,
            )

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

        if source.dataset_code is DatasetCode.CAPITAL:
            capital_records = cast(tuple[CapitalRecord, ...], records)
            known_symbols = self._persistence.known_symbols(
                {record.symbol for record in capital_records}
            )
            capital_validation = validate_capital(capital_records, known_symbols=known_symbols)
            completed = self._completed(
                run,
                len(records),
                len(capital_validation.accepted),
                capital_validation.rejected_rows,
            )
            if completed is not None:
                quality_results = tuple(
                    QualityResult(
                        quality_result_id=self._uuid_factory(),
                        ingestion_id=completed.ingestion_id,
                        dataset_code=DatasetCode.CAPITAL,
                        rule_code=finding.rule_code,
                        severity=QualitySeverity.ERROR,
                        status=QualityStatus.FAILED,
                        message=finding.message,
                        natural_key=finding.natural_key,
                    )
                    for finding in capital_validation.findings
                )
                self._persistence.commit_capital_batch(
                    completed,
                    None,
                    self._envelopes(completed.ingestion_id, capital_validation.accepted),
                    quality_results,
                )
            return self._summary(
                source,
                completed,
                dry_run,
                len(records),
                len(capital_validation.accepted),
                capital_validation.rejected_rows,
            )

        if source.dataset_code is DatasetCode.STOCK_DAILY_INDICATOR:
            indicator_records = cast(tuple[StockDailyIndicatorSnapshotRecord, ...], records)
            indicator_validation = validate_stock_daily_indicators(
                indicator_records,
                known_symbols=self._persistence.known_symbols(
                    {record.symbol for record in indicator_records}
                ),
                known_trading_dates=self._persistence.known_trading_dates(
                    {record.trade_date for record in indicator_records}
                ),
            )
            completed = self._completed(
                run,
                len(indicator_records),
                len(indicator_validation.accepted),
                indicator_validation.rejected_rows,
            )
            if completed is not None:
                quality_results = tuple(
                    QualityResult(
                        quality_result_id=self._uuid_factory(),
                        ingestion_id=completed.ingestion_id,
                        dataset_code=DatasetCode.STOCK_DAILY_INDICATOR,
                        rule_code=finding.rule_code,
                        severity=QualitySeverity.ERROR,
                        status=QualityStatus.FAILED,
                        message=finding.message,
                        natural_key=finding.natural_key,
                    )
                    for finding in indicator_validation.findings
                )
                self._persistence.commit_stock_daily_indicator_batch(
                    completed,
                    None,
                    self._envelopes(completed.ingestion_id, indicator_validation.accepted),
                    quality_results,
                )
            return self._summary(
                source,
                completed,
                dry_run,
                len(indicator_records),
                len(indicator_validation.accepted),
                indicator_validation.rejected_rows,
            )

        if source.dataset_code is DatasetCode.DEDUCTED_PROFIT:
            profit_records = cast(tuple[DeductedProfitRecord, ...], records)
            validated = validate_deducted_profits(
                profit_records,
                known_symbols=self._persistence.known_symbols(
                    {record.symbol for record in profit_records}
                ),
            )
            completed = self._completed(run, len(profit_records), len(validated), 0)
            if completed is not None:
                self._persistence.commit_deducted_profit_batch(
                    completed,
                    None,
                    self._envelopes(completed.ingestion_id, validated),
                )
            return self._summary(source, completed, dry_run, len(profit_records), len(validated), 0)

        if source.dataset_code is DatasetCode.SHAREHOLDER_COUNT:
            shareholder_records = cast(tuple[ShareholderCountRecord, ...], records)
            supported_shareholder_records = tuple(
                record for record in shareholder_records if not record.symbol.startswith("BSE:")
            )
            unsupported_exchange_rows = len(shareholder_records) - len(
                supported_shareholder_records
            )
            validated_shareholder_records = validate_shareholder_counts(
                supported_shareholder_records,
                known_symbols=self._persistence.known_symbols(
                    {record.symbol for record in supported_shareholder_records}
                ),
            )
            fetched_shareholder_rows = (
                source.manifest.row_count if source.manifest is not None else len(records)
            )
            missing_source_rows = fetched_shareholder_rows - len(shareholder_records)
            rejected_shareholder_rows = missing_source_rows + unsupported_exchange_rows
            completed = self._completed(
                run,
                fetched_shareholder_rows,
                len(validated_shareholder_records),
                rejected_shareholder_rows,
            )
            if completed is not None:
                shareholder_quality = tuple(
                    result
                    for result in (
                        shareholder_count_missing_source_quality_result(
                            quality_result_id=self._uuid_factory(),
                            ingestion_id=completed.ingestion_id,
                            rejected_rows=missing_source_rows,
                        )
                        if missing_source_rows
                        else None,
                        shareholder_count_unsupported_exchange_quality_result(
                            quality_result_id=self._uuid_factory(),
                            ingestion_id=completed.ingestion_id,
                            rejected_rows=unsupported_exchange_rows,
                        )
                        if unsupported_exchange_rows
                        else None,
                    )
                    if result is not None
                )
                self._persistence.commit_shareholder_count_batches(
                    (
                        PreparedShareholderCountBatch(
                            run=completed,
                            manifest=None,
                            records=self._envelopes(
                                completed.ingestion_id, validated_shareholder_records
                            ),
                            quality_results=shareholder_quality,
                        ),
                    )
                )
            return self._summary(
                source,
                completed,
                dry_run,
                fetched_shareholder_rows,
                len(validated_shareholder_records),
                rejected_shareholder_rows,
            )

        if source.dataset_code is DatasetCode.CLASSIFICATION_CATALOG:
            if len(records) != 1 or not isinstance(records[0], ClassificationCatalogSnapshotRecord):
                raise ProviderError("classification catalog replay must contain one snapshot")
            catalog_record = records[0]
            catalog_findings = validate_catalog(catalog_record)
            fetched_count = max(len(catalog_record.definitions), 1)
            accepted_count = 0 if catalog_findings else len(catalog_record.definitions)
            rejected_count = fetched_count if catalog_findings else 0
            completed = self._completed(
                run,
                fetched_count,
                accepted_count,
                rejected_count,
            )
            if completed is not None:
                self._persistence.commit_classification_catalog_batch(
                    completed,
                    None,
                    IngestionEnvelope(completed.ingestion_id, catalog_record),
                    self._classification_quality(completed, catalog_findings),
                )
            return self._summary(
                source,
                completed,
                dry_run,
                fetched_count,
                accepted_count,
                rejected_count,
            )

        if source.dataset_code is DatasetCode.CLASSIFICATION_MEMBERS:
            if len(records) != 1 or not isinstance(records[0], ClassificationMemberSnapshotRecord):
                raise ProviderError("classification member replay must contain one snapshot")
            member_record = records[0]
            key = (
                member_record.namespace,
                member_record.classification_type,
                member_record.classification_code,
                member_record.snapshot_date,
            )
            member_findings = validate_member_snapshot(
                member_record,
                known_classifications=self._persistence.known_classification_snapshots({key}),
                known_symbols=self._persistence.known_symbols(set(member_record.members)),
            )
            member_fetched = max(len(member_record.members), 1)
            member_accepted = 0 if member_findings else len(member_record.members)
            member_rejected = member_fetched if member_findings else 0
            completed = self._completed(run, member_fetched, member_accepted, member_rejected)
            if completed is not None:
                self._persistence.commit_classification_members_batch(
                    completed,
                    None,
                    IngestionEnvelope(completed.ingestion_id, member_record),
                    self._classification_quality(completed, member_findings),
                )
            return self._summary(
                source,
                completed,
                dry_run,
                member_fetched,
                member_accepted,
                member_rejected,
            )

        if source.dataset_code is DatasetCode.BOARD_INDEX:
            if not records or any(not isinstance(record, BoardIndexRecord) for record in records):
                raise ProviderError("board-index replay must contain BoardIndex records")
            board_records = cast(tuple[BoardIndexRecord, ...], records)
            completed = self._completed(run, len(board_records), len(board_records), 0)
            if completed is not None:
                self._persistence.commit_board_index_batch(
                    completed,
                    None,
                    self._envelopes(completed.ingestion_id, board_records),
                )
            return self._summary(
                source,
                completed,
                dry_run,
                len(board_records),
                len(board_records),
                0,
            )

        if source.dataset_code is DatasetCode.BOARD_INDEX_DAILY_BAR:
            if any(not isinstance(record, BoardIndexDailyBarRecord) for record in records):
                raise ProviderError("board-index daily-bar replay contains an unexpected record")
            board_bars = cast(tuple[BoardIndexDailyBarRecord, ...], records)
            board_validation = validate_board_index_daily_bars(
                board_bars,
                known_board_ids=self._persistence.known_board_ids(
                    {record.board_id for record in board_bars}
                ),
                known_trading_dates=self._persistence.known_trading_dates(
                    {record.trade_date for record in board_bars}
                ),
            )
            completed = self._completed(
                run,
                len(board_bars),
                len(board_validation.accepted),
                board_validation.rejected_rows,
            )
            if completed is not None:
                self._persistence.commit_board_index_daily_bar_batch(
                    completed,
                    None,
                    self._envelopes(completed.ingestion_id, board_validation.accepted),
                    self._board_index_quality(completed, board_validation.findings),
                )
            return self._summary(
                source,
                completed,
                dry_run,
                len(board_bars),
                len(board_validation.accepted),
                board_validation.rejected_rows,
            )

        if source.dataset_code is DatasetCode.BOARD_INDEX_CONSTITUENT_SNAPSHOT:
            if len(records) != 1 or not isinstance(records[0], BoardIndexConstituentSnapshotRecord):
                raise ProviderError("board-index constituent replay must contain one snapshot")
            board_members = records[0]
            board_findings = validate_board_index_constituent_snapshot(
                board_members,
                known_board_ids=self._persistence.known_board_ids({board_members.board_id}),
                known_symbols=self._persistence.known_symbols(set(board_members.members)),
                known_trading_dates=self._persistence.known_trading_dates(
                    {board_members.trade_date}
                ),
            )
            fetched_count = max(len(board_members.members), 1)
            accepted_count = 0 if board_findings else len(board_members.members)
            rejected_count = fetched_count if board_findings else 0
            completed = self._completed(run, fetched_count, accepted_count, rejected_count)
            if completed is not None:
                self._persistence.commit_board_index_constituents_batch(
                    completed,
                    None,
                    IngestionEnvelope(completed.ingestion_id, board_members),
                    self._board_index_quality(completed, board_findings),
                )
            return self._summary(
                source,
                completed,
                dry_run,
                fetched_count,
                accepted_count,
                rejected_count,
            )

        daily_records = cast(tuple[DailyBarRecord, ...], records)
        known_symbols = self._persistence.known_symbols({record.symbol for record in daily_records})
        known_dates = self._persistence.known_trading_dates(
            {record.trade_date for record in daily_records}
        )
        daily_findings = validate_daily_bars(
            daily_records,
            known_symbols=known_symbols,
            known_trading_dates=known_dates,
        )
        blocked_keys = {
            (finding.symbol, finding.trade_date)
            for finding in daily_findings
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
                for finding in daily_findings
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
        request_params["replay_source_requested_at"] = source.requested_at.isoformat()
        return IngestionRun(
            ingestion_id=self._uuid_factory(),
            provider_code=source.provider_code,
            dataset_code=(
                DatasetCode.DRAGON_TIGER
                if source.dataset_code is DatasetCode.TRADING_BILLBOARD
                else source.dataset_code
            ),
            status=IngestionStatus.RUNNING,
            requested_at=now,
            started_at=now,
            request_params=request_params,
            replayed_from_raw_id=source.manifest.raw_id if source.manifest else None,
        )

    def _classification_quality(
        self, run: IngestionRun, findings: Sequence[ClassificationFinding]
    ) -> tuple[QualityResult, ...]:
        return tuple(
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
        )

    def _board_index_quality(
        self, run: IngestionRun, findings: Sequence[BoardIndexFinding]
    ) -> tuple[QualityResult, ...]:
        return tuple(
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
        )

    def _dragon_tiger_quality(
        self,
        run: IngestionRun | None,
        findings: Sequence[DragonTigerFinding],
    ) -> tuple[QualityResult, ...]:
        if run is None:
            return ()
        return tuple(
            QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=run.ingestion_id,
                dataset_code=DatasetCode.DRAGON_TIGER,
                rule_code=finding.rule_code,
                severity=QualitySeverity.ERROR,
                status=QualityStatus.FAILED,
                message=finding.message,
                natural_key=finding.natural_key,
            )
            for finding in findings
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
    def _envelopes[
        RecordT: SecurityRecord
        | CalculatedTradingDay
        | DailyBarRecord
        | CapitalRecord
        | ClassificationRecord
        | BoardIndexProviderRecord
        | StockDailyIndicatorSnapshotRecord
        | DeductedProfitRecord
        | ShareholderCountRecord
    ](ingestion_id: UUID, records: Sequence[RecordT]) -> tuple[IngestionEnvelope[RecordT], ...]:
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
