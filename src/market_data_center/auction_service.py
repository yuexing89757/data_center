"""Bounded opening-auction quote collection session orchestration."""

from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from time import sleep
from uuid import UUID, uuid4

from market_data_center.domain.auction import (
    AuctionCollectionSession,
    AuctionCollectionSummary,
    AuctionPoolMember,
    AuctionQuoteSample,
    AuctionRoundStatus,
    AuctionRoundSummary,
    AuctionSessionStatus,
    QuoteSemantics,
    auction_phase,
    auction_window,
    calculate_auction_quote_metric,
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
from market_data_center.domain.realtime_quote import (
    RealtimeQuoteFinding,
    validate_realtime_quotes,
)
from market_data_center.persistence.auction_postgres import PostgreSQLAuctionPersistence
from market_data_center.providers.contracts import RealtimeQuoteFetch, RealtimeQuoteProvider
from market_data_center.raw_store import LocalRawStore


class AuctionWindowUnavailable(RuntimeError):
    """Collection cannot run outside the live auction window."""


class AuctionCollectionService:
    def __init__(
        self,
        persistence: PostgreSQLAuctionPersistence,
        provider: RealtimeQuoteProvider,
        raw_store: LocalRawStore,
        *,
        cadence_seconds: int = 5,
        max_retries: int = 1,
        retry_budget_seconds: float = 1.0,
        order_book_semantics_verified: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleeper: Callable[[float], None] = sleep,
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        if not 1 <= cadence_seconds <= 60 or not 0 <= max_retries <= 1:
            raise ValueError("invalid auction cadence or retry bound")
        self._persistence = persistence
        self._provider = provider
        self._raw_store = raw_store
        self._cadence = cadence_seconds
        self._max_retries = max_retries
        self._retry_budget = retry_budget_seconds
        self._semantics_verified = order_book_semantics_verified
        self._clock = clock
        self._sleeper = sleeper
        self._uuid_factory = uuid_factory

    def preflight(self, trade_date: date) -> dict[str, object]:
        if not self._persistence.is_trading_day(trade_date):
            return {"trade_date": trade_date.isoformat(), "trading_day": False}
        pool = self._persistence.load_exact_pool(trade_date)
        start, end = auction_window(trade_date)
        rounds = int((end - start).total_seconds() // self._cadence) + 1
        return {
            "trade_date": trade_date.isoformat(),
            "trading_day": True,
            "pool_snapshot_id": str(pool.snapshot_id),
            "pool_snapshot_version": pool.version,
            "member_count": len(pool.members),
            "cadence_seconds": self._cadence,
            "expected_rounds": rounds,
            "expected_quotes": rounds * len(pool.members),
            "live_collection": False,
            "semantics_verified": self._semantics_verified,
        }

    def collect(self, trade_date: date) -> AuctionCollectionSummary:
        if not self._persistence.is_trading_day(trade_date):
            return AuctionCollectionSummary("skipped", None, 0, 0, 0, 0, 0, 0)
        pool = self._persistence.load_exact_pool(trade_date)
        start, end = auction_window(trade_date)
        now = self._clock()
        if now > end:
            raise AuctionWindowUnavailable("opening-auction window has already ended")
        if now < start:
            self._sleeper((start - now).total_seconds())
            now = self._clock()
        expected_rounds = int((end - start).total_seconds() // self._cadence) + 1
        session = self._persistence.create_or_resume_session(
            AuctionCollectionSession(
                self._uuid_factory(),
                pool.snapshot_id,
                pool.version,
                pool.basis_trade_date,
                pool.effective_trade_date,
                start,
                end,
                self._cadence,
                expected_rounds,
                expected_rounds * len(pool.members),
                self._provider.source_code,
                AuctionSessionStatus.RUNNING,
                now,
            )
        )
        if session.status is not AuctionSessionStatus.RUNNING:
            return _summary(session)
        completed = self._persistence.completed_sequences(session.session_id)
        try:
            current = self._clock()
            first_seq = max(0, int((current - start).total_seconds() // self._cadence))
            for sample_seq in range(first_seq, expected_rounds):
                if sample_seq in completed:
                    continue
                scheduled_at = start + timedelta(seconds=sample_seq * self._cadence)
                current = self._clock()
                if current > scheduled_at + timedelta(seconds=self._cadence):
                    continue
                if current < scheduled_at:
                    self._sleeper((scheduled_at - current).total_seconds())
                self._collect_round(session, pool.members, sample_seq, scheduled_at)
            finished = self._persistence.finish_session(session.session_id, self._clock())
            return _summary(finished)
        except BaseException as error:
            self._persistence.fail_session(session.session_id, self._clock(), type(error).__name__)
            raise

    def _collect_round(
        self,
        session: AuctionCollectionSession,
        members: Sequence[AuctionPoolMember],
        sample_seq: int,
        scheduled_at: datetime,
    ) -> None:
        ingestion_id = self._uuid_factory()
        started_at = self._clock()
        run = IngestionRun(
            ingestion_id,
            ProviderCode.PYTDX_HQ,
            DatasetCode.FIVE_LEVEL_QUOTE,
            IngestionStatus.RUNNING,
            started_at,
            started_at,
            request_params={
                "session_id": str(session.session_id),
                "pool_snapshot_id": str(session.pool_snapshot_id),
                "sample_seq": sample_seq,
                "scheduled_at": scheduled_at.isoformat(),
                "expected_symbols": len(members),
            },
        )
        self._persistence.create_ingestion_run(run)
        try:
            symbols = tuple(item.symbol for item in members)
            fetched = self._fetch_with_bounded_retry(symbols, scheduled_at)
            collected_at = max(
                (record.observed_at for record in fetched.records),
                default=self._clock(),
            )
            validation = validate_realtime_quotes(
                fetched.records,
                known_symbols=set(symbols),
                known_stock_symbols=set(symbols),
                now=max(collected_at, self._clock()),
            )
            accepted_symbols = {record.symbol for record in validation.accepted}
            failed_symbols = set(symbols) - accepted_symbols
            samples = tuple(
                AuctionQuoteSample(
                    session.session_id,
                    session.pool_snapshot_id,
                    sample_seq,
                    scheduled_at,
                    record.observed_at,
                    auction_phase(scheduled_at),
                    (
                        QuoteSemantics.VERIFIED_ORDER_BOOK
                        if self._semantics_verified
                        else QuoteSemantics.AUCTION_INDICATIVE
                    ),
                    record,
                )
                for record in validation.accepted
            )
            member_by_symbol = {item.symbol: item for item in members}
            metrics = tuple(
                calculate_auction_quote_metric(
                    sample,
                    upper_limit=member_by_symbol[sample.quote.symbol].upper_limit,
                    order_book_semantics_verified=self._semantics_verified,
                    calculated_at=self._clock(),
                    price_limit_rule_version=member_by_symbol[
                        sample.quote.symbol
                    ].price_limit_rule_version,
                )
                for sample in samples
            )
            findings = _quality_results(
                ingestion_id,
                sample_seq,
                failed_symbols,
                validation.findings,
                self._uuid_factory,
            )
            stored = self._raw_store.write_jsonl(
                provider="pytdx_hq",
                dataset=DatasetCode.FIVE_LEVEL_QUOTE.value,
                partition_date=session.effective_trade_date,
                ingestion_id=ingestion_id,
                rows=fetched.raw_rows,
                schema_version=fetched.schema_version,
            )
            manifest = RawManifest(
                self._uuid_factory(),
                ingestion_id,
                stored.object_path,
                RawFileFormat.JSONL,
                stored.content_sha256,
                stored.byte_size,
                stored.row_count,
                stored.schema_version,
            )
            failed_count = len(failed_symbols)
            status = (
                AuctionRoundStatus.SUCCEEDED
                if not failed_count
                else AuctionRoundStatus.PARTIAL
                if samples
                else AuctionRoundStatus.FAILED
            )
            finished_at = self._clock()
            terminal = replace(
                run,
                status={
                    AuctionRoundStatus.SUCCEEDED: IngestionStatus.SUCCEEDED,
                    AuctionRoundStatus.PARTIAL: IngestionStatus.PARTIAL,
                    AuctionRoundStatus.FAILED: IngestionStatus.FAILED,
                }[status],
                finished_at=finished_at,
                fetched_rows=len(symbols),
                accepted_rows=len(samples),
                rejected_rows=failed_count,
                error_summary="ProviderError: quote round incomplete" if failed_count else None,
            )
            round_summary = AuctionRoundSummary(
                sample_seq,
                status,
                len(symbols),
                len(samples),
                failed_count,
                scheduled_at,
                collected_at,
                max(0, int((collected_at - scheduled_at).total_seconds() * 1000)),
            )
            self._persistence.commit_round(
                terminal, manifest, round_summary, samples, metrics, findings
            )
        except Exception as error:
            self._persistence.fail_ingestion_run(
                replace(
                    run,
                    status=IngestionStatus.FAILED,
                    finished_at=self._clock(),
                    error_summary=f"{type(error).__name__}: auction round failed",
                )
            )
            raise

    def _fetch_with_bounded_retry(
        self, symbols: tuple[str, ...], scheduled_at: datetime
    ) -> RealtimeQuoteFetch:
        result = self._provider.fetch_five_level_quotes(symbols)
        if not result.failed_symbols or not self._max_retries:
            return result
        deadline = scheduled_at + timedelta(seconds=self._cadence)
        if self._clock() + timedelta(seconds=self._retry_budget) >= deadline:
            return result
        retry = self._provider.fetch_five_level_quotes(result.failed_symbols)
        records = {item.symbol: item for item in (*result.records, *retry.records)}
        failed = tuple(symbol for symbol in symbols if symbol not in records)
        return RealtimeQuoteFetch(
            (*result.raw_rows, *retry.raw_rows),
            tuple(records[symbol] for symbol in symbols if symbol in records),
            symbols,
            failed,
            result.schema_version,
        )


def _quality_results(
    ingestion_id: UUID,
    sample_seq: int,
    failed_symbols: set[str],
    validation_findings: Sequence[RealtimeQuoteFinding],
    uuid_factory: Callable[[], UUID],
) -> tuple[QualityResult, ...]:
    results = [
        QualityResult(
            uuid_factory(),
            ingestion_id,
            DatasetCode.FIVE_LEVEL_QUOTE,
            "auction_quote.missing_symbol",
            QualitySeverity.ERROR,
            QualityStatus.FAILED,
            "provider did not produce an accepted quote for an expected frozen-pool symbol",
            {"symbol": symbol, "sample_seq": sample_seq},
        )
        for symbol in sorted(failed_symbols)
    ]
    for finding in validation_findings:
        severity = finding.severity
        results.append(
            QualityResult(
                uuid_factory(),
                ingestion_id,
                DatasetCode.FIVE_LEVEL_QUOTE,
                finding.rule_code,
                severity,
                QualityStatus.FAILED,
                finding.message,
                finding.natural_key,
            )
        )
    return tuple(results)


def _summary(session: AuctionCollectionSession) -> AuctionCollectionSummary:
    return AuctionCollectionSummary(
        session.status.value,
        session.session_id,
        session.expected_quotes,
        session.successful_quotes,
        session.failed_quotes,
        session.successful_rounds,
        session.partial_rounds,
        session.failed_rounds,
    )
