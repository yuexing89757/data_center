"""Collect end-of-day and call-auction five-level quote snapshots for limit-up pool members."""

from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import Engine, text

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
    CallAuctionSnapshotRecord,
    EodQuoteSnapshotRecord,
    FiveLevelQuoteSnapshotRecord,
    RealtimeQuoteFinding,
    validate_realtime_quotes,
)
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.providers.pytdx_hq import PytdxHqProvider
from market_data_center.raw_store import LocalRawStore
from market_data_center.settings import PytdxHqSettings, WorkerSettings

LIMIT_UP_POOL_CODE = "CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP"
SHANGHAI_TIME_ZONE = ZoneInfo("Asia/Shanghai")


class EodQuoteSnapshotUnavailable(RuntimeError):
    """The exact live-date ready pool required for collection is unavailable."""


def _limit_up_symbols(engine: Engine, trade_date: date) -> list[str]:
    """Return symbols from the latest ready limit-up pool for the given basis date."""
    with engine.connect() as conn:
        snapshot_id = conn.execute(
            text(
                """
                select s.snapshot_id
                from stock_pool.snapshot s
                where s.pool_code = :code
                  and s.basis_trade_date = :d
                  and s.status = 'ready'
                order by s.version desc
                limit 1
                """
            ),
            {"code": LIMIT_UP_POOL_CODE, "d": trade_date},
        ).scalar_one_or_none()
        if snapshot_id is None:
            raise EodQuoteSnapshotUnavailable(
                f"ready limit-up pool is unavailable for {trade_date}"
            )
        rows = conn.execute(
            text(
                """
                select m.symbol
                from stock_pool.member m
                where m.snapshot_id = :snapshot_id
                order by m.symbol
                """
            ),
            {"snapshot_id": snapshot_id},
        ).all()
    return [r[0] for r in rows]


def _to_eod_records(
    quotes: Sequence[FiveLevelQuoteSnapshotRecord],
    trade_date: date,
    upper_limits: dict[str, Decimal],
) -> list[EodQuoteSnapshotRecord]:
    records: list[EodQuoteSnapshotRecord] = []
    for q in quotes:
        bids = q.bid_levels
        asks = q.ask_levels
        bid1 = bids[0] if bids else None
        seal = None
        if bid1 and bid1.price is not None and bid1.volume is not None:
            limit = upper_limits.get(q.symbol)
            if limit is not None and bid1.price >= limit:
                seal = bid1.price * bid1.volume
        records.append(
            EodQuoteSnapshotRecord(
                symbol=q.symbol,
                trade_date=trade_date,
                last_price=q.last_price,
                previous_close=q.previous_close,
                bid1_price=bids[0].price if len(bids) > 0 else None,
                bid1_volume=bids[0].volume if len(bids) > 0 else None,
                bid2_price=bids[1].price if len(bids) > 1 else None,
                bid2_volume=bids[1].volume if len(bids) > 1 else None,
                bid3_price=bids[2].price if len(bids) > 2 else None,
                bid3_volume=bids[2].volume if len(bids) > 2 else None,
                bid4_price=bids[3].price if len(bids) > 3 else None,
                bid4_volume=bids[3].volume if len(bids) > 3 else None,
                bid5_price=bids[4].price if len(bids) > 4 else None,
                bid5_volume=bids[4].volume if len(bids) > 4 else None,
                ask1_price=asks[0].price if len(asks) > 0 else None,
                ask1_volume=asks[0].volume if len(asks) > 0 else None,
                ask2_price=asks[1].price if len(asks) > 1 else None,
                ask2_volume=asks[1].volume if len(asks) > 1 else None,
                ask3_price=asks[2].price if len(asks) > 2 else None,
                ask3_volume=asks[2].volume if len(asks) > 2 else None,
                ask4_price=asks[3].price if len(asks) > 3 else None,
                ask4_volume=asks[3].volume if len(asks) > 3 else None,
                ask5_price=asks[4].price if len(asks) > 4 else None,
                ask5_volume=asks[4].volume if len(asks) > 4 else None,
                seal_amount=seal,
                source_code="pytdx_hq",
            )
        )
    return records


def _to_auction_records(
    quotes: Sequence[FiveLevelQuoteSnapshotRecord], trade_date: date
) -> list[CallAuctionSnapshotRecord]:
    records: list[CallAuctionSnapshotRecord] = []
    for q in quotes:
        premium = None
        if q.last_price is not None and q.previous_close is not None and q.previous_close > 0:
            premium = (q.last_price - q.previous_close) / q.previous_close * Decimal(100)
        records.append(
            CallAuctionSnapshotRecord(
                symbol=q.symbol,
                trade_date=trade_date,
                last_price=q.last_price,
                previous_close=q.previous_close,
                cumulative_volume=q.cumulative_volume,
                cumulative_amount=q.cumulative_amount,
                auction_premium_pct=premium,
                source_code="pytdx_hq",
            )
        )
    return records


def _upper_limits(engine: Engine, trade_date: date, symbols: list[str]) -> dict[str, Decimal]:
    """Fetch daily price limits for the given symbols and date."""
    if not symbols:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                select symbol, upper_limit
                from derived.daily_price_limit
                where trade_date = :d and symbol = any(:syms)
                """
            ),
            {"d": trade_date, "syms": symbols},
        ).all()
    return {r[0]: r[1] for r in rows}


def _start_run(
    engine: Engine, dataset: DatasetCode, trade_date: date
) -> tuple[IngestionRun, PostgreSQLPersistence]:
    persistence = PostgreSQLPersistence(engine)
    now = datetime.now(UTC)
    run = IngestionRun(
        ingestion_id=uuid4(),
        provider_code=ProviderCode.PYTDX_HQ,
        dataset_code=dataset,
        status=IngestionStatus.RUNNING,
        requested_at=now,
        started_at=now,
        request_params={"trade_date": trade_date.isoformat()},
    )
    persistence.create_ingestion_run(run)
    return run, persistence


def _finish_run(
    run: IngestionRun,
    *,
    fetched_rows: int,
    accepted_rows: int,
    rejected_rows: int,
    error_summary: str | None = None,
) -> IngestionRun:
    if error_summary is not None and accepted_rows == 0:
        status = IngestionStatus.FAILED
    elif rejected_rows:
        status = IngestionStatus.PARTIAL
    else:
        status = IngestionStatus.SUCCEEDED
    return replace(
        run,
        status=status,
        finished_at=datetime.now(UTC),
        fetched_rows=fetched_rows,
        accepted_rows=accepted_rows,
        rejected_rows=rejected_rows,
        error_summary=error_summary,
    )


def collect_eod_quotes(
    engine: Engine,
    trade_date: date,
    *,
    raw_store: LocalRawStore | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> int:
    """Collect end-of-day five-level quotes for the day's limit-up pool."""
    now = clock()
    if now.astimezone(SHANGHAI_TIME_ZONE).date() != trade_date:
        raise EodQuoteSnapshotUnavailable(
            "end-of-day quotes are live facts and cannot backfill a different trade date"
        )
    symbols = _limit_up_symbols(engine, trade_date)
    if not symbols:
        print(f"no limit-up pool members for {trade_date}")
        return 0
    print(f"collecting eod quotes for {len(symbols)} limit-up symbols")
    upper_limits = _upper_limits(engine, trade_date, symbols)
    run, persistence = _start_run(engine, DatasetCode.EOD_QUOTE_SNAPSHOT, trade_date)
    try:
        with PytdxHqProvider(PytdxHqSettings()) as provider:
            fetch = provider.fetch_five_level_quotes(symbols)
        store = raw_store or LocalRawStore(WorkerSettings().raw_data_root)  # type: ignore[call-arg]
        stored = store.write_jsonl(
            provider=ProviderCode.PYTDX_HQ.value,
            dataset=DatasetCode.EOD_QUOTE_SNAPSHOT.value,
            partition_date=trade_date,
            ingestion_id=run.ingestion_id,
            rows=fetch.raw_rows,
            schema_version=fetch.schema_version,
        )
        manifest = RawManifest(
            uuid4(),
            run.ingestion_id,
            stored.object_path,
            RawFileFormat.JSONL,
            stored.content_sha256,
            stored.byte_size,
            stored.row_count,
            stored.schema_version,
        )
        validation = validate_realtime_quotes(
            fetch.records,
            known_symbols=set(symbols),
            known_stock_symbols=set(symbols),
            now=max((record.observed_at for record in fetch.records), default=clock()),
        )
    except Exception as error:
        failed = _finish_run(
            run,
            fetched_rows=0,
            accepted_rows=0,
            rejected_rows=0,
            error_summary=f"{type(error).__name__}: {error}",
        )
        persistence.fail_ingestion_run(failed)
        raise
    records = _to_eod_records(validation.accepted, trade_date, upper_limits)
    accepted_symbols = {record.symbol for record in validation.accepted}
    failed_symbols = set(symbols) - accepted_symbols
    quality_results = _eod_quality_results(run.ingestion_id, failed_symbols, validation.findings)
    completed = _finish_run(
        run,
        fetched_rows=len(fetch.requested_symbols),
        accepted_rows=len(records),
        rejected_rows=len(failed_symbols),
        error_summary=(
            f"{len(failed_symbols)} symbol(s) had no usable quote" if failed_symbols else None
        ),
    )
    persistence.commit_eod_quotes(completed, records, manifest, quality_results)
    print(f"eod quotes: {len(records)} snapshots written")
    return len(records)


def _eod_quality_results(
    ingestion_id: UUID,
    failed_symbols: set[str],
    findings: Sequence[RealtimeQuoteFinding],
) -> tuple[QualityResult, ...]:
    results = [
        QualityResult(
            uuid4(),
            ingestion_id,
            DatasetCode.EOD_QUOTE_SNAPSHOT,
            "eod_quote.missing_symbol",
            QualitySeverity.ERROR,
            QualityStatus.FAILED,
            "provider did not produce an accepted quote for an expected pool symbol",
            {"symbol": symbol},
        )
        for symbol in sorted(failed_symbols)
    ]
    results.extend(
        QualityResult(
            uuid4(),
            ingestion_id,
            DatasetCode.EOD_QUOTE_SNAPSHOT,
            finding.rule_code,
            finding.severity,
            QualityStatus.FAILED,
            finding.message,
            finding.natural_key,
        )
        for finding in findings
    )
    return tuple(results)


def collect_call_auction(engine: Engine, trade_date: date) -> int:
    """Collect call-auction snapshots for the day's limit-up pool."""
    symbols = _limit_up_symbols(engine, trade_date)
    if not symbols:
        print(f"no limit-up pool members for {trade_date}")
        return 0
    print(f"collecting call-auction for {len(symbols)} limit-up symbols")
    run, persistence = _start_run(engine, DatasetCode.CALL_AUCTION_SNAPSHOT, trade_date)
    try:
        with PytdxHqProvider(PytdxHqSettings()) as provider:
            fetch = provider.fetch_five_level_quotes(symbols)
    except Exception as error:
        failed = _finish_run(
            run,
            fetched_rows=0,
            accepted_rows=0,
            rejected_rows=0,
            error_summary=f"{type(error).__name__}: {error}",
        )
        persistence.fail_ingestion_run(failed)
        raise
    records = _to_auction_records(fetch.records, trade_date)
    completed = _finish_run(
        run,
        fetched_rows=len(fetch.requested_symbols),
        accepted_rows=len(records),
        rejected_rows=len(fetch.failed_symbols),
        error_summary=(
            f"{len(fetch.failed_symbols)} symbol(s) had no usable quote"
            if fetch.failed_symbols
            else None
        ),
    )
    persistence.commit_call_auction(completed, records)
    print(f"call-auction: {len(records)} snapshots written")
    return len(records)
