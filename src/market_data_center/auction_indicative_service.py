"""Operator-controlled single-symbol current-day auction indicative collection."""

from datetime import UTC, date, datetime
from uuid import uuid4

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
from market_data_center.persistence.auction_indicative_postgres import (
    PostgreSQLAuctionIndicativePersistence,
)
from market_data_center.providers.eastmoney_auction import EastmoneyAuctionIndicativeProvider
from market_data_center.raw_store import LocalRawStore


def collect_current_day_auction_indicative(
    persistence: PostgreSQLAuctionIndicativePersistence,
    raw_store: LocalRawStore,
    symbol: str,
    trade_date: date,
    *,
    now: datetime,
    provider: EastmoneyAuctionIndicativeProvider | None = None,
) -> tuple[IngestionRun, int]:
    if not persistence.is_trading_day(trade_date):
        raise ValueError("requested date is not a known CN_A_SHARE trading day")
    ingestion_id = uuid4()
    started = datetime.now(UTC)
    batch = (provider or EastmoneyAuctionIndicativeProvider()).fetch_current_day(
        symbol, trade_date, now=now
    )
    records = tuple(batch.records)
    stored = raw_store.write_jsonl(
        provider="eastmoney",
        dataset=DatasetCode.CALL_AUCTION_INDICATIVE_DETAIL.value,
        partition_date=trade_date,
        ingestion_id=ingestion_id,
        rows=batch.raw_rows,
        schema_version=batch.schema_version,
    )
    excluded = len(batch.raw_rows) - len(records)
    rejected = 0
    status = IngestionStatus.SUCCEEDED if records else IngestionStatus.PARTIAL
    quality = (
        QualityResult(
            quality_result_id=uuid4(),
            ingestion_id=ingestion_id,
            dataset_code=DatasetCode.CALL_AUCTION_INDICATIVE_DETAIL,
            rule_code="auction_indicative.auction_window_filter",
            severity=QualitySeverity.INFO if records else QualitySeverity.ERROR,
            status=QualityStatus.PASSED if records else QualityStatus.FAILED,
            message=(
                f"accepted {len(records)} current-day auction-window records; "
                f"excluded {excluded} expected out-of-window source rows"
            ),
            natural_key={"symbol": symbol, "trade_date": trade_date.isoformat()},
        ),
    )
    run = IngestionRun(
        ingestion_id=ingestion_id,
        provider_code=ProviderCode.EASTMONEY,
        dataset_code=DatasetCode.CALL_AUCTION_INDICATIVE_DETAIL,
        status=status,
        requested_at=started,
        started_at=started,
        finished_at=datetime.now(UTC),
        request_params=dict(batch.request_params),
        fetched_rows=len(batch.raw_rows),
        accepted_rows=len(records),
        rejected_rows=rejected,
        error_summary=None if records else "no valid auction-window indicative records",
    )
    manifest = RawManifest(
        raw_id=uuid4(),
        ingestion_id=ingestion_id,
        object_path=stored.object_path,
        file_format=RawFileFormat.JSONL,
        content_sha256=stored.content_sha256,
        byte_size=stored.byte_size,
        row_count=stored.row_count,
        schema_version=stored.schema_version,
    )
    version = persistence.commit(run, manifest, quality, records)
    return run, version
