"""PostgreSQL persistence for explicit append-only realtime quote batches."""

from collections.abc import Sequence
from json import dumps
from uuid import UUID

from sqlalchemy import Engine, text
from sqlalchemy.engine import Connection

from market_data_center.domain.ingestion import IngestionRun, QualityResult, RawManifest
from market_data_center.domain.realtime_quote import FiveLevelQuoteSnapshotRecord


class PostgreSQLRealtimeQuotePersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def known_stock_symbols(self, symbols: Sequence[str]) -> set[str]:
        with self._engine.connect() as connection:
            rows = connection.execute(
                text("""select symbol from core.security
                        where symbol = any(:symbols) and security_type='stock'"""),
                {"symbols": list(symbols)},
            ).scalars()
            return set(rows)

    def create_run(self, run: IngestionRun) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text("""insert into ingestion.ingestion_run
                (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,
                 request_params,fetched_rows,accepted_rows,rejected_rows)
                values (
                    :id,:provider,:dataset,:status,:requested,:started,
                    cast(:params as jsonb),0,0,0
                )
                """),
                {
                    "id": run.ingestion_id,
                    "provider": run.provider_code.value,
                    "dataset": run.dataset_code.value,
                    "status": run.status.value,
                    "requested": run.requested_at,
                    "started": run.started_at,
                    "params": dumps(run.request_params, sort_keys=True),
                },
            )

    def fail_run(self, run: IngestionRun) -> None:
        with self._engine.begin() as connection:
            _update_run(connection, run)

    def commit(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: Sequence[QualityResult],
        records: Sequence[FiveLevelQuoteSnapshotRecord],
    ) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text("""insert into ingestion.raw_manifest
                (raw_id,ingestion_id,storage_backend,object_path,file_format,content_sha256,
                 byte_size,row_count,schema_version)
                values (:raw,:ingestion,:backend,:path,:format,:sha,:bytes,:rows,:schema)"""),
                {
                    "raw": manifest.raw_id,
                    "ingestion": manifest.ingestion_id,
                    "backend": manifest.storage_backend,
                    "path": manifest.object_path,
                    "format": manifest.file_format.value,
                    "sha": manifest.content_sha256,
                    "bytes": manifest.byte_size,
                    "rows": manifest.row_count,
                    "schema": manifest.schema_version,
                },
            )
            if records:
                connection.execute(
                    text("""insert into realtime.stock_quote_snapshot (
                        ingestion_id,raw_id,symbol,observed_at,source_timestamp,quote_status,
                        last_price,previous_close,open,high,low,cumulative_volume,cumulative_amount,
                        bid1_price,bid1_volume,bid2_price,bid2_volume,bid3_price,bid3_volume,
                        bid4_price,bid4_volume,bid5_price,bid5_volume,
                        ask1_price,ask1_volume,ask2_price,ask2_volume,ask3_price,ask3_volume,
                        ask4_price,ask4_volume,ask5_price,ask5_volume,source_code
                    ) values (
                        :ingestion,:raw,:symbol,:observed,:source_timestamp,:quote_status,
                        :last_price,:previous_close,:open,:high,:low,:cumulative_volume,
                        :cumulative_amount,
                        :bid1_price,:bid1_volume,:bid2_price,:bid2_volume,:bid3_price,:bid3_volume,
                        :bid4_price,:bid4_volume,:bid5_price,:bid5_volume,
                        :ask1_price,:ask1_volume,:ask2_price,:ask2_volume,:ask3_price,:ask3_volume,
                        :ask4_price,:ask4_volume,:ask5_price,:ask5_volume,'tencent_quote'
                    )"""),
                    [
                        _record_parameters(record, run.ingestion_id, manifest.raw_id)
                        for record in records
                    ],
                )
            for item in quality:
                connection.execute(
                    text("""insert into audit.quality_result
                    (quality_result_id,ingestion_id,dataset_code,rule_code,severity,status,message,
                     natural_key,details) values
                    (:id,:ingestion,:dataset,:rule,:severity,:status,:message,
                     cast(:natural as jsonb),cast(:details as jsonb))"""),
                    {
                        "id": item.quality_result_id,
                        "ingestion": item.ingestion_id,
                        "dataset": item.dataset_code.value,
                        "rule": item.rule_code,
                        "severity": item.severity.value,
                        "status": item.status.value,
                        "message": item.message,
                        "natural": dumps(item.natural_key, default=str, sort_keys=True),
                        "details": dumps(item.details, default=str, sort_keys=True),
                    },
                )
            _update_run(connection, run)


def _update_run(connection: Connection, run: IngestionRun) -> None:
    connection.execute(
        text("""update ingestion.ingestion_run set
            status=:status,finished_at=:finished,fetched_rows=:fetched,
            accepted_rows=:accepted,rejected_rows=:rejected,error_summary=:error
            where ingestion_id=:id"""),
        {
            "id": run.ingestion_id,
            "status": run.status.value,
            "finished": run.finished_at,
            "fetched": run.fetched_rows,
            "accepted": run.accepted_rows,
            "rejected": run.rejected_rows,
            "error": run.error_summary,
        },
    )


def _record_parameters(
    record: FiveLevelQuoteSnapshotRecord, ingestion_id: UUID, raw_id: UUID
) -> dict[str, object]:
    result: dict[str, object] = {
        "ingestion": ingestion_id,
        "raw": raw_id,
        "symbol": record.symbol,
        "observed": record.observed_at,
        "source_timestamp": record.source_timestamp,
        "quote_status": record.quote_status.value,
        "last_price": record.last_price,
        "previous_close": record.previous_close,
        "open": record.open,
        "high": record.high,
        "low": record.low,
        "cumulative_volume": record.cumulative_volume,
        "cumulative_amount": record.cumulative_amount,
    }
    for side, levels in (("bid", record.bid_levels), ("ask", record.ask_levels)):
        for level in levels:
            result[f"{side}{level.level}_price"] = level.price
            result[f"{side}{level.level}_volume"] = level.volume
    return result
