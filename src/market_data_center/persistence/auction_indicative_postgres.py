"""Atomic PostgreSQL persistence for auction indicative detail attempts."""

from collections.abc import Sequence

from sqlalchemy import Engine, text

from market_data_center.domain.auction_indicative import CallAuctionIndicativeDetailRecord
from market_data_center.domain.ingestion import IngestionRun, QualityResult, RawManifest


class PostgreSQLAuctionIndicativePersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def is_trading_day(self, trade_date: object) -> bool:
        with self._engine.connect() as connection:
            value = connection.execute(
                text("""select is_trading_day from core.trading_calendar
                        where market='CN_A_SHARE' and trade_date=:trade_date"""),
                {"trade_date": trade_date},
            ).scalar_one_or_none()
        return value is True

    def commit(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: Sequence[QualityResult],
        records: Sequence[CallAuctionIndicativeDetailRecord],
    ) -> int:
        with self._engine.begin() as connection:
            version = connection.execute(
                text("""select coalesce(max(version),0)+1
                        from realtime.call_auction_indicative_snapshot
                        where symbol=:symbol and trade_date=:trade_date"""),
                {
                    "symbol": run.request_params["symbol"],
                    "trade_date": run.request_params["trade_date"],
                },
            ).scalar_one()
            connection.execute(
                text("""insert into ingestion.ingestion_run
                (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at,
                 request_params,fetched_rows,accepted_rows,rejected_rows,error_summary)
                values (:id,:provider,:dataset,:status,:requested,:started,:finished,
                        cast(:params as jsonb),
                        :fetched,:accepted,:rejected,:error)"""),
                {
                    "id": run.ingestion_id,
                    "provider": run.provider_code.value,
                    "dataset": run.dataset_code.value,
                    "status": run.status.value,
                    "requested": run.requested_at,
                    "started": run.started_at,
                    "finished": run.finished_at,
                    "params": _json(run.request_params),
                    "fetched": run.fetched_rows,
                    "accepted": run.accepted_rows,
                    "rejected": run.rejected_rows,
                    "error": run.error_summary,
                },
            )
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
            connection.execute(
                text("""insert into realtime.call_auction_indicative_snapshot
                (ingestion_id,raw_id,symbol,trade_date,version,status,source_code,record_count)
                values (:ingestion,:raw,:symbol,:date,:version,:status,'eastmoney',:count)"""),
                {
                    "ingestion": run.ingestion_id,
                    "raw": manifest.raw_id,
                    "symbol": run.request_params["symbol"],
                    "date": run.request_params["trade_date"],
                    "version": version,
                    "status": run.status.value,
                    "count": len(records),
                },
            )
            if records:
                connection.execute(
                    text("""insert into realtime.call_auction_indicative_detail
                    (ingestion_id,symbol,trade_date,source_sequence,observed_at,indicative_price,
                     displayed_volume_shares,source_display_classification)
                    values
                    (:ingestion,:symbol,:date,:sequence,:observed,:price,:volume,:display)"""),
                    [
                        {
                            "ingestion": run.ingestion_id,
                            "symbol": r.symbol,
                            "date": r.trade_date,
                            "sequence": r.source_sequence,
                            "observed": r.observed_at,
                            "price": r.indicative_price,
                            "volume": r.displayed_volume_shares,
                            "display": r.source_display_classification.value,
                        }
                        for r in records
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
                        "natural": _json(item.natural_key),
                        "details": _json(item.details),
                    },
                )
        return int(version)


def _json(value: object) -> str:
    from json import dumps

    return dumps(value, default=str, separators=(",", ":"), sort_keys=True)
