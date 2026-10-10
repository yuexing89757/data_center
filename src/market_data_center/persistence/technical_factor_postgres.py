"""Atomic publication of provider technical-factor snapshots."""

from collections.abc import Collection, Mapping, Sequence
from datetime import date
from decimal import Decimal
from json import dumps

from sqlalchemy import text

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    RawManifest,
)
from market_data_center.domain.records import IngestionEnvelope
from market_data_center.domain.stock_technical_factor import StockTechnicalFactorSnapshotRecord
from market_data_center.persistence.postgres import (
    INSERT_QUALITY_RESULT,
    UPDATE_INGESTION_RUN,
    PostgreSQLPersistence,
)

UPSERT_TECHNICAL_FACTOR = text("""
insert into core.stock_technical_factor_snapshot (
    symbol, trade_date, market, source_code, factor_catalog_version,
    unadjusted, forward_adjusted, backward_adjusted, common, ingestion_id
) values (
    :symbol, :trade_date, :market, :source_code, :factor_catalog_version,
    cast(:unadjusted as jsonb), cast(:forward_adjusted as jsonb),
    cast(:backward_adjusted as jsonb), cast(:common as jsonb), :ingestion_id
)
on conflict (symbol, trade_date) do update set
    market = excluded.market,
    source_code = excluded.source_code,
    factor_catalog_version = excluded.factor_catalog_version,
    unadjusted = excluded.unadjusted,
    forward_adjusted = excluded.forward_adjusted,
    backward_adjusted = excluded.backward_adjusted,
    common = excluded.common,
    ingestion_id = excluded.ingestion_id
""")


def _numeric_json(values: Mapping[str, Decimal | int | None]) -> str:
    parts: list[str] = []
    for key, value in values.items():
        if value is None:
            encoded = "null"
        elif (isinstance(value, Decimal) and value.is_finite()) or type(value) is int:
            encoded = str(value)
        else:
            raise ValueError("Technical-factor JSON requires finite Decimal, int or None")
        parts.append(f"{dumps(key)}:{encoded}")
    return "{" + ",".join(parts) + "}"


class PostgreSQLTechnicalFactorPersistence(PostgreSQLPersistence):
    def technical_factor_securities(self, trade_date: date) -> Collection[str]:
        with self._engine.connect() as connection:
            return set(
                connection.execute(
                    text("""
select symbol from core.security
where security_type = 'stock'
  and (ipo_date is null or ipo_date <= :trade_date)
  and (delisting_date is null or delisting_date >= :trade_date)
"""),
                    {"trade_date": trade_date},
                ).scalars()
            )

    def technical_factor_coverage_floor(self, trade_date: date) -> int:
        with self._engine.connect() as connection:
            previous = connection.execute(
                text("""
select count(*) from core.stock_technical_factor_snapshot snapshot
join ingestion.ingestion_run run using (ingestion_id)
where snapshot.trade_date = (
    select max(s.trade_date) from core.stock_technical_factor_snapshot s
    join ingestion.ingestion_run r using (ingestion_id)
    where s.trade_date < :trade_date and r.status = 'succeeded'
) and run.status = 'succeeded'
group by snapshot.trade_date
"""),
                {"trade_date": trade_date},
            ).scalar_one_or_none()
            if previous is None:
                previous = connection.execute(
                    text("""
select count(*) from core.security
where security_type = 'stock'
  and (ipo_date is null or ipo_date <= :trade_date)
  and (delisting_date is null or delisting_date >= :trade_date)
"""),
                    {"trade_date": trade_date},
                ).scalar_one()
        return max(1, (int(previous) * 9 + 9) // 10)

    def commit_technical_factor_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[StockTechnicalFactorSnapshotRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None:
        if (
            run.dataset_code is not DatasetCode.STOCK_TECHNICAL_FACTOR
            or run.provider_code is not ProviderCode.TUSHARE
        ):
            raise ValueError("Technical-factor run has an unexpected dataset or provider")
        if run.status is not IngestionStatus.SUCCEEDED or not records:
            raise ValueError("Technical-factor publication requires a nonempty successful batch")
        if run.accepted_rows != len(records) or run.fetched_rows < len(records):
            raise ValueError("Technical-factor publication counts do not match its records")
        if manifest is None:
            if run.replayed_from_raw_id is None:
                raise ValueError("Live technical-factor batch requires a Raw manifest")
        elif manifest.ingestion_id != run.ingestion_id or run.replayed_from_raw_id is not None:
            raise ValueError("Technical-factor manifest does not match its run")
        if any(envelope.ingestion_id != run.ingestion_id for envelope in records):
            raise ValueError("Technical-factor envelope does not match its run")
        if any(
            result.ingestion_id != run.ingestion_id or result.dataset_code != run.dataset_code
            for result in quality_results
        ):
            raise ValueError("Technical-factor quality result does not match its run")
        parameters: list[dict[str, object]] = []
        keys: set[tuple[str, date]] = set()
        for envelope in records:
            record = envelope.record
            key = record.symbol, record.trade_date
            if key in keys or record.source_code != run.provider_code.value:
                raise ValueError("Technical-factor duplicate key or source mismatch")
            keys.add(key)
            if run.request_params.get("trade_date") != record.trade_date.isoformat():
                raise ValueError("Technical-factor date does not match its requested date")
            parameters.append(
                {
                    "symbol": record.symbol,
                    "trade_date": record.trade_date,
                    "market": record.market.value,
                    "source_code": record.source_code,
                    "factor_catalog_version": record.factor_catalog_version,
                    "unadjusted": _numeric_json(record.unadjusted),
                    "forward_adjusted": _numeric_json(record.forward_adjusted),
                    "backward_adjusted": _numeric_json(record.backward_adjusted),
                    "common": _numeric_json(record.common),
                    "ingestion_id": envelope.ingestion_id,
                }
            )
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if quality_results:
                connection.execute(INSERT_QUALITY_RESULT, self._quality_parameters(quality_results))
            connection.execute(
                text("""
delete from core.stock_technical_factor_snapshot
where trade_date = :trade_date and not (symbol = any(:symbols))
"""),
                {
                    "trade_date": records[0].record.trade_date,
                    "symbols": [envelope.record.symbol for envelope in records],
                },
            )
            connection.execute(UPSERT_TECHNICAL_FACTOR, parameters)
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))
