"""Atomic append-only PostgreSQL persistence for Dragon Tiger List snapshots."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from json import dumps
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, text

from market_data_center.domain.dragon_tiger import (
    DragonTigerSnapshotBatch,
    validate_dragon_tiger_batch,
)
from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    QualityResult,
    RawManifest,
)


@dataclass(frozen=True, slots=True)
class DragonTigerCommitSummary:
    snapshot_id: UUID
    trade_date: date
    version: int
    event_count: int
    status: str
    idempotent: bool = False


class PostgreSQLDragonTigerPersistence:
    """Register one Raw-backed immutable source revision in one database transaction."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def commit_snapshot(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        batch: DragonTigerSnapshotBatch,
        quality: Sequence[QualityResult],
    ) -> DragonTigerCommitSummary:
        _coherence(run, manifest, batch, quality)
        with self._engine.begin() as connection:
            connection.execute(
                text("select pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
                {"scope": f"dragon_tiger:{batch.trade_date.isoformat()}"},
            )
            existing = (
                connection.execute(
                    text("""
select snapshot_id,version,event_count,status from dragon_tiger.source_snapshot
where trade_date=:trade_date and input_hash=:input_hash
"""),
                    {"trade_date": batch.trade_date, "input_hash": batch.input_hash},
                )
                .mappings()
                .first()
            )
            if existing is not None:
                run_exists = bool(
                    connection.scalar(
                        text("""
select exists(select 1 from ingestion.ingestion_run where ingestion_id=:ingestion_id)
"""),
                        {"ingestion_id": run.ingestion_id},
                    )
                )
                if not run_exists:
                    _insert_ingestion(connection, run, manifest, quality)
                return DragonTigerCommitSummary(
                    existing["snapshot_id"],
                    batch.trade_date,
                    existing["version"],
                    existing["event_count"],
                    existing["status"],
                    True,
                )
            _validate_against_core(connection, batch)
            version = int(
                connection.scalar(
                    text("""
select coalesce(max(version),0)+1 from dragon_tiger.source_snapshot
where trade_date=:trade_date
"""),
                    {"trade_date": batch.trade_date},
                )
            )
            snapshot_id = uuid4()
            _insert_ingestion(connection, run, manifest, quality)
            connection.execute(
                text("""
insert into dragon_tiger.source_snapshot (
 snapshot_id,ingestion_id,raw_id,trade_date,version,source_code,status,observed_at,
 input_hash,content_hash,observation_count,event_count,partial_reasons
) values (
 :snapshot_id,:ingestion_id,:raw_id,:trade_date,:version,:source_code,:status,
 :observed_at,:input_hash,:content_hash,:observation_count,:event_count,
 cast(:partial_reasons as jsonb)
)
"""),
                {
                    "snapshot_id": snapshot_id,
                    "ingestion_id": run.ingestion_id,
                    "raw_id": manifest.raw_id,
                    "trade_date": batch.trade_date,
                    "version": version,
                    "source_code": batch.source_code,
                    "status": batch.status.value,
                    "observed_at": batch.observed_at,
                    "input_hash": batch.input_hash,
                    "content_hash": batch.content_hash,
                    "observation_count": len(batch.observations),
                    "event_count": len(batch.events),
                    "partial_reasons": dumps(batch.partial_reasons),
                },
            )
            observation_ids = _insert_observations(
                connection, snapshot_id, run.ingestion_id, manifest.raw_id, batch
            )
            seat_ids = _seat_ids(connection, run.ingestion_id, batch)
            event_ids = _insert_events(
                connection,
                snapshot_id,
                version,
                run.ingestion_id,
                manifest.raw_id,
                observation_ids,
                batch,
            )
            _insert_reasons(
                connection,
                run.ingestion_id,
                manifest.raw_id,
                observation_ids,
                event_ids,
                batch,
            )
            _insert_activities(
                connection,
                run.ingestion_id,
                manifest.raw_id,
                observation_ids,
                event_ids,
                seat_ids,
                batch,
            )
            _insert_summaries(connection, event_ids, batch)
            if batch.partial_reasons:
                connection.execute(
                    text("""
insert into dragon_tiger.snapshot_quality
 (snapshot_id,rule_code,severity,identity,message)
values (:snapshot_id,'dragon_tiger.partial_source','warning',:identity,:message)
"""),
                    [
                        {
                            "snapshot_id": snapshot_id,
                            "identity": str(index),
                            "message": reason,
                        }
                        for index, reason in enumerate(batch.partial_reasons)
                    ],
                )
            return DragonTigerCommitSummary(
                snapshot_id,
                batch.trade_date,
                version,
                len(batch.events),
                batch.status.value,
            )


def _coherence(
    run: IngestionRun,
    manifest: RawManifest,
    batch: DragonTigerSnapshotBatch,
    quality: Sequence[QualityResult],
) -> None:
    if run.dataset_code is not DatasetCode.DRAGON_TIGER:
        raise ValueError("Dragon Tiger persistence requires dragon_tiger dataset")
    if run.status not in {IngestionStatus.SUCCEEDED, IngestionStatus.PARTIAL}:
        raise ValueError("Dragon Tiger persistence requires a terminal successful/partial run")
    expected = (
        IngestionStatus.SUCCEEDED if batch.status.value == "complete" else IngestionStatus.PARTIAL
    )
    if run.status is not expected:
        raise ValueError("ingestion and snapshot statuses are incoherent")
    if manifest.ingestion_id != run.ingestion_id or manifest.row_count != run.fetched_rows:
        raise ValueError("Raw manifest and ingestion run are incoherent")
    if any(result.ingestion_id != run.ingestion_id for result in quality):
        raise ValueError("quality lineage does not match ingestion")
    if any(result.dataset_code is not DatasetCode.DRAGON_TIGER for result in quality):
        raise ValueError("quality dataset does not match Dragon Tiger")


def _validate_against_core(connection: Connection, batch: DragonTigerSnapshotBatch) -> None:
    symbols = sorted({event.symbol for event in batch.events})
    known_symbols = set(
        connection.scalars(
            text("select symbol from core.security where symbol = any(:symbols)"),
            {"symbols": symbols},
        )
    )
    names = {
        (str(row.symbol), batch.trade_date): str(row.name)
        for row in connection.execute(
            text("""
select symbol,name from core.security_name_history
where symbol = any(:symbols) and effective_from <= :trade_date
  and (effective_to is null or effective_to >= :trade_date)
"""),
            {"symbols": symbols, "trade_date": batch.trade_date},
        )
    }
    bars = connection.execute(
        text("""
select symbol,close,previous_close from core.daily_bar
where symbol = any(:symbols) and trade_date=:trade_date
"""),
        {"symbols": symbols, "trade_date": batch.trade_date},
    ).mappings()
    closes: dict[tuple[str, date], Decimal] = {}
    previous: dict[tuple[str, date], Decimal] = {}
    for row in bars:
        key = (str(row["symbol"]), batch.trade_date)
        if isinstance(row["close"], Decimal):
            closes[key] = row["close"]
        if isinstance(row["previous_close"], Decimal):
            previous[key] = row["previous_close"]
    trading_dates = set(
        connection.scalars(
            text("""
select trade_date from core.trading_calendar
where market='CN_A_SHARE' and trade_date=:trade_date and is_trading_day
"""),
            {"trade_date": batch.trade_date},
        )
    )
    result = validate_dragon_tiger_batch(
        batch,
        known_symbols=known_symbols,
        known_trading_dates=trading_dates,
        historical_names=names,
        unadjusted_closes=closes,
        previous_closes=previous,
    )
    if not result.accepted:
        rules = ",".join(sorted({finding.rule_code for finding in result.findings}))
        raise ValueError(f"Dragon Tiger batch failed validation: {rules}")


def _insert_ingestion(
    connection: Connection,
    run: IngestionRun,
    manifest: RawManifest,
    quality: Sequence[QualityResult],
) -> None:
    connection.execute(
        text("""
insert into ingestion.ingestion_run (
 ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at,
 request_params,fetched_rows,accepted_rows,rejected_rows,error_summary,replayed_from_raw_id
) values (
 :id,:provider,:dataset,:status,:requested,:started,:finished,cast(:params as jsonb),
 :fetched,:accepted,:rejected,:error,:replayed
)
"""),
        {
            "id": run.ingestion_id,
            "provider": run.provider_code.value,
            "dataset": run.dataset_code.value,
            "status": run.status.value,
            "requested": run.requested_at,
            "started": run.started_at,
            "finished": run.finished_at,
            "params": dumps(dict(run.request_params), sort_keys=True),
            "fetched": run.fetched_rows,
            "accepted": run.accepted_rows,
            "rejected": run.rejected_rows,
            "error": run.error_summary,
            "replayed": run.replayed_from_raw_id,
        },
    )
    connection.execute(
        text("""
insert into ingestion.raw_manifest (
 raw_id,ingestion_id,storage_backend,object_path,file_format,content_sha256,
 byte_size,row_count,schema_version
) values (:raw_id,:ingestion_id,:backend,:path,:format,:sha,:bytes,:rows,:schema)
"""),
        {
            "raw_id": manifest.raw_id,
            "ingestion_id": manifest.ingestion_id,
            "backend": manifest.storage_backend,
            "path": manifest.object_path,
            "format": manifest.file_format.value,
            "sha": manifest.content_sha256,
            "bytes": manifest.byte_size,
            "rows": manifest.row_count,
            "schema": manifest.schema_version,
        },
    )
    if quality:
        connection.execute(
            text("""
insert into audit.quality_result (
 quality_result_id,ingestion_id,dataset_code,rule_code,severity,status,message,
 natural_key,details
) values (
 :id,:ingestion_id,:dataset,:rule,:severity,:status,:message,
 cast(:natural_key as jsonb),cast(:details as jsonb)
)
"""),
            [
                {
                    "id": row.quality_result_id,
                    "ingestion_id": row.ingestion_id,
                    "dataset": row.dataset_code.value,
                    "rule": row.rule_code,
                    "severity": row.severity.value,
                    "status": row.status.value,
                    "message": row.message,
                    "natural_key": dumps(row.natural_key) if row.natural_key else None,
                    "details": dumps(dict(row.details), sort_keys=True),
                }
                for row in quality
            ],
        )


def _insert_observations(
    connection: Connection,
    snapshot_id: UUID,
    ingestion_id: UUID,
    raw_id: UUID,
    batch: DragonTigerSnapshotBatch,
) -> dict[str, UUID]:
    ids = {row.source_event_key: uuid4() for row in batch.observations}
    if batch.observations:
        connection.execute(
            text("""
insert into dragon_tiger.source_observation (
 observation_id,snapshot_id,ingestion_id,raw_id,source_event_key,symbol,trade_date,
 source_code,observed_at,source_name,source_status_text
) values (
 :id,:snapshot_id,:ingestion_id,:raw_id,:source_event_key,:symbol,:trade_date,
 :source_code,:observed_at,:source_name,:source_status_text
)
"""),
            [
                {
                    "id": ids[row.source_event_key],
                    "snapshot_id": snapshot_id,
                    "ingestion_id": ingestion_id,
                    "raw_id": raw_id,
                    "source_event_key": row.source_event_key,
                    "symbol": row.symbol,
                    "trade_date": row.trade_date,
                    "source_code": row.source_code,
                    "observed_at": row.observed_at,
                    "source_name": row.source_name,
                    "source_status_text": row.source_status_text,
                }
                for row in batch.observations
            ],
        )
    return ids


def _seat_ids(
    connection: Connection, ingestion_id: UUID, batch: DragonTigerSnapshotBatch
) -> dict[str, UUID]:
    result: dict[str, UUID] = {}
    for seat in batch.seats:
        existing = (
            connection.execute(
                text("""
select seat_id,canonical_name,seat_type,broker_name,branch_name,region,valid_to,
 source_name,normalization_status from dragon_tiger.seat
where identity_key=:identity_key and valid_from=:valid_from
"""),
                {"identity_key": seat.identity_key, "valid_from": seat.valid_from},
            )
            .mappings()
            .first()
        )
        expected: Mapping[str, object] = {
            "canonical_name": seat.canonical_name,
            "seat_type": seat.seat_type.value,
            "broker_name": seat.broker_name,
            "branch_name": seat.branch_name,
            "region": seat.region,
            "valid_to": seat.valid_to,
            "source_name": seat.source_name,
            "normalization_status": seat.normalization_status.value,
        }
        if existing is not None:
            if any(existing[key] != value for key, value in expected.items()):
                raise ValueError("seat identity revision conflicts with an immutable interval")
            result[seat.identity_key] = existing["seat_id"]
            continue
        seat_id = uuid4()
        connection.execute(
            text("""
insert into dragon_tiger.seat (
 seat_id,identity_key,canonical_name,seat_type,broker_name,branch_name,region,
 valid_from,valid_to,source_name,normalization_status,source_ingestion_id
) values (
 :seat_id,:identity_key,:canonical_name,:seat_type,:broker_name,:branch_name,:region,
 :valid_from,:valid_to,:source_name,:normalization_status,:ingestion_id
)
"""),
            {
                "seat_id": seat_id,
                "identity_key": seat.identity_key,
                "canonical_name": seat.canonical_name,
                "seat_type": seat.seat_type.value,
                "broker_name": seat.broker_name,
                "branch_name": seat.branch_name,
                "region": seat.region,
                "valid_from": seat.valid_from,
                "valid_to": seat.valid_to,
                "source_name": seat.source_name,
                "normalization_status": seat.normalization_status.value,
                "ingestion_id": ingestion_id,
            },
        )
        result[seat.identity_key] = seat_id
    return result


def _insert_events(
    connection: Connection,
    snapshot_id: UUID,
    revision: int,
    ingestion_id: UUID,
    raw_id: UUID,
    observation_ids: Mapping[str, UUID],
    batch: DragonTigerSnapshotBatch,
) -> dict[tuple[str, date], UUID]:
    ids = {event.natural_key: uuid4() for event in batch.events}
    if batch.events:
        connection.execute(
            text("""
insert into dragon_tiger.event (
 event_id,snapshot_id,observation_id,symbol,trade_date,revision,historical_name,market,
 unadjusted_close,change_percent,turnover_amount_cny,turnover_rate_percent,event_status,
 source_ingestion_id,source_raw_id
) values (
 :event_id,:snapshot_id,:observation_id,:symbol,:trade_date,:revision,:name,:market,
 :close,:change,:turnover,:turnover_rate,:status,:ingestion_id,:raw_id
)
"""),
            [
                {
                    "event_id": ids[event.natural_key],
                    "snapshot_id": snapshot_id,
                    "observation_id": observation_ids[event.source_event_key],
                    "symbol": event.symbol,
                    "trade_date": event.trade_date,
                    "revision": revision,
                    "name": event.historical_name,
                    "market": event.market,
                    "close": event.close,
                    "change": event.change_percent,
                    "turnover": event.turnover_amount_cny,
                    "turnover_rate": event.turnover_rate_percent,
                    "status": event.status.value,
                    "ingestion_id": ingestion_id,
                    "raw_id": raw_id,
                }
                for event in batch.events
            ],
        )
    return ids


def _insert_reasons(
    connection: Connection,
    ingestion_id: UUID,
    raw_id: UUID,
    observation_ids: Mapping[str, UUID],
    event_ids: Mapping[tuple[str, date], UUID],
    batch: DragonTigerSnapshotBatch,
) -> None:
    source_keys = {event.natural_key: event.source_event_key for event in batch.events}
    if batch.reasons:
        connection.execute(
            text("""
insert into dragon_tiger.reason (
 reason_id,event_id,observation_id,reason_code,reason_name,source_original_text,
 display_order,source_numeric_value,source_numeric_unit,source_ingestion_id,source_raw_id
) values (
 :id,:event_id,:observation_id,:code,:name,:original,:display_order,:numeric_value,
 :numeric_unit,:ingestion_id,:raw_id
)
"""),
            [
                {
                    "id": uuid4(),
                    "event_id": event_ids[row.event_key],
                    "observation_id": observation_ids[source_keys[row.event_key]],
                    "code": row.reason_code,
                    "name": row.reason_name,
                    "original": row.source_original_text,
                    "display_order": row.display_order,
                    "numeric_value": row.source_numeric_value,
                    "numeric_unit": row.source_numeric_unit,
                    "ingestion_id": ingestion_id,
                    "raw_id": raw_id,
                }
                for row in batch.reasons
            ],
        )


def _insert_activities(
    connection: Connection,
    ingestion_id: UUID,
    raw_id: UUID,
    observation_ids: Mapping[str, UUID],
    event_ids: Mapping[tuple[str, date], UUID],
    seat_ids: Mapping[str, UUID],
    batch: DragonTigerSnapshotBatch,
) -> None:
    source_keys = {event.natural_key: event.source_event_key for event in batch.events}
    if batch.activities:
        connection.execute(
            text("""
insert into dragon_tiger.seat_activity (
 activity_id,event_id,seat_id,observation_id,side,buy_amount_cny,sell_amount_cny,
 net_amount_cny,buy_rank,sell_rank,source_seat_name,source_order,
 source_ingestion_id,source_raw_id
) values (
 :id,:event_id,:seat_id,:observation_id,:side,:buy,:sell,:net,:buy_rank,:sell_rank,
 :source_name,:source_order,:ingestion_id,:raw_id
)
"""),
            [
                {
                    "id": uuid4(),
                    "event_id": event_ids[row.event_key],
                    "seat_id": seat_ids[row.seat_identity_key],
                    "observation_id": observation_ids[source_keys[row.event_key]],
                    "side": row.side.value,
                    "buy": row.buy_amount_cny,
                    "sell": row.sell_amount_cny,
                    "net": row.net_amount_cny,
                    "buy_rank": row.buy_rank,
                    "sell_rank": row.sell_rank,
                    "source_name": row.source_seat_name,
                    "source_order": row.source_order,
                    "ingestion_id": ingestion_id,
                    "raw_id": raw_id,
                }
                for row in batch.activities
            ],
        )


def _insert_summaries(
    connection: Connection,
    event_ids: Mapping[tuple[str, date], UUID],
    batch: DragonTigerSnapshotBatch,
) -> None:
    if batch.summaries:
        connection.execute(
            text("""
insert into dragon_tiger.event_summary (
 event_id,calculation_version,calculated_at,total_buy_amount_cny,total_sell_amount_cny,
 total_net_amount_cny,institution_buy_amount_cny,institution_sell_amount_cny,
 institution_net_amount_cny,top5_buy_amount_cny,top5_sell_amount_cny,
 top5_buy_concentration_ratio,top5_sell_concentration_ratio,activity_count,
 institution_activity_count
) values (
 :event_id,:version,:calculated_at,:total_buy,:total_sell,:total_net,:institution_buy,
 :institution_sell,:institution_net,:top5_buy,:top5_sell,:top5_buy_ratio,
 :top5_sell_ratio,:activity_count,:institution_count
)
"""),
            [
                {
                    "event_id": event_ids[row.event_key],
                    "version": row.calculation_version,
                    "calculated_at": row.calculated_at,
                    "total_buy": row.total_buy_amount_cny,
                    "total_sell": row.total_sell_amount_cny,
                    "total_net": row.total_net_amount_cny,
                    "institution_buy": row.institution_buy_amount_cny,
                    "institution_sell": row.institution_sell_amount_cny,
                    "institution_net": row.institution_net_amount_cny,
                    "top5_buy": row.top5_buy_amount_cny,
                    "top5_sell": row.top5_sell_amount_cny,
                    "top5_buy_ratio": row.top5_buy_concentration_ratio,
                    "top5_sell_ratio": row.top5_sell_concentration_ratio,
                    "activity_count": row.activity_count,
                    "institution_count": row.institution_activity_count,
                }
                for row in batch.summaries
            ],
        )
