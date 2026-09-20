"""Atomic PostgreSQL persistence for immutable same-day limit-down snapshots."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from json import dumps
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, RowMapping, text

from market_data_center.domain.ingestion import IngestionRun, QualityResult, RawManifest
from market_data_center.domain.today_limit_down import (
    LimitDownSourceRecord,
    TodayLimitDownDependencies,
    TodayLimitDownMember,
    TodayLimitDownSnapshotStatus,
    UpstreamState,
)

EMPTY_HASH = sha256(b"").hexdigest()
RULE_VERSION = "cn_a_mainboard_limit_down_v1"
ALGORITHM_VERSION = "today_limit_down_snapshot_v1"


@dataclass(frozen=True, slots=True)
class TodayLimitDownFillSummary:
    status: str
    trade_date: date
    version: int
    candidate_count: int
    member_count: int
    rejected_count: int
    snapshot_id: UUID
    idempotent: bool = False


class PostgreSQLTodayLimitDownPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def dependencies(self, trade_date: date) -> TodayLimitDownDependencies:
        with self._engine.connect() as connection:
            is_trading_day = bool(
                connection.scalar(
                    text("""
select is_trading_day from core.trading_calendar
where market='CN_A_SHARE' and trade_date=:trade_date
"""),
                    {"trade_date": trade_date},
                )
            )
            states = {
                str(row.workflow_code): str(row.status)
                for row in connection.execute(
                    text("""
select distinct on (workflow_code) workflow_code, status
from operations.workflow_run
where workflow_code in ('daily_market','stock_daily_indicator')
  and (scheduled_for at time zone 'Asia/Shanghai')::date=:trade_date
order by workflow_code, started_at desc
"""),
                    {"trade_date": trade_date},
                ).all()
            }
            exact_pool = bool(
                connection.scalar(
                    text("""
select exists (
  select 1 from stock_pool.snapshot
  where pool_code='CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN'
    and basis_trade_date=:trade_date and status='ready'
)
"""),
                    {"trade_date": trade_date},
                )
            )
        return TodayLimitDownDependencies(
            trade_date,
            is_trading_day,
            _state(states.get("daily_market")),
            _state(states.get("stock_daily_indicator")),
            exact_pool,
        )

    def create_ingestion_run(self, run: IngestionRun) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text("""
insert into ingestion.ingestion_run (
 ingestion_id,provider_code,dataset_code,status,requested_at,started_at,request_params
) values (:id,:provider,:dataset,:status,:requested,:started,cast(:params as jsonb))
"""),
                {
                    "id": run.ingestion_id,
                    "provider": run.provider_code.value,
                    "dataset": run.dataset_code.value,
                    "status": run.status.value,
                    "requested": run.requested_at,
                    "started": run.started_at,
                    "params": dumps(dict(run.request_params), sort_keys=True),
                },
            )

    def fail_ingestion_run(self, run: IngestionRun) -> None:
        with self._engine.begin() as connection:
            _update_ingestion_run(connection, run)

    def commit_deferred(
        self, trade_date: date, reasons: Sequence[str]
    ) -> TodayLimitDownFillSummary:
        input_hash = _hash({"trade_date": trade_date.isoformat(), "reasons": sorted(reasons)})
        with self._engine.begin() as connection:
            existing = _existing(connection, trade_date, input_hash)
            if existing is not None:
                return _summary(existing, True)
            version = _next_version(connection, trade_date)
            snapshot_id = uuid4()
            connection.execute(
                text("""
insert into today_limit_down.snapshot (
 snapshot_id,trade_date,version,status,member_count,candidate_count,rejected_count,
 content_hash,input_hash,rule_version,algorithm_version,generated_at
) values (:id,:date,:version,'deferred',0,0,0,:content,:input,:rule,:algorithm,:now)
"""),
                {
                    "id": snapshot_id,
                    "date": trade_date,
                    "version": version,
                    "content": EMPTY_HASH,
                    "input": input_hash,
                    "rule": RULE_VERSION,
                    "algorithm": ALGORITHM_VERSION,
                    "now": datetime.now(UTC),
                },
            )
            _insert_snapshot_quality(connection, snapshot_id, reasons, "error")
            return TodayLimitDownFillSummary("deferred", trade_date, version, 0, 0, 0, snapshot_id)

    def commit_failed(
        self,
        trade_date: date,
        run: IngestionRun,
        reason: str,
        manifest: RawManifest | None = None,
        quality: Sequence[QualityResult] = (),
    ) -> TodayLimitDownFillSummary:
        input_hash = _hash(
            {"trade_date": trade_date.isoformat(), "source_failure": reason, "status": "failed"}
        )
        with self._engine.begin() as connection:
            if manifest is None:
                _update_ingestion_run(connection, run)
            else:
                self._insert_ingestion_artifacts(connection, run, manifest, (), quality, set())
            existing = _existing(connection, trade_date, input_hash)
            if existing is not None:
                return _summary(existing, True)
            version = _next_version(connection, trade_date)
            snapshot_id = uuid4()
            calculation_id = uuid4()
            now = datetime.now(UTC)
            _insert_calculation(
                connection,
                calculation_id,
                trade_date,
                input_hash,
                run.ingestion_id,
                now,
                0,
                "failed",
                reason,
            )
            _insert_snapshot(
                connection,
                snapshot_id,
                calculation_id,
                trade_date,
                version,
                "failed",
                0,
                0,
                0,
                EMPTY_HASH,
                input_hash,
                run.ingestion_id,
                now,
            )
            _insert_snapshot_quality(connection, snapshot_id, (reason,), "error")
            return TodayLimitDownFillSummary("failed", trade_date, version, 0, 0, 0, snapshot_id)

    def commit_snapshot(
        self,
        *,
        trade_date: date,
        requested_status: TodayLimitDownSnapshotStatus,
        run: IngestionRun,
        manifest: RawManifest,
        source_records: Sequence[LimitDownSourceRecord],
        ingestion_quality: Sequence[QualityResult],
    ) -> TodayLimitDownFillSummary:
        if manifest.ingestion_id != run.ingestion_id or manifest.row_count != run.fetched_rows:
            raise ValueError("Raw manifest and ingestion run are incoherent")
        if any(record.trade_date != trade_date for record in source_records):
            raise ValueError("source observation date does not match snapshot date")
        with self._engine.begin() as connection:
            pool = (
                connection.execute(
                    text("""
select snapshot_id, calculation_id, member_count
from stock_pool.snapshot
where pool_code='CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN'
  and basis_trade_date=:trade_date and status='ready'
order by version desc limit 1
"""),
                    {"trade_date": trade_date},
                )
                .mappings()
                .one()
            )
            candidates = (
                connection.execute(
                    text("""
select m.symbol, split_part(m.symbol,':',2) code,
       nh.name historical_name, nh.ingestion_id name_ingestion_id,
       db.close, db.previous_close, db.ingestion_id daily_bar_ingestion_id,
       ple.limit_price, dpl.calculation_id pool_calculation_id,
       ind.free_float_shares, ind.ingestion_id indicator_ingestion_id,
       e.ingestion_id order_book_ingestion_id,
       e.ask1_price,e.ask1_volume,e.ask2_price,e.ask2_volume,e.ask3_price,e.ask3_volume,
       e.ask4_price,e.ask4_volume,e.ask5_price,e.ask5_volume
from stock_pool.member m
join stock_pool.snapshot s on s.snapshot_id=m.snapshot_id
left join derived.price_limit_event ple on ple.calculation_id=s.calculation_id
 and ple.symbol=m.symbol and ple.trade_date=s.basis_trade_date and ple.direction='down'
left join derived.daily_price_limit dpl on dpl.calculation_id=s.calculation_id
 and dpl.symbol=m.symbol and dpl.trade_date=s.basis_trade_date
left join core.daily_bar db on db.symbol=m.symbol and db.trade_date=s.basis_trade_date
left join core.stock_daily_indicator ind on ind.symbol=m.symbol
 and ind.trade_date=s.basis_trade_date
left join core.security_name_history nh on nh.symbol=m.symbol
 and nh.effective_from<=s.basis_trade_date
 and (nh.effective_to is null or nh.effective_to>=s.basis_trade_date)
left join realtime.eod_quote_snapshot e on e.symbol=m.symbol
 and e.trade_date=s.basis_trade_date
where m.snapshot_id=:snapshot_id and m.direction='down'
order by m.symbol
"""),
                    {"snapshot_id": pool["snapshot_id"]},
                )
                .mappings()
                .all()
            )
            if len(candidates) != pool["member_count"]:
                raise RuntimeError("exact ready down-pool candidate count is incoherent")
            source_by_symbol = {record.symbol: record for record in source_records}
            members: list[TodayLimitDownMember] = []
            member_rows: list[dict[str, object]] = []
            partial_reasons: list[str] = []
            warning_reasons: list[str] = []
            for row in candidates:
                missing = _missing_core(row)
                if missing:
                    partial_reasons.append(f"missing_{missing}:{row['symbol']}")
                    continue
                source = source_by_symbol.get(str(row["symbol"]))
                if source is None:
                    partial_reasons.append(f"missing_source_observation:{row['symbol']}")
                if row["order_book_ingestion_id"] is None:
                    warning_reasons.append(f"missing_order_book:{row['symbol']}")
                elif any(
                    (row[f"ask{level}_price"] is None) != (row[f"ask{level}_volume"] is None)
                    for level in range(1, 6)
                ):
                    warning_reasons.append(f"invalid_order_book_pairs:{row['symbol']}")
                elif row["ask1_price"] is not None and Decimal(row["ask1_price"]) != Decimal(
                    row["limit_price"]
                ):
                    warning_reasons.append(f"ask1_not_at_limit:{row['symbol']}")
                member = _member(row, source)
                members.append(member)
                member_rows.append(
                    _member_params(member, row, source, run.ingestion_id, manifest.raw_id)
                )
            input_hash = _hash(
                {
                    "pool_snapshot_id": str(pool["snapshot_id"]),
                    "source_sha256": manifest.content_sha256,
                    "members": [_content(member) for member in members],
                    "upstream_lineage": [
                        {
                            "symbol": str(row["symbol"]),
                            "daily": str(row["daily_bar_ingestion_id"]),
                            "indicator": str(row["indicator_ingestion_id"]),
                            "name": str(row["name_ingestion_id"]),
                            "order_book": str(row["order_book_ingestion_id"]),
                        }
                        for row in candidates
                    ],
                    "requested_status": requested_status.value,
                }
            )
            existing = _existing(connection, trade_date, input_hash)
            self._insert_ingestion_artifacts(
                connection,
                run,
                manifest,
                source_records,
                ingestion_quality,
                {str(row["symbol"]) for row in candidates},
            )
            if existing is not None:
                return _summary(existing, True)
            version = _next_version(connection, trade_date)
            snapshot_id = uuid4()
            calculation_id = uuid4()
            rejected = len(candidates) - len(members)
            status = (
                TodayLimitDownSnapshotStatus.PARTIAL
                if partial_reasons or requested_status is TodayLimitDownSnapshotStatus.PARTIAL
                else TodayLimitDownSnapshotStatus.READY
            )
            now = datetime.now(UTC)
            _insert_calculation(
                connection,
                calculation_id,
                trade_date,
                input_hash,
                run.ingestion_id,
                now,
                len(members),
                "succeeded",
                None,
            )
            _insert_snapshot(
                connection,
                snapshot_id,
                calculation_id,
                trade_date,
                version,
                status.value,
                len(members),
                len(candidates),
                rejected,
                _hash([_content(member) for member in members]),
                input_hash,
                run.ingestion_id,
                now,
            )
            for item in member_rows:
                item["snapshot_id"] = snapshot_id
            if member_rows:
                connection.execute(_INSERT_MEMBER, member_rows)
            _insert_snapshot_quality(connection, snapshot_id, partial_reasons, "error")
            _insert_snapshot_quality(connection, snapshot_id, warning_reasons, "warning")
            return TodayLimitDownFillSummary(
                status.value,
                trade_date,
                version,
                len(candidates),
                len(members),
                rejected,
                snapshot_id,
            )

    @staticmethod
    def _insert_ingestion_artifacts(
        connection: Connection,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[LimitDownSourceRecord],
        quality: Sequence[QualityResult],
        supported_symbols: set[str],
    ) -> None:
        connection.execute(
            text("""
insert into ingestion.raw_manifest (
 raw_id,ingestion_id,storage_backend,object_path,file_format,content_sha256,
 byte_size,row_count,schema_version
) values (:raw,:ingestion,:backend,:path,:format,:sha,:bytes,:rows,:schema)
"""),
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
        observations = [record for record in records if record.symbol in supported_symbols]
        if observations:
            connection.execute(
                text("""
insert into today_limit_down.source_observation (
 ingestion_id,raw_id,symbol,trade_date,source_code,observed_at,source_name,
 first_limit_down_at,last_limit_down_at,open_count,consecutive_limit_down_days,
 source_reported_sealed_funds_cny
) values (:ingestion,:raw,:symbol,:date,:source,:observed,:name,:first,:last,:opens,
 :consecutive,:funds)
"""),
                [
                    {
                        "ingestion": run.ingestion_id,
                        "raw": manifest.raw_id,
                        "symbol": record.symbol,
                        "date": record.trade_date,
                        "source": record.source_code,
                        "observed": run.finished_at,
                        "name": record.source_name,
                        "first": record.first_limit_down_at,
                        "last": record.last_limit_down_at,
                        "opens": record.open_count,
                        "consecutive": record.consecutive_limit_down_days,
                        "funds": record.source_reported_sealed_funds_cny,
                    }
                    for record in observations
                ],
            )
        if quality:
            connection.execute(
                text("""
insert into audit.quality_result (
 quality_result_id,ingestion_id,dataset_code,rule_code,severity,status,natural_key,message,details
) values (:id,:ingestion,:dataset,:rule,:severity,:status,cast(:key as jsonb),:message,
 cast(:details as jsonb))
"""),
                [
                    {
                        "id": item.quality_result_id,
                        "ingestion": item.ingestion_id,
                        "dataset": item.dataset_code.value,
                        "rule": item.rule_code,
                        "severity": item.severity.value,
                        "status": item.status.value,
                        "key": dumps(item.natural_key) if item.natural_key else None,
                        "message": item.message,
                        "details": dumps(dict(item.details)),
                    }
                    for item in quality
                ],
            )
        _update_ingestion_run(connection, run)


_INSERT_MEMBER = text("""
insert into today_limit_down.member (
 snapshot_id,symbol,code,historical_name,previous_close,close,limit_price,change_percent,
 free_float_shares,free_float_market_cap_cny,first_limit_down_at,last_limit_down_at,open_count,
 consecutive_limit_down_days,duration_semantics,source_reported_sealed_funds_cny,
 closing_ask1_price,closing_ask1_volume_shares,closing_ask2_price,closing_ask2_volume_shares,
 closing_ask3_price,closing_ask3_volume_shares,closing_ask4_price,closing_ask4_volume_shares,
 closing_ask5_price,closing_ask5_volume_shares,closing_ask1_sealing_amount_cny,
 daily_bar_ingestion_id,indicator_ingestion_id,name_ingestion_id,pool_calculation_id,
 source_observation_ingestion_id,source_observation_raw_id,order_book_ingestion_id
) values (
 :snapshot_id,:symbol,:code,:name,:previous,:close,:limit,:change,:shares,:cap,:first,:last,
 :opens,:consecutive,'unavailable_without_event_stream',:source_funds,
 :ask1_price,:ask1_volume,:ask2_price,:ask2_volume,:ask3_price,:ask3_volume,
 :ask4_price,:ask4_volume,:ask5_price,:ask5_volume,:ask1_amount,
 :daily_ingestion,:indicator_ingestion,:name_ingestion,:pool_calculation,
 :source_ingestion,:source_raw,:order_book_ingestion
)
""")


def _state(value: object) -> UpstreamState:
    if value in {state.value for state in UpstreamState if state is not UpstreamState.MISSING}:
        return UpstreamState(str(value))
    return UpstreamState.MISSING


def _missing_core(row: RowMapping) -> str | None:
    for key in (
        "historical_name",
        "name_ingestion_id",
        "close",
        "previous_close",
        "daily_bar_ingestion_id",
        "limit_price",
        "pool_calculation_id",
        "free_float_shares",
        "indicator_ingestion_id",
    ):
        if row[key] is None:
            return key
    return None


def _member(row: RowMapping, source: LimitDownSourceRecord | None) -> TodayLimitDownMember:
    close = Decimal(row["close"])
    previous = Decimal(row["previous_close"])
    limit_price = Decimal(row["limit_price"])
    asks: dict[str, Decimal | int | None] = {}
    for level in range(1, 6):
        price = row[f"ask{level}_price"]
        volume = row[f"ask{level}_volume"]
        paired = (price is None) == (volume is None)
        asks[f"closing_ask{level}_price"] = Decimal(price) if paired and price is not None else None
        asks[f"closing_ask{level}_volume_shares"] = (
            int(volume) if paired and volume is not None else None
        )
    ask1_price = asks["closing_ask1_price"]
    ask1_volume = asks["closing_ask1_volume_shares"]
    amount = (
        ask1_price * ask1_volume
        if isinstance(ask1_price, Decimal)
        and isinstance(ask1_volume, int)
        and ask1_price == limit_price
        else None
    )
    return TodayLimitDownMember(
        symbol=str(row["symbol"]),
        code=str(row["code"]),
        historical_name=str(row["historical_name"]),
        previous_close=previous,
        close=close,
        limit_price=limit_price,
        change_percent=((close / previous - Decimal(1)) * Decimal(100)).quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        ),
        free_float_shares=int(row["free_float_shares"]),
        free_float_market_cap_cny=close * int(row["free_float_shares"]),
        first_limit_down_at=None,
        last_limit_down_at=source.last_limit_down_at if source else None,
        open_count=source.open_count if source else None,
        consecutive_limit_down_days=source.consecutive_limit_down_days if source else None,
        source_reported_sealed_funds_cny=(
            source.source_reported_sealed_funds_cny if source else None
        ),
        closing_ask1_price=asks["closing_ask1_price"],  # type: ignore[arg-type]
        closing_ask1_volume_shares=asks["closing_ask1_volume_shares"],  # type: ignore[arg-type]
        closing_ask2_price=asks["closing_ask2_price"],  # type: ignore[arg-type]
        closing_ask2_volume_shares=asks["closing_ask2_volume_shares"],  # type: ignore[arg-type]
        closing_ask3_price=asks["closing_ask3_price"],  # type: ignore[arg-type]
        closing_ask3_volume_shares=asks["closing_ask3_volume_shares"],  # type: ignore[arg-type]
        closing_ask4_price=asks["closing_ask4_price"],  # type: ignore[arg-type]
        closing_ask4_volume_shares=asks["closing_ask4_volume_shares"],  # type: ignore[arg-type]
        closing_ask5_price=asks["closing_ask5_price"],  # type: ignore[arg-type]
        closing_ask5_volume_shares=asks["closing_ask5_volume_shares"],  # type: ignore[arg-type]
        closing_ask1_sealing_amount_cny=amount,
    )


def _member_params(
    member: TodayLimitDownMember,
    row: RowMapping,
    source: LimitDownSourceRecord | None,
    source_ingestion_id: UUID,
    raw_id: UUID,
) -> dict[str, object]:
    result: dict[str, object] = {
        "symbol": member.symbol,
        "code": member.code,
        "name": member.historical_name,
        "previous": member.previous_close,
        "close": member.close,
        "limit": member.limit_price,
        "change": member.change_percent,
        "shares": member.free_float_shares,
        "cap": member.free_float_market_cap_cny,
        "first": member.first_limit_down_at,
        "last": member.last_limit_down_at,
        "opens": member.open_count,
        "consecutive": member.consecutive_limit_down_days,
        "source_funds": member.source_reported_sealed_funds_cny,
        "ask1_amount": member.closing_ask1_sealing_amount_cny,
        "daily_ingestion": row["daily_bar_ingestion_id"],
        "indicator_ingestion": row["indicator_ingestion_id"],
        "name_ingestion": row["name_ingestion_id"],
        "pool_calculation": row["pool_calculation_id"],
        "source_ingestion": source_ingestion_id if source else None,
        "source_raw": raw_id if source else None,
        "order_book_ingestion": row["order_book_ingestion_id"],
    }
    for level in range(1, 6):
        result[f"ask{level}_price"] = getattr(member, f"closing_ask{level}_price")
        result[f"ask{level}_volume"] = getattr(member, f"closing_ask{level}_volume_shares")
    return result


def _content(member: TodayLimitDownMember) -> dict[str, object]:
    return {field: str(getattr(member, field)) for field in member.__dataclass_fields__}


def _hash(value: object) -> str:
    return sha256(
        dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _next_version(connection: Connection, trade_date: date) -> int:
    return int(
        connection.scalar(
            text(
                "select coalesce(max(version),0)+1 "
                "from today_limit_down.snapshot where trade_date=:d"
            ),
            {"d": trade_date},
        )
    )


def _existing(connection: Connection, trade_date: date, input_hash: str) -> RowMapping | None:
    result: RowMapping | None = (
        connection.execute(
            text("""
select snapshot_id,trade_date,version,status,candidate_count,member_count,rejected_count
from today_limit_down.snapshot where trade_date=:d and input_hash=:h
"""),
            {"d": trade_date, "h": input_hash},
        )
        .mappings()
        .one_or_none()
    )
    return result


def _summary(row: RowMapping, idempotent: bool) -> TodayLimitDownFillSummary:
    return TodayLimitDownFillSummary(
        str(row["status"]),
        row["trade_date"],
        int(row["version"]),
        int(row["candidate_count"]),
        int(row["member_count"]),
        int(row["rejected_count"]),
        row["snapshot_id"],
        idempotent,
    )


def _insert_snapshot_quality(
    connection: Connection, snapshot_id: UUID, reasons: Sequence[str], severity: str
) -> None:
    if not reasons:
        return
    connection.execute(
        text("""
insert into today_limit_down.calculation_quality (snapshot_id,rule_code,severity,symbol,message)
values (:snapshot,:rule,:severity,:symbol,:message)
"""),
        [
            {
                "snapshot": snapshot_id,
                "rule": reason.split(":", 1)[0],
                "severity": severity,
                "symbol": reason.split(":", 1)[1] if ":" in reason else "",
                "message": reason,
            }
            for reason in dict.fromkeys(reasons)
        ],
    )


def _insert_calculation(
    connection: Connection,
    calculation_id: UUID,
    trade_date: date,
    input_hash: str,
    source_ingestion_id: UUID,
    now: datetime,
    rows: int,
    status: str,
    error: str | None,
) -> None:
    connection.execute(
        text("""
insert into derived.calculation_run (
 calculation_id,calculation_code,algorithm_version,mode,start_date,end_date,status,
 input_watermark,input_hash,requested_at,calculated_at,finished_at,output_rows,error_summary
) values (:id,'today_limit_down_snapshot',:algorithm,'incremental',:date,:date,:status,
 cast(:watermark as jsonb),:input,:now,:now,:now,:rows,:error)
"""),
        {
            "id": calculation_id,
            "algorithm": ALGORITHM_VERSION,
            "date": trade_date,
            "status": status,
            "watermark": dumps({"source_ingestion_id": str(source_ingestion_id)}),
            "input": input_hash,
            "now": now,
            "rows": rows,
            "error": error,
        },
    )


def _insert_snapshot(
    connection: Connection,
    snapshot_id: UUID,
    calculation_id: UUID,
    trade_date: date,
    version: int,
    status: str,
    members: int,
    candidates: int,
    rejected: int,
    content_hash: str,
    input_hash: str,
    source_ingestion_id: UUID,
    now: datetime,
) -> None:
    connection.execute(
        text("""
insert into today_limit_down.snapshot (
 snapshot_id,calculation_id,trade_date,version,status,member_count,candidate_count,
 rejected_count,content_hash,input_hash,rule_version,algorithm_version,
 source_ingestion_id,generated_at
) values (:id,:calculation,:date,:version,:status,:members,:candidates,:rejected,
 :content,:input,:rule,:algorithm,:source,:now)
"""),
        {
            "id": snapshot_id,
            "calculation": calculation_id,
            "date": trade_date,
            "version": version,
            "status": status,
            "members": members,
            "candidates": candidates,
            "rejected": rejected,
            "content": content_hash,
            "input": input_hash,
            "rule": RULE_VERSION,
            "algorithm": ALGORITHM_VERSION,
            "source": source_ingestion_id,
            "now": now,
        },
    )


def _update_ingestion_run(connection: Connection, run: IngestionRun) -> None:
    connection.execute(
        text("""
update ingestion.ingestion_run set status=:status,finished_at=:finished,
 fetched_rows=:fetched,accepted_rows=:accepted,rejected_rows=:rejected,error_summary=:error
where ingestion_id=:id
"""),
        {
            "status": run.status.value,
            "finished": run.finished_at,
            "fetched": run.fetched_rows,
            "accepted": run.accepted_rows,
            "rejected": run.rejected_rows,
            "error": run.error_summary,
            "id": run.ingestion_id,
        },
    )
