"""Atomic PostgreSQL persistence for DragonTiger facts."""

from collections.abc import Sequence
from datetime import date
from hashlib import sha256
from json import dumps
from typing import cast
from uuid import UUID

from sqlalchemy import Connection, Engine, text

from market_data_center.domain.dragon_tiger import (
    DragonTigerEventRecord,
    SeatTradeRecord,
    TradingSeatType,
    dragon_tiger_content_hash,
)
from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    RawManifest,
)
from market_data_center.dragon_tiger_service import DragonTigerCollectionSummary


class PostgreSQLDragonTigerPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def is_trading_day(self, trade_date: date) -> bool:
        with self._engine.connect() as connection:
            value = connection.scalar(
                text("""
                    select is_trading_day from core.trading_calendar
                    where market='CN_A_SHARE' and trade_date=:trade_date
                """),
                {"trade_date": trade_date},
            )
        return value is True

    def period_start_date(self, trade_date: date, session_count: int) -> date:
        if session_count < 1:
            raise ValueError("session_count must be positive")
        with self._engine.connect() as connection:
            rows = connection.scalars(
                text("""
                    select trade_date from core.trading_calendar
                    where market='CN_A_SHARE' and is_trading_day=true
                      and trade_date <= :trade_date
                    order by trade_date desc limit :session_count
                """),
                {"trade_date": trade_date, "session_count": session_count},
            ).all()
        if len(rows) != session_count:
            raise ValueError("trading calendar cannot resolve the requested period")
        return cast(date, min(rows))

    def known_stock_symbols(self, trade_date: date) -> frozenset[str]:
        with self._engine.connect() as connection:
            symbols = connection.scalars(
                text("""
                    select symbol from core.security
                    where security_type='stock'
                      and (ipo_date is null or ipo_date <= :trade_date)
                      and (delisting_date is null or delisting_date >= :trade_date)
                """),
                {"trade_date": trade_date},
            ).all()
        return frozenset(symbols)

    def known_trading_dates(self, start_date: date, end_date: date) -> frozenset[date]:
        with self._engine.connect() as connection:
            dates = connection.scalars(
                text("""
                    select trade_date from core.trading_calendar
                    where market='CN_A_SHARE' and is_trading_day=true
                      and trade_date between :start_date and :end_date
                """),
                {"start_date": start_date, "end_date": end_date},
            ).all()
        return frozenset(dates)

    def commit_success(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: Sequence[QualityResult],
        records: Sequence[DragonTigerEventRecord],
    ) -> DragonTigerCollectionSummary:
        return self._commit(run, manifest, quality, records, insert_run=True)

    def commit_replay(
        self,
        run: IngestionRun,
        quality: Sequence[QualityResult],
        records: Sequence[DragonTigerEventRecord],
    ) -> DragonTigerCollectionSummary:
        return self._commit(run, None, quality, records, insert_run=False)

    def commit_failure(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality: Sequence[QualityResult],
    ) -> None:
        _require_run(run, IngestionStatus.FAILED)
        with self._engine.begin() as connection:
            _insert_run(connection, run)
            if manifest is not None:
                _insert_manifest(connection, manifest)
            _insert_quality(connection, quality)

    def _commit(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality: Sequence[QualityResult],
        records: Sequence[DragonTigerEventRecord],
        *,
        insert_run: bool,
    ) -> DragonTigerCollectionSummary:
        _require_run(run, IngestionStatus.SUCCEEDED)
        if not records:
            raise ValueError("successful DragonTiger persistence requires events")
        unchanged = 0
        with self._engine.begin() as connection:
            if insert_run:
                _insert_run(connection, run)
            if manifest is not None:
                _insert_manifest(connection, manifest)
            _insert_quality(connection, quality)
            for record in records:
                reason_id = _upsert_reason(connection, record)
                resolved_seats = {
                    trade.source_record_id: _resolve_seat(connection, trade)
                    for trade in record.seat_trades
                }
                content_hash = dragon_tiger_content_hash(record)
                connection.execute(
                    text("select pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                    {"key": f"{record.source_code}:{record.source_record_id}"},
                )
                current = connection.execute(
                    text("""
                        select event_id, content_hash from billboard.dragon_tiger_event
                        where source_code=:source_code and source_record_id=:source_record_id
                        for update
                    """),
                    {
                        "source_code": record.source_code,
                        "source_record_id": record.source_record_id,
                    },
                ).one_or_none()
                params = _event_params(record, reason_id, run.ingestion_id, content_hash)
                if current is None:
                    event_id = connection.scalar(
                        text("""
                            insert into billboard.dragon_tiger_event (
                                symbol, trade_date, period_type, period_start_date, period_end_date,
                                reason_id, reason_name_raw, close_price, change_pct,
                                turnover_amount, turnover_rate, amplitude, lhb_buy_amount,
                                lhb_sell_amount, source_code, source_record_id, ingestion_id,
                                content_hash
                            ) values (
                                :symbol, :trade_date, :period_type, :period_start_date,
                                :period_end_date, :reason_id, :reason_name_raw, :close_price,
                                :change_pct, :turnover_amount, :turnover_rate, :amplitude,
                                :lhb_buy_amount, :lhb_sell_amount, :source_code,
                                :source_record_id, :ingestion_id, :content_hash
                            ) returning event_id
                        """),
                        params,
                    )
                    if not isinstance(event_id, UUID):
                        raise RuntimeError("DragonTiger event insert returned no UUID")
                    _insert_trades(connection, event_id, run.ingestion_id, record, resolved_seats)
                elif current.content_hash == content_hash:
                    unchanged += 1
                else:
                    event_id = current.event_id
                    connection.execute(
                        text("""
                            update billboard.dragon_tiger_event set
                                symbol=:symbol, trade_date=:trade_date,
                                period_type=:period_type, period_start_date=:period_start_date,
                                period_end_date=:period_end_date, reason_id=:reason_id,
                                reason_name_raw=:reason_name_raw, close_price=:close_price,
                                change_pct=:change_pct, turnover_amount=:turnover_amount,
                                turnover_rate=:turnover_rate, amplitude=:amplitude,
                                lhb_buy_amount=:lhb_buy_amount, lhb_sell_amount=:lhb_sell_amount,
                                ingestion_id=:ingestion_id, content_hash=:content_hash
                            where event_id=:event_id
                        """),
                        {**params, "event_id": event_id},
                    )
                    connection.execute(
                        text("delete from billboard.seat_trade where event_id=:event_id"),
                        {"event_id": event_id},
                    )
                    _insert_trades(connection, event_id, run.ingestion_id, record, resolved_seats)
            if not insert_run:
                _update_run(connection, run)
        return DragonTigerCollectionSummary(
            status=run.status.value,
            ingestion_id=run.ingestion_id,
            trade_date=records[0].trade_date,
            fetched_rows=run.fetched_rows,
            accepted_events=len(records),
            accepted_seat_trades=sum(len(record.seat_trades) for record in records),
            filtered_rows=run.rejected_rows,
            unchanged_events=unchanged,
        )


def _upsert_reason(connection: Connection, record: DragonTigerEventRecord) -> UUID:
    reason = record.reason
    reason_id = connection.scalar(
        text("""
            insert into billboard.dragon_tiger_reason (
                reason_code, reason_name, reason_type, period_type
            ) values (:reason_code, :reason_name, :reason_type, :period_type)
            on conflict (reason_code) do update set
                reason_name=excluded.reason_name,
                reason_type=excluded.reason_type,
                period_type=excluded.period_type
            returning reason_id
        """),
        {
            "reason_code": reason.reason_code,
            "reason_name": reason.reason_name,
            "reason_type": reason.reason_type.value,
            "period_type": reason.period_type.value,
        },
    )
    if not isinstance(reason_id, UUID):
        raise RuntimeError("DragonTiger reason upsert returned no UUID")
    connection.execute(
        text("""
            insert into billboard.reason_source_alias (
                source_code, source_reason_code, source_reason_name, period_type, reason_id
            ) values (
                :source_code, :source_reason_code, :source_reason_name, :period_type, :reason_id
            ) on conflict (source_code, source_reason_code, source_reason_name, period_type)
            do update set reason_id=excluded.reason_id, last_seen_at=now()
        """),
        {
            "source_code": reason.source_code,
            "source_reason_code": reason.source_reason_code,
            "source_reason_name": reason.source_reason_name,
            "period_type": reason.period_type.value,
            "reason_id": reason_id,
        },
    )
    return reason_id


def _resolve_seat(connection: Connection, trade: SeatTradeRecord) -> UUID | None:
    if trade.seat_source_key is None:
        if trade.seat_name_raw in {
            "机构专用",
            "沪股通专用",
            "深股通专用",
            "北向资金专用",
        }:
            return None
        existing = connection.scalar(
            text("""
                select seat_id from billboard.trading_seat_alias
                where source_code=:source_code and alias_name=:alias_name
            """),
            {"source_code": trade.source_code, "alias_name": trade.seat_name_raw},
        )
        if existing is not None and not isinstance(existing, UUID):
            raise RuntimeError("DragonTiger Alias resolution returned an invalid seat UUID")
        resolved = existing if isinstance(existing, UUID) else trade.seat_id
        if resolved is not None:
            _touch_seat_dates(connection, resolved, trade.trade_date)
        return resolved
    connection.execute(
        text("select pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"dragon_tiger_seat:{trade.source_code}:{trade.seat_source_key}"},
    )
    by_key = connection.execute(
        text("""
            select alias_id, seat_id, source_seat_key
            from billboard.trading_seat_alias
            where source_code=:source_code and source_seat_key=:source_seat_key
        """),
        {
            "source_code": trade.source_code,
            "source_seat_key": trade.seat_source_key,
        },
    ).one_or_none()
    by_name = connection.execute(
        text("""
            select alias_id, seat_id, source_seat_key
            from billboard.trading_seat_alias
            where source_code=:source_code and alias_name=:alias_name
        """),
        {"source_code": trade.source_code, "alias_name": trade.seat_name_raw},
    ).one_or_none()
    if by_key is not None and by_name is not None and by_key.seat_id != by_name.seat_id:
        raise RuntimeError("DragonTiger reliable seat key conflicts with its Alias")
    if (
        by_key is None
        and by_name is not None
        and by_name.source_seat_key not in {None, trade.seat_source_key}
    ):
        raise RuntimeError("DragonTiger Alias is already bound to another reliable seat key")
    seat_id = by_key.seat_id if by_key is not None else by_name.seat_id if by_name else None
    if seat_id is None:
        seat_type = (
            TradingSeatType.NORTHBOUND
            if trade.is_northbound
            else TradingSeatType.INSTITUTION
            if trade.is_institution
            else TradingSeatType.BROKER
        )
        seat_id = connection.scalar(
            text("""
                insert into billboard.trading_seat (
                    canonical_name, seat_type, first_seen_date, last_seen_date
                ) values (:canonical_name, :seat_type, :trade_date, :trade_date)
                returning seat_id
            """),
            {
                "canonical_name": trade.seat_name_raw,
                "seat_type": seat_type.value,
                "trade_date": trade.trade_date,
            },
        )
    if not isinstance(seat_id, UUID):
        raise RuntimeError("DragonTiger seat resolution returned no UUID")
    _touch_seat_dates(connection, seat_id, trade.trade_date)
    alias_params = {
        "seat_id": seat_id,
        "source_code": trade.source_code,
        "source_seat_key": trade.seat_source_key,
        "alias_name": trade.seat_name_raw,
    }
    if by_key is not None:
        if by_name is None:
            connection.execute(
                text("""
                    update billboard.trading_seat_alias set alias_name=:alias_name
                    where alias_id=:alias_id
                """),
                {"alias_id": by_key.alias_id, "alias_name": trade.seat_name_raw},
            )
    elif by_name is not None:
        connection.execute(
            text("""
                update billboard.trading_seat_alias set source_seat_key=:source_seat_key
                where alias_id=:alias_id
            """),
            {"alias_id": by_name.alias_id, "source_seat_key": trade.seat_source_key},
        )
    else:
        connection.execute(
            text("""
                insert into billboard.trading_seat_alias (
                    seat_id, source_code, source_seat_key, alias_name
                ) values (:seat_id, :source_code, :source_seat_key, :alias_name)
            """),
            alias_params,
        )
    return seat_id


def _touch_seat_dates(connection: Connection, seat_id: UUID, trade_date: date) -> None:
    connection.execute(
        text("""
            update billboard.trading_seat set
                first_seen_date=least(first_seen_date, :trade_date),
                last_seen_date=greatest(last_seen_date, :trade_date)
            where seat_id=:seat_id
        """),
        {"seat_id": seat_id, "trade_date": trade_date},
    )


def _event_params(
    record: DragonTigerEventRecord,
    reason_id: UUID,
    ingestion_id: UUID,
    content_hash: str,
) -> dict[str, object]:
    return {
        "symbol": record.symbol,
        "trade_date": record.trade_date,
        "period_type": record.period_type.value,
        "period_start_date": record.period_start_date,
        "period_end_date": record.period_end_date,
        "reason_id": reason_id,
        "reason_name_raw": record.reason_name_raw,
        "close_price": record.close_price,
        "change_pct": record.change_pct,
        "turnover_amount": record.turnover_amount,
        "turnover_rate": record.turnover_rate,
        "amplitude": record.amplitude,
        "lhb_buy_amount": record.lhb_buy_amount,
        "lhb_sell_amount": record.lhb_sell_amount,
        "source_code": record.source_code,
        "source_record_id": record.source_record_id,
        "ingestion_id": ingestion_id,
        "content_hash": content_hash,
    }


def _insert_trades(
    connection: Connection,
    event_id: UUID,
    ingestion_id: UUID,
    record: DragonTigerEventRecord,
    resolved_seats: dict[str, UUID | None],
) -> None:
    connection.execute(
        text("""
            insert into billboard.seat_trade (
                event_id, source_code, source_event_id, source_record_id,
                symbol, trade_date, seat_id, seat_source_key, seat_name_raw,
                buy_amount, sell_amount, buy_rank, sell_rank, is_institution,
                is_northbound, ingestion_id, content_hash
            ) values (
                :event_id, :source_code, :source_event_id, :source_record_id,
                :symbol, :trade_date, :seat_id, :seat_source_key, :seat_name_raw,
                :buy_amount, :sell_amount, :buy_rank, :sell_rank, :is_institution,
                :is_northbound, :ingestion_id, :content_hash
            )
        """),
        [
            {
                "event_id": event_id,
                "source_code": trade.source_code,
                "source_event_id": trade.source_event_id,
                "source_record_id": trade.source_record_id,
                "symbol": trade.symbol,
                "trade_date": trade.trade_date,
                "seat_id": resolved_seats[trade.source_record_id],
                "seat_source_key": trade.seat_source_key,
                "seat_name_raw": trade.seat_name_raw,
                "buy_amount": trade.buy_amount,
                "sell_amount": trade.sell_amount,
                "buy_rank": trade.buy_rank,
                "sell_rank": trade.sell_rank,
                "is_institution": trade.is_institution,
                "is_northbound": trade.is_northbound,
                "ingestion_id": ingestion_id,
                "content_hash": _seat_trade_hash(trade),
            }
            for trade in record.seat_trades
        ],
    )


def _seat_trade_hash(trade: SeatTradeRecord) -> str:
    value = dumps(
        {
            "source_record_id": trade.source_record_id,
            "source_event_id": trade.source_event_id,
            "symbol": trade.symbol,
            "trade_date": trade.trade_date,
            "seat_source_key": trade.seat_source_key,
            "seat_name_raw": trade.seat_name_raw,
            "buy_amount": trade.buy_amount,
            "sell_amount": trade.sell_amount,
            "buy_rank": trade.buy_rank,
            "sell_rank": trade.sell_rank,
            "is_institution": trade.is_institution,
            "is_northbound": trade.is_northbound,
            "source_code": trade.source_code,
        },
        default=str,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(value.encode("utf-8")).hexdigest()


def _insert_run(connection: Connection, run: IngestionRun) -> None:
    connection.execute(
        text("""
            insert into ingestion.ingestion_run (
                ingestion_id, provider_code, dataset_code, status,
                requested_at, started_at, finished_at, request_params,
                fetched_rows, accepted_rows, rejected_rows, error_summary,
                replayed_from_raw_id
            ) values (
                :ingestion_id, :provider_code, :dataset_code, :status,
                :requested_at, :started_at, :finished_at, cast(:request_params as jsonb),
                :fetched_rows, :accepted_rows, :rejected_rows, :error_summary,
                :replayed_from_raw_id
            )
        """),
        _run_params(run),
    )


def _update_run(connection: Connection, run: IngestionRun) -> None:
    connection.execute(
        text("""
            update ingestion.ingestion_run set status=:status, finished_at=:finished_at,
                fetched_rows=:fetched_rows, accepted_rows=:accepted_rows,
                rejected_rows=:rejected_rows, error_summary=:error_summary
            where ingestion_id=:ingestion_id
        """),
        _run_params(run),
    )


def _run_params(run: IngestionRun) -> dict[str, object]:
    return {
        "ingestion_id": run.ingestion_id,
        "provider_code": run.provider_code.value,
        "dataset_code": run.dataset_code.value,
        "status": run.status.value,
        "requested_at": run.requested_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "request_params": _json(run.request_params),
        "fetched_rows": run.fetched_rows,
        "accepted_rows": run.accepted_rows,
        "rejected_rows": run.rejected_rows,
        "error_summary": run.error_summary,
        "replayed_from_raw_id": run.replayed_from_raw_id,
    }


def _insert_manifest(connection: Connection, manifest: RawManifest) -> None:
    connection.execute(
        text("""
            insert into ingestion.raw_manifest (
                raw_id, ingestion_id, storage_backend, object_path, file_format,
                content_sha256, byte_size, row_count, schema_version
            ) values (
                :raw_id, :ingestion_id, :storage_backend, :object_path, :file_format,
                :content_sha256, :byte_size, :row_count, :schema_version
            )
        """),
        {
            "raw_id": manifest.raw_id,
            "ingestion_id": manifest.ingestion_id,
            "storage_backend": manifest.storage_backend,
            "object_path": manifest.object_path,
            "file_format": manifest.file_format.value,
            "content_sha256": manifest.content_sha256,
            "byte_size": manifest.byte_size,
            "row_count": manifest.row_count,
            "schema_version": manifest.schema_version,
        },
    )


def _insert_quality(connection: Connection, quality: Sequence[QualityResult]) -> None:
    if not quality:
        return
    connection.execute(
        text("""
            insert into audit.quality_result (
                quality_result_id, ingestion_id, dataset_code, rule_code,
                severity, status, message, natural_key, details
            ) values (
                :quality_result_id, :ingestion_id, :dataset_code, :rule_code,
                :severity, :status, :message,
                cast(:natural_key as jsonb), cast(:details as jsonb)
            )
        """),
        [
            {
                "quality_result_id": item.quality_result_id,
                "ingestion_id": item.ingestion_id,
                "dataset_code": item.dataset_code.value,
                "rule_code": item.rule_code,
                "severity": item.severity.value,
                "status": item.status.value,
                "message": item.message,
                "natural_key": _json(item.natural_key),
                "details": _json(item.details),
            }
            for item in quality
        ],
    )


def _require_run(run: IngestionRun, status: IngestionStatus) -> None:
    if run.provider_code not in {ProviderCode.EASTMONEY, ProviderCode.TUSHARE}:
        raise ValueError("DragonTiger run provider is invalid")
    if run.dataset_code is not DatasetCode.DRAGON_TIGER:
        raise ValueError("DragonTiger run dataset is invalid")
    if run.status is not status:
        raise ValueError(f"DragonTiger run must be {status.value}")


def _json(value: object) -> str:
    return dumps(value, default=str, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
