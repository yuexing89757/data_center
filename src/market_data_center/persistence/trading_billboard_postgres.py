"""Atomic and idempotent PostgreSQL persistence for trading billboard aggregates."""

from collections.abc import Sequence
from datetime import date
from json import dumps
from uuid import UUID

from sqlalchemy import Connection, Engine, text

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    RawManifest,
)
from market_data_center.domain.trading_billboard import (
    TradingBillboardRecord,
    TradingBillboardSeatRecord,
    trading_billboard_content_hash,
)
from market_data_center.trading_billboard_service import (
    TradingBillboardCollectionSummary,
)


class PostgreSQLTradingBillboardPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def is_trading_day(self, trade_date: date) -> bool:
        with self._engine.connect() as connection:
            value = connection.scalar(
                text("""
                    select is_trading_day
                    from core.trading_calendar
                    where market='CN_A_SHARE' and trade_date=:trade_date
                """),
                {"trade_date": trade_date},
            )
        return value is True

    def known_stock_symbols(self, trade_date: date) -> frozenset[str]:
        with self._engine.connect() as connection:
            symbols = connection.scalars(
                text("""
                    select symbol
                    from core.security
                    where security_type='stock'
                      and (ipo_date is null or ipo_date <= :trade_date)
                      and (delisting_date is null or delisting_date >= :trade_date)
                """),
                {"trade_date": trade_date},
            ).all()
        return frozenset(symbols)

    def commit_success(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: Sequence[QualityResult],
        records: Sequence[TradingBillboardRecord],
    ) -> TradingBillboardCollectionSummary:
        return self._commit(run, manifest, quality, records, insert_run=True)

    def commit_replay(
        self,
        run: IngestionRun,
        quality: Sequence[QualityResult],
        records: Sequence[TradingBillboardRecord],
    ) -> TradingBillboardCollectionSummary:
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
        records: Sequence[TradingBillboardRecord],
        *,
        insert_run: bool,
    ) -> TradingBillboardCollectionSummary:
        _require_run(run, IngestionStatus.SUCCEEDED)
        if not records:
            raise ValueError("successful trading billboard persistence requires records")
        unchanged = 0
        with self._engine.begin() as connection:
            if insert_run:
                _insert_run(connection, run)
            if manifest is not None:
                _insert_manifest(connection, manifest)
            _insert_quality(connection, quality)
            for record in records:
                content_hash = trading_billboard_content_hash(record)
                connection.execute(
                    text("select pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                    {"key": f"{record.source_code}:{record.source_event_id}"},
                )
                current = connection.execute(
                    text("""
                        select entry_id, content_hash
                        from billboard.entry
                        where source_code=:source_code and source_event_id=:source_event_id
                        for update
                    """),
                    {
                        "source_code": record.source_code,
                        "source_event_id": record.source_event_id,
                    },
                ).one_or_none()
                if current is None:
                    entry_id = connection.scalar(
                        text("""
                            insert into billboard.entry (
                                symbol, trade_date, source_event_id, reason_code, reason_text,
                                close_price, change_rate_pct, turnover_rate_pct, market_amount,
                                buy_amount, sell_amount, net_amount, deal_amount,
                                deal_to_market_pct, net_to_market_pct, free_float_market_value,
                                source_code, ingestion_id, content_hash
                            ) values (
                                :symbol, :trade_date, :source_event_id, :reason_code, :reason_text,
                                :close_price, :change_rate_pct, :turnover_rate_pct, :market_amount,
                                :buy_amount, :sell_amount, :net_amount, :deal_amount,
                                :deal_to_market_pct, :net_to_market_pct, :free_float_market_value,
                                :source_code, :ingestion_id, :content_hash
                            ) returning entry_id
                        """),
                        _entry_params(record, run.ingestion_id, content_hash),
                    )
                    if not isinstance(entry_id, UUID):  # pragma: no cover - database contract
                        raise RuntimeError("trading billboard entry insert returned no UUID")
                    _insert_seats(connection, entry_id, run.ingestion_id, record)
                    continue
                entry_id = current.entry_id
                if current.content_hash == content_hash:
                    unchanged += 1
                    continue
                connection.execute(
                    text("""
                        update billboard.entry set
                            symbol=:symbol,
                            trade_date=:trade_date,
                            source_event_id=:source_event_id,
                            reason_code=:reason_code,
                            reason_text=:reason_text,
                            close_price=:close_price,
                            change_rate_pct=:change_rate_pct,
                            turnover_rate_pct=:turnover_rate_pct,
                            market_amount=:market_amount,
                            buy_amount=:buy_amount,
                            sell_amount=:sell_amount,
                            net_amount=:net_amount,
                            deal_amount=:deal_amount,
                            deal_to_market_pct=:deal_to_market_pct,
                            net_to_market_pct=:net_to_market_pct,
                            free_float_market_value=:free_float_market_value,
                            source_code=:source_code,
                            ingestion_id=:ingestion_id,
                            content_hash=:content_hash
                        where entry_id=:entry_id
                    """),
                    {
                        **_entry_params(record, run.ingestion_id, content_hash),
                        "entry_id": entry_id,
                    },
                )
                connection.execute(
                    text("delete from billboard.seat where entry_id=:entry_id"),
                    {"entry_id": entry_id},
                )
                _insert_seats(connection, entry_id, run.ingestion_id, record)

            if not insert_run:
                _update_run(connection, run)

        accepted_seats = sum(len(record.buy_seats) + len(record.sell_seats) for record in records)
        return TradingBillboardCollectionSummary(
            status=run.status.value,
            ingestion_id=run.ingestion_id,
            trade_date=records[0].trade_date,
            fetched_rows=run.fetched_rows,
            accepted_entries=len(records),
            accepted_seats=accepted_seats,
            filtered_rows=run.rejected_rows,
            unchanged_entries=unchanged,
        )


def _entry_params(
    record: TradingBillboardRecord, ingestion_id: UUID, content_hash: str
) -> dict[str, object]:
    return {
        "symbol": record.symbol,
        "trade_date": record.trade_date,
        "source_event_id": record.source_event_id,
        "reason_code": record.reason_code,
        "reason_text": record.reason_text,
        "close_price": record.close_price,
        "change_rate_pct": record.change_rate_pct,
        "turnover_rate_pct": record.turnover_rate_pct,
        "market_amount": record.market_amount,
        "buy_amount": record.buy_amount,
        "sell_amount": record.sell_amount,
        "net_amount": record.net_amount,
        "deal_amount": record.deal_amount,
        "deal_to_market_pct": record.deal_to_market_pct,
        "net_to_market_pct": record.net_to_market_pct,
        "free_float_market_value": record.free_float_market_value,
        "source_code": record.source_code,
        "ingestion_id": ingestion_id,
        "content_hash": content_hash,
    }


def _insert_seats(
    connection: Connection,
    entry_id: UUID,
    ingestion_id: UUID,
    record: TradingBillboardRecord,
) -> None:
    seats = (*record.buy_seats, *record.sell_seats)
    connection.execute(
        text("""
            insert into billboard.seat (
                entry_id, source_code, source_event_id, symbol, trade_date,
                side, rank, seat_code, seat_name, buy_amount, sell_amount,
                net_amount, buy_to_market_pct, sell_to_market_pct, ingestion_id
            ) values (
                :entry_id, :source_code, :source_event_id, :symbol, :trade_date,
                :side, :rank, :seat_code, :seat_name, :buy_amount, :sell_amount,
                :net_amount, :buy_to_market_pct, :sell_to_market_pct, :ingestion_id
            )
        """),
        [_seat_params(entry_id, ingestion_id, seat) for seat in seats],
    )


def _seat_params(
    entry_id: UUID, ingestion_id: UUID, seat: TradingBillboardSeatRecord
) -> dict[str, object]:
    return {
        "entry_id": entry_id,
        "source_code": seat.source_code,
        "source_event_id": seat.source_event_id,
        "symbol": seat.symbol,
        "trade_date": seat.trade_date,
        "side": seat.side.value,
        "rank": seat.rank,
        "seat_code": seat.seat_code,
        "seat_name": seat.seat_name,
        "buy_amount": seat.buy_amount,
        "sell_amount": seat.sell_amount,
        "net_amount": seat.net_amount,
        "buy_to_market_pct": seat.buy_to_market_pct,
        "sell_to_market_pct": seat.sell_to_market_pct,
        "ingestion_id": ingestion_id,
    }


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
        {
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
        },
    )


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


def _update_run(connection: Connection, run: IngestionRun) -> None:
    connection.execute(
        text("""
            update ingestion.ingestion_run set
                status=:status,
                finished_at=:finished_at,
                fetched_rows=:fetched_rows,
                accepted_rows=:accepted_rows,
                rejected_rows=:rejected_rows,
                error_summary=:error_summary
            where ingestion_id=:ingestion_id
        """),
        {
            "ingestion_id": run.ingestion_id,
            "status": run.status.value,
            "finished_at": run.finished_at,
            "fetched_rows": run.fetched_rows,
            "accepted_rows": run.accepted_rows,
            "rejected_rows": run.rejected_rows,
            "error_summary": run.error_summary,
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
    if run.provider_code is not ProviderCode.EASTMONEY:
        raise ValueError("trading billboard run provider must be eastmoney")
    if run.dataset_code is not DatasetCode.TRADING_BILLBOARD:
        raise ValueError("trading billboard run dataset is invalid")
    if run.status is not status:
        raise ValueError(f"trading billboard run must be {status.value}")


def _json(value: object) -> str:
    return dumps(value, default=str, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
