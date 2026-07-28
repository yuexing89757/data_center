"""Transactional, idempotent PostgreSQL writes for phase-one facts."""

from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date
from json import dumps
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, bindparam, text
from sqlalchemy.sql.elements import TextClause

from market_data_center.domain.entities import CalculatedTradingDay
from market_data_center.domain.ingestion import IngestionRun, QualityResult, RawManifest
from market_data_center.domain.records import DailyBarRecord, SecurityRecord

INSERT_INGESTION_RUN = text("""
insert into ingestion.ingestion_run (
    ingestion_id, provider_code, dataset_code, status, requested_at, started_at,
    finished_at, request_params, fetched_rows, accepted_rows, rejected_rows, error_summary
) values (
    :ingestion_id, :provider_code, :dataset_code, :status, :requested_at, :started_at,
    :finished_at, cast(:request_params as jsonb), :fetched_rows, :accepted_rows,
    :rejected_rows, :error_summary
)
""")

UPDATE_INGESTION_RUN = text("""
update ingestion.ingestion_run set
    status = :status,
    finished_at = :finished_at,
    fetched_rows = :fetched_rows,
    accepted_rows = :accepted_rows,
    rejected_rows = :rejected_rows,
    error_summary = :error_summary
where ingestion_id = :ingestion_id
""")

INSERT_RAW_MANIFEST = text("""
insert into ingestion.raw_manifest (
    raw_id, ingestion_id, storage_backend, object_path, file_format,
    content_sha256, byte_size, row_count, schema_version
) values (
    :raw_id, :ingestion_id, :storage_backend, :object_path, :file_format,
    :content_sha256, :byte_size, :row_count, :schema_version
)
""")

INSERT_QUALITY_RESULT = text("""
insert into audit.quality_result (
    quality_result_id, ingestion_id, dataset_code, rule_code, severity,
    status, natural_key, message, details
) values (
    :quality_result_id, :ingestion_id, :dataset_code, :rule_code, :severity,
    :status, cast(:natural_key as jsonb), :message, cast(:details as jsonb)
)
""")

UPSERT_SECURITY = text("""
insert into core.security (
    symbol, code, exchange, current_name, security_type, status,
    ipo_date, delisting_date, source_code, ingestion_id
) values (
    :symbol, :code, :exchange, :current_name, :security_type, :status,
    :ipo_date, :delisting_date, :source_code, :ingestion_id
)
on conflict (symbol) do update set
    current_name = excluded.current_name,
    security_type = excluded.security_type,
    status = excluded.status,
    ipo_date = excluded.ipo_date,
    delisting_date = excluded.delisting_date,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
where core.security.code = excluded.code
  and core.security.exchange = excluded.exchange
""")

CLOSE_SECURITY_NAME = text("""
update core.security_name_history
set effective_to = :effective_from - 1
where symbol = :symbol
  and effective_to is null
  and name <> :name
""")

INSERT_SECURITY_NAME = text("""
insert into core.security_name_history (
    symbol, name, effective_from, effective_to, source_code, ingestion_id
)
select
    :symbol, :name, :effective_from, null, :source_code, :ingestion_id
where not exists (
    select 1
    from core.security_name_history
    where symbol = :symbol
      and effective_to is null
      and name = :name
)
on conflict (symbol, effective_from) do nothing
""")

UPSERT_TRADING_DAY = text("""
insert into core.trading_calendar (
    market, trade_date, is_trading_day, previous_trading_day,
    next_trading_day, source_code, ingestion_id
) values (
    :market, :trade_date, :is_trading_day, :previous_trading_day,
    :next_trading_day, :source_code, :ingestion_id
)
on conflict (market, trade_date) do update set
    is_trading_day = excluded.is_trading_day,
    previous_trading_day = excluded.previous_trading_day,
    next_trading_day = excluded.next_trading_day,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
""")

UPSERT_DAILY_BAR = text("""
insert into core.daily_bar (
    symbol, trade_date, market, open, high, low, close, previous_close,
    volume, amount, trade_status, is_st, source_code, ingestion_id
) values (
    :symbol, :trade_date, :market, :open, :high, :low, :close, :previous_close,
    :volume, :amount, :trade_status, :is_st, :source_code, :ingestion_id
)
on conflict (symbol, trade_date) do update set
    market = excluded.market,
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    previous_close = excluded.previous_close,
    volume = excluded.volume,
    amount = excluded.amount,
    trade_status = excluded.trade_status,
    is_st = excluded.is_st,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
""")


class PostgreSQLPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @contextmanager
    def task_lock(self, task_key: str) -> Iterator[None]:
        with self._engine.connect() as connection:
            acquired = connection.execute(
                text("select pg_try_advisory_lock(hashtextextended(:task_key, 0))"),
                {"task_key": task_key},
            ).scalar_one()
            if not acquired:
                raise RuntimeError(f"ingestion task is already running: {task_key}")
            try:
                yield
            finally:
                connection.execute(
                    text("select pg_advisory_unlock(hashtextextended(:task_key, 0))"),
                    {"task_key": task_key},
                )

    def create_ingestion_run(self, run: IngestionRun) -> None:
        parameters = {
            "ingestion_id": run.ingestion_id,
            "provider_code": run.provider_code.value,
            "dataset_code": run.dataset_code.value,
            "status": run.status.value,
            "requested_at": run.requested_at,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "request_params": dumps(dict(run.request_params), ensure_ascii=False),
            "fetched_rows": run.fetched_rows,
            "accepted_rows": run.accepted_rows,
            "rejected_rows": run.rejected_rows,
            "error_summary": run.error_summary,
        }
        with self._engine.begin() as connection:
            connection.execute(INSERT_INGESTION_RUN, parameters)

    def update_ingestion_run(self, run: IngestionRun) -> None:
        parameters = {
            "ingestion_id": run.ingestion_id,
            "status": run.status.value,
            "finished_at": run.finished_at,
            "fetched_rows": run.fetched_rows,
            "accepted_rows": run.accepted_rows,
            "rejected_rows": run.rejected_rows,
            "error_summary": run.error_summary,
        }
        with self._engine.begin() as connection:
            connection.execute(UPDATE_INGESTION_RUN, parameters)

    def fail_ingestion_run(self, run: IngestionRun) -> None:
        self.update_ingestion_run(run)

    def insert_raw_manifest(self, manifest: RawManifest) -> None:
        parameters = {
            "raw_id": manifest.raw_id,
            "ingestion_id": manifest.ingestion_id,
            "storage_backend": manifest.storage_backend,
            "object_path": manifest.object_path,
            "file_format": manifest.file_format.value,
            "content_sha256": manifest.content_sha256,
            "byte_size": manifest.byte_size,
            "row_count": manifest.row_count,
            "schema_version": manifest.schema_version,
        }
        with self._engine.begin() as connection:
            connection.execute(INSERT_RAW_MANIFEST, parameters)

    def insert_quality_results(self, results: Iterable[QualityResult]) -> None:
        parameters = [
            {
                "quality_result_id": result.quality_result_id,
                "ingestion_id": result.ingestion_id,
                "dataset_code": result.dataset_code.value,
                "rule_code": result.rule_code,
                "severity": result.severity.value,
                "status": result.status.value,
                "natural_key": dumps(result.natural_key, ensure_ascii=False)
                if result.natural_key is not None
                else None,
                "message": result.message,
                "details": dumps(dict(result.details), ensure_ascii=False),
            }
            for result in results
        ]
        self._execute_many(INSERT_QUALITY_RESULT, parameters)

    def upsert_securities(self, records: Iterable[SecurityRecord], ingestion_id: UUID) -> None:
        parameters = self._security_parameters(records, ingestion_id)
        self._execute_many(UPSERT_SECURITY, parameters)

    def upsert_trading_days(
        self, records: Iterable[CalculatedTradingDay], ingestion_id: UUID
    ) -> None:
        parameters = self._trading_day_parameters(records, ingestion_id)
        self._execute_many(UPSERT_TRADING_DAY, parameters)

    def upsert_daily_bars(self, records: Iterable[DailyBarRecord], ingestion_id: UUID) -> None:
        parameters = self._daily_bar_parameters(records, ingestion_id)
        self._execute_many(UPSERT_DAILY_BAR, parameters)

    def known_symbols(self, symbols: Collection[str]) -> set[str]:
        if not symbols:
            return set()
        statement = text("select symbol from core.security where symbol in :symbols").bindparams(
            bindparam("symbols", expanding=True)
        )
        with self._engine.connect() as connection:
            return set(connection.execute(statement, {"symbols": list(symbols)}).scalars())

    def listed_stock_symbols(self) -> list[str]:
        statement = text("""
select symbol
from core.security
where security_type = 'stock' and status = 'listed'
order by symbol
""")
        with self._engine.connect() as connection:
            return list(connection.execute(statement).scalars())

    def symbols_with_daily_bars(self, start_date: date, end_date: date) -> set[str]:
        statement = text("""
select distinct symbol from core.daily_bar
where trade_date between :start_date and :end_date
""")
        with self._engine.connect() as connection:
            return set(
                connection.execute(
                    statement, {"start_date": start_date, "end_date": end_date}
                ).scalars()
            )

    def known_trading_dates(self, dates: Collection[date]) -> set[date]:
        if not dates:
            return set()
        statement = text("""
select trade_date
from core.trading_calendar
where market = 'CN_A_SHARE'
  and is_trading_day
  and trade_date in :dates
""").bindparams(bindparam("dates", expanding=True))
        with self._engine.connect() as connection:
            return set(connection.execute(statement, {"dates": list(dates)}).scalars())

    def commit_security_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[SecurityRecord],
    ) -> None:
        with self._engine.begin() as connection:
            connection.execute(INSERT_RAW_MANIFEST, self._manifest_parameters(manifest))
            if records:
                security_parameters = self._security_parameters(records, run.ingestion_id)
                connection.execute(UPSERT_SECURITY, security_parameters)
                name_parameters = self._security_name_parameters(
                    records, run.ingestion_id, run.requested_at.date()
                )
                connection.execute(CLOSE_SECURITY_NAME, name_parameters)
                connection.execute(INSERT_SECURITY_NAME, name_parameters)
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_trading_calendar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[CalculatedTradingDay],
    ) -> None:
        with self._engine.begin() as connection:
            connection.execute(INSERT_RAW_MANIFEST, self._manifest_parameters(manifest))
            if records:
                connection.execute(
                    UPSERT_TRADING_DAY,
                    self._trading_day_parameters(records, run.ingestion_id),
                )
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[DailyBarRecord],
        quality_results: Sequence[QualityResult],
    ) -> None:
        with self._engine.begin() as connection:
            connection.execute(INSERT_RAW_MANIFEST, self._manifest_parameters(manifest))
            if quality_results:
                connection.execute(INSERT_QUALITY_RESULT, self._quality_parameters(quality_results))
            if records:
                connection.execute(
                    UPSERT_DAILY_BAR, self._daily_bar_parameters(records, run.ingestion_id)
                )
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    @staticmethod
    def _run_update_parameters(run: IngestionRun) -> dict[str, object]:
        return {
            "ingestion_id": run.ingestion_id,
            "status": run.status.value,
            "finished_at": run.finished_at,
            "fetched_rows": run.fetched_rows,
            "accepted_rows": run.accepted_rows,
            "rejected_rows": run.rejected_rows,
            "error_summary": run.error_summary,
        }

    @staticmethod
    def _manifest_parameters(manifest: RawManifest) -> dict[str, object]:
        return {
            "raw_id": manifest.raw_id,
            "ingestion_id": manifest.ingestion_id,
            "storage_backend": manifest.storage_backend,
            "object_path": manifest.object_path,
            "file_format": manifest.file_format.value,
            "content_sha256": manifest.content_sha256,
            "byte_size": manifest.byte_size,
            "row_count": manifest.row_count,
            "schema_version": manifest.schema_version,
        }

    @staticmethod
    def _security_parameters(
        records: Iterable[SecurityRecord], ingestion_id: UUID
    ) -> list[dict[str, object]]:
        return [
            {
                "symbol": record.symbol,
                "code": record.code,
                "exchange": record.exchange.value,
                "current_name": record.name,
                "security_type": record.security_type.value,
                "status": record.status.value,
                "ipo_date": record.ipo_date,
                "delisting_date": record.delisting_date,
                "source_code": record.source_code,
                "ingestion_id": ingestion_id,
            }
            for record in records
        ]

    @staticmethod
    def _trading_day_parameters(
        records: Iterable[CalculatedTradingDay], ingestion_id: UUID
    ) -> list[dict[str, object]]:
        return [
            {
                "market": record.market.value,
                "trade_date": record.trade_date,
                "is_trading_day": record.is_trading_day,
                "previous_trading_day": record.previous_trading_day,
                "next_trading_day": record.next_trading_day,
                "source_code": record.source_code,
                "ingestion_id": ingestion_id,
            }
            for record in records
        ]

    @staticmethod
    def _security_name_parameters(
        records: Iterable[SecurityRecord], ingestion_id: UUID, effective_from: date
    ) -> list[dict[str, object]]:
        return [
            {
                "symbol": record.symbol,
                "name": record.name,
                "effective_from": effective_from,
                "source_code": record.source_code,
                "ingestion_id": ingestion_id,
            }
            for record in records
        ]

    @staticmethod
    def _daily_bar_parameters(
        records: Iterable[DailyBarRecord], ingestion_id: UUID
    ) -> list[dict[str, object]]:
        return [
            {
                "symbol": record.symbol,
                "trade_date": record.trade_date,
                "market": record.market.value,
                "open": record.open,
                "high": record.high,
                "low": record.low,
                "close": record.close,
                "previous_close": record.previous_close,
                "volume": record.volume,
                "amount": record.amount,
                "trade_status": record.trade_status.value,
                "is_st": record.is_st,
                "source_code": record.source_code,
                "ingestion_id": ingestion_id,
            }
            for record in records
        ]

    @staticmethod
    def _quality_parameters(results: Iterable[QualityResult]) -> list[dict[str, object]]:
        return [
            {
                "quality_result_id": result.quality_result_id,
                "ingestion_id": result.ingestion_id,
                "dataset_code": result.dataset_code.value,
                "rule_code": result.rule_code,
                "severity": result.severity.value,
                "status": result.status.value,
                "natural_key": dumps(result.natural_key, ensure_ascii=False)
                if result.natural_key is not None
                else None,
                "message": result.message,
                "details": dumps(dict(result.details), ensure_ascii=False),
            }
            for result in results
        ]

    def _execute_many(self, statement: TextClause, parameters: Sequence[Mapping[str, Any]]) -> None:
        if not parameters:
            return
        with self._engine.begin() as connection:
            connection.execute(statement, parameters)
