"""Transactional, idempotent PostgreSQL writes for phase-one facts."""

from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from json import dumps
from typing import cast
from uuid import UUID

from sqlalchemy import Connection, Engine, RowMapping, bindparam, text

from market_data_center.domain.board_index import (
    BoardIndexConstituentSnapshotRecord,
    BoardIndexDailyBarRecord,
    BoardIndexProviderRecord,
    BoardIndexRecord,
)
from market_data_center.domain.classification import (
    ClassificationCatalogSnapshotRecord,
    ClassificationMemberSnapshotRecord,
    ClassificationType,
)
from market_data_center.domain.entities import CalculatedTradingDay
from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    ProviderCode,
    QualityResult,
    RawFileFormat,
    RawManifest,
    ReplaySource,
)
from market_data_center.domain.records import (
    CapitalRecord,
    DailyBarRecord,
    DistributionRecord,
    IngestionEnvelope,
    RightsIssueRecord,
    SecurityRecord,
    ShareCapitalRecord,
)

INSERT_INGESTION_RUN = text("""
insert into ingestion.ingestion_run (
    ingestion_id, provider_code, dataset_code, status, requested_at, started_at,
    finished_at, request_params, fetched_rows, accepted_rows, rejected_rows, error_summary,
    replayed_from_raw_id
) values (
    :ingestion_id, :provider_code, :dataset_code, :status, :requested_at, :started_at,
    :finished_at, cast(:request_params as jsonb), :fetched_rows, :accepted_rows,
    :rejected_rows, :error_summary, :replayed_from_raw_id
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
    status = case
        when excluded.status = 'unknown' then core.security.status
        else excluded.status
    end,
    ipo_date = coalesce(excluded.ipo_date, core.security.ipo_date),
    delisting_date = coalesce(excluded.delisting_date, core.security.delisting_date),
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

UPSERT_SHARE_CAPITAL = text("""
insert into capital.share_capital (
    symbol, effective_date, total_shares, restricted_shares, circulating_shares,
    listed_a_shares, change_reason, source_code, ingestion_id
) values (
    :symbol, :effective_date, :total_shares, :restricted_shares, :circulating_shares,
    :listed_a_shares, :change_reason, :source_code, :ingestion_id
)
on conflict (symbol, effective_date) do update set
    total_shares = excluded.total_shares,
    restricted_shares = excluded.restricted_shares,
    circulating_shares = excluded.circulating_shares,
    listed_a_shares = excluded.listed_a_shares,
    change_reason = excluded.change_reason,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
""")

UPSERT_DISTRIBUTION = text("""
insert into capital.distribution (
    symbol, report_period, announcement_date, record_date, ex_date,
    cash_dividend_per_share, bonus_share_ratio, transfer_share_ratio,
    status, source_code, ingestion_id
) values (
    :symbol, :report_period, :announcement_date, :record_date, :ex_date,
    :cash_dividend_per_share, :bonus_share_ratio, :transfer_share_ratio,
    :status, :source_code, :ingestion_id
)
on conflict (symbol, report_period) do update set
    announcement_date = excluded.announcement_date,
    record_date = excluded.record_date,
    ex_date = excluded.ex_date,
    cash_dividend_per_share = excluded.cash_dividend_per_share,
    bonus_share_ratio = excluded.bonus_share_ratio,
    transfer_share_ratio = excluded.transfer_share_ratio,
    status = excluded.status,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
""")

UPSERT_RIGHTS_ISSUE = text("""
insert into capital.rights_issue (
    symbol, record_date, announcement_date, ex_date, payment_start_date,
    payment_end_date, listing_date, rights_ratio, rights_price, base_shares,
    proceeds, source_code, ingestion_id
) values (
    :symbol, :record_date, :announcement_date, :ex_date, :payment_start_date,
    :payment_end_date, :listing_date, :rights_ratio, :rights_price, :base_shares,
    :proceeds, :source_code, :ingestion_id
)
on conflict (symbol, record_date) do update set
    announcement_date = excluded.announcement_date,
    ex_date = excluded.ex_date,
    payment_start_date = excluded.payment_start_date,
    payment_end_date = excluded.payment_end_date,
    listing_date = excluded.listing_date,
    rights_ratio = excluded.rights_ratio,
    rights_price = excluded.rights_price,
    base_shares = excluded.base_shares,
    proceeds = excluded.proceeds,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
""")

UPSERT_CLASSIFICATION_CATALOG = text("""
insert into classification.catalog_snapshot (
    namespace, classification_type, snapshot_date, definition_count,
    source_code, ingestion_id
) values (
    :namespace, :classification_type, :snapshot_date, :definition_count,
    :source_code, :ingestion_id
)
on conflict (namespace, classification_type, snapshot_date) do update set
    definition_count = excluded.definition_count,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
""")

INSERT_CLASSIFICATION_DEFINITION = text("""
insert into classification.definition_snapshot (
    namespace, classification_type, snapshot_date, classification_code,
    name, level, parent_code, source_code, ingestion_id
) values (
    :namespace, :classification_type, :snapshot_date, :classification_code,
    :name, :level, :parent_code, :source_code, :ingestion_id
)
on conflict (namespace, classification_type, snapshot_date, classification_code)
do update set
    name = excluded.name,
    level = excluded.level,
    parent_code = excluded.parent_code,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
""")

DELETE_STALE_CLASSIFICATION_DEFINITIONS = text("""
delete from classification.definition_snapshot
where namespace = :namespace
  and classification_type = :classification_type
  and snapshot_date = :snapshot_date
  and classification_code not in :classification_codes
""").bindparams(bindparam("classification_codes", expanding=True))

UPSERT_CLASSIFICATION_MEMBER_SNAPSHOT = text("""
insert into classification.member_snapshot (
    namespace, classification_type, classification_code, snapshot_date,
    member_count, source_code, ingestion_id
) values (
    :namespace, :classification_type, :classification_code, :snapshot_date,
    :member_count, :source_code, :ingestion_id
)
on conflict (namespace, classification_type, classification_code, snapshot_date)
do update set
    member_count = excluded.member_count,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
""")

INSERT_CLASSIFICATION_MEMBER = text("""
insert into classification.member_snapshot_item (
    namespace, classification_type, classification_code, snapshot_date,
    symbol, source_code, ingestion_id
) values (
    :namespace, :classification_type, :classification_code, :snapshot_date,
    :symbol, :source_code, :ingestion_id
)
""")

UPSERT_BOARD_INDEX = text("""
insert into core.board_index (
    board_id, board_code, namespace, name, board_type, market, status,
    source_code, ingestion_id
) values (
    :board_id, :board_code, :namespace, :name, :board_type, :market, :status,
    :source_code, :ingestion_id
)
on conflict (board_id) do update set
    name = excluded.name,
    board_type = excluded.board_type,
    market = excluded.market,
    status = excluded.status,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
where core.board_index.board_code = excluded.board_code
  and core.board_index.namespace = excluded.namespace
""")

UPSERT_BOARD_INDEX_DAILY_BAR = text("""
insert into core.board_index_daily_bar (
    board_id, trade_date, market, open, high, low, close, volume, amount,
    source_code, ingestion_id
) values (
    :board_id, :trade_date, :market, :open, :high, :low, :close, :volume, :amount,
    :source_code, :ingestion_id
)
on conflict (board_id, trade_date) do update set
    market = excluded.market,
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume,
    amount = excluded.amount,
    source_code = excluded.source_code,
    ingestion_id = excluded.ingestion_id
""")

INSERT_BOARD_INDEX_CONSTITUENT = text("""
insert into core.board_index_constituent_snapshot (
    board_id, trade_date, symbol, source_code, ingestion_id
) values (
    :board_id, :trade_date, :symbol, :source_code, :ingestion_id
)
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
            "replayed_from_raw_id": run.replayed_from_raw_id,
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

    def replay_source(self, ingestion_id: UUID) -> ReplaySource:
        statement = text("""
select
    run.ingestion_id,
    run.provider_code,
    run.dataset_code,
    run.requested_at,
    run.request_params,
    manifest.raw_id,
    manifest.object_path,
    manifest.file_format,
    manifest.content_sha256,
    manifest.byte_size,
    manifest.row_count,
    manifest.schema_version,
    manifest.storage_backend
from ingestion.ingestion_run run
left join ingestion.raw_manifest manifest using (ingestion_id)
where run.ingestion_id = :ingestion_id
order by manifest.created_at
""")
        with self._engine.connect() as connection:
            rows = connection.execute(statement, {"ingestion_id": ingestion_id}).mappings().all()
        if not rows:
            raise LookupError(f"ingestion run does not exist: {ingestion_id}")
        if len(rows) > 1:
            raise RuntimeError(f"ingestion run has multiple Raw manifests: {ingestion_id}")
        return self._replay_source_from_row(rows[0])

    def daily_bar_replay_sources(
        self, symbol: str, start_date: date, end_date: date
    ) -> list[ReplaySource]:
        variants = _source_symbol_variants(symbol)
        statement = text("""
select
    run.ingestion_id,
    run.provider_code,
    run.dataset_code,
    run.requested_at,
    run.request_params,
    manifest.raw_id,
    manifest.object_path,
    manifest.file_format,
    manifest.content_sha256,
    manifest.byte_size,
    manifest.row_count,
    manifest.schema_version,
    manifest.storage_backend
from ingestion.ingestion_run run
join ingestion.raw_manifest manifest using (ingestion_id)
where run.dataset_code = 'daily_bar'
  and run.status in ('succeeded', 'partial')
  and run.request_params ->> 'source_symbol' in :source_symbols
  and replace(run.request_params ->> 'start_date', '-', '') <= :end_key
  and replace(run.request_params ->> 'end_date', '-', '') >= :start_key
order by run.requested_at, manifest.created_at
""").bindparams(bindparam("source_symbols", expanding=True))
        with self._engine.connect() as connection:
            rows = connection.execute(
                statement,
                {
                    "source_symbols": variants,
                    "start_key": start_date.strftime("%Y%m%d"),
                    "end_key": end_date.strftime("%Y%m%d"),
                },
            ).mappings()
            return [self._replay_source_from_row(row) for row in rows]

    def stale_ingestion_run_ids(self, stale_before: datetime) -> list[UUID]:
        statement = text("""
select ingestion_id
from ingestion.ingestion_run
where status = 'running'
  and started_at < :stale_before
order by started_at, ingestion_id
""")
        with self._engine.connect() as connection:
            return list(connection.execute(statement, {"stale_before": stale_before}).scalars())

    def recover_stale_ingestion_runs(
        self, stale_before: datetime, finished_at: datetime, reason: str
    ) -> list[UUID]:
        statement = text("""
update ingestion.ingestion_run
set status = 'failed',
    finished_at = :finished_at,
    error_summary = :reason
where status = 'running'
  and started_at < :stale_before
returning ingestion_id
""")
        with self._engine.begin() as connection:
            return list(
                connection.execute(
                    statement,
                    {
                        "stale_before": stale_before,
                        "finished_at": finished_at,
                        "reason": reason,
                    },
                ).scalars()
            )

    def known_symbols(self, symbols: Collection[str]) -> set[str]:
        if not symbols:
            return set()
        statement = text("select symbol from core.security where symbol in :symbols").bindparams(
            bindparam("symbols", expanding=True)
        )
        with self._engine.connect() as connection:
            return set(connection.execute(statement, {"symbols": list(symbols)}).scalars())

    def known_board_ids(self, board_ids: Collection[str]) -> set[str]:
        if not board_ids:
            return set()
        statement = text(
            "select board_id from core.board_index where board_id in :board_ids"
        ).bindparams(bindparam("board_ids", expanding=True))
        with self._engine.connect() as connection:
            return set(connection.execute(statement, {"board_ids": list(board_ids)}).scalars())

    def known_classification_snapshots(
        self, keys: Collection[tuple[str, ClassificationType, str, date]]
    ) -> set[tuple[str, ClassificationType, str, date]]:
        known: set[tuple[str, ClassificationType, str, date]] = set()
        statement = text("""
select 1
from classification.definition_snapshot
where namespace = :namespace
  and classification_type = :classification_type
  and classification_code = :classification_code
  and snapshot_date = :snapshot_date
""")
        with self._engine.connect() as connection:
            for namespace, classification_type, code, snapshot_date in keys:
                exists = connection.execute(
                    statement,
                    {
                        "namespace": namespace,
                        "classification_type": classification_type.value,
                        "classification_code": code,
                        "snapshot_date": snapshot_date,
                    },
                ).first()
                if exists is not None:
                    known.add((namespace, classification_type, code, snapshot_date))
        return known

    def listed_stock_symbols(self) -> list[str]:
        statement = text("""
select symbol
from core.security
where security_type = 'stock' and status = 'listed'
order by symbol
""")
        with self._engine.connect() as connection:
            return list(connection.execute(statement).scalars())

    def stock_symbols_missing_daily_bars(self, start_date: date, end_date: date) -> list[str]:
        """Return listed stocks missing at least one eligible trading-day fact.

        Eligibility follows the unified A-share calendar and the security listing
        window.  Checking the complete range avoids treating a partially ingested
        symbol as complete during a resumed bulk run.
        """
        statement = text("""
select s.symbol
from core.security s
join core.trading_calendar c
  on c.market = 'CN_A_SHARE'
 and c.is_trading_day
 and c.trade_date between :start_date and :end_date
 and (s.ipo_date is null or c.trade_date >= s.ipo_date)
 and (s.delisting_date is null or c.trade_date <= s.delisting_date)
left join core.daily_bar b
  on b.symbol = s.symbol
 and b.trade_date = c.trade_date
where s.security_type = 'stock'
  and s.status = 'listed'
group by s.symbol
having count(*) filter (where b.symbol is null) > 0
order by s.symbol
""")
        with self._engine.connect() as connection:
            return list(
                connection.execute(
                    statement, {"start_date": start_date, "end_date": end_date}
                ).scalars()
            )

    def has_complete_calendar_range(self, start_date: date, end_date: date) -> bool:
        expected_days = (end_date - start_date).days + 1
        if expected_days < 1:
            return False
        statement = text("""
select count(*)
from core.trading_calendar
where market = 'CN_A_SHARE'
  and trade_date between :start_date and :end_date
""")
        with self._engine.connect() as connection:
            actual_days = connection.execute(
                statement, {"start_date": start_date, "end_date": end_date}
            ).scalar_one()
        return int(actual_days) == expected_days

    def trading_day_boundaries(
        self, start_date: date, end_date: date
    ) -> tuple[date | None, date | None]:
        statement = text("""
select
    max(trade_date) filter (where trade_date < :start_date),
    min(trade_date) filter (where trade_date > :end_date)
from core.trading_calendar
where market = 'CN_A_SHARE'
  and is_trading_day
  and (trade_date < :start_date or trade_date > :end_date)
""")
        with self._engine.connect() as connection:
            row = connection.execute(
                statement, {"start_date": start_date, "end_date": end_date}
            ).one()
        return row[0], row[1]

    def latest_trading_date(self, start_date: date, end_date: date) -> date | None:
        statement = text("""
select max(trade_date)
from core.trading_calendar
where market = 'CN_A_SHARE'
  and is_trading_day
  and trade_date between :start_date and :end_date
""")
        with self._engine.connect() as connection:
            return connection.execute(
                statement, {"start_date": start_date, "end_date": end_date}
            ).scalar_one_or_none()

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
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[SecurityRecord]],
    ) -> None:
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if records:
                self._ensure_envelope_ids(records, run.ingestion_id)
                security_parameters = self._security_envelope_parameters(records)
                connection.execute(UPSERT_SECURITY, security_parameters)
                name_parameters = self._security_name_envelope_parameters(
                    records, run.requested_at.date()
                )
                connection.execute(CLOSE_SECURITY_NAME, name_parameters)
                connection.execute(INSERT_SECURITY_NAME, name_parameters)
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_trading_calendar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[CalculatedTradingDay]],
    ) -> None:
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if records:
                self._ensure_envelope_ids(records, run.ingestion_id)
                connection.execute(
                    UPSERT_TRADING_DAY,
                    self._trading_day_envelope_parameters(records),
                )
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[DailyBarRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None:
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if quality_results:
                connection.execute(INSERT_QUALITY_RESULT, self._quality_parameters(quality_results))
            if records:
                self._ensure_envelope_ids(records, run.ingestion_id)
                connection.execute(UPSERT_DAILY_BAR, self._daily_bar_envelope_parameters(records))
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_capital_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[CapitalRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None:
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if quality_results:
                connection.execute(INSERT_QUALITY_RESULT, self._quality_parameters(quality_results))
            if records:
                self._ensure_envelope_ids(records, run.ingestion_id)
                share_capital = [
                    cast(IngestionEnvelope[ShareCapitalRecord], envelope)
                    for envelope in records
                    if isinstance(envelope.record, ShareCapitalRecord)
                ]
                distributions = [
                    cast(IngestionEnvelope[DistributionRecord], envelope)
                    for envelope in records
                    if isinstance(envelope.record, DistributionRecord)
                ]
                rights_issues = [
                    cast(IngestionEnvelope[RightsIssueRecord], envelope)
                    for envelope in records
                    if isinstance(envelope.record, RightsIssueRecord)
                ]
                if share_capital:
                    connection.execute(
                        UPSERT_SHARE_CAPITAL,
                        self._share_capital_envelope_parameters(share_capital),
                    )
                if distributions:
                    connection.execute(
                        UPSERT_DISTRIBUTION,
                        self._distribution_envelope_parameters(distributions),
                    )
                if rights_issues:
                    connection.execute(
                        UPSERT_RIGHTS_ISSUE,
                        self._rights_issue_envelope_parameters(rights_issues),
                    )
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_classification_catalog_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        record: IngestionEnvelope[ClassificationCatalogSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None:
        self._ensure_single_envelope(record, run.ingestion_id)
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if quality_results:
                connection.execute(INSERT_QUALITY_RESULT, self._quality_parameters(quality_results))
            if not quality_results:
                params = self._classification_catalog_parameters(record)
                header = cast(dict[str, object], params["header"])
                definitions = cast(list[dict[str, object]], params["definitions"])
                connection.execute(UPSERT_CLASSIFICATION_CATALOG, header)
                if definitions:
                    connection.execute(INSERT_CLASSIFICATION_DEFINITION, definitions)
                connection.execute(
                    DELETE_STALE_CLASSIFICATION_DEFINITIONS,
                    {
                        **header,
                        "classification_codes": [
                            item["classification_code"] for item in definitions
                        ],
                    },
                )
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_classification_members_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        record: IngestionEnvelope[ClassificationMemberSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None:
        self._ensure_single_envelope(record, run.ingestion_id)
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if quality_results:
                connection.execute(INSERT_QUALITY_RESULT, self._quality_parameters(quality_results))
            if not quality_results:
                params = self._classification_member_parameters(record)
                header = cast(dict[str, object], params["header"])
                members = cast(list[dict[str, object]], params["members"])
                connection.execute(UPSERT_CLASSIFICATION_MEMBER_SNAPSHOT, header)
                connection.execute(
                    text("""
delete from classification.member_snapshot_item
where namespace = :namespace
  and classification_type = :classification_type
  and classification_code = :classification_code
  and snapshot_date = :snapshot_date
"""),
                    header,
                )
                if members:
                    connection.execute(INSERT_CLASSIFICATION_MEMBER, members)
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_board_index_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[BoardIndexRecord]],
    ) -> None:
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if records:
                self._ensure_envelope_ids(records, run.ingestion_id)
                connection.execute(
                    UPSERT_BOARD_INDEX,
                    self._board_index_envelope_parameters(records),
                )
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_board_index_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[BoardIndexDailyBarRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None:
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if quality_results:
                connection.execute(INSERT_QUALITY_RESULT, self._quality_parameters(quality_results))
            if records:
                self._ensure_envelope_ids(records, run.ingestion_id)
                connection.execute(
                    UPSERT_BOARD_INDEX_DAILY_BAR,
                    self._board_index_daily_bar_envelope_parameters(records),
                )
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_board_index_constituents_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        record: IngestionEnvelope[BoardIndexConstituentSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None:
        self._ensure_single_envelope(record, run.ingestion_id)
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if quality_results:
                connection.execute(INSERT_QUALITY_RESULT, self._quality_parameters(quality_results))
            if not quality_results:
                parameters = self._board_index_constituent_parameters(record)
                header = cast(dict[str, object], parameters["header"])
                members = cast(list[dict[str, object]], parameters["members"])
                connection.execute(
                    text("""
delete from core.board_index_constituent_snapshot
where board_id = :board_id and trade_date = :trade_date
"""),
                    header,
                )
                if members:
                    connection.execute(INSERT_BOARD_INDEX_CONSTITUENT, members)
            connection.execute(UPDATE_INGESTION_RUN, self._run_update_parameters(run))

    def commit_rejected_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality_results: Sequence[QualityResult],
    ) -> None:
        with self._engine.begin() as connection:
            self._insert_manifest(connection, manifest)
            if quality_results:
                connection.execute(INSERT_QUALITY_RESULT, self._quality_parameters(quality_results))
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

    @classmethod
    def _insert_manifest(cls, connection: Connection, manifest: RawManifest | None) -> None:
        if manifest is not None:
            connection.execute(INSERT_RAW_MANIFEST, cls._manifest_parameters(manifest))

    @staticmethod
    def _replay_source_from_row(row: RowMapping) -> ReplaySource:
        raw_id = row["raw_id"]
        manifest = (
            RawManifest(
                raw_id=cast(UUID, raw_id),
                ingestion_id=cast(UUID, row["ingestion_id"]),
                object_path=str(row["object_path"]),
                file_format=RawFileFormat(str(row["file_format"])),
                content_sha256=str(row["content_sha256"]),
                byte_size=int(cast(int, row["byte_size"])),
                row_count=int(cast(int, row["row_count"])),
                schema_version=str(row["schema_version"]),
                storage_backend=str(row["storage_backend"]),
            )
            if raw_id is not None
            else None
        )
        request_params = row["request_params"]
        if not isinstance(request_params, Mapping):
            raise RuntimeError("ingestion request_params is not a mapping")
        return ReplaySource(
            source_ingestion_id=cast(UUID, row["ingestion_id"]),
            provider_code=ProviderCode(str(row["provider_code"])),
            dataset_code=DatasetCode(str(row["dataset_code"])),
            requested_at=cast(datetime, row["requested_at"]),
            request_params=cast(Mapping[str, object], request_params),
            manifest=manifest,
        )

    @staticmethod
    def _security_envelope_parameters(
        records: Iterable[IngestionEnvelope[SecurityRecord]],
    ) -> list[dict[str, object]]:
        return [
            {
                "symbol": envelope.record.symbol,
                "code": envelope.record.code,
                "exchange": envelope.record.exchange.value,
                "current_name": envelope.record.name,
                "security_type": envelope.record.security_type.value,
                "status": envelope.record.status.value,
                "ipo_date": envelope.record.ipo_date,
                "delisting_date": envelope.record.delisting_date,
                "source_code": envelope.record.source_code,
                "ingestion_id": envelope.ingestion_id,
            }
            for envelope in records
        ]

    @staticmethod
    def _security_name_envelope_parameters(
        records: Iterable[IngestionEnvelope[SecurityRecord]], effective_from: date
    ) -> list[dict[str, object]]:
        return [
            {
                "symbol": envelope.record.symbol,
                "name": envelope.record.name,
                "effective_from": effective_from,
                "source_code": envelope.record.source_code,
                "ingestion_id": envelope.ingestion_id,
            }
            for envelope in records
        ]

    @staticmethod
    def _trading_day_envelope_parameters(
        records: Iterable[IngestionEnvelope[CalculatedTradingDay]],
    ) -> list[dict[str, object]]:
        return [
            {
                "market": envelope.record.market.value,
                "trade_date": envelope.record.trade_date,
                "is_trading_day": envelope.record.is_trading_day,
                "previous_trading_day": envelope.record.previous_trading_day,
                "next_trading_day": envelope.record.next_trading_day,
                "source_code": envelope.record.source_code,
                "ingestion_id": envelope.ingestion_id,
            }
            for envelope in records
        ]

    @staticmethod
    def _daily_bar_envelope_parameters(
        records: Iterable[IngestionEnvelope[DailyBarRecord]],
    ) -> list[dict[str, object]]:
        return [
            {
                "symbol": envelope.record.symbol,
                "trade_date": envelope.record.trade_date,
                "market": envelope.record.market.value,
                "open": envelope.record.open,
                "high": envelope.record.high,
                "low": envelope.record.low,
                "close": envelope.record.close,
                "previous_close": envelope.record.previous_close,
                "volume": envelope.record.volume,
                "amount": envelope.record.amount,
                "trade_status": envelope.record.trade_status.value,
                "is_st": envelope.record.is_st,
                "source_code": envelope.record.source_code,
                "ingestion_id": envelope.ingestion_id,
            }
            for envelope in records
        ]

    @staticmethod
    def _share_capital_envelope_parameters(
        records: Iterable[IngestionEnvelope[ShareCapitalRecord]],
    ) -> list[dict[str, object]]:
        return [
            {
                "symbol": envelope.record.symbol,
                "effective_date": envelope.record.effective_date,
                "total_shares": envelope.record.total_shares,
                "restricted_shares": envelope.record.restricted_shares,
                "circulating_shares": envelope.record.circulating_shares,
                "listed_a_shares": envelope.record.listed_a_shares,
                "change_reason": envelope.record.change_reason,
                "source_code": envelope.record.source_code,
                "ingestion_id": envelope.ingestion_id,
            }
            for envelope in records
        ]

    @staticmethod
    def _distribution_envelope_parameters(
        records: Iterable[IngestionEnvelope[DistributionRecord]],
    ) -> list[dict[str, object]]:
        return [
            {
                "symbol": envelope.record.symbol,
                "report_period": envelope.record.report_period,
                "announcement_date": envelope.record.announcement_date,
                "record_date": envelope.record.record_date,
                "ex_date": envelope.record.ex_date,
                "cash_dividend_per_share": envelope.record.cash_dividend_per_share,
                "bonus_share_ratio": envelope.record.bonus_share_ratio,
                "transfer_share_ratio": envelope.record.transfer_share_ratio,
                "status": envelope.record.status.value,
                "source_code": envelope.record.source_code,
                "ingestion_id": envelope.ingestion_id,
            }
            for envelope in records
        ]

    @staticmethod
    def _rights_issue_envelope_parameters(
        records: Iterable[IngestionEnvelope[RightsIssueRecord]],
    ) -> list[dict[str, object]]:
        return [
            {
                "symbol": envelope.record.symbol,
                "record_date": envelope.record.record_date,
                "announcement_date": envelope.record.announcement_date,
                "ex_date": envelope.record.ex_date,
                "payment_start_date": envelope.record.payment_start_date,
                "payment_end_date": envelope.record.payment_end_date,
                "listing_date": envelope.record.listing_date,
                "rights_ratio": envelope.record.rights_ratio,
                "rights_price": envelope.record.rights_price,
                "base_shares": envelope.record.base_shares,
                "proceeds": envelope.record.proceeds,
                "source_code": envelope.record.source_code,
                "ingestion_id": envelope.ingestion_id,
            }
            for envelope in records
        ]

    @staticmethod
    def _classification_catalog_parameters(
        envelope: IngestionEnvelope[ClassificationCatalogSnapshotRecord],
    ) -> dict[str, object]:
        record = envelope.record
        header: dict[str, object] = {
            "namespace": record.namespace,
            "classification_type": record.classification_type.value,
            "snapshot_date": record.snapshot_date,
            "definition_count": len(record.definitions),
            "source_code": record.source_code,
            "ingestion_id": envelope.ingestion_id,
        }
        definitions = [
            {
                **header,
                "classification_code": definition.code,
                "name": definition.name,
                "level": definition.level,
                "parent_code": definition.parent_code,
            }
            for definition in record.definitions
        ]
        return {"header": header, "definitions": definitions}

    @staticmethod
    def _classification_member_parameters(
        envelope: IngestionEnvelope[ClassificationMemberSnapshotRecord],
    ) -> dict[str, object]:
        record = envelope.record
        header: dict[str, object] = {
            "namespace": record.namespace,
            "classification_type": record.classification_type.value,
            "classification_code": record.classification_code,
            "snapshot_date": record.snapshot_date,
            "member_count": len(record.members),
            "source_code": record.source_code,
            "ingestion_id": envelope.ingestion_id,
        }
        members = [{"symbol": symbol, **header} for symbol in record.members]
        return {"header": header, "members": members}

    @staticmethod
    def _board_index_envelope_parameters(
        records: Iterable[IngestionEnvelope[BoardIndexRecord]],
    ) -> list[dict[str, object]]:
        return [
            {
                "board_id": envelope.record.board_id,
                "board_code": envelope.record.board_code,
                "namespace": envelope.record.namespace,
                "name": envelope.record.name,
                "board_type": envelope.record.board_type.value,
                "market": envelope.record.market.value,
                "status": envelope.record.status.value,
                "source_code": envelope.record.source_code,
                "ingestion_id": envelope.ingestion_id,
            }
            for envelope in records
        ]

    @staticmethod
    def _board_index_daily_bar_envelope_parameters(
        records: Iterable[IngestionEnvelope[BoardIndexDailyBarRecord]],
    ) -> list[dict[str, object]]:
        return [
            {
                "board_id": envelope.record.board_id,
                "trade_date": envelope.record.trade_date,
                "market": envelope.record.market.value,
                "open": envelope.record.open,
                "high": envelope.record.high,
                "low": envelope.record.low,
                "close": envelope.record.close,
                "volume": envelope.record.volume,
                "amount": envelope.record.amount,
                "source_code": envelope.record.source_code,
                "ingestion_id": envelope.ingestion_id,
            }
            for envelope in records
        ]

    @staticmethod
    def _board_index_constituent_parameters(
        envelope: IngestionEnvelope[BoardIndexConstituentSnapshotRecord],
    ) -> dict[str, object]:
        record = envelope.record
        header: dict[str, object] = {
            "board_id": record.board_id,
            "trade_date": record.trade_date,
            "source_code": record.source_code,
            "ingestion_id": envelope.ingestion_id,
        }
        members = [{"symbol": symbol, **header} for symbol in record.members]
        return {"header": header, "members": members}

    @staticmethod
    def _ensure_single_envelope(envelope: IngestionEnvelope[object], ingestion_id: UUID) -> None:
        if envelope.ingestion_id != ingestion_id:
            raise ValueError("ingestion envelope does not match the batch run")

    @staticmethod
    def _ensure_envelope_ids[
        RecordT: SecurityRecord
        | CalculatedTradingDay
        | DailyBarRecord
        | CapitalRecord
        | BoardIndexProviderRecord
    ](records: Iterable[IngestionEnvelope[RecordT]], ingestion_id: UUID) -> None:
        if any(envelope.ingestion_id != ingestion_id for envelope in records):
            raise ValueError("ingestion envelope does not match the batch run")

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


def _source_symbol_variants(symbol: str) -> tuple[str, ...]:
    try:
        exchange, code = symbol.upper().split(":", maxsplit=1)
        prefix = {"SSE": "sh", "SZSE": "sz", "BSE": "bj"}[exchange]
    except (KeyError, ValueError) as error:
        raise ValueError(f"unsupported standard symbol: {symbol}") from error
    variants = {symbol.upper(), code, f"{prefix}.{code}"}
    if exchange in {"SSE", "SZSE"}:
        variants.add(f"{'1' if exchange == 'SSE' else '0'}.{code}")
    return tuple(sorted(variants))
