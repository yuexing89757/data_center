"""PostgreSQL loading and atomic persistence for immutable stock pools."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import date
from json import dumps
from typing import cast
from uuid import UUID

from sqlalchemy import Connection, Engine, RowMapping, text

from market_data_center.domain.derived import CalculationRun
from market_data_center.domain.records import Exchange, SecurityStatus, SecurityType, TradeStatus
from market_data_center.domain.stock_pool import (
    PRICE_LIMIT_ALGORITHM_VERSION,
    StockPoolBuildInput,
    StockPoolCalculationOutput,
    StockPoolCandidate,
    StockPoolSnapshot,
)

CALCULATION_CODE = "cn_a_mainboard_price_limit_pools"


class StockPoolDependencyNotReady(RuntimeError):
    """An exact upstream workflow or data snapshot is not ready."""


class PostgreSQLStockPoolPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @contextmanager
    def build_lock(self, basis_trade_date: date) -> Iterator[None]:
        key = f"stock-pool:{basis_trade_date.isoformat()}"
        with self._engine.connect() as connection:
            acquired = connection.execute(
                text("select pg_try_advisory_lock(hashtextextended(:key, 0))"), {"key": key}
            ).scalar_one()
            if not acquired:
                raise RuntimeError("stock-pool build is already running for this date")
            try:
                yield
            finally:
                connection.execute(
                    text("select pg_advisory_unlock(hashtextextended(:key, 0))"), {"key": key}
                )

    def resolve_basis_date(self, as_of_date: date) -> tuple[date, date]:
        with self._engine.connect() as connection:
            row = connection.execute(
                text("""
select trade_date, next_trading_day
from core.trading_calendar
where market='CN_A_SHARE' and is_trading_day and trade_date <= :as_of_date
  and next_trading_day is not null
order by trade_date desc limit 1
"""),
                {"as_of_date": as_of_date},
            ).one_or_none()
        if row is None:
            raise StockPoolDependencyNotReady("no basis/effective trading-date pair is available")
        return row[0], row[1]

    def load_build_input(
        self, basis_trade_date: date
    ) -> tuple[StockPoolBuildInput, dict[str, str | None]]:
        with (
            self._engine.connect().execution_options(
                isolation_level="REPEATABLE READ"
            ) as connection,
            connection.begin(),
        ):
            effective = connection.execute(
                text("""
select next_trading_day from core.trading_calendar
where market='CN_A_SHARE' and trade_date=:basis and is_trading_day
"""),
                {"basis": basis_trade_date},
            ).scalar_one_or_none()
            if effective is None:
                raise StockPoolDependencyNotReady("basis date has no exact next trading day")
            dependencies = self._dependency_runs(connection, basis_trade_date)
            if set(dependencies) != {"daily_market", "stock_daily_indicator"}:
                raise StockPoolDependencyNotReady(
                    "exact succeeded-or-partial daily market and indicator workflows are required"
                )
            rows = connection.execute(_SELECT_CANDIDATES, {"basis": basis_trade_date}).mappings()
            candidates = tuple(_candidate(row) for row in rows)
            if not candidates:
                raise StockPoolDependencyNotReady("security universe is empty")
            watermark = {
                "basis_trade_date": basis_trade_date.isoformat(),
                "effective_trade_date": effective.isoformat(),
                "daily_market_workflow_run_id": dependencies["daily_market"],
                "stock_daily_indicator_workflow_run_id": dependencies["stock_daily_indicator"],
            }
        return StockPoolBuildInput(basis_trade_date, effective, candidates), watermark

    @staticmethod
    def _dependency_runs(connection: Connection, basis: date) -> dict[str, str]:
        rows = connection.execute(
            text("""
select distinct on (workflow_code) workflow_code, workflow_run_id::text
from operations.workflow_run
where workflow_code in ('daily_market','stock_daily_indicator')
  and status in ('succeeded','partial')
  and (scheduled_for at time zone 'Asia/Shanghai')::date=:basis
order by workflow_code, finished_at desc
"""),
            {"basis": basis},
        )
        return {row[0]: row[1] for row in rows}

    def succeeded_calculation_id(self, basis: date, input_hash: str) -> UUID | None:
        with self._engine.connect() as connection:
            return connection.execute(
                text("""
select calculation_id from derived.calculation_run
where calculation_code=:code and algorithm_version=:version
  and start_date=:basis and end_date=:basis and input_hash=:input_hash
  and status='succeeded'
order by finished_at desc limit 1
"""),
                {
                    "code": CALCULATION_CODE,
                    "version": PRICE_LIMIT_ALGORITHM_VERSION,
                    "basis": basis,
                    "input_hash": input_hash,
                },
            ).scalar_one_or_none()

    def snapshot_ids_for_calculation(self, calculation_id: UUID) -> tuple[UUID, ...]:
        with self._engine.connect() as connection:
            return tuple(
                connection.execute(
                    text("""
select snapshot_id from stock_pool.snapshot
where calculation_id=:calculation_id order by pool_code
"""),
                    {"calculation_id": calculation_id},
                ).scalars()
            )

    def create_calculation_run(self, run: CalculationRun) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text("""
insert into derived.calculation_run (
 calculation_id, calculation_code, algorithm_version, mode, start_date, end_date,
 status, input_watermark, input_hash, requested_at, output_rows
) values (
 :calculation_id, :calculation_code, :algorithm_version, :mode, :start_date, :end_date,
 :status, cast(:input_watermark as jsonb), :input_hash, :requested_at, :output_rows
)
"""),
                _calculation_parameters(run),
            )

    def fail_calculation_run(self, run: CalculationRun) -> None:
        with self._engine.begin() as connection:
            connection.execute(_UPDATE_CALCULATION, _calculation_parameters(run))

    def commit_build(
        self,
        run: CalculationRun,
        output: StockPoolCalculationOutput,
        snapshots: tuple[StockPoolSnapshot, ...],
    ) -> None:
        with self._engine.begin() as connection:
            if output.daily_price_limits:
                connection.execute(
                    text("""
insert into derived.daily_price_limit (
 calculation_id, symbol, trade_date, previous_close, upper_limit, lower_limit,
 limit_ratio, price_tick, is_st, rule_version, algorithm_version
) values (
 :calculation_id, :symbol, :trade_date, :previous_close, :upper_limit, :lower_limit,
 :limit_ratio, :price_tick, :is_st, :rule_version, :algorithm_version
)
"""),
                    [
                        {"calculation_id": run.calculation_id, **_slots(record)}
                        for record in output.daily_price_limits
                    ],
                )
            if output.events:
                connection.execute(
                    text("""
insert into derived.price_limit_event (
 calculation_id, symbol, trade_date, direction, close, limit_price,
 rule_version, algorithm_version
) values (
 :calculation_id, :symbol, :trade_date, :direction, :close, :limit_price,
 :rule_version, :algorithm_version
)
"""),
                    [
                        {"calculation_id": run.calculation_id, **_slots(record)}
                        for record in output.events
                    ],
                )
            connection.execute(
                text("""
insert into stock_pool.snapshot (
 snapshot_id, calculation_id, pool_code, basis_trade_date, effective_trade_date,
 version, status, member_count, candidate_count, rejected_count, content_hash,
 input_hash, rule_version, algorithm_version, generated_at
) values (
 :snapshot_id, :calculation_id, :pool_code, :basis_trade_date, :effective_trade_date,
 :version, :status, :member_count, :candidate_count, :rejected_count, :content_hash,
 :input_hash, :rule_version, :algorithm_version, :generated_at
)
"""),
                [_slots(snapshot) for snapshot in snapshots],
            )
            snapshot_by_pool = {item.pool_code: item.snapshot_id for item in snapshots}
            if output.members:
                connection.execute(
                    text("""
insert into stock_pool.member (snapshot_id, symbol, direction)
values (:snapshot_id, :symbol, :direction)
"""),
                    [
                        {
                            "snapshot_id": snapshot_by_pool[item.pool_code],
                            "symbol": item.symbol,
                            "direction": item.direction.value,
                        }
                        for item in output.members
                    ],
                )
            if output.findings:
                connection.execute(
                    text("""
insert into stock_pool.calculation_quality (
 calculation_id, rule_code, severity, symbol, message
) values (:calculation_id, :rule_code, :severity, :symbol, :message)
"""),
                    [
                        {"calculation_id": run.calculation_id, **_slots(item)}
                        for item in output.findings
                    ],
                )
            connection.execute(_UPDATE_CALCULATION, _calculation_parameters(run))

    def next_snapshot_version(self, pool_code: str, effective_date: date) -> int:
        with self._engine.begin() as connection:
            connection.execute(
                text("select pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"stock-pool-version:{pool_code}:{effective_date}"},
            )
            return int(
                connection.execute(
                    text("""
select coalesce(max(version),0)+1 from stock_pool.snapshot
where pool_code=:pool_code and effective_trade_date=:effective_date
"""),
                    {"pool_code": pool_code, "effective_date": effective_date},
                ).scalar_one()
            )

    def inspect_ready_snapshot(
        self, pool_code: str, effective_date: date, version: int | None = None
    ) -> Mapping[str, object]:
        with self._engine.connect() as connection:
            snapshot = (
                connection.execute(
                    text("""
select snapshot_id, pool_code, basis_trade_date, effective_trade_date, version,
 member_count, candidate_count, rejected_count, content_hash, rule_version,
 algorithm_version, generated_at
from stock_pool.snapshot
where pool_code=:pool_code and effective_trade_date=:effective_date and status='ready'
  and (:version is null or version=:version)
order by version desc limit 1
"""),
                    {
                        "pool_code": pool_code,
                        "effective_date": effective_date,
                        "version": version,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if snapshot is None:
                raise LookupError("exact ready stock-pool snapshot does not exist")
            members = tuple(
                connection.execute(
                    text("""
select symbol from stock_pool.member where snapshot_id=:snapshot_id order by symbol
"""),
                    {"snapshot_id": snapshot["snapshot_id"]},
                ).scalars()
            )
        return {**dict(snapshot), "snapshot_id": str(snapshot["snapshot_id"]), "members": members}


_SELECT_CANDIDATES = text("""
with prior_five_dates as (
    select trade_date from core.trading_calendar
    where market='CN_A_SHARE' and is_trading_day and trade_date < :basis
    order by trade_date desc limit 5
)
select s.symbol, s.code, s.exchange, s.security_type, s.status, s.ipo_date,
       case when s.ipo_date is null then null else (
           select count(*) from core.trading_calendar c
           where c.market='CN_A_SHARE' and c.is_trading_day
             and c.trade_date between s.ipo_date and :basis
       ) end as listing_trading_day_number,
       (select count(*) from core.daily_bar p
        where p.symbol=s.symbol and p.trade_date in (select trade_date from prior_five_dates)
          and p.trade_status in ('trading','unknown')) as prior_five_bar_count,
       b.trade_status, b.previous_close, b.open, b.high, b.low, b.close, b.is_st,
       b.ingestion_id as daily_bar_ingestion_id,
       i.ingestion_id as indicator_ingestion_id,
       i.free_float_turnover_rate_pct, i.free_float_shares, i.circulating_market_value
from core.security s
left join core.daily_bar b on b.symbol=s.symbol and b.trade_date=:basis
left join core.stock_daily_indicator i on i.symbol=s.symbol and i.trade_date=:basis
where s.exchange in ('SSE','SZSE') and s.security_type='stock' and s.status='listed'
  and (
      (s.exchange='SSE' and (
          s.code between '600000' and '603999' or s.code between '605000' and '605999'
      ))
      or (s.exchange='SZSE' and s.code between '000001' and '004999'
          and s.code not between '001001' and '001199')
  )
order by s.symbol
""")

_UPDATE_CALCULATION = text("""
update derived.calculation_run set status=:status, calculated_at=:calculated_at,
 finished_at=:finished_at, output_rows=:output_rows, error_summary=:error_summary
where calculation_id=:calculation_id
""")


def _candidate(row: RowMapping) -> StockPoolCandidate:
    return StockPoolCandidate(
        symbol=str(row["symbol"]),
        code=str(row["code"]),
        exchange=Exchange(str(row["exchange"])),
        security_type=SecurityType(str(row["security_type"])),
        security_status=SecurityStatus(str(row["status"])),
        ipo_date=row["ipo_date"],
        listing_trading_day_number=row["listing_trading_day_number"],
        prior_five_bar_count=cast(int, row["prior_five_bar_count"]),
        trade_status=TradeStatus(str(row["trade_status"])) if row["trade_status"] else None,
        previous_close=row["previous_close"],
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        is_st=row["is_st"],
        daily_bar_ingestion_id=row["daily_bar_ingestion_id"],
        indicator_ingestion_id=row["indicator_ingestion_id"],
        free_float_turnover_rate_pct=row["free_float_turnover_rate_pct"],
        free_float_shares=row["free_float_shares"],
        circulating_market_value=row["circulating_market_value"],
    )


def _slots(value: object) -> dict[str, object]:
    return {
        name: (item.value if hasattr(item, "value") else item)
        for name in value.__slots__  # type: ignore[attr-defined]
        for item in (getattr(value, name),)
    }


def _calculation_parameters(run: CalculationRun) -> dict[str, object]:
    return {
        "calculation_id": run.calculation_id,
        "calculation_code": run.calculation_code,
        "algorithm_version": run.algorithm_version,
        "mode": run.mode.value,
        "start_date": run.start_date,
        "end_date": run.end_date,
        "status": run.status.value,
        "input_watermark": dumps(run.input_watermark, sort_keys=True),
        "input_hash": run.input_hash,
        "requested_at": run.requested_at,
        "calculated_at": run.calculated_at,
        "finished_at": run.finished_at,
        "output_rows": run.output_rows,
        "error_summary": run.error_summary,
    }
