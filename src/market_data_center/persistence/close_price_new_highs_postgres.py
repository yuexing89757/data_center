"""PostgreSQL input loading and immutable publication for 120-day closing highs."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import date
from json import dumps
from uuid import UUID

from sqlalchemy import Engine, RowMapping, text

from market_data_center.domain.close_price_new_highs import (
    ClosePriceNewHighCalculation,
    ClosePriceNewHighCandidate,
    ClosePriceNewHighInput,
    ClosePriceNewHighSnapshot,
)
from market_data_center.domain.derived import CalculationRun
from market_data_center.domain.records import TradeStatus

CALCULATION_CODE = "cn_a_close_price_new_highs_120d"


class ClosePriceNewHighsDependencyNotReady(RuntimeError):
    """An exact calendar, workflow, or market-data dependency is not ready."""


class PostgreSQLClosePriceNewHighsPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @contextmanager
    def build_lock(self, trade_date: date) -> Iterator[None]:
        key = f"close-price-new-highs-120d:{trade_date.isoformat()}"
        with self._engine.connect() as connection:
            acquired = connection.execute(
                text("select pg_try_advisory_lock(hashtextextended(:key, 0))"), {"key": key}
            ).scalar_one()
            if not acquired:
                raise RuntimeError("closing-high snapshot build is already running for this date")
            try:
                yield
            finally:
                connection.execute(
                    text("select pg_advisory_unlock(hashtextextended(:key, 0))"), {"key": key}
                )

    def load_input(
        self, trade_date: date
    ) -> tuple[ClosePriceNewHighInput, dict[str, str | None]]:
        with (
            self._engine.connect().execution_options(
                isolation_level="REPEATABLE READ"
            ) as connection,
            connection.begin(),
        ):
            days = tuple(
                connection.execute(
                    text("""
select trade_date
from core.trading_calendar
where market='CN_A_SHARE' and is_trading_day and trade_date <= :trade_date
order by trade_date desc
limit 120
"""),
                    {"trade_date": trade_date},
                ).scalars()
            )
            if len(days) != 120 or days[0] != trade_date:
                raise ClosePriceNewHighsDependencyNotReady(
                    "exact target date and 120 trading sessions are required"
                )
            workflow_run_id = connection.execute(
                text("""
select workflow_run_id::text
from operations.workflow_run
where workflow_code='daily_market'
  and status in ('succeeded','partial')
  and (scheduled_for at time zone 'Asia/Shanghai')::date=:trade_date
order by finished_at desc
limit 1
"""),
                {"trade_date": trade_date},
            ).scalar_one_or_none()
            if workflow_run_id is None:
                raise ClosePriceNewHighsDependencyNotReady(
                    "exact terminal daily_market workflow is required"
                )
            rows = connection.execute(
                _SELECT_CANDIDATES,
                {"trade_date": trade_date, "first_trade_date": days[-1]},
            ).mappings()
            candidates = tuple(_candidate(row) for row in rows)
            if not candidates:
                raise ClosePriceNewHighsDependencyNotReady("SSE/SZSE stock universe is empty")
            if len(candidates) > 10_000:
                raise ClosePriceNewHighsDependencyNotReady(
                    "SSE/SZSE stock universe exceeds 10,000"
                )
            watermark = {
                "trade_date": trade_date.isoformat(),
                "first_trade_date": days[-1].isoformat(),
                "daily_market_workflow_run_id": workflow_run_id,
            }
        return ClosePriceNewHighInput(trade_date, days[-1], len(days), candidates), watermark

    def existing_snapshot(self, trade_date: date, input_hash: str) -> tuple[UUID, UUID] | None:
        with self._engine.connect() as connection:
            row = connection.execute(
                text("""
select calculation_id, snapshot_id
from derived.close_price_new_high_120d_snapshot
where trade_date=:trade_date and input_hash=:input_hash and status='ready'
order by version desc
limit 1
"""),
                {"trade_date": trade_date, "input_hash": input_hash},
            ).one_or_none()
        return (row[0], row[1]) if row is not None else None

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
                _run_parameters(run),
            )

    def next_snapshot_version(self, trade_date: date) -> int:
        with self._engine.connect() as connection:
            version = connection.execute(
                text("""
select coalesce(max(version),0)+1
from derived.close_price_new_high_120d_snapshot
where trade_date=:trade_date
"""),
                {"trade_date": trade_date},
            ).scalar_one()
        return int(version)

    def publish(
        self,
        run: CalculationRun,
        calculation: ClosePriceNewHighCalculation,
        snapshot: ClosePriceNewHighSnapshot,
    ) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text("""
update derived.calculation_run
set status=:status, calculated_at=:calculated_at, finished_at=:finished_at,
    output_rows=:output_rows, error_summary=null
where calculation_id=:calculation_id and status='running'
"""),
                _run_parameters(run),
            )
            connection.execute(
                text("""
insert into derived.close_price_new_high_120d_snapshot (
 snapshot_id, calculation_id, trade_date, version, status,
 candidate_count, eligible_history_count, omitted_count, member_count,
 incomplete_history_count, non_trading_bar_count, nonpositive_price_count,
 missing_name_count, input_hash, content_hash, algorithm_version, generated_at
) values (
 :snapshot_id, :calculation_id, :trade_date, :version, 'ready',
 :candidate_count, :eligible_history_count, :omitted_count, :member_count,
 :incomplete_history_count, :non_trading_bar_count, :nonpositive_price_count,
 :missing_name_count, :input_hash, :content_hash, :algorithm_version, :generated_at
)
"""),
                {
                    "snapshot_id": snapshot.snapshot_id,
                    "calculation_id": snapshot.calculation_id,
                    "trade_date": snapshot.trade_date,
                    "version": snapshot.version,
                    "candidate_count": snapshot.candidate_count,
                    "eligible_history_count": snapshot.eligible_history_count,
                    "omitted_count": snapshot.omitted_count,
                    "member_count": snapshot.member_count,
                    "incomplete_history_count": snapshot.incomplete_history_count,
                    "non_trading_bar_count": snapshot.non_trading_bar_count,
                    "nonpositive_price_count": snapshot.nonpositive_price_count,
                    "missing_name_count": snapshot.missing_name_count,
                    "input_hash": snapshot.input_hash,
                    "content_hash": snapshot.content_hash,
                    "algorithm_version": snapshot.algorithm_version,
                    "generated_at": snapshot.generated_at,
                },
            )
            if calculation.members:
                connection.execute(
                    text("""
insert into derived.close_price_new_high_120d_member (
 snapshot_id, symbol, display_name, close, previous_119d_high, breakout_pct
) values (
 :snapshot_id, :symbol, :display_name, :close, :previous_119d_high, :breakout_pct
)
"""),
                    [
                        {
                            "snapshot_id": snapshot.snapshot_id,
                            "symbol": member.symbol,
                            "display_name": member.display_name,
                            "close": member.close,
                            "previous_119d_high": member.previous_119d_high,
                            "breakout_pct": member.breakout_pct,
                        }
                        for member in calculation.members
                    ],
                )

    def fail_calculation_run(self, run: CalculationRun) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text("""
update derived.calculation_run
set status=:status, finished_at=:finished_at, output_rows=0,
    error_summary=:error_summary
where calculation_id=:calculation_id and status='running'
"""),
                _run_parameters(run),
            )


def _candidate(row: RowMapping) -> ClosePriceNewHighCandidate:
    current_status = row["current_status"]
    return ClosePriceNewHighCandidate(
        symbol=row["symbol"],
        code=row["code"],
        display_name=row["display_name"],
        valid_bar_count=row["valid_bar_count"],
        close=row["close"],
        current_status=TradeStatus(current_status) if current_status is not None else None,
        previous_119d_high=row["previous_119d_high"],
        has_non_trading_bar=row["has_non_trading_bar"],
        has_nonpositive_price=row["has_nonpositive_price"],
    )


def _run_parameters(run: CalculationRun) -> Mapping[str, object]:
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


_SELECT_CANDIDATES = text("""
with candidates as (
    select s.symbol, s.code
    from core.security s
    where s.exchange in ('SSE','SZSE')
      and s.security_type='stock'
      and (s.ipo_date is null or s.ipo_date <= :trade_date)
      and (s.delisting_date is null or s.delisting_date >= :trade_date)
), bars_in_window as materialized (
    select b.symbol, b.trade_date, b.close, b.trade_status
    from core.daily_bar b
    where b.market='CN_A_SHARE'
      and b.trade_date between :first_trade_date and :trade_date
), stats as (
    select c.symbol, c.code,
           count(*) filter (
               where b.close > 0 and b.trade_status in ('trading','unknown')
           )::integer as valid_bar_count,
           max(b.close) filter (where b.trade_date=:trade_date) as close,
           max(b.trade_status) filter (where b.trade_date=:trade_date) as current_status,
           max(b.close) filter (
               where b.trade_date < :trade_date and b.close > 0
                 and b.trade_status in ('trading','unknown')
           ) as previous_119d_high,
           coalesce(bool_or(
               b.trade_status is not null
               and b.trade_status not in ('trading','unknown')
           ),false) as has_non_trading_bar,
           coalesce(bool_or(b.close is not null and b.close <= 0),false)
               as has_nonpositive_price
    from candidates c
    left join bars_in_window b on b.symbol=c.symbol
    group by c.symbol,c.code
)
select stats.*, name_history.name as display_name
from stats
left join lateral (
    select nh.name
    from core.security_name_history nh
    where nh.symbol=stats.symbol
      and nh.effective_from <= :trade_date
      and (nh.effective_to is null or nh.effective_to >= :trade_date)
    order by nh.effective_from desc
    limit 1
) name_history on true
order by stats.symbol
""")
