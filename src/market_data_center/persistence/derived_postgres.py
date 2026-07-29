"""PostgreSQL loading and atomic persistence for versioned derived calculations."""

from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import date, datetime
from typing import cast
from uuid import UUID

from sqlalchemy import Connection, Engine, RowMapping, text

from market_data_center.domain.classification import ClassificationType
from market_data_center.domain.derived import (
    CalculationRun,
    ClassificationMembershipSnapshot,
    DerivedCalculationInput,
    DerivedCalculationOutput,
)
from market_data_center.domain.records import (
    CorporateActionStatus,
    DailyBarRecord,
    DistributionRecord,
    Market,
    RightsIssueRecord,
    ShareCapitalRecord,
    TradeStatus,
)


class PostgreSQLDerivedPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @contextmanager
    def calculation_lock(
        self, calculation_code: str, algorithm_version: str, start_date: date, end_date: date
    ) -> Iterator[None]:
        task_key = f"{calculation_code}:{algorithm_version}:{start_date}:{end_date}"
        with self._engine.connect() as connection:
            acquired = connection.execute(
                text("select pg_try_advisory_lock(hashtextextended(:task_key, 0))"),
                {"task_key": task_key},
            ).scalar_one()
            if not acquired:
                raise RuntimeError("derived calculation is already running for this range")
            try:
                yield
            finally:
                connection.execute(
                    text("select pg_advisory_unlock(hashtextextended(:task_key, 0))"),
                    {"task_key": task_key},
                )

    def load_calculation_source(
        self, start_date: date, end_date: date
    ) -> tuple[DerivedCalculationInput, dict[str, str | None]]:
        with (
            self._engine.connect().execution_options(
                isolation_level="REPEATABLE READ"
            ) as connection,
            connection.begin(),
        ):
            inputs = self._load_inputs(connection, start_date, end_date)
            watermark = self._load_watermark(connection, start_date, end_date)
        return inputs, watermark

    def succeeded_calculation_id(
        self,
        *,
        calculation_code: str,
        algorithm_version: str,
        start_date: date,
        end_date: date,
        input_hash: str,
    ) -> UUID | None:
        statement = text("""
select calculation_id
from derived.calculation_run
where calculation_code = :calculation_code
  and algorithm_version = :algorithm_version
  and start_date = :start_date
  and end_date = :end_date
  and input_hash = :input_hash
  and status = 'succeeded'
order by calculated_at desc
limit 1
""")
        with self._engine.connect() as connection:
            return connection.execute(
                statement,
                {
                    "calculation_code": calculation_code,
                    "algorithm_version": algorithm_version,
                    "start_date": start_date,
                    "end_date": end_date,
                    "input_hash": input_hash,
                },
            ).scalar_one_or_none()

    def create_calculation_run(self, run: CalculationRun) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text("""
insert into derived.calculation_run (
    calculation_id, calculation_code, algorithm_version, mode,
    start_date, end_date, status, input_watermark, input_hash, requested_at,
    calculated_at, finished_at, output_rows, error_summary
) values (
    :calculation_id, :calculation_code, :algorithm_version, :mode,
    :start_date, :end_date, :status, cast(:input_watermark as jsonb), :input_hash,
    :requested_at, :calculated_at, :finished_at, :output_rows, :error_summary
)
"""),
                _run_parameters(run),
            )

    def fail_calculation_run(self, run: CalculationRun) -> None:
        with self._engine.begin() as connection:
            connection.execute(_UPDATE_CALCULATION_RUN, _run_parameters(run))

    def commit_calculation(self, run: CalculationRun, output: DerivedCalculationOutput) -> None:
        with self._engine.begin() as connection:
            if output.adjusted_daily_bars:
                connection.execute(
                    text("""
insert into derived.adjusted_daily_bar (
    calculation_id, symbol, trade_date, adjustment_type, adjustment_factor,
    open, high, low, close, previous_close
) values (
    :calculation_id, :symbol, :trade_date, :adjustment_type, :adjustment_factor,
    :open, :high, :low, :close, :previous_close
)
"""),
                    [
                        {
                            "calculation_id": run.calculation_id,
                            "symbol": record.symbol,
                            "trade_date": record.trade_date,
                            "adjustment_type": record.adjustment_type.value,
                            "adjustment_factor": record.adjustment_factor,
                            "open": record.open,
                            "high": record.high,
                            "low": record.low,
                            "close": record.close,
                            "previous_close": record.previous_close,
                        }
                        for record in output.adjusted_daily_bars
                    ],
                )
            if output.daily_metrics:
                connection.execute(
                    text("""
insert into derived.daily_metric (
    calculation_id, symbol, trade_date, total_return_1d,
    moving_average_5, moving_average_10, moving_average_20
) values (
    :calculation_id, :symbol, :trade_date, :total_return_1d,
    :moving_average_5, :moving_average_10, :moving_average_20
)
"""),
                    [
                        {
                            "calculation_id": run.calculation_id,
                            "symbol": record.symbol,
                            "trade_date": record.trade_date,
                            "total_return_1d": record.total_return_1d,
                            "moving_average_5": record.moving_average_5,
                            "moving_average_10": record.moving_average_10,
                            "moving_average_20": record.moving_average_20,
                        }
                        for record in output.daily_metrics
                    ],
                )
            if output.market_capitalizations:
                connection.execute(
                    text("""
insert into derived.market_capitalization (
    calculation_id, symbol, trade_date,
    total_market_cap, circulating_market_cap
) values (
    :calculation_id, :symbol, :trade_date,
    :total_market_cap, :circulating_market_cap
)
"""),
                    [
                        {
                            "calculation_id": run.calculation_id,
                            "symbol": record.symbol,
                            "trade_date": record.trade_date,
                            "total_market_cap": record.total_market_cap,
                            "circulating_market_cap": record.circulating_market_cap,
                        }
                        for record in output.market_capitalizations
                    ],
                )
            if output.classification_metrics:
                connection.execute(
                    text("""
insert into metrics.classification_daily_metric (
    calculation_id, namespace, classification_type, classification_code,
    membership_snapshot_date, trade_date, member_count, priced_member_count,
    advancing_count, declining_count, unchanged_count, total_volume,
    total_amount, equal_weight_return, total_market_cap, market_cap_member_count
) values (
    :calculation_id, :namespace, :classification_type, :classification_code,
    :membership_snapshot_date, :trade_date, :member_count, :priced_member_count,
    :advancing_count, :declining_count, :unchanged_count, :total_volume,
    :total_amount, :equal_weight_return, :total_market_cap, :market_cap_member_count
)
"""),
                    [
                        {
                            "calculation_id": run.calculation_id,
                            "namespace": record.namespace,
                            "classification_type": record.classification_type.value,
                            "classification_code": record.classification_code,
                            "membership_snapshot_date": record.membership_snapshot_date,
                            "trade_date": record.trade_date,
                            "member_count": record.member_count,
                            "priced_member_count": record.priced_member_count,
                            "advancing_count": record.advancing_count,
                            "declining_count": record.declining_count,
                            "unchanged_count": record.unchanged_count,
                            "total_volume": record.total_volume,
                            "total_amount": record.total_amount,
                            "equal_weight_return": record.equal_weight_return,
                            "total_market_cap": record.total_market_cap,
                            "market_cap_member_count": record.market_cap_member_count,
                        }
                        for record in output.classification_metrics
                    ],
                )
            connection.execute(_UPDATE_CALCULATION_RUN, _run_parameters(run))

    def _load_inputs(
        self, connection: Connection, start_date: date, end_date: date
    ) -> DerivedCalculationInput:
        parameters = {"start_date": start_date, "end_date": end_date}
        bars = tuple(
            _daily_bar(row) for row in connection.execute(_SELECT_DAILY_BARS, parameters).mappings()
        )
        distributions = tuple(
            _distribution(row)
            for row in connection.execute(_SELECT_DISTRIBUTIONS, parameters).mappings()
        )
        rights_issues = tuple(
            _rights_issue(row)
            for row in connection.execute(_SELECT_RIGHTS_ISSUES, parameters).mappings()
        )
        share_capital = tuple(
            _share_capital(row)
            for row in connection.execute(_SELECT_SHARE_CAPITAL, parameters).mappings()
        )
        memberships = _membership_snapshots(
            connection.execute(_SELECT_MEMBERSHIPS, parameters).mappings()
        )
        return DerivedCalculationInput(
            daily_bars=bars,
            distributions=distributions,
            rights_issues=rights_issues,
            share_capital=share_capital,
            memberships=memberships,
        )

    @staticmethod
    def _load_watermark(
        connection: Connection, start_date: date, end_date: date
    ) -> dict[str, str | None]:
        row = (
            connection.execute(_SELECT_WATERMARK, {"start_date": start_date, "end_date": end_date})
            .mappings()
            .one()
        )
        watermark: dict[str, str | None] = {}
        for key in row:
            value = cast(datetime | None, row[key])
            watermark[key] = value.isoformat() if value is not None else None
        return watermark


_SELECT_DAILY_BARS = text("""
with target_symbols as (
    select distinct symbol
    from core.daily_bar
    where trade_date between :start_date and :end_date
)
select symbol, trade_date, market, open, high, low, close, previous_close,
       volume, amount, trade_status, is_st, source_code
from core.daily_bar
where trade_date <= :end_date
  and symbol in (select symbol from target_symbols)
order by symbol, trade_date
""")

_SELECT_DISTRIBUTIONS = text("""
with target_symbols as (
    select distinct symbol
    from core.daily_bar
    where trade_date between :start_date and :end_date
)
select symbol, report_period, announcement_date, record_date, ex_date,
       cash_dividend_per_share, bonus_share_ratio, transfer_share_ratio,
       status, source_code
from capital.distribution
where ex_date <= :end_date
  and symbol in (select symbol from target_symbols)
order by symbol, report_period
""")

_SELECT_RIGHTS_ISSUES = text("""
with target_symbols as (
    select distinct symbol
    from core.daily_bar
    where trade_date between :start_date and :end_date
)
select symbol, record_date, announcement_date, ex_date, payment_start_date,
       payment_end_date, listing_date, rights_ratio, rights_price,
       base_shares, proceeds, source_code
from capital.rights_issue
where ex_date <= :end_date
  and symbol in (select symbol from target_symbols)
order by symbol, record_date
""")

_SELECT_SHARE_CAPITAL = text("""
with target_symbols as (
    select distinct symbol
    from core.daily_bar
    where trade_date between :start_date and :end_date
)
select symbol, effective_date, total_shares, restricted_shares,
       circulating_shares, listed_a_shares, change_reason, source_code
from capital.share_capital
where effective_date <= :end_date
  and symbol in (select symbol from target_symbols)
order by symbol, effective_date
""")

_SELECT_MEMBERSHIPS = text("""
select snapshot.namespace, snapshot.classification_type,
       snapshot.classification_code, snapshot.snapshot_date, item.symbol
from classification.member_snapshot snapshot
left join classification.member_snapshot_item item
  on item.namespace = snapshot.namespace
 and item.classification_type = snapshot.classification_type
 and item.classification_code = snapshot.classification_code
 and item.snapshot_date = snapshot.snapshot_date
where snapshot.snapshot_date <= :end_date
order by snapshot.namespace, snapshot.classification_type,
         snapshot.classification_code, snapshot.snapshot_date, item.symbol
""")

_SELECT_WATERMARK = text("""
with target_symbols as (
    select distinct symbol
    from core.daily_bar
    where trade_date between :start_date and :end_date
)
select
    (select max(updated_at) from core.daily_bar
        where trade_date <= :end_date
          and symbol in (select symbol from target_symbols))
        as daily_bar,
    (select max(updated_at) from capital.share_capital
        where effective_date <= :end_date
          and symbol in (select symbol from target_symbols))
        as share_capital,
    (select max(updated_at) from capital.distribution
        where ex_date <= :end_date
          and symbol in (select symbol from target_symbols))
        as distribution,
    (select max(updated_at) from capital.rights_issue
        where ex_date <= :end_date
          and symbol in (select symbol from target_symbols))
        as rights_issue,
    (select max(updated_at) from classification.catalog_snapshot
        where snapshot_date <= :end_date) as classification_catalog,
    (select max(updated_at) from classification.member_snapshot
        where snapshot_date <= :end_date) as classification_members
""")

_UPDATE_CALCULATION_RUN = text("""
update derived.calculation_run set
    status = :status,
    calculated_at = :calculated_at,
    finished_at = :finished_at,
    output_rows = :output_rows,
    error_summary = :error_summary
where calculation_id = :calculation_id
""")


def _run_parameters(run: CalculationRun) -> dict[str, object]:
    from json import dumps

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


def _daily_bar(row: RowMapping) -> DailyBarRecord:
    return DailyBarRecord(
        symbol=str(row["symbol"]),
        trade_date=cast(date, row["trade_date"]),
        market=Market(str(row["market"])),
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        previous_close=row["previous_close"],
        volume=row["volume"],
        amount=row["amount"],
        trade_status=TradeStatus(str(row["trade_status"])),
        is_st=row["is_st"],
        source_code=str(row["source_code"]),
    )


def _distribution(row: RowMapping) -> DistributionRecord:
    return DistributionRecord(
        symbol=str(row["symbol"]),
        report_period=cast(date, row["report_period"]),
        announcement_date=cast(date | None, row["announcement_date"]),
        record_date=cast(date | None, row["record_date"]),
        ex_date=cast(date | None, row["ex_date"]),
        cash_dividend_per_share=row["cash_dividend_per_share"],
        bonus_share_ratio=row["bonus_share_ratio"],
        transfer_share_ratio=row["transfer_share_ratio"],
        status=CorporateActionStatus(str(row["status"])),
        source_code=str(row["source_code"]),
    )


def _rights_issue(row: RowMapping) -> RightsIssueRecord:
    return RightsIssueRecord(
        symbol=str(row["symbol"]),
        record_date=cast(date, row["record_date"]),
        announcement_date=cast(date | None, row["announcement_date"]),
        ex_date=cast(date | None, row["ex_date"]),
        payment_start_date=cast(date | None, row["payment_start_date"]),
        payment_end_date=cast(date | None, row["payment_end_date"]),
        listing_date=cast(date | None, row["listing_date"]),
        rights_ratio=row["rights_ratio"],
        rights_price=row["rights_price"],
        base_shares=row["base_shares"],
        proceeds=row["proceeds"],
        source_code=str(row["source_code"]),
    )


def _share_capital(row: RowMapping) -> ShareCapitalRecord:
    return ShareCapitalRecord(
        symbol=str(row["symbol"]),
        effective_date=cast(date, row["effective_date"]),
        total_shares=int(row["total_shares"]),
        restricted_shares=row["restricted_shares"],
        circulating_shares=row["circulating_shares"],
        listed_a_shares=row["listed_a_shares"],
        change_reason=cast(str | None, row["change_reason"]),
        source_code=str(row["source_code"]),
    )


def _membership_snapshots(
    rows: Iterable[RowMapping],
) -> tuple[ClassificationMembershipSnapshot, ...]:
    grouped: dict[tuple[str, ClassificationType, str, date], list[str]] = defaultdict(list)
    for row in rows:
        key = (
            str(row["namespace"]),
            ClassificationType(str(row["classification_type"])),
            str(row["classification_code"]),
            cast(date, row["snapshot_date"]),
        )
        if row["symbol"] is not None:
            grouped[key].append(str(row["symbol"]))
        else:
            grouped.setdefault(key, [])
    return tuple(
        ClassificationMembershipSnapshot(
            namespace=namespace,
            classification_type=classification_type,
            classification_code=code,
            snapshot_date=snapshot_date,
            members=tuple(members),
        )
        for (namespace, classification_type, code, snapshot_date), members in grouped.items()
    )
