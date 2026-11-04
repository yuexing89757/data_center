"""Bounded certified input snapshots and immutable five-day volume publication."""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime, timedelta
from json import dumps
from uuid import UUID

from sqlalchemy import Engine, RowMapping, text

from market_data_center.domain.derived import CalculationRun
from market_data_center.domain.records import TradeStatus
from market_data_center.domain.stock_volume_average import (
    ALGORITHM_VERSION,
    CALCULATION_CODE,
    StockVolumeInput,
    VolumeCalculation,
    VolumeDay,
    VolumeWindowInput,
)

InputEvidence = dict[str, list[dict[str, object]]]


class VolumeAverageDependencyNotReady(RuntimeError):
    """An exact calendar, lifecycle or daily-market dependency is unavailable."""


class PostgreSQLStockVolumeAveragePersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def is_trading_day(self, trade_date: date) -> bool | None:
        with self._engine.connect() as connection:
            return connection.execute(
                text("""
select is_trading_day from core.trading_calendar
where market='CN_A_SHARE' and trade_date=:day
"""),
                {"day": trade_date},
            ).scalar_one_or_none()

    @contextmanager
    def build_lock(self, trade_date: date) -> Iterator[None]:
        with self._engine.connect() as connection:
            key = f"stock-volume-average-5d:{trade_date.isoformat()}"
            acquired = connection.execute(
                text("select pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": key}
            ).scalar_one()
            if not acquired:
                raise RuntimeError("five-day volume build is already running for this date")
            try:
                yield
            finally:
                connection.execute(
                    text("select pg_advisory_unlock(hashtextextended(:key,0))"), {"key": key}
                )

    def load_input(
        self, trade_date: date
    ) -> tuple[VolumeWindowInput, dict[str, str | None], InputEvidence]:
        with (
            self._engine.connect().execution_options(isolation_level="REPEATABLE READ") as c,
            c.begin(),
        ):
            days = tuple(
                reversed(
                    c.execute(
                        text("""
select trade_date from core.trading_calendar
where market='CN_A_SHARE' and is_trading_day
  and trade_date between :lower and :day
order by trade_date desc limit 5
"""),
                        {"lower": trade_date - timedelta(days=366), "day": trade_date},
                    )
                    .scalars()
                    .all()
                )
            )
            if len(days) != 5 or days[-1] != trade_date:
                raise VolumeAverageDependencyNotReady("exact target and five trading days required")
            calendar_count = c.execute(
                text("""
select count(*) from core.trading_calendar
where market='CN_A_SHARE' and trade_date between :first and :day
"""),
                {"first": days[0], "day": trade_date},
            ).scalar_one()
            if calendar_count != (trade_date - days[0]).days + 1:
                raise VolumeAverageDependencyNotReady("calendar has missing natural dates")
            workflow = (
                c.execute(
                    text("""
select workflow_run_id, status from operations.workflow_run
where workflow_code='daily_market'
  and (scheduled_for at time zone 'Asia/Shanghai')::date=:day
order by scheduled_for desc, attempt desc limit 1
"""),
                    {"day": trade_date},
                )
                .mappings()
                .one_or_none()
            )
            if workflow is None or workflow["status"] not in {"succeeded", "partial"}:
                raise VolumeAverageDependencyNotReady(
                    "exact terminal daily_market workflow required"
                )
            securities = (
                c.execute(
                    text("""
select symbol, ipo_date, delisting_date, status, updated_at
from core.security
where security_type='stock' and exchange in ('SSE','SZSE','BSE')
  and (ipo_date is null or ipo_date <= :day)
  and (delisting_date is null or delisting_date >= :day)
order by symbol limit 10001
"""),
                    {"day": trade_date},
                )
                .mappings()
                .all()
            )
            if not 0 < len(securities) <= 10_000:
                raise VolumeAverageDependencyNotReady(
                    "stock universe must contain 1 to 10000 stocks"
                )
            if any(
                s["ipo_date"] is None or (s["status"] != "listed" and s["delisting_date"] is None)
                for s in securities
            ):
                raise VolumeAverageDependencyNotReady("stock lifecycle is unknown")
            statement = text("""
select b.symbol, b.trade_date, b.volume, b.trade_status, b.ingestion_id, b.updated_at,
       b.quantity_quality->'field_groups'->'daily_bar.volume' as volume_quality,
       certificate.evidence_hash, certificate.normalizer_version
from audit.unit_safe_daily_bar b
left join audit.quantity_unit_certification certificate
  on certificate.ingestion_id=b.ingestion_id
 and certificate.unit_contract_version='stock-quantity.v1'
 and certificate.field_group='daily_bar.volume'
where b.symbol=any(cast(:symbols as text[]))
  and b.trade_date=any(cast(:days as date[]))
order by b.symbol,b.trade_date
""")
            # Large ANY arrays select a full daily_bar scan; keep index reads bounded.
            rows: list[RowMapping] = []
            for offset in range(0, len(securities), 100):
                rows.extend(
                    c.execute(
                        statement,
                        {
                            "symbols": [s["symbol"] for s in securities[offset : offset + 100]],
                            "days": list(days),
                        },
                    )
                    .mappings()
                    .all()
                )
            indexed = {(r["symbol"], r["trade_date"]): r for r in rows}
            if len(indexed) != len(rows):
                raise ValueError("duplicate daily input")
            stocks = []
            evidence: InputEvidence = {}
            for security in securities:
                symbol = security["symbol"]
                observations = []
                proof = []
                for day in days:
                    row = indexed.get((symbol, day))
                    quality = row["volume_quality"] if row is not None else None
                    certified = quality is not None and quality.get("status") == "certified"
                    observation = VolumeDay(
                        day,
                        row["volume"] if row is not None else None,
                        present=row is not None,
                        certified=certified,
                        trade_status=TradeStatus(row["trade_status"])
                        if row
                        else TradeStatus.UNKNOWN,
                    )
                    observations.append(observation)
                    proof.append(
                        {
                            "trade_date": day.isoformat(),
                            "volume": observation.volume,
                            "present": observation.present,
                            "trade_status": observation.trade_status.value,
                            "volume_quality": quality,
                            "ingestion_id": str(row["ingestion_id"]) if row else None,
                            "unit_contract_version": "stock-quantity.v1",
                            "field_group": "daily_bar.volume",
                            "evidence_hash": row["evidence_hash"] if row else None,
                            "normalizer_version": row["normalizer_version"] if row else None,
                        }
                    )
                stocks.append(
                    StockVolumeInput(
                        symbol,
                        security["ipo_date"],
                        security["delisting_date"],
                        tuple(observations),
                    )
                )
                evidence[symbol] = proof
            watermark = {
                "daily_market_workflow_run_id": str(workflow["workflow_run_id"]),
                "window_start_date": days[0].isoformat(),
                "unit_contract_version": "stock-quantity.v1",
                "daily_bar_updated_at": max((str(r["updated_at"]) for r in rows), default=None),
                "security_updated_at": max(str(s["updated_at"]) for s in securities),
            }
            return VolumeWindowInput(trade_date, days, tuple(stocks)), watermark, evidence

    def existing_calculation(self, trade_date: date, input_hash: str) -> UUID | None:
        with self._engine.connect() as c:
            return c.execute(
                text("""
select calculation_id from derived.calculation_run
where calculation_code=:code and algorithm_version=:algorithm
  and start_date=:day and end_date=:day and input_hash=:hash and status='succeeded'
"""),
                {
                    "code": CALCULATION_CODE,
                    "algorithm": ALGORITHM_VERSION,
                    "day": trade_date,
                    "hash": input_hash,
                },
            ).scalar_one_or_none()

    def current_publication(self, trade_date: date) -> tuple[UUID, int, str] | None:
        with self._engine.connect() as c:
            row = c.execute(
                text("""
select calculation_id, version, evidence_hash from derived.stock_volume_average_5d_snapshot
where trade_date=:day order by version desc limit 1
"""),
                {"day": trade_date},
            ).one_or_none()
            return (row[0], row[1], row[2]) if row is not None else None

    def create_calculation_run(self, run: CalculationRun) -> None:
        with self._engine.begin() as c:
            c.execute(
                text("""
insert into derived.calculation_run (
 calculation_id,calculation_code,algorithm_version,mode,start_date,end_date,status,
 input_watermark,input_hash,requested_at,output_rows
) values (:calculation_id,:calculation_code,:algorithm_version,:mode,:start_date,:end_date,
 :status,cast(:input_watermark as jsonb),:input_hash,:requested_at,:output_rows)
"""),
                {**asdict(run), "input_watermark": dumps(run.input_watermark)},
            )

    def publish(
        self,
        run: CalculationRun | None,
        calculation_id: UUID,
        trade_date: date,
        calculation: VolumeCalculation,
        evidence: InputEvidence,
        published_at: datetime,
        *,
        watermark: dict[str, str | None],
        evidence_hash: str,
    ) -> int:
        with self._engine.begin() as c:
            if run is not None:
                if run.calculation_id != calculation_id or run.end_date != trade_date:
                    raise ValueError("publication calculation does not match")
                changed = c.execute(
                    text("""
update derived.calculation_run set status='succeeded',calculated_at=:calculated_at,
 finished_at=:finished_at,output_rows=:output_rows
where calculation_id=:calculation_id and status='running'
"""),
                    asdict(run),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("running calculation is required")
                c.execute(
                    text("""
insert into derived.stock_volume_average_5d (
 calculation_id,symbol,trade_date,window_start_date,volume_average_5d,
 covered_days,quality_reasons,input_evidence
) values (:calculation_id,:symbol,:trade_date,:window_start_date,:volume_average_5d,
 :covered_days,:quality_reasons,cast(:input_evidence as jsonb))
"""),
                    [
                        {
                            **asdict(row),
                            "calculation_id": calculation_id,
                            "quality_reasons": list(row.quality_reasons),
                            "input_evidence": dumps(evidence[row.symbol]),
                        }
                        for row in calculation.records
                    ],
                )
            version = c.execute(
                text("""
insert into derived.stock_volume_average_5d_snapshot (
 trade_date,version,calculation_id,published_at,candidate_count,complete_count,
 input_evidence,input_watermark,evidence_hash
) select :day,coalesce(max(version),0)+1,:id,:published,:candidates,:complete,
         cast(:evidence as jsonb),cast(:watermark as jsonb),:evidence_hash
  from derived.stock_volume_average_5d_snapshot where trade_date=:day
returning version
"""),
                {
                    "day": trade_date,
                    "id": calculation_id,
                    "published": published_at,
                    "candidates": len(calculation.records),
                    "complete": calculation.complete_count,
                    "evidence": dumps(evidence),
                    "watermark": dumps(watermark),
                    "evidence_hash": evidence_hash,
                },
            ).scalar_one()
            return int(version)

    def fail_calculation_run(self, run: CalculationRun) -> None:
        with self._engine.begin() as c:
            c.execute(
                text("""
update derived.calculation_run set status='failed',finished_at=:finished_at,
 output_rows=0,error_summary=:error_summary
where calculation_id=:calculation_id and status='running'
"""),
                asdict(run),
            )
