"""Fixed-scope, bounded transactions for ADR-0078; no filesystem or archive access."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import Connection, Engine, text

from market_data_center.direct_retention_service import BATCH_ROWS

SHANGHAI = ZoneInfo("Asia/Shanghai")
PREDICATES = {
    "quality": "q.created_at < :before",
    "monitor_input": "r.trade_date < :cutoff and r.status <> 'RUNNING'",
}
SOURCES = {
    "quality": "audit.quality_result q",
    "monitor_input": (
        "regulation.monitor_input q join regulation.calculation_run r using(calculation_id)"
    ),
}


class PostgreSQLDirectRetentionPersistence:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @contextmanager
    def transaction(self, *, readonly: bool = False) -> Iterator[Connection]:
        with self.engine.begin() as connection:
            if readonly:
                connection.execute(text("set transaction read only"))
            connection.execute(text("set local lock_timeout='2s'"))
            connection.execute(text("set local statement_timeout='30s'"))
            yield connection

    def parameters(self, cutoff: date) -> dict[str, Any]:
        return {"cutoff": cutoff, "before": datetime.combine(cutoff, time.min, SHANGHAI)}

    def delete_batch(self, target: str, cutoff: date, limit: int) -> int:
        if target not in PREDICATES or not 1 <= limit <= BATCH_ROWS:
            raise ValueError("invalid retention scope or batch size")
        if target == "quality":
            statement = """
            with candidates as materialized (
                select q.quality_result_id from audit.quality_result q
                where q.created_at < :before
                order by q.created_at,q.quality_result_id limit :limit
            ), deleted as (
                delete from audit.quality_result q using candidates c
                where q.quality_result_id=c.quality_result_id and q.created_at < :before
                returning 1
            ) select count(*) from deleted
            """
        else:
            # Expire a whole publication before deleting any of its input rows.
            # Readers must never mistake a partially purged batch for complete data.
            with self.transaction() as connection:
                batch = connection.execute(
                    text("""
                select m.calculation_id from regulation.monitor_context m
                join regulation.calculation_run r using(calculation_id)
                where r.trade_date < :cutoff and r.status <> 'RUNNING'
                  and exists(select 1 from regulation.monitor_input q
                             where q.calculation_id=m.calculation_id)
                order by r.trade_date,m.calculation_id limit 1
                for update of m skip locked
                """),
                    self.parameters(cutoff),
                ).scalar_one_or_none()
                if batch is None:
                    return 0
                connection.execute(
                    text("""
                update regulation.monitor_context m
                set inputs_expired_at=coalesce(m.inputs_expired_at,current_timestamp)
                where m.calculation_id=:batch
                """),
                    {"batch": batch},
                )
                # A separate statement makes the marker visible to DELETE RLS.
                count = connection.execute(
                    text("""
            with candidates as materialized (
                select q.calculation_id,q.symbol from regulation.monitor_input q
                where q.calculation_id=:batch order by q.symbol limit :limit
            ), deleted as (
                delete from regulation.monitor_input q using candidates c
                where q.calculation_id=c.calculation_id and q.symbol=c.symbol returning 1
            ) select count(*) from deleted
                """),
                    {"batch": batch, "limit": limit},
                ).scalar_one()
            return int(count)
        with self.transaction() as connection:
            count = connection.execute(
                text(statement), self.parameters(cutoff) | {"limit": limit}
            ).scalar_one()
        return int(count)

    def has_expired(self, target: str, cutoff: date) -> bool:
        if target not in PREDICATES:
            raise ValueError("invalid retention target")
        with self.transaction(readonly=True) as connection:
            return bool(
                connection.execute(
                    text(
                        f"select exists(select 1 from {SOURCES[target]} where {PREDICATES[target]})"
                    ),
                    self.parameters(cutoff),
                ).scalar_one()
            )

    def preview(self, cutoff: date) -> dict[str, object]:
        result: dict[str, object] = {
            "cutoff": cutoff.isoformat(),
            "quality_before": self.parameters(cutoff)["before"].isoformat(),
            "monitor_trade_date_before": cutoff.isoformat(),
            "archive_required": False,
        }
        with self.transaction(readonly=True) as connection:
            for target in PREDICATES:
                count = int(
                    connection.execute(
                        text(
                            f"select count(*) from (select 1 from {SOURCES[target]} "
                            f"where {PREDICATES[target]} limit 10001) bounded"
                        ),
                        self.parameters(cutoff),
                    ).scalar_one()
                )
                result[target] = {"rows": min(count, 10000), "counts_complete": count <= 10000}
        return result
