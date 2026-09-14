"""Atomic PostgreSQL publication of the reviewed hot-money catalog."""

from datetime import date

from sqlalchemy import Engine, text

from market_data_center.hot_money_catalog_service import HotMoneyCatalog
from market_data_center.hot_money_tushare_catalog import StableSeatTradeFact


class PostgreSQLHotMoneyPersistence:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def replace_catalog(self, catalog: HotMoneyCatalog) -> None:
        seat_ids = tuple(mapping.seat_id for mapping in catalog.mappings)
        with self._engine.begin() as connection:
            connection.execute(
                text("select pg_advisory_xact_lock(hashtextextended(:key,0))"),
                {"key": "hot-money-catalog"},
            )
            if seat_ids:
                known = set(
                    connection.scalars(
                        text("""
select seat_id from billboard.trading_seat
where seat_id=any(cast(:seat_ids as uuid[]))
"""),
                        {"seat_ids": list(seat_ids)},
                    )
                )
                if set(seat_ids) - known:
                    raise ValueError("hot-money catalog references unknown stable seats")
            connection.execute(text("update billboard.hot_money_actor set is_active=false"))
            for actor in catalog.actors:
                connection.execute(
                    text("""
insert into billboard.hot_money_actor (actor_code,canonical_name,aliases,is_active)
values (:actor_code,:canonical_name,:aliases,:is_active)
on conflict (actor_code) do update set
 canonical_name=excluded.canonical_name,aliases=excluded.aliases,is_active=excluded.is_active
"""),
                    {
                        "actor_code": actor.actor_code,
                        "canonical_name": actor.canonical_name,
                        "aliases": list(actor.aliases),
                        "is_active": actor.is_active,
                    },
                )
            connection.execute(text("delete from billboard.hot_money_seat_mapping"))
            for mapping in catalog.mappings:
                connection.execute(
                    text("""
insert into billboard.hot_money_seat_mapping (
 actor_id,seat_id,valid_from,valid_to,source_alias_name,evidence_note,
 review_status,reviewed_at,catalog_version
)
select actor_id,:seat_id,:valid_from,:valid_to,:source_alias_name,:evidence_note,
       :review_status,:reviewed_at,:catalog_version
from billboard.hot_money_actor where actor_code=:actor_code
"""),
                    {
                        "actor_code": mapping.actor_code,
                        "seat_id": mapping.seat_id,
                        "valid_from": mapping.valid_from,
                        "valid_to": mapping.valid_to,
                        "source_alias_name": mapping.source_alias_name,
                        "evidence_note": mapping.evidence_note,
                        "review_status": mapping.review_status.value,
                        "reviewed_at": mapping.reviewed_at,
                        "catalog_version": mapping.catalog_version,
                    },
                )

    def load_matchable_seat_facts(
        self, start_date: date, end_date: date
    ) -> tuple[StableSeatTradeFact, ...]:
        with self._engine.connect() as connection:
            rows = connection.execute(
                text("""
select distinct trade_date,symbol,buy_amount,sell_amount,seat_id
from billboard.seat_trade
where trade_date between :start_date and :end_date
  and seat_id is not null
order by trade_date,symbol,buy_amount nulls first,sell_amount nulls first,seat_id
"""),
                {"start_date": start_date, "end_date": end_date},
            )
            return tuple(
                StableSeatTradeFact(
                    row.trade_date,
                    row.symbol,
                    row.buy_amount,
                    row.sell_amount,
                    row.seat_id,
                )
                for row in rows
            )
