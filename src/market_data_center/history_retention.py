"""ADR-0070 fixed retention catalog. SQL identifiers/predicates are never user input."""

from calendar import monthrange
from dataclasses import dataclass
from datetime import date


def month_cutoff(day: date) -> date:
    year, month0 = divmod(day.year * 12 + day.month - 2, 12)
    return date(year, month0 + 1, min(day.day, monthrange(year, month0 + 1)[1]))


@dataclass(frozen=True)
class RetentionPolicy:
    table: str
    predicate: str = "t.trade_date < :cutoff"
    # Child-to-parent deletion order; p is the locked parent record.
    children: tuple[tuple[str, str], ...] = ()


POLICIES = (
    RetentionPolicy(
        "realtime.call_auction_indicative_snapshot",
        children=(("realtime.call_auction_indicative_detail", "t.ingestion_id=p.ingestion_id"),),
    ),
    RetentionPolicy("realtime.call_auction_market_snapshot"),
    RetentionPolicy("realtime.call_auction_snapshot"),
    RetentionPolicy("realtime.eod_quote_snapshot"),
    RetentionPolicy(
        "realtime.auction_collection_session",
        "t.effective_trade_date < :cutoff and t.finished_at is not null",
        tuple(
            (table, "t.session_id=p.session_id")
            for table in (
                "derived.auction_quote_metric",
                "realtime.five_level_quote_snapshot",
                "realtime.auction_collection_round",
            )
        ),
    ),
    RetentionPolicy(
        "realtime.call_auction_market_series_session",
        "t.trade_date < :cutoff and t.finished_at is not null "
        "and not exists(select 1 from realtime.call_auction_market_series_snapshot s "
        "where s.session_id=t.session_id) "
        "and not exists(select 1 from realtime.call_auction_market_series_snapshot_history s "
        "where s.session_id=t.session_id)",
        (("realtime.call_auction_market_series_round", "t.session_id=p.session_id"),),
    ),
    RetentionPolicy("core.stock_technical_factor_snapshot"),
    RetentionPolicy("core.board_moneyflow"),
    RetentionPolicy(
        "billboard.dragon_tiger_event",
        children=(("billboard.seat_trade", "t.event_id=p.event_id"),),
    ),
    RetentionPolicy("billboard.trading_seat_profile_daily", "t.as_of_date < :cutoff"),
    RetentionPolicy(
        "regulation.status",
        "t.trade_date < :cutoff and not exists(select 1 from regulation.monitor_context m "
        "where m.calculation_id=t.calculation_id or m.parent_calculation_id=t.calculation_id)",
        tuple(
            (table, "t.calculation_id=p.calculation_id and t.symbol=p.symbol")
            for table in (
                "regulation.warning",
                "regulation.rule_result",
            )
        ),
    ),
    RetentionPolicy(
        "stock_pool.snapshot",
        "t.effective_trade_date < :cutoff "
        "and not exists(select 1 from realtime.auction_collection_session a "
        "where a.pool_snapshot_id=t.snapshot_id) "
        "and not exists(select 1 from realtime.five_level_quote_snapshot a "
        "where a.pool_snapshot_id=t.snapshot_id)",
        (("stock_pool.member", "t.snapshot_id=p.snapshot_id"),),
    ),
    RetentionPolicy(
        "stock_pool.calculation_quality",
        "t.created_at < cast(:cutoff as date)::timestamp at time zone 'Asia/Shanghai' "
        "and not exists(select 1 from stock_pool.snapshot s "
        "where s.calculation_id=t.calculation_id)",
    ),
    *(
        RetentionPolicy(
            f"{schema}.snapshot",
            children=tuple(
                (f"{schema}.{child}", "t.snapshot_id=p.snapshot_id")
                for child in ("member", "calculation_quality")
            ),
        )
        for schema in ("today_limit_up", "today_limit_down")
    ),
    RetentionPolicy("today_limit_up.source_observation"),
    RetentionPolicy("today_limit_down.source_observation"),
    RetentionPolicy(
        "derived.close_price_new_high_120d_snapshot",
        children=(("derived.close_price_new_high_120d_member", "t.snapshot_id=p.snapshot_id"),),
    ),
)


def table_predicates() -> dict[str, str]:
    """Same fixed scope is embedded in the ordered migration; contract tests compare it."""
    result = {}
    for policy in POLICIES:
        result[policy.table] = policy.predicate
        for table, join in policy.children:
            result[table] = (
                f"exists(select 1 from {policy.table} p where {join} "
                f"and ({policy.predicate.replace('t.', 'p.')}))"
            )
    return result
