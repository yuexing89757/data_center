"""Ephemeral Kaipanla auction-pool observations, with explicit source units."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal


@dataclass(frozen=True)
class KaipanlaAuctionItem:
    symbol: str
    code: str
    name: str | None
    board_name: str | None
    change_pct: Decimal | None
    actual_float_market_value_cny: Decimal | None
    limit_up_bid_amount_cny: Decimal | None
    auction_change_pct: Decimal | None
    auction_net_amount_cny: Decimal | None
    auction_turnover_rate_pct: Decimal | None
    auction_amount_cny: Decimal | None


@dataclass(frozen=True)
class KaipanlaAuctionPage:
    query_mode: Literal["history", "current"]
    requested_date: date | None
    trade_date: date
    observed_at: datetime
    exclude_st: bool
    offset: int
    limit: int
    returned_count: int
    next_offset: int | None
    items: tuple[KaipanlaAuctionItem, ...]
    total: None = None
    source_code: str = "kaipanla"
    pool_scope: str = "kaipanla_auction_pool"
    persisted: bool = False
    sort_by: str = "limit_up_bid_amount_cny"
    sort_order: str = "desc"
