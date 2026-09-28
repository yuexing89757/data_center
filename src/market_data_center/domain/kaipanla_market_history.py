"""Ephemeral, category-specific Kaipanla historical stock-list observations."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

HistoryListKind = Literal["auction", "limit_up", "limit_down", "broken_limit_up"]


@dataclass(frozen=True)
class AuctionHistoryItem:
    symbol: str
    code: str
    name: str | None
    board_name: str | None
    limit_up_bid_amount_cny: Decimal | None
    auction_change_pct: Decimal | None


@dataclass(frozen=True)
class LimitUpHistoryItem:
    symbol: str
    code: str
    name: str | None
    limit_up_at: datetime | None
    status: str | None
    reason: str | None


@dataclass(frozen=True)
class LimitDownHistoryItem:
    symbol: str
    code: str
    name: str | None
    limit_down_at: datetime | None
    sealed_amount_cny: Decimal | None
    board_name: str | None


@dataclass(frozen=True)
class BrokenLimitUpHistoryItem:
    symbol: str
    code: str
    name: str | None
    change_pct: Decimal | None
    limit_up_at: datetime | None
    opened_at: datetime | None


@dataclass(frozen=True)
class MarketHistoryPage:
    list_type: HistoryListKind
    requested_date: date
    trade_date: date
    observed_at: datetime
    offset: int
    limit: int
    returned_count: int
    next_offset: int | None
    items: tuple[
        AuctionHistoryItem | LimitUpHistoryItem | LimitDownHistoryItem | BrokenLimitUpHistoryItem,
        ...,
    ]
    total: None = None
    source_code: str = "kaipanla"
    persisted: bool = False
