"""Ephemeral board observations; source indicators are not internal classifications."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

ForecastStatus = Literal["available", "not_provided", "years_unverified"]


@dataclass(frozen=True)
class KaipanlaBoardItem:
    board_code: str
    name: str | None
    strength: Decimal | None
    change_pct: Decimal | None
    change_speed_pct: Decimal | None
    amount_cny: Decimal | None
    main_net_amount_cny: Decimal | None
    volume_ratio: Decimal | None
    large_order_net_amount_cny: Decimal | None


@dataclass(frozen=True)
class KaipanlaProfitForecast:
    year: int
    net_profit_cny: Decimal | None


@dataclass(frozen=True)
class KaipanlaBoardMember:
    symbol: str
    code: str
    name: str | None
    concept: str | None
    last_price: Decimal | None
    change_pct: Decimal | None
    amount_cny: Decimal | None
    actual_float_market_value_cny: Decimal | None
    main_buy_amount_cny: Decimal | None
    main_sell_amount_cny: Decimal | None
    main_net_amount_cny: Decimal | None
    buy_share_pct: Decimal | None
    sell_share_pct: Decimal | None
    volume_ratio: Decimal | None
    height_label: str | None
    leader_label: str | None
    close_limit_bid_amount_cny: Decimal | None
    max_limit_bid_amount_cny: Decimal | None
    total_market_value_cny: Decimal | None
    float_market_value_cny: Decimal | None
    leading_count: int | None
    large_order_net_amount_cny: Decimal | None
    forecast_status: ForecastStatus
    profit_forecasts: tuple[KaipanlaProfitForecast, ...]


@dataclass(frozen=True, kw_only=True)
class _BoardPage:
    query_mode: Literal["latest", "history"]
    requested_date: date | None
    trade_date: date
    observed_at: datetime
    offset: int
    limit: int
    returned_count: int
    next_offset: int | None
    total: None = None
    source_code: str = "kaipanla"
    persisted: bool = False


@dataclass(frozen=True, kw_only=True)
class KaipanlaBoardRankingPage(_BoardPage):
    items: tuple[KaipanlaBoardItem, ...]


@dataclass(frozen=True, kw_only=True)
class KaipanlaBoardMembersPage(_BoardPage):
    board_code: str
    forecast_years: tuple[int, ...]
    items: tuple[KaipanlaBoardMember, ...]
