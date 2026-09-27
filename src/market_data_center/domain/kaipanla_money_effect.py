"""Ephemeral Kaipanla market-emotion observations."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal


@dataclass(frozen=True)
class MoneyEffectItem:
    trade_date: date
    board_success_rate_pct: Decimal
    board_profit_rate_pct: Decimal


@dataclass(frozen=True)
class MoneyEffectPage:
    observed_at: datetime
    offset: int
    limit: int
    returned_count: int
    next_offset: int | None
    items: tuple[MoneyEffectItem, ...]
    source_code: str = "kaipanla"
    source_layout: str = "kaipanla.market_emotion.money_effect.v1"
    persisted: bool = False
    total: None = None
