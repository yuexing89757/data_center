"""Ephemeral, normalized Xuangubao stock-pool observations."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

PoolType = Literal["limit_up", "broken_limit_up", "previous_limit_up"]
DateVerification = Literal["event_dates_consistent", "unverified"]


@dataclass(frozen=True)
class XuangubaoPoolItem:
    symbol: str
    code: str
    name: str | None
    change_pct: Decimal | None
    consecutive_limit_up_days: int | None
    previous_consecutive_limit_up_days: int | None
    first_limit_up_at: datetime | None
    last_limit_up_at: datetime | None
    last_broken_limit_up_at: datetime | None
    previous_last_limit_up_at: datetime | None
    topics: tuple[str, ...]


@dataclass(frozen=True)
class XuangubaoPoolPage:
    pool_type: PoolType
    requested_date: date | None
    source_event_date: date | None
    date_verification: DateVerification
    observed_at: datetime
    returned_count: int
    items: tuple[XuangubaoPoolItem, ...]
    source_code: str = "xuangubao"
    persisted: bool = False
