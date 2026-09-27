"""Ephemeral source observations; never inputs to the regulation calculator."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal


@dataclass(frozen=True)
class AlertItem:
    symbol: str
    code: str
    name: str | None
    source_values: tuple[str | int | Decimal | None, ...] = ()
    last_price: Decimal | None = None
    change_pct: Decimal | None = None
    start_date: date | None = None
    end_date: date | None = None
    announcement_date: date | None = None
    document_url: str | None = None


@dataclass(frozen=True)
class AlertGroup:
    key: str
    items: tuple[AlertItem, ...]


@dataclass(frozen=True)
class AlertSnapshot:
    source_layout: str
    observed_at: datetime
    source_timestamp: datetime | None
    requested_date: date | None
    trade_date: date | None
    date_matches_request: bool | None
    groups: tuple[AlertGroup, ...]
    offset: int | None = None
    limit: int | None = None
    total: int | None = None
    next_offset: int | None = None
    next_date: date | None = None
    missing_codes: tuple[str, ...] = ()
    source_code: str = "kaipanla"
    persisted: bool = False
