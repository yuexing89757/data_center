"""Domain entities derived from provider-neutral records."""

from dataclasses import dataclass
from datetime import date
from uuid import UUID

from market_data_center.domain.records import Market


@dataclass(frozen=True, slots=True)
class SecurityNameHistory:
    symbol: str
    name: str
    effective_from: date
    effective_to: date | None
    source_code: str
    ingestion_id: UUID

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("security name must not be blank")
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("effective_to must not precede effective_from")


@dataclass(frozen=True, slots=True)
class CalculatedTradingDay:
    market: Market
    trade_date: date
    is_trading_day: bool
    previous_trading_day: date | None
    next_trading_day: date | None
    source_code: str
