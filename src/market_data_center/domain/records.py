"""Provider-neutral records defined by the accepted phase-one ADR."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from uuid import UUID


class Exchange(StrEnum):
    SSE = "SSE"
    SZSE = "SZSE"
    BSE = "BSE"


class Market(StrEnum):
    CN_A_SHARE = "CN_A_SHARE"


class SecurityType(StrEnum):
    STOCK = "stock"
    INDEX = "index"
    OTHER = "other"
    CONVERTIBLE_BOND = "convertible_bond"
    ETF = "etf"
    UNKNOWN = "unknown"


class SecurityStatus(StrEnum):
    LISTED = "listed"
    DELISTED = "delisted"
    UNKNOWN = "unknown"


class TradeStatus(StrEnum):
    TRADING = "trading"
    SUSPENDED = "suspended"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class SecurityRecord:
    symbol: str
    code: str
    exchange: Exchange
    name: str
    security_type: SecurityType
    status: SecurityStatus
    ipo_date: date | None
    delisting_date: date | None
    source_code: str

    def __post_init__(self) -> None:
        expected_symbol = f"{self.exchange.value}:{self.code}"
        if self.symbol != expected_symbol:
            raise ValueError(f"symbol must equal {expected_symbol}")
        if not self.code or not self.code.isdigit():
            raise ValueError("code must contain only digits")
        if not self.name.strip():
            raise ValueError("name must not be blank")
        if self.ipo_date and self.delisting_date and self.delisting_date < self.ipo_date:
            raise ValueError("delisting_date must not precede ipo_date")


@dataclass(frozen=True, slots=True)
class TradingDayRecord:
    market: Market
    trade_date: date
    is_trading_day: bool
    source_code: str


@dataclass(frozen=True, slots=True)
class DailyBarRecord:
    symbol: str
    trade_date: date
    market: Market
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    previous_close: Decimal | None
    volume: int | None
    amount: Decimal | None
    trade_status: TradeStatus
    is_st: bool | None
    source_code: str

    def __post_init__(self) -> None:
        numeric_values = (
            self.open,
            self.high,
            self.low,
            self.close,
            self.previous_close,
            self.amount,
        )
        if any(value is not None and value < 0 for value in numeric_values):
            raise ValueError("prices and amount must not be negative")
        if self.volume is not None and self.volume < 0:
            raise ValueError("volume must not be negative")
        if self.low is not None and self.high is not None and self.low > self.high:
            raise ValueError("low must not exceed high")
        if self.low is not None and self.high is not None:
            for field_name in ("open", "close"):
                value = getattr(self, field_name)
                if value is not None and not self.low <= value <= self.high:
                    raise ValueError(f"{field_name} must be within [low, high]")


@dataclass(frozen=True, slots=True)
class IngestionEnvelope[RecordT: SecurityRecord | TradingDayRecord | DailyBarRecord]:
    ingestion_id: UUID
    record: RecordT
