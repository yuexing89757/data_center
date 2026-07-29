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


class CorporateActionStatus(StrEnum):
    PLANNED = "planned"
    APPROVED = "approved"
    IMPLEMENTED = "implemented"
    CANCELLED = "cancelled"
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
class ShareCapitalRecord:
    symbol: str
    effective_date: date
    total_shares: int
    restricted_shares: int | None
    circulating_shares: int | None
    listed_a_shares: int | None
    change_reason: str | None
    source_code: str

    def __post_init__(self) -> None:
        if self.total_shares <= 0:
            raise ValueError("total_shares must be positive")
        components = (self.restricted_shares, self.circulating_shares, self.listed_a_shares)
        if any(value is not None and value < 0 for value in components):
            raise ValueError("share counts must not be negative")
        if any(value is not None and value > self.total_shares for value in components):
            raise ValueError("share components must not exceed total_shares")


@dataclass(frozen=True, slots=True)
class DistributionRecord:
    symbol: str
    report_period: date
    announcement_date: date | None
    record_date: date | None
    ex_date: date | None
    cash_dividend_per_share: Decimal | None
    bonus_share_ratio: Decimal | None
    transfer_share_ratio: Decimal | None
    status: CorporateActionStatus
    source_code: str

    def __post_init__(self) -> None:
        values = (
            self.cash_dividend_per_share,
            self.bonus_share_ratio,
            self.transfer_share_ratio,
        )
        if any(value is not None and value < 0 for value in values):
            raise ValueError("distribution values must not be negative")
        if not any(value is not None and value > 0 for value in values):
            raise ValueError("distribution must contain a positive cash or share allocation")


@dataclass(frozen=True, slots=True)
class RightsIssueRecord:
    symbol: str
    record_date: date
    announcement_date: date | None
    ex_date: date | None
    payment_start_date: date | None
    payment_end_date: date | None
    listing_date: date | None
    rights_ratio: Decimal
    rights_price: Decimal
    base_shares: int | None
    proceeds: Decimal | None
    source_code: str

    def __post_init__(self) -> None:
        if self.rights_ratio <= 0:
            raise ValueError("rights_ratio must be positive")
        if self.rights_price < 0:
            raise ValueError("rights_price must not be negative")
        if self.base_shares is not None and self.base_shares <= 0:
            raise ValueError("base_shares must be positive")
        if self.proceeds is not None and self.proceeds < 0:
            raise ValueError("proceeds must not be negative")
        if (
            self.payment_start_date is not None
            and self.payment_end_date is not None
            and self.payment_end_date < self.payment_start_date
        ):
            raise ValueError("payment_end_date must not precede payment_start_date")


type CapitalRecord = ShareCapitalRecord | DistributionRecord | RightsIssueRecord


@dataclass(frozen=True, slots=True)
class IngestionEnvelope[RecordT]:
    ingestion_id: UUID
    record: RecordT
