"""Provider-neutral records for immutable 120-session closing-high snapshots."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from market_data_center.domain.records import TradeStatus

CLOSE_PRICE_NEW_HIGHS_ALGORITHM_VERSION = "1.0.0"
CLOSE_PRICE_NEW_HIGHS_SESSION_COUNT = 120
CLOSE_PRICE_NEW_HIGHS_CANDIDATE_LIMIT = 10_000


@dataclass(frozen=True, slots=True)
class ClosePriceNewHighCandidate:
    symbol: str
    code: str
    display_name: str | None
    valid_bar_count: int
    close: Decimal | None
    current_status: TradeStatus | None
    previous_119d_high: Decimal | None
    has_non_trading_bar: bool
    has_nonpositive_price: bool


@dataclass(frozen=True, slots=True)
class ClosePriceNewHighInput:
    trade_date: date
    first_trade_date: date
    session_count: int
    candidates: tuple[ClosePriceNewHighCandidate, ...]

    def __post_init__(self) -> None:
        if self.session_count != CLOSE_PRICE_NEW_HIGHS_SESSION_COUNT:
            raise ValueError("closing-high input requires exactly 120 trading sessions")
        if self.first_trade_date > self.trade_date:
            raise ValueError("first trading date must not follow the target date")
        if len(self.candidates) > CLOSE_PRICE_NEW_HIGHS_CANDIDATE_LIMIT:
            raise ValueError("closing-high candidate count exceeds 10,000")
        if len({item.symbol for item in self.candidates}) != len(self.candidates):
            raise ValueError("closing-high candidates must be unique by symbol")


@dataclass(frozen=True, slots=True)
class ClosePriceNewHighMember:
    symbol: str
    code: str
    display_name: str
    close: Decimal
    previous_119d_high: Decimal
    breakout_pct: Decimal

    def __post_init__(self) -> None:
        if self.close <= self.previous_119d_high or self.previous_119d_high <= 0:
            raise ValueError("closing-high member must strictly exceed a positive previous high")


@dataclass(frozen=True, slots=True)
class ClosePriceNewHighCalculation:
    trade_date: date
    first_trade_date: date
    session_count: int
    candidates: tuple[ClosePriceNewHighCandidate, ...]
    members: tuple[ClosePriceNewHighMember, ...]
    candidate_count: int
    eligible_history_count: int
    omitted_count: int
    incomplete_history_count: int
    non_trading_bar_count: int
    nonpositive_price_count: int
    missing_name_count: int
    input_hash: str
    content_hash: str

    @property
    def member_count(self) -> int:
        return len(self.members)


@dataclass(frozen=True, slots=True)
class ClosePriceNewHighSnapshot:
    snapshot_id: UUID
    calculation_id: UUID
    trade_date: date
    version: int
    candidate_count: int
    eligible_history_count: int
    omitted_count: int
    member_count: int
    incomplete_history_count: int
    non_trading_bar_count: int
    nonpositive_price_count: int
    missing_name_count: int
    input_hash: str
    content_hash: str
    algorithm_version: str
    generated_at: datetime


@dataclass(frozen=True, slots=True)
class ClosePriceNewHighBuildSummary:
    status: str
    calculation_id: UUID | None
    snapshot_id: UUID | None
    trade_date: date
    candidate_count: int
    member_count: int
    omitted_count: int
