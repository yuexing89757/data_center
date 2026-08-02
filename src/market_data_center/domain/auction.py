"""Opening-auction collection sessions, samples, and objective metrics."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from uuid import UUID
from zoneinfo import ZoneInfo

from market_data_center.domain.realtime_quote import (
    FiveLevelQuoteMetric,
    FiveLevelQuoteSnapshotRecord,
    calculate_five_level_quote_metric,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
AUCTION_START = time(9, 15)
AUCTION_NON_CANCELLABLE_START = time(9, 20)
AUCTION_FINAL = time(9, 25)
AUCTION_METRIC_ALGORITHM_VERSION = "1.0.0"


class AuctionPhase(StrEnum):
    CANCELLABLE = "cancellable"
    NON_CANCELLABLE = "non_cancellable"
    FINAL_MATCH = "final_match"


class AuctionSessionStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


class AuctionRoundStatus(StrEnum):
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


class QuoteSemantics(StrEnum):
    AUCTION_INDICATIVE = "auction_indicative"
    VERIFIED_ORDER_BOOK = "verified_order_book"


@dataclass(frozen=True, slots=True)
class AuctionPoolMember:
    symbol: str
    upper_limit: Decimal
    price_limit_rule_version: str


@dataclass(frozen=True, slots=True)
class AuctionPoolSnapshotInput:
    snapshot_id: UUID
    version: int
    basis_trade_date: date
    effective_trade_date: date
    members: tuple[AuctionPoolMember, ...]


def auction_window(trade_date: date) -> tuple[datetime, datetime]:
    return (
        datetime.combine(trade_date, AUCTION_START, SHANGHAI).astimezone(UTC),
        datetime.combine(trade_date, AUCTION_FINAL, SHANGHAI).astimezone(UTC),
    )


def auction_phase(scheduled_at: datetime) -> AuctionPhase:
    if scheduled_at.utcoffset() is None:
        raise ValueError("scheduled_at must be timezone-aware")
    local = scheduled_at.astimezone(SHANGHAI)
    local_time = local.time().replace(tzinfo=None)
    if AUCTION_START <= local_time < AUCTION_NON_CANCELLABLE_START:
        return AuctionPhase.CANCELLABLE
    if AUCTION_NON_CANCELLABLE_START <= local_time < AUCTION_FINAL:
        return AuctionPhase.NON_CANCELLABLE
    if local_time == AUCTION_FINAL:
        return AuctionPhase.FINAL_MATCH
    raise ValueError("scheduled_at is outside the opening-auction window")


@dataclass(frozen=True, slots=True)
class AuctionCollectionSession:
    session_id: UUID
    pool_snapshot_id: UUID
    pool_snapshot_version: int
    basis_trade_date: date
    effective_trade_date: date
    window_start: datetime
    window_end: datetime
    cadence_seconds: int
    expected_rounds: int
    expected_quotes: int
    provider_code: str
    status: AuctionSessionStatus
    started_at: datetime
    finished_at: datetime | None = None
    successful_rounds: int = 0
    partial_rounds: int = 0
    failed_rounds: int = 0
    successful_quotes: int = 0
    failed_quotes: int = 0
    error_summary: str | None = None

    def __post_init__(self) -> None:
        if self.pool_snapshot_version < 1 or self.cadence_seconds < 1:
            raise ValueError("auction snapshot version and cadence must be positive")
        if self.effective_trade_date <= self.basis_trade_date:
            raise ValueError("auction effective date must follow basis date")
        _require_utc(self.window_start, "window_start")
        _require_utc(self.window_end, "window_end")
        _require_utc(self.started_at, "started_at")
        if self.finished_at is not None:
            _require_utc(self.finished_at, "finished_at")
            if self.finished_at < self.started_at:
                raise ValueError("auction finish cannot precede start")
        if self.window_end <= self.window_start:
            raise ValueError("auction window end must follow start")
        calculated_rounds = (
            int((self.window_end - self.window_start).total_seconds() // self.cadence_seconds) + 1
        )
        if self.expected_rounds != calculated_rounds or self.expected_quotes < 1:
            raise ValueError("auction expected counts do not match window/cadence")
        counts = (
            self.successful_rounds,
            self.partial_rounds,
            self.failed_rounds,
            self.successful_quotes,
            self.failed_quotes,
        )
        if any(value < 0 for value in counts):
            raise ValueError("auction session counts cannot be negative")
        if not self.provider_code.strip():
            raise ValueError("auction provider code cannot be blank")


@dataclass(frozen=True, slots=True)
class AuctionQuoteSample:
    session_id: UUID
    pool_snapshot_id: UUID
    sample_seq: int
    scheduled_at: datetime
    collected_at: datetime
    phase: AuctionPhase
    semantics: QuoteSemantics
    quote: FiveLevelQuoteSnapshotRecord

    def __post_init__(self) -> None:
        if self.sample_seq < 0:
            raise ValueError("auction sample sequence cannot be negative")
        _require_utc(self.scheduled_at, "scheduled_at")
        _require_utc(self.collected_at, "collected_at")
        if self.collected_at < self.scheduled_at - timedelta(seconds=5):
            raise ValueError("auction collection cannot materially precede schedule")
        if self.phase is not auction_phase(self.scheduled_at):
            raise ValueError("auction phase does not match scheduled_at")
        if self.quote.observed_at != self.collected_at:
            raise ValueError("quote observed_at must equal auction collected_at")


@dataclass(frozen=True, slots=True)
class AuctionQuoteMetric:
    spread: Decimal | None
    mid_price: Decimal | None
    bid_depth_5: int | None
    ask_depth_5: int | None
    imbalance_5: Decimal | None
    seal_amount: Decimal | None
    calculated_at: datetime
    algorithm_version: str = AUCTION_METRIC_ALGORITHM_VERSION
    price_limit_rule_version: str | None = None


def calculate_auction_quote_metric(
    sample: AuctionQuoteSample,
    *,
    upper_limit: Decimal,
    order_book_semantics_verified: bool,
    calculated_at: datetime,
    price_limit_rule_version: str,
) -> AuctionQuoteMetric:
    _require_utc(calculated_at, "calculated_at")
    if (
        not order_book_semantics_verified
        or sample.semantics is not QuoteSemantics.VERIFIED_ORDER_BOOK
    ):
        return AuctionQuoteMetric(None, None, None, None, None, None, calculated_at)
    base: FiveLevelQuoteMetric = calculate_five_level_quote_metric(sample.quote)
    bid1 = sample.quote.bid_levels[0]
    seal_amount = (
        bid1.price * Decimal(bid1.volume)
        if bid1.price == upper_limit and bid1.volume is not None
        else None
    )
    return AuctionQuoteMetric(
        base.spread,
        base.mid_price,
        base.bid_depth_5,
        base.ask_depth_5,
        base.imbalance_5,
        seal_amount,
        calculated_at,
        price_limit_rule_version=price_limit_rule_version,
    )


@dataclass(frozen=True, slots=True)
class AuctionRoundSummary:
    sample_seq: int
    status: AuctionRoundStatus
    expected_quotes: int
    successful_quotes: int
    failed_quotes: int
    scheduled_at: datetime
    collected_at: datetime
    latency_ms: int


@dataclass(frozen=True, slots=True)
class AuctionCollectionSummary:
    status: str
    session_id: UUID | None
    expected_quotes: int
    successful_quotes: int
    failed_quotes: int
    successful_rounds: int
    partial_rounds: int
    failed_rounds: int


def _require_utc(value: datetime, field_name: str) -> None:
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be an aware UTC datetime")
