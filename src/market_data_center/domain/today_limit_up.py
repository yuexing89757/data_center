"""Provider-neutral facts for immutable same-day limit-up snapshots."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum


class TodayLimitUpSnapshotStatus(StrEnum):
    READY = "ready"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    FAILED = "failed"


class UpstreamState(StrEnum):
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    MISSING = "missing"


@dataclass(frozen=True, slots=True)
class TodayLimitUpDependencies:
    trade_date: date
    is_trading_day: bool
    daily_market: UpstreamState
    stock_daily_indicator: UpstreamState
    exact_ready_limit_up_pool: bool


class DurationSemantics(StrEnum):
    UNAVAILABLE_WITHOUT_EVENT_STREAM = "unavailable_without_event_stream"
    SOURCE_REPORTED_CUMULATIVE = "source_reported_cumulative"


@dataclass(frozen=True, slots=True)
class LimitUpSourceRecord:
    trade_date: date
    symbol: str
    source_name: str | None
    first_limit_up_at: datetime | None
    last_limit_up_at: datetime | None
    open_count: int | None
    source_reported_sealed_funds_cny: Decimal | None
    consecutive_limit_up_days: int | None = None
    source_code: str = "akshare"

    def __post_init__(self) -> None:
        if self.source_code != "akshare":
            raise ValueError("limit-up source record requires the akshare boundary")
        if self.open_count is not None and self.open_count < 0:
            raise ValueError("open_count must be nonnegative")
        if self.consecutive_limit_up_days is not None and self.consecutive_limit_up_days < 1:
            raise ValueError("consecutive_limit_up_days must be positive")
        if (
            self.first_limit_up_at is not None
            and self.last_limit_up_at is not None
            and self.first_limit_up_at > self.last_limit_up_at
        ):
            raise ValueError("first_limit_up_at cannot follow last_limit_up_at")
        for value in (self.first_limit_up_at, self.last_limit_up_at):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError("limit-up timestamps must be timezone-aware")
            if value is not None and value.date() != self.trade_date:
                raise ValueError("limit-up timestamp must belong to trade_date")
        if (
            self.source_reported_sealed_funds_cny is not None
            and self.source_reported_sealed_funds_cny < 0
        ):
            raise ValueError("source-reported sealed funds must be nonnegative")


@dataclass(frozen=True, slots=True)
class TodayLimitUpMember:
    symbol: str
    code: str
    historical_name: str
    previous_close: Decimal
    close: Decimal
    limit_price: Decimal
    change_percent: Decimal
    free_float_shares: int
    free_float_market_cap_cny: Decimal
    first_limit_up_at: datetime | None = None
    last_limit_up_at: datetime | None = None
    open_count: int | None = None
    limit_up_duration_seconds: int | None = None
    duration_semantics: DurationSemantics = DurationSemantics.UNAVAILABLE_WITHOUT_EVENT_STREAM
    source_reported_sealed_funds_cny: Decimal | None = None
    closing_bid1_price: Decimal | None = None
    closing_bid1_volume_shares: int | None = None
    closing_bid1_sealing_amount_cny: Decimal | None = None
    volume: int | None = None
    amount_cny: Decimal | None = None
    free_float_turnover_rate_pct: Decimal | None = None
    consecutive_limit_up_days: int | None = None

    def __post_init__(self) -> None:
        if min(self.previous_close, self.close, self.limit_price) <= 0:
            raise ValueError("canonical prices must be positive")
        if self.close != self.limit_price:
            raise ValueError("limit-up member close must exactly equal limit_price")
        if self.free_float_shares <= 0:
            raise ValueError("free_float_shares must be positive")
        if self.free_float_market_cap_cny != self.close * self.free_float_shares:
            raise ValueError("free-float market cap must equal close * free_float_shares")
        expected_change = ((self.close / self.previous_close - Decimal(1)) * Decimal(100)).quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        )
        if self.change_percent != expected_change:
            raise ValueError("change_percent does not match canonical prices")
        bid_pair = (self.closing_bid1_price, self.closing_bid1_volume_shares)
        if (bid_pair[0] is None) != (bid_pair[1] is None):
            raise ValueError("closing bid-1 price and volume must be paired")
        if self.closing_bid1_sealing_amount_cny is not None:
            if self.closing_bid1_price != self.limit_price:
                raise ValueError("computed bid-1 amount requires bid-1 at limit price")
            if self.closing_bid1_price is None or self.closing_bid1_volume_shares is None:
                raise ValueError("computed bid-1 amount requires price and volume")
            if self.closing_bid1_sealing_amount_cny != (
                self.closing_bid1_price * self.closing_bid1_volume_shares
            ):
                raise ValueError("bid-1 sealing amount must equal bid-1 price * volume")
        if self.limit_up_duration_seconds is None:
            if self.duration_semantics is not DurationSemantics.UNAVAILABLE_WITHOUT_EVENT_STREAM:
                raise ValueError("missing duration requires unavailable semantics")
        elif self.duration_semantics is not DurationSemantics.SOURCE_REPORTED_CUMULATIVE:
            raise ValueError("duration must be source-reported cumulative in v1")
        if self.volume is not None and self.volume < 0:
            raise ValueError("volume must be nonnegative")
        if self.amount_cny is not None and self.amount_cny < 0:
            raise ValueError("amount_cny must be nonnegative")
        if self.free_float_turnover_rate_pct is not None and self.free_float_turnover_rate_pct < 0:
            raise ValueError("free_float_turnover_rate_pct must be nonnegative")
        if self.consecutive_limit_up_days is not None and self.consecutive_limit_up_days < 1:
            raise ValueError("consecutive_limit_up_days must be positive")
