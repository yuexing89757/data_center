"""Provider-neutral facts for immutable same-day limit-down snapshots."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum


class TodayLimitDownSnapshotStatus(StrEnum):
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
class TodayLimitDownDependencies:
    trade_date: date
    is_trading_day: bool
    daily_market: UpstreamState
    stock_daily_indicator: UpstreamState
    exact_ready_limit_down_pool: bool


class DurationSemantics(StrEnum):
    UNAVAILABLE_WITHOUT_EVENT_STREAM = "unavailable_without_event_stream"


@dataclass(frozen=True, slots=True)
class LimitDownSourceRecord:
    trade_date: date
    symbol: str
    source_name: str | None
    first_limit_down_at: datetime | None
    last_limit_down_at: datetime | None
    open_count: int | None
    source_reported_sealed_funds_cny: Decimal | None
    consecutive_limit_down_days: int | None = None
    source_code: str = "akshare"

    def __post_init__(self) -> None:
        if self.source_code != "akshare":
            raise ValueError("limit-down source requires akshare")
        if self.first_limit_down_at is not None:
            raise ValueError("first_limit_down_at is unavailable from this source")
        if self.open_count is not None and self.open_count < 0:
            raise ValueError("open_count must be nonnegative")
        if self.consecutive_limit_down_days is not None and self.consecutive_limit_down_days < 1:
            raise ValueError("consecutive_limit_down_days must be positive")
        if (
            self.source_reported_sealed_funds_cny is not None
            and self.source_reported_sealed_funds_cny < 0
        ):
            raise ValueError("source-reported sealed funds must be nonnegative")
        if self.last_limit_down_at is not None:
            value = self.last_limit_down_at
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("limit-down timestamp must be timezone-aware")
            if value.date() != self.trade_date:
                raise ValueError("limit-down timestamp must belong to trade_date")


@dataclass(frozen=True, slots=True)
class TodayLimitDownMember:
    symbol: str
    code: str
    historical_name: str
    previous_close: Decimal
    close: Decimal
    limit_price: Decimal
    change_percent: Decimal
    free_float_shares: int
    free_float_market_cap_cny: Decimal
    first_limit_down_at: datetime | None = None
    last_limit_down_at: datetime | None = None
    open_count: int | None = None
    consecutive_limit_down_days: int | None = None
    limit_down_duration_seconds: int | None = None
    duration_semantics: DurationSemantics = DurationSemantics.UNAVAILABLE_WITHOUT_EVENT_STREAM
    source_reported_sealed_funds_cny: Decimal | None = None
    closing_ask1_price: Decimal | None = None
    closing_ask1_volume_shares: int | None = None
    closing_ask2_price: Decimal | None = None
    closing_ask2_volume_shares: int | None = None
    closing_ask3_price: Decimal | None = None
    closing_ask3_volume_shares: int | None = None
    closing_ask4_price: Decimal | None = None
    closing_ask4_volume_shares: int | None = None
    closing_ask5_price: Decimal | None = None
    closing_ask5_volume_shares: int | None = None
    closing_ask1_sealing_amount_cny: Decimal | None = None

    def __post_init__(self) -> None:
        if min(self.previous_close, self.close, self.limit_price) <= 0:
            raise ValueError("canonical prices must be positive")
        if self.close != self.limit_price:
            raise ValueError("limit-down close must be exactly equal to limit_price")
        if self.free_float_shares <= 0:
            raise ValueError("free_float_shares must be positive")
        if self.free_float_market_cap_cny != self.close * self.free_float_shares:
            raise ValueError("free-float market cap must equal close * shares")
        expected_change = ((self.close / self.previous_close - Decimal(1)) * Decimal(100)).quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        )
        if self.change_percent != expected_change:
            raise ValueError("change_percent does not match canonical prices")
        if self.first_limit_down_at is not None or self.limit_down_duration_seconds is not None:
            raise ValueError("first seal and cumulative duration are unavailable")
        if self.duration_semantics is not DurationSemantics.UNAVAILABLE_WITHOUT_EVENT_STREAM:
            raise ValueError("duration semantics must remain unavailable")
        if self.open_count is not None and self.open_count < 0:
            raise ValueError("open_count must be nonnegative")
        if self.consecutive_limit_down_days is not None and self.consecutive_limit_down_days < 1:
            raise ValueError("consecutive_limit_down_days must be positive")
        if (
            self.source_reported_sealed_funds_cny is not None
            and self.source_reported_sealed_funds_cny < 0
        ):
            raise ValueError("source funds must be nonnegative")
        levels = (
            (self.closing_ask1_price, self.closing_ask1_volume_shares),
            (self.closing_ask2_price, self.closing_ask2_volume_shares),
            (self.closing_ask3_price, self.closing_ask3_volume_shares),
            (self.closing_ask4_price, self.closing_ask4_volume_shares),
            (self.closing_ask5_price, self.closing_ask5_volume_shares),
        )
        for price, volume in levels:
            if (price is None) != (volume is None):
                raise ValueError("closing ask price and volume must be paired")
            if price is not None and price < 0:
                raise ValueError("closing ask price must be nonnegative")
            if volume is not None and volume < 0:
                raise ValueError("closing ask volume must be nonnegative")
        if self.closing_ask1_sealing_amount_cny is not None:
            if self.closing_ask1_price != self.limit_price:
                raise ValueError("ask-1 sealing amount requires ask-1 at limit price")
            if self.closing_ask1_volume_shares is None:
                raise ValueError("ask-1 sealing amount requires volume")
            if self.closing_ask1_sealing_amount_cny != (
                self.closing_ask1_price * self.closing_ask1_volume_shares
            ):
                raise ValueError("ask-1 sealing amount must equal price * volume")
