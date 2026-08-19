"""Provider-neutral realtime five-level quote facts and objective metrics."""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from itertools import pairwise
from re import fullmatch
from zoneinfo import ZoneInfo

from market_data_center.domain.ingestion import QualitySeverity
from market_data_center.domain.records import Market


class QuoteStatus(StrEnum):
    TRADING = "trading"
    SUSPENDED = "suspended"
    CLOSED = "closed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    level: int
    price: Decimal | None
    volume: int | None

    def __post_init__(self) -> None:
        if not 1 <= self.level <= 5:
            raise ValueError("order-book level must be between 1 and 5")
        if self.price is not None and self.volume is None:
            raise ValueError("order-book volume must be present when price is present")
        if self.price is not None:
            if not isinstance(self.price, Decimal):
                raise TypeError("order-book price must use Decimal")
            if self.price <= 0:
                raise ValueError("order-book price must be positive")
        if self.volume is not None and self.volume < 0:
            raise ValueError("order-book volume must not be negative")


@dataclass(frozen=True, slots=True)
class FiveLevelQuoteSnapshotRecord:
    symbol: str
    market: Market
    observed_at: datetime
    source_timestamp: datetime | None
    quote_status: QuoteStatus
    last_price: Decimal | None
    previous_close: Decimal | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    cumulative_volume: int | None
    cumulative_amount: Decimal | None
    bid_levels: tuple[OrderBookLevel, ...]
    ask_levels: tuple[OrderBookLevel, ...]
    source_code: str

    def __post_init__(self) -> None:
        if fullmatch(r"(?:SSE|SZSE|BSE):[0-9]{6}", self.symbol) is None:
            raise ValueError("symbol must use the standard exchange:code format")
        if self.market is not Market.CN_A_SHARE:
            raise ValueError("realtime stock quote market must be CN_A_SHARE")
        _require_utc(self.observed_at, "observed_at")
        if self.source_timestamp is not None and self.source_timestamp.utcoffset() is None:
            raise ValueError("source_timestamp must be timezone-aware")
        if not self.source_code.strip():
            raise ValueError("source_code must not be blank")

        prices_and_amount = (
            self.last_price,
            self.previous_close,
            self.open,
            self.high,
            self.low,
            self.cumulative_amount,
        )
        if any(value is not None and not isinstance(value, Decimal) for value in prices_and_amount):
            raise TypeError("quote prices and amount must use Decimal")
        if any(value is not None and value < 0 for value in prices_and_amount):
            raise ValueError("quote prices and amount must not be negative")
        if self.cumulative_volume is not None and self.cumulative_volume < 0:
            raise ValueError("cumulative_volume must not be negative")
        if self.low is not None and self.high is not None:
            if self.low > self.high:
                raise ValueError("quote low must not exceed high")
            for field_name in ("open", "last_price"):
                value = getattr(self, field_name)
                if value is not None and not self.low <= value <= self.high:
                    raise ValueError(f"{field_name} must be within [low, high]")

        validate_order_book_levels(self.bid_levels, descending=True, side="bid")
        validate_order_book_levels(self.ask_levels, descending=False, side="ask")


@dataclass(frozen=True, slots=True)
class FiveLevelQuoteMetric:
    spread: Decimal | None
    mid_price: Decimal | None
    bid_depth_5: int | None
    ask_depth_5: int | None
    imbalance_5: Decimal | None


@dataclass(frozen=True, slots=True)
class RealtimeQuoteFinding:
    rule_code: str
    severity: QualitySeverity
    message: str
    natural_key: Mapping[str, object]

    @property
    def blocks_core_write(self) -> bool:
        return self.severity is QualitySeverity.ERROR


@dataclass(frozen=True, slots=True)
class RealtimeQuoteValidationResult:
    accepted: tuple[FiveLevelQuoteSnapshotRecord, ...]
    findings: tuple[RealtimeQuoteFinding, ...]
    rejected_rows: int


def realtime_quote_natural_key(record: FiveLevelQuoteSnapshotRecord) -> tuple[str, datetime]:
    return record.symbol, record.observed_at


def realtime_quote_natural_key_json(
    record: FiveLevelQuoteSnapshotRecord,
) -> dict[str, object]:
    return {"symbol": record.symbol, "observed_at": record.observed_at.isoformat()}


def calculate_five_level_quote_metric(
    record: FiveLevelQuoteSnapshotRecord,
) -> FiveLevelQuoteMetric:
    bid1 = record.bid_levels[0]
    ask1 = record.ask_levels[0]
    spread = ask1.price - bid1.price if ask1.price is not None and bid1.price is not None else None
    mid_price = (
        (ask1.price + bid1.price) / Decimal(2)
        if ask1.price is not None and bid1.price is not None
        else None
    )
    bid_depth = _complete_depth(record.bid_levels)
    ask_depth = _complete_depth(record.ask_levels)
    depth_total = bid_depth + ask_depth if bid_depth is not None and ask_depth is not None else None
    imbalance = (
        Decimal(bid_depth - ask_depth) / Decimal(depth_total)
        if bid_depth is not None
        and ask_depth is not None
        and depth_total is not None
        and depth_total > 0
        else None
    )
    return FiveLevelQuoteMetric(spread, mid_price, bid_depth, ask_depth, imbalance)


def validate_realtime_quotes(
    records: Sequence[FiveLevelQuoteSnapshotRecord],
    *,
    known_symbols: Collection[str],
    known_stock_symbols: Collection[str],
    now: datetime | None = None,
    allowed_future_skew: timedelta = timedelta(seconds=5),
    source_time_warning: timedelta = timedelta(seconds=15),
) -> RealtimeQuoteValidationResult:
    validation_time = now or datetime.now(UTC)
    _require_utc(validation_time, "now")
    grouped: dict[tuple[str, datetime], list[FiveLevelQuoteSnapshotRecord]] = {}
    for record in records:
        grouped.setdefault(realtime_quote_natural_key(record), []).append(record)

    accepted: list[FiveLevelQuoteSnapshotRecord] = []
    findings: list[RealtimeQuoteFinding] = []
    rejected_rows = 0
    for grouped_records in grouped.values():
        record = grouped_records[0]
        key = realtime_quote_natural_key_json(record)
        blocking: list[RealtimeQuoteFinding] = []
        warnings: list[RealtimeQuoteFinding] = []
        if any(candidate != record for candidate in grouped_records[1:]):
            blocking.append(
                _finding(
                    "realtime_quote.conflicting_snapshot",
                    QualitySeverity.ERROR,
                    "batch contains conflicting quote snapshots for one natural key",
                    key,
                )
            )
        if record.symbol not in known_symbols:
            blocking.append(
                _finding(
                    "realtime_quote.unknown_symbol",
                    QualitySeverity.ERROR,
                    "quote snapshot references an unknown Security symbol",
                    key,
                )
            )
        elif record.symbol not in known_stock_symbols:
            blocking.append(
                _finding(
                    "realtime_quote.unsupported_security_type",
                    QualitySeverity.ERROR,
                    "realtime five-level quotes currently accept stock securities only",
                    key,
                )
            )
        if record.observed_at > validation_time + allowed_future_skew:
            blocking.append(
                _finding(
                    "realtime_quote.future_observation",
                    QualitySeverity.ERROR,
                    "quote observed_at is later than the allowed Worker clock skew",
                    key,
                )
            )
        if record.source_timestamp is None:
            warnings.append(
                _finding(
                    "realtime_quote.missing_source_timestamp",
                    QualitySeverity.WARNING,
                    "provider did not supply a reliable complete source timestamp",
                    key,
                )
            )
        elif (
            abs(record.observed_at - record.source_timestamp.astimezone(UTC)) > source_time_warning
        ):
            warnings.append(
                _finding(
                    "realtime_quote.source_time_drift",
                    QualitySeverity.WARNING,
                    "source timestamp differs materially from the Worker observation time",
                    key,
                )
            )
        bid1 = record.bid_levels[0]
        ask1 = record.ask_levels[0]
        if bid1.price is not None and ask1.price is not None and bid1.price > ask1.price:
            warnings.append(
                _finding(
                    "realtime_quote.crossed_book",
                    QualitySeverity.WARNING,
                    "best bid is greater than best ask",
                    key,
                )
            )
        if record.quote_status is QuoteStatus.TRADING and (
            bid1.price is None or ask1.price is None
        ):
            warnings.append(
                _finding(
                    "realtime_quote.missing_best_level",
                    QualitySeverity.WARNING,
                    "trading quote is missing a best bid or best ask",
                    key,
                )
            )
        if record.source_code == "pytdx_hq":
            warnings.append(
                _finding(
                    "realtime_quote.lot_precision",
                    QualitySeverity.INFO,
                    "pytdx displayed quantities have 100-share source precision",
                    key,
                )
            )

        findings.extend(blocking)
        findings.extend(warnings)
        if blocking:
            rejected_rows += len(grouped_records)
        else:
            accepted.append(record)
    return RealtimeQuoteValidationResult(tuple(accepted), tuple(findings), rejected_rows)


def validate_order_book_levels(
    levels: tuple[OrderBookLevel, ...], *, descending: bool, side: str
) -> None:
    if len(levels) != 5 or tuple(item.level for item in levels) != (1, 2, 3, 4, 5):
        raise ValueError(f"{side} levels must contain level 1 through 5 exactly once")
    seen_absent = False
    prices: list[Decimal] = []
    for item in levels:
        if item.price is None:
            seen_absent = True
            continue
        if seen_absent:
            raise ValueError(f"{side} levels must be contiguous before absent levels")
        prices.append(item.price)
    pairs = pairwise(prices)
    if descending and any(left <= right for left, right in pairs):
        raise ValueError("bid prices must strictly decrease by level")
    if not descending and any(left >= right for left, right in pairs):
        raise ValueError("ask prices must strictly increase by level")


def _complete_depth(levels: tuple[OrderBookLevel, ...]) -> int | None:
    if any(item.volume is None for item in levels):
        return None
    return sum(item.volume for item in levels if item.volume is not None)


def _require_utc(value: datetime, field_name: str) -> None:
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be an aware UTC datetime")


def _finding(
    rule_code: str,
    severity: QualitySeverity,
    message: str,
    natural_key: Mapping[str, object],
) -> RealtimeQuoteFinding:
    return RealtimeQuoteFinding(rule_code, severity, message, natural_key)


@dataclass(frozen=True, slots=True)
class EodQuoteSnapshotRecord:
    """End-of-day five-level quote snapshot for limit-up pool members."""

    symbol: str
    trade_date: date
    source_code: str
    last_price: Decimal | None = None
    previous_close: Decimal | None = None
    bid1_price: Decimal | None = None
    bid1_volume: int | None = None
    bid2_price: Decimal | None = None
    bid2_volume: int | None = None
    bid3_price: Decimal | None = None
    bid3_volume: int | None = None
    bid4_price: Decimal | None = None
    bid4_volume: int | None = None
    bid5_price: Decimal | None = None
    bid5_volume: int | None = None
    ask1_price: Decimal | None = None
    ask1_volume: int | None = None
    ask2_price: Decimal | None = None
    ask2_volume: int | None = None
    ask3_price: Decimal | None = None
    ask3_volume: int | None = None
    ask4_price: Decimal | None = None
    ask4_volume: int | None = None
    ask5_price: Decimal | None = None
    ask5_volume: int | None = None
    seal_amount: Decimal | None = None

    def __post_init__(self) -> None:
        if self.last_price is not None and self.last_price < 0:
            raise ValueError("last_price must be non-negative")
        if self.seal_amount is not None and self.seal_amount < 0:
            raise ValueError("seal_amount must be non-negative")


@dataclass(frozen=True, slots=True)
class CallAuctionSnapshotRecord:
    """Call-auction snapshot for limit-up pool members (09:15-09:25)."""

    symbol: str
    trade_date: date
    source_code: str
    last_price: Decimal | None = None
    previous_close: Decimal | None = None
    cumulative_volume: int | None = None
    cumulative_amount: Decimal | None = None
    auction_premium_pct: Decimal | None = None
    observed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.observed_at is not None and self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.cumulative_volume is not None and self.cumulative_volume < 0:
            raise ValueError("cumulative_volume must be non-negative")
        if self.cumulative_amount is not None and self.cumulative_amount < 0:
            raise ValueError("cumulative_amount must be non-negative")


@dataclass(frozen=True, slots=True)
class CallAuctionMarketSnapshotRecord:
    """Full-market call-auction source fact observed before continuous trading."""

    symbol: str
    trade_date: date
    observed_at: datetime
    source_code: str
    last_price: Decimal | None = None
    previous_close: Decimal | None = None
    high_price: Decimal | None = None
    low_price: Decimal | None = None
    cumulative_volume: int | None = None
    cumulative_amount: Decimal | None = None
    bid_levels: tuple[OrderBookLevel, ...] = field(default_factory=lambda: _empty_levels())
    ask_levels: tuple[OrderBookLevel, ...] = field(default_factory=lambda: _empty_levels())
    seal_amount: Decimal | None = None

    def __post_init__(self) -> None:
        if fullmatch(r"(?:SSE|SZSE):[0-9]{6}", self.symbol) is None:
            raise ValueError("call-auction market snapshot symbol must use SSE/SZSE:code format")
        if self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        observed_at_shanghai = self.observed_at.astimezone(ZoneInfo("Asia/Shanghai"))
        if observed_at_shanghai.date() != self.trade_date:
            raise ValueError("observed_at Shanghai date must equal trade_date")
        window_start = datetime.combine(
            self.trade_date, time(9, 25), tzinfo=ZoneInfo("Asia/Shanghai")
        )
        window_end = datetime.combine(
            self.trade_date, time(9, 30), tzinfo=ZoneInfo("Asia/Shanghai")
        )
        if observed_at_shanghai < window_start:
            raise ValueError("observed_at must be at or after 09:25 Asia/Shanghai")
        if observed_at_shanghai >= window_end:
            raise ValueError("observed_at must be before 09:30 Asia/Shanghai")
        if not self.source_code.strip():
            raise ValueError("source_code must not be blank")

        prices_and_amount = (
            self.last_price,
            self.previous_close,
            self.high_price,
            self.low_price,
            self.cumulative_amount,
        )
        if any(value is not None and not isinstance(value, Decimal) for value in prices_and_amount):
            raise TypeError("call-auction prices and amount must use Decimal")
        if any(value is not None and value < 0 for value in prices_and_amount):
            raise ValueError("call-auction prices and amount must not be negative")
        if self.cumulative_volume is not None:
            if isinstance(self.cumulative_volume, bool) or not isinstance(
                self.cumulative_volume, int
            ):
                raise TypeError("cumulative_volume must be an integer share count")
            if self.cumulative_volume < 0:
                raise ValueError("cumulative_volume must not be negative")
        if (
            self.high_price is not None
            and self.low_price is not None
            and self.high_price < self.low_price
        ):
            raise ValueError("high_price must not be lower than low_price")
        if (
            self.last_price is not None
            and self.high_price is not None
            and self.last_price > self.high_price
        ):
            raise ValueError("last_price must be within supplied price bounds")
        if (
            self.last_price is not None
            and self.low_price is not None
            and self.last_price < self.low_price
        ):
            raise ValueError("last_price must be within supplied price bounds")
        validate_order_book_levels(self.bid_levels, descending=True, side="bid")
        validate_order_book_levels(self.ask_levels, descending=False, side="ask")
        bid1 = self.bid_levels[0]
        expected_seal_amount = (
            bid1.price * Decimal(bid1.volume)
            if (
                all(level.volume in (None, 0) for level in self.ask_levels[:3])
                and bid1.price is not None
                and bid1.volume is not None
            )
            else None
        )
        if self.seal_amount != expected_seal_amount:
            raise ValueError("seal_amount must match the auction order-book rule")


def _empty_levels() -> tuple[OrderBookLevel, ...]:
    return tuple(OrderBookLevel(level, None, None) for level in range(1, 6))
