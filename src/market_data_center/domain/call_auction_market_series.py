"""Objective full-market source facts sampled during the opening call auction."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from json import dumps
from re import fullmatch
from uuid import UUID
from zoneinfo import ZoneInfo

SHANGHAI_ZONE = ZoneInfo("Asia/Shanghai")
SERIES_START = time(9, 15)
SERIES_OPENING_TRADE_START = time(9, 25)
SERIES_CADENCE_SECONDS = 20
SERIES_ROUND_COUNT = 32


class MarketSeriesStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


class MarketSeriesValueSemantics(StrEnum):
    AUCTION_INDICATIVE = "auction_indicative"
    OPENING_TRADE = "opening_trade"
    LEGACY_SOURCE_QUOTE = "legacy_source_quote"


def series_slots(trade_date: date) -> tuple[datetime, ...]:
    """Return the 32 immutable sample timestamps as UTC datetimes."""
    start = datetime.combine(trade_date, SERIES_START, SHANGHAI_ZONE).astimezone(UTC)
    cadence = timedelta(seconds=SERIES_CADENCE_SECONDS)
    return tuple(start + cadence * sample_seq for sample_seq in range(SERIES_ROUND_COUNT))


def universe_hash(symbols: Sequence[str]) -> str:
    """Hash the canonical ordered frozen universe."""
    frozen = tuple(symbols)
    if not frozen or frozen != tuple(sorted(set(frozen))):
        raise ValueError("market series universe must be sorted and unique")
    if any(fullmatch(r"(?:SSE|SZSE):[0-9]{6}", symbol) is None for symbol in frozen):
        raise ValueError("market series universe supports SSE/SZSE stocks only")
    canonical = dumps(frozen, ensure_ascii=False, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class MarketSeriesSession:
    session_id: UUID
    workflow_run_id: UUID
    trade_date: date
    window_start: datetime
    window_end: datetime
    cadence_seconds: int
    expected_rounds: int
    universe_symbols: tuple[str, ...]
    universe_count: int
    universe_hash: str
    status: MarketSeriesStatus
    started_at: datetime
    finished_at: datetime | None = None
    successful_rounds: int = 0
    partial_rounds: int = 0
    failed_rounds: int = 0
    successful_quotes: int = 0
    failed_quotes: int = 0
    error_summary: str | None = None

    def __post_init__(self) -> None:
        slots = series_slots(self.trade_date)
        expected_window_end = slots[-1] + timedelta(seconds=SERIES_CADENCE_SECONDS)
        if self.window_start != slots[0] or self.window_end != expected_window_end:
            raise ValueError("market series session window must be 09:15:00-09:25:40")
        if self.cadence_seconds != SERIES_CADENCE_SECONDS:
            raise ValueError("market series cadence must be 20 seconds")
        if self.expected_rounds != SERIES_ROUND_COUNT:
            raise ValueError("market series expected_rounds must be 32")
        _require_utc(self.started_at, "started_at")
        if self.finished_at is not None:
            _require_utc(self.finished_at, "finished_at")
            if self.finished_at < self.started_at:
                raise ValueError("finished_at must not precede started_at")
        if self.universe_count != len(self.universe_symbols):
            raise ValueError("universe_count must equal universe_symbols length")
        if self.universe_hash != universe_hash(self.universe_symbols):
            raise ValueError("universe_hash must match the frozen universe")
        counts = (
            self.successful_rounds,
            self.partial_rounds,
            self.failed_rounds,
            self.successful_quotes,
            self.failed_quotes,
        )
        if any(value < 0 for value in counts):
            raise ValueError("market series counts must not be negative")
        if self.successful_rounds + self.partial_rounds + self.failed_rounds > 32:
            raise ValueError("market series round counts exceed expected_rounds")
        expected_quotes = self.universe_count * self.expected_rounds
        if self.successful_quotes + self.failed_quotes > expected_quotes:
            raise ValueError("market series quote counts exceed the frozen universe")
        if self.status is MarketSeriesStatus.RUNNING:
            if self.finished_at is not None:
                raise ValueError("running session must not have finished_at")
        elif self.finished_at is None:
            raise ValueError("terminal session requires finished_at")
        if self.status is MarketSeriesStatus.SUCCEEDED and (
            self.successful_rounds != self.expected_rounds
            or self.partial_rounds != 0
            or self.failed_rounds != 0
            or self.successful_quotes != expected_quotes
            or self.failed_quotes != 0
        ):
            raise ValueError("succeeded session requires all rounds and quotes")
        if self.error_summary is not None and len(self.error_summary) > 500:
            raise ValueError("error_summary must not exceed 500 characters")


@dataclass(frozen=True, slots=True)
class MarketSeriesRound:
    session_id: UUID
    sample_seq: int
    scheduled_at: datetime
    collected_at: datetime | None
    status: MarketSeriesStatus
    attempt_count: int
    expected_quotes: int
    successful_quotes: int
    failed_quotes: int
    selected_ingestion_id: UUID | None
    error_summary: str | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.sample_seq < SERIES_ROUND_COUNT:
            raise ValueError("sample_seq must be between 0 and 31")
        _require_utc(self.scheduled_at, "scheduled_at")
        local_date = self.scheduled_at.astimezone(SHANGHAI_ZONE).date()
        if self.scheduled_at != series_slots(local_date)[self.sample_seq]:
            raise ValueError("scheduled_at must match sample_seq")
        if self.collected_at is not None:
            _require_utc(self.collected_at, "collected_at")
            if self.collected_at < self.scheduled_at:
                raise ValueError("collected_at must not precede scheduled_at")
        if not 0 <= self.attempt_count <= 2:
            raise ValueError("attempt_count must be between 0 and 2")
        if self.expected_quotes <= 0:
            raise ValueError("expected_quotes must be positive")
        if self.successful_quotes < 0 or self.failed_quotes < 0:
            raise ValueError("round quote counts must not be negative")
        if self.status is MarketSeriesStatus.RUNNING:
            if (
                self.collected_at is not None
                or self.attempt_count != 0
                or self.successful_quotes != 0
                or self.failed_quotes != 0
                or self.selected_ingestion_id is not None
            ):
                raise ValueError("running round must not contain terminal results")
        else:
            if self.collected_at is None:
                raise ValueError("terminal round requires collected_at")
            if self.successful_quotes + self.failed_quotes != self.expected_quotes:
                raise ValueError("terminal round counts must equal expected_quotes")
        if self.status is MarketSeriesStatus.SUCCEEDED and (
            self.attempt_count < 1
            or self.successful_quotes != self.expected_quotes
            or self.failed_quotes != 0
            or self.selected_ingestion_id is None
        ):
            raise ValueError("succeeded round requires one complete selected ingestion")
        if self.status is MarketSeriesStatus.PARTIAL and (
            self.attempt_count < 1 or self.selected_ingestion_id is None
        ):
            raise ValueError("partial round requires a selected ingestion")
        if self.status is MarketSeriesStatus.FAILED and (
            self.attempt_count != 0
            or self.successful_quotes != 0
            or self.failed_quotes != self.expected_quotes
            or self.selected_ingestion_id is not None
        ):
            raise ValueError("failed round represents a missed slot without an ingestion")
        if self.error_summary is not None and len(self.error_summary) > 500:
            raise ValueError("error_summary must not exceed 500 characters")


@dataclass(frozen=True, slots=True)
class MarketSeriesSnapshotRecord:
    symbol: str
    trade_date: date
    session_id: UUID
    sample_seq: int
    scheduled_at: datetime
    observed_at: datetime
    source_code: str
    value_semantics: MarketSeriesValueSemantics
    last_price: Decimal | None = None
    previous_close: Decimal | None = None
    high_price: Decimal | None = None
    low_price: Decimal | None = None
    cumulative_volume: int | None = None
    cumulative_amount: Decimal | None = None

    def __post_init__(self) -> None:
        if fullmatch(r"(?:SSE|SZSE):[0-9]{6}", self.symbol) is None:
            raise ValueError("market series symbol must use SSE/SZSE:code format")
        if not 0 <= self.sample_seq < SERIES_ROUND_COUNT:
            raise ValueError("sample_seq must be between 0 and 31")
        _require_utc(self.scheduled_at, "scheduled_at")
        if self.scheduled_at != series_slots(self.trade_date)[self.sample_seq]:
            raise ValueError("scheduled_at must match trade_date and sample_seq")
        _require_utc(self.observed_at, "observed_at")
        deadline = self.scheduled_at + timedelta(seconds=SERIES_CADENCE_SECONDS)
        if not self.scheduled_at <= self.observed_at < deadline:
            raise ValueError("observed_at must be within the scheduled round")
        if self.source_code != "pytdx_hq":
            raise ValueError("market series source_code must be pytdx_hq")
        if not isinstance(self.value_semantics, MarketSeriesValueSemantics):
            raise TypeError("value_semantics must be a MarketSeriesValueSemantics")
        scheduled_time = self.scheduled_at.astimezone(SHANGHAI_ZONE).time()
        if (
            self.value_semantics is MarketSeriesValueSemantics.AUCTION_INDICATIVE
            and scheduled_time >= SERIES_OPENING_TRADE_START
        ):
            raise ValueError("auction_indicative must be scheduled before 09:25")
        if (
            self.value_semantics is MarketSeriesValueSemantics.OPENING_TRADE
            and scheduled_time < SERIES_OPENING_TRADE_START
        ):
            raise ValueError("opening_trade must not be scheduled before 09:25")
        decimal_values = (
            self.last_price,
            self.previous_close,
            self.high_price,
            self.low_price,
            self.cumulative_amount,
        )
        if any(value is not None and not isinstance(value, Decimal) for value in decimal_values):
            raise TypeError("market series prices and amount must use Decimal")
        if any(value is not None and value < 0 for value in decimal_values):
            raise ValueError("market series prices and amount must not be negative")
        if self.cumulative_volume is not None:
            if isinstance(self.cumulative_volume, bool) or not isinstance(
                self.cumulative_volume, int
            ):
                raise TypeError("cumulative_volume must be an integer share count")
            if self.cumulative_volume < 0:
                raise ValueError("cumulative_volume must not be negative")
        if self.value_semantics is MarketSeriesValueSemantics.AUCTION_INDICATIVE:
            indicative_values = (
                self.last_price,
                self.cumulative_volume,
                self.cumulative_amount,
            )
            if any(value is None for value in indicative_values) and not all(
                value is None for value in indicative_values
            ):
                raise ValueError(
                    "auction indicative price, volume and amount must be jointly present"
                )
            if (
                self.last_price is not None
                and self.cumulative_volume is not None
                and self.cumulative_amount != self.last_price * self.cumulative_volume
            ):
                raise ValueError("auction indicative amount must equal price multiplied by volume")
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
        ) or (
            self.last_price is not None
            and self.low_price is not None
            and self.last_price < self.low_price
        ):
            raise ValueError("last_price must be within supplied price bounds")


def _require_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC")
