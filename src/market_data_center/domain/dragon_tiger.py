"""Provider-neutral Dragon Tiger List facts, validation, and objective summaries."""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from re import fullmatch
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


class DragonTigerSnapshotStatus(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"


class DragonTigerEventStatus(StrEnum):
    OBSERVED = "observed"
    PARTIAL = "partial"
    RETRACTED = "retracted"


class DragonTigerSeatType(StrEnum):
    INSTITUTION = "institution"
    BROKER_BRANCH = "broker_branch"
    OTHER = "other"
    UNKNOWN = "unknown"


class DragonTigerNormalizationStatus(StrEnum):
    MATCHED = "matched"
    PROVISIONAL = "provisional"
    UNMATCHED = "unmatched"


class DragonTigerActivitySide(StrEnum):
    BUY = "buy"
    SELL = "sell"
    BOTH = "both"


@dataclass(frozen=True, slots=True)
class DragonTigerSourceObservation:
    source_event_key: str
    symbol: str
    trade_date: date
    observed_at: datetime
    source_name: str
    source_status_text: str | None = None
    source_code: str = "eastmoney"

    def __post_init__(self) -> None:
        _symbol(self.symbol)
        _text(self.source_event_key, "source_event_key")
        _text(self.source_name, "source_name")
        if self.source_code != "eastmoney":
            raise ValueError("unsupported Dragon Tiger source")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.observed_at.astimezone(SHANGHAI).date() < self.trade_date:
            raise ValueError("observation cannot precede its Shanghai trade date")


@dataclass(frozen=True, slots=True)
class DragonTigerEvent:
    symbol: str
    trade_date: date
    historical_name: str
    market: str
    close: Decimal
    change_percent: Decimal
    turnover_amount_cny: Decimal
    turnover_rate_percent: Decimal | None
    status: DragonTigerEventStatus
    source_event_key: str

    def __post_init__(self) -> None:
        _symbol(self.symbol)
        _text(self.historical_name, "historical_name")
        _text(self.source_event_key, "source_event_key")
        if self.market != "CN_A_SHARE":
            raise ValueError("market must be CN_A_SHARE")
        for name, value in (
            ("close", self.close),
            ("change_percent", self.change_percent),
            ("turnover_amount_cny", self.turnover_amount_cny),
        ):
            _decimal(value, name)
        if self.turnover_rate_percent is not None:
            _decimal(self.turnover_rate_percent, "turnover_rate_percent")
        if self.close <= 0:
            raise ValueError("unadjusted close must be positive")
        if self.turnover_amount_cny < 0:
            raise ValueError("turnover_amount_cny must be nonnegative")
        if self.turnover_rate_percent is not None and self.turnover_rate_percent < 0:
            raise ValueError("turnover_rate_percent must be nonnegative")

    @property
    def natural_key(self) -> tuple[str, date]:
        return self.symbol, self.trade_date


@dataclass(frozen=True, slots=True)
class DragonTigerReason:
    event_symbol: str
    trade_date: date
    reason_code: str
    reason_name: str
    source_original_text: str
    display_order: int
    source_numeric_value: Decimal | None = None
    source_numeric_unit: str | None = None

    def __post_init__(self) -> None:
        _symbol(self.event_symbol)
        if not fullmatch(r"[a-z][a-z0-9_]{1,63}", self.reason_code):
            raise ValueError("reason_code must be a normalized lower_snake_case code")
        _text(self.reason_name, "reason_name")
        _text(self.source_original_text, "source_original_text")
        if self.display_order < 0:
            raise ValueError("display_order must be nonnegative")
        if (self.source_numeric_value is None) != (self.source_numeric_unit is None):
            raise ValueError("source numeric value and unit must be present together")
        if self.source_numeric_unit is not None:
            _text(self.source_numeric_unit, "source_numeric_unit")
        if self.source_numeric_value is not None:
            _decimal(self.source_numeric_value, "source_numeric_value")

    @property
    def event_key(self) -> tuple[str, date]:
        return self.event_symbol, self.trade_date


@dataclass(frozen=True, slots=True)
class DragonTigerSeat:
    identity_key: str
    canonical_name: str
    seat_type: DragonTigerSeatType
    valid_from: date
    source_name: str
    normalization_status: DragonTigerNormalizationStatus
    broker_name: str | None = None
    branch_name: str | None = None
    region: str | None = None
    valid_to: date | None = None

    def __post_init__(self) -> None:
        if not fullmatch(r"[a-z0-9][a-z0-9:_-]{2,127}", self.identity_key):
            raise ValueError("identity_key must be stable and normalized")
        _text(self.canonical_name, "canonical_name")
        _text(self.source_name, "source_name")
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ValueError("seat valid_to must not precede valid_from")
        if self.seat_type is DragonTigerSeatType.BROKER_BRANCH and not self.broker_name:
            raise ValueError("broker_branch seat requires broker_name")


@dataclass(frozen=True, slots=True)
class DragonTigerSeatActivity:
    event_symbol: str
    trade_date: date
    seat_identity_key: str
    side: DragonTigerActivitySide
    buy_amount_cny: Decimal
    sell_amount_cny: Decimal
    net_amount_cny: Decimal
    source_seat_name: str
    source_order: int
    buy_rank: int | None = None
    sell_rank: int | None = None

    def __post_init__(self) -> None:
        _symbol(self.event_symbol)
        _text(self.seat_identity_key, "seat_identity_key")
        _text(self.source_seat_name, "source_seat_name")
        for name, value in (
            ("buy_amount_cny", self.buy_amount_cny),
            ("sell_amount_cny", self.sell_amount_cny),
            ("net_amount_cny", self.net_amount_cny),
        ):
            _decimal(value, name)
        if self.buy_amount_cny < 0 or self.sell_amount_cny < 0:
            raise ValueError("seat activity amounts must be nonnegative")
        if self.net_amount_cny != self.buy_amount_cny - self.sell_amount_cny:
            raise ValueError("net_amount_cny must equal buy minus sell")
        if self.source_order < 0:
            raise ValueError("source_order must be nonnegative")
        if self.buy_rank is not None and self.buy_rank <= 0:
            raise ValueError("buy_rank must be positive")
        if self.sell_rank is not None and self.sell_rank <= 0:
            raise ValueError("sell_rank must be positive")
        if self.side is DragonTigerActivitySide.BUY and self.sell_amount_cny != 0:
            raise ValueError("buy-only activity cannot contain a sell amount")
        if self.side is DragonTigerActivitySide.SELL and self.buy_amount_cny != 0:
            raise ValueError("sell-only activity cannot contain a buy amount")
        if self.side is DragonTigerActivitySide.BOTH and (
            self.buy_amount_cny == 0 or self.sell_amount_cny == 0
        ):
            raise ValueError("both-side activity requires positive buy and sell amounts")

    @property
    def event_key(self) -> tuple[str, date]:
        return self.event_symbol, self.trade_date


@dataclass(frozen=True, slots=True)
class DragonTigerEventSummary:
    event_symbol: str
    trade_date: date
    calculation_version: str
    calculated_at: datetime
    total_buy_amount_cny: Decimal
    total_sell_amount_cny: Decimal
    total_net_amount_cny: Decimal
    institution_buy_amount_cny: Decimal
    institution_sell_amount_cny: Decimal
    institution_net_amount_cny: Decimal
    top5_buy_amount_cny: Decimal
    top5_sell_amount_cny: Decimal
    top5_buy_concentration_ratio: Decimal | None
    top5_sell_concentration_ratio: Decimal | None
    activity_count: int
    institution_activity_count: int

    def __post_init__(self) -> None:
        amounts = (
            self.total_buy_amount_cny,
            self.total_sell_amount_cny,
            self.institution_buy_amount_cny,
            self.institution_sell_amount_cny,
            self.top5_buy_amount_cny,
            self.top5_sell_amount_cny,
        )
        for amount in amounts:
            _decimal(amount, "summary_amount")
        for net_amount in (
            self.total_net_amount_cny,
            self.institution_net_amount_cny,
        ):
            _decimal(net_amount, "summary_net_amount")
        for concentration_ratio in (
            self.top5_buy_concentration_ratio,
            self.top5_sell_concentration_ratio,
        ):
            if concentration_ratio is not None:
                _decimal(concentration_ratio, "concentration_ratio")
        if any(amount < 0 for amount in amounts):
            raise ValueError("summary component amounts must be nonnegative")
        if self.total_net_amount_cny != self.total_buy_amount_cny - self.total_sell_amount_cny:
            raise ValueError("summary total net must equal buy minus sell")
        if self.institution_net_amount_cny != (
            self.institution_buy_amount_cny - self.institution_sell_amount_cny
        ):
            raise ValueError("summary institution net must equal buy minus sell")
        if (
            self.activity_count < 0
            or not 0 <= self.institution_activity_count <= self.activity_count
        ):
            raise ValueError("summary activity counts are invalid")
        if self.calculated_at.tzinfo is None:
            raise ValueError("calculated_at must be timezone-aware")
        if (
            self.top5_buy_amount_cny > self.total_buy_amount_cny
            or _ratio(self.top5_buy_amount_cny, self.total_buy_amount_cny)
            != self.top5_buy_concentration_ratio
        ):
            raise ValueError("buy concentration must be exactly recomputable")
        if (
            self.top5_sell_amount_cny > self.total_sell_amount_cny
            or _ratio(self.top5_sell_amount_cny, self.total_sell_amount_cny)
            != self.top5_sell_concentration_ratio
        ):
            raise ValueError("sell concentration must be exactly recomputable")

    @property
    def event_key(self) -> tuple[str, date]:
        return self.event_symbol, self.trade_date


@dataclass(frozen=True, slots=True)
class DragonTigerSnapshotBatch:
    trade_date: date
    observed_at: datetime
    status: DragonTigerSnapshotStatus
    input_hash: str
    content_hash: str
    observations: tuple[DragonTigerSourceObservation, ...]
    events: tuple[DragonTigerEvent, ...]
    reasons: tuple[DragonTigerReason, ...]
    seats: tuple[DragonTigerSeat, ...]
    activities: tuple[DragonTigerSeatActivity, ...]
    summaries: tuple[DragonTigerEventSummary, ...]
    partial_reasons: tuple[str, ...] = ()
    source_code: str = "eastmoney"

    def __post_init__(self) -> None:
        for name, value in (("input_hash", self.input_hash), ("content_hash", self.content_hash)):
            if not fullmatch(r"[0-9a-f]{64}", value):
                raise ValueError(f"{name} must be a lowercase SHA-256")
        if self.source_code != "eastmoney":
            raise ValueError("unsupported Dragon Tiger source")
        if self.observed_at.tzinfo is None:
            raise ValueError("snapshot observed_at must be timezone-aware")
        if self.observed_at.astimezone(SHANGHAI).date() < self.trade_date:
            raise ValueError("snapshot cannot precede its Shanghai trade date")
        if self.status is DragonTigerSnapshotStatus.PARTIAL and not self.partial_reasons:
            raise ValueError("partial snapshot requires explicit reasons")
        if self.status is DragonTigerSnapshotStatus.COMPLETE and self.partial_reasons:
            raise ValueError("complete snapshot cannot contain partial reasons")


@dataclass(frozen=True, slots=True)
class DragonTigerFinding:
    rule_code: str
    message: str
    natural_key: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class DragonTigerValidationResult:
    accepted: bool
    findings: tuple[DragonTigerFinding, ...]


def calculate_dragon_tiger_summary(
    event: DragonTigerEvent,
    activities: Sequence[DragonTigerSeatActivity],
    seats: Mapping[str, DragonTigerSeat],
    *,
    calculated_at: datetime,
    calculation_version: str = "dragon_tiger_summary_v1",
) -> DragonTigerEventSummary:
    """Sum stored activities; concentration is largest five amounts divided by stored total."""
    scoped = tuple(activity for activity in activities if activity.event_key == event.natural_key)
    total_buy = sum((row.buy_amount_cny for row in scoped), Decimal(0))
    total_sell = sum((row.sell_amount_cny for row in scoped), Decimal(0))
    institutions = tuple(
        row
        for row in scoped
        if seats[row.seat_identity_key].seat_type is DragonTigerSeatType.INSTITUTION
    )
    institution_buy = sum((row.buy_amount_cny for row in institutions), Decimal(0))
    institution_sell = sum((row.sell_amount_cny for row in institutions), Decimal(0))
    top5_buy = sum(sorted((row.buy_amount_cny for row in scoped), reverse=True)[:5], Decimal(0))
    top5_sell = sum(sorted((row.sell_amount_cny for row in scoped), reverse=True)[:5], Decimal(0))
    return DragonTigerEventSummary(
        event.symbol,
        event.trade_date,
        calculation_version,
        calculated_at,
        total_buy,
        total_sell,
        total_buy - total_sell,
        institution_buy,
        institution_sell,
        institution_buy - institution_sell,
        top5_buy,
        top5_sell,
        _ratio(top5_buy, total_buy),
        _ratio(top5_sell, total_sell),
        len(scoped),
        len(institutions),
    )


def validate_dragon_tiger_batch(
    batch: DragonTigerSnapshotBatch,
    *,
    known_symbols: Collection[str],
    known_trading_dates: Collection[date],
    historical_names: Mapping[tuple[str, date], str],
    unadjusted_closes: Mapping[tuple[str, date], Decimal],
    previous_closes: Mapping[tuple[str, date], Decimal],
) -> DragonTigerValidationResult:
    findings: list[DragonTigerFinding] = []
    if batch.trade_date not in known_trading_dates:
        findings.append(_finding("dragon_tiger.non_trading_date", batch.trade_date.isoformat()))
    event_keys = [event.natural_key for event in batch.events]
    _duplicates(event_keys, "dragon_tiger.duplicate_event", findings)
    observation_keys = [row.source_event_key for row in batch.observations]
    _duplicates(observation_keys, "dragon_tiger.duplicate_source_event", findings)
    seat_keys = [seat.identity_key for seat in batch.seats]
    _duplicates(seat_keys, "dragon_tiger.duplicate_seat_identity", findings)
    event_set, observation_set, seat_set = set(event_keys), set(observation_keys), set(seat_keys)
    observations = {row.source_event_key: row for row in batch.observations}
    for observation in batch.observations:
        if observation.trade_date != batch.trade_date:
            findings.append(_finding("dragon_tiger.observation_date_mismatch", observation.symbol))
        if observation.symbol not in known_symbols:
            findings.append(_finding("dragon_tiger.unknown_symbol", observation.symbol))
        if observation.source_event_key not in {event.source_event_key for event in batch.events}:
            findings.append(
                _finding("dragon_tiger.orphan_source_observation", observation.source_event_key)
            )
    for event in batch.events:
        if event.trade_date != batch.trade_date:
            findings.append(_finding("dragon_tiger.date_mismatch", event.symbol))
        if event.symbol not in known_symbols:
            findings.append(_finding("dragon_tiger.unknown_symbol", event.symbol))
        if event.source_event_key not in observation_set:
            findings.append(_finding("dragon_tiger.missing_source_observation", event.symbol))
        elif observations[event.source_event_key].symbol != event.symbol:
            findings.append(_finding("dragon_tiger.observation_symbol_mismatch", event.symbol))
        historical_name = historical_names.get(event.natural_key)
        if historical_name is None:
            findings.append(_finding("dragon_tiger.missing_historical_name", event.symbol))
        elif historical_name != event.historical_name:
            findings.append(_finding("dragon_tiger.historical_name_mismatch", event.symbol))
        close = unadjusted_closes.get(event.natural_key)
        if close is None:
            findings.append(_finding("dragon_tiger.missing_daily_close", event.symbol))
        elif close != event.close:
            findings.append(_finding("dragon_tiger.daily_close_mismatch", event.symbol))
        previous_close = previous_closes.get(event.natural_key)
        if previous_close is None or previous_close <= 0:
            findings.append(_finding("dragon_tiger.missing_previous_close", event.symbol))
        elif ((event.close / previous_close - 1) * 100).quantize(
            Decimal("0.0000000001")
        ) != event.change_percent:
            findings.append(_finding("dragon_tiger.change_percent_mismatch", event.symbol))
    reason_keys: list[tuple[tuple[str, date], str]] = []
    reason_orders: list[tuple[tuple[str, date], int]] = []
    for reason in batch.reasons:
        if reason.event_key not in event_set:
            findings.append(_finding("dragon_tiger.orphan_reason", reason.reason_code))
        reason_keys.append((reason.event_key, reason.reason_code))
        reason_orders.append((reason.event_key, reason.display_order))
    _duplicates(reason_keys, "dragon_tiger.duplicate_reason", findings)
    _duplicates(reason_orders, "dragon_tiger.duplicate_reason_order", findings)
    activity_keys: list[tuple[tuple[str, date], str]] = []
    source_orders: list[tuple[tuple[str, date], int]] = []
    for activity in batch.activities:
        if activity.event_key not in event_set:
            findings.append(_finding("dragon_tiger.orphan_activity", activity.event_symbol))
        if activity.seat_identity_key not in seat_set:
            findings.append(_finding("dragon_tiger.unknown_seat", activity.seat_identity_key))
        else:
            seat = next(
                row for row in batch.seats if row.identity_key == activity.seat_identity_key
            )
            if seat.valid_from > batch.trade_date or (
                seat.valid_to is not None and seat.valid_to < batch.trade_date
            ):
                findings.append(_finding("dragon_tiger.seat_outside_validity", seat.identity_key))
        activity_keys.append((activity.event_key, activity.seat_identity_key))
        source_orders.append((activity.event_key, activity.source_order))
    _duplicates(activity_keys, "dragon_tiger.duplicate_seat_activity", findings)
    _duplicates(source_orders, "dragon_tiger.duplicate_activity_order", findings)
    summary_keys = [summary.event_key for summary in batch.summaries]
    _duplicates(summary_keys, "dragon_tiger.duplicate_summary", findings)
    for key in summary_keys:
        if key not in event_set:
            findings.append(_finding("dragon_tiger.orphan_summary", repr(key)))
    summaries = {summary.event_key: summary for summary in batch.summaries}
    seats = {seat.identity_key: seat for seat in batch.seats}
    for event in batch.events:
        if batch.status is DragonTigerSnapshotStatus.COMPLETE and not any(
            reason.event_key == event.natural_key for reason in batch.reasons
        ):
            findings.append(_finding("dragon_tiger.complete_event_missing_reason", event.symbol))
        if batch.status is DragonTigerSnapshotStatus.COMPLETE and not any(
            activity.event_key == event.natural_key for activity in batch.activities
        ):
            findings.append(_finding("dragon_tiger.complete_event_missing_activity", event.symbol))
        summary = summaries.get(event.natural_key)
        if summary is None:
            findings.append(_finding("dragon_tiger.missing_summary", event.symbol))
            continue
        if all(a.seat_identity_key in seats for a in batch.activities):
            expected = calculate_dragon_tiger_summary(
                event,
                batch.activities,
                seats,
                calculated_at=summary.calculated_at,
                calculation_version=summary.calculation_version,
            )
            if expected != summary:
                findings.append(_finding("dragon_tiger.summary_mismatch", event.symbol))
    return DragonTigerValidationResult(not findings, tuple(findings))


def _symbol(value: str) -> None:
    if not fullmatch(r"(SSE|SZSE|BSE):[0-9]{6}", value):
        raise ValueError("symbol must be standardized")


def _decimal(value: Decimal, name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator == 0:
        return None
    return (numerator / denominator).quantize(Decimal("0.000000000001"))


def _text(value: str, name: str) -> None:
    if not value.strip():
        raise ValueError(f"{name} must not be blank")


def _duplicates(values: Sequence[object], rule: str, findings: list[DragonTigerFinding]) -> None:
    seen: set[object] = set()
    reported: set[object] = set()
    for value in values:
        if value in seen and value not in reported:
            findings.append(_finding(rule, repr(value)))
            reported.add(value)
        seen.add(value)


def _finding(rule: str, value: str) -> DragonTigerFinding:
    return DragonTigerFinding(rule, rule, {"identity": value})
