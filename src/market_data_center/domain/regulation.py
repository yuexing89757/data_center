"""Provider-neutral contracts for deterministic regulation calculations."""

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from market_data_center.domain.records import Exchange
from market_data_center.domain.stock_pool import DailyPriceLimit

REGULATION_RULES_EFFECTIVE_FROM = date(2026, 7, 6)
_STANDARD_SYMBOL = re.compile(r"^(SSE|SZSE):[0-9]{6}$")
_LOWERCASE_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class RegulationSegment(StrEnum):
    SSE_MAIN = "SSE_MAIN"
    SZSE_MAIN = "SZSE_MAIN"
    GEM = "GEM"


class RegulationRuleLevel(StrEnum):
    ABNORMAL = "ABNORMAL"
    SERIOUS_ABNORMAL = "SERIOUS_ABNORMAL"


class RegulationRuleKind(StrEnum):
    CUMULATIVE_DEVIATION = "CUMULATIVE_DEVIATION"
    TURNOVER_COMPOSITE = "TURNOVER_COMPOSITE"
    EVENT_COUNT = "EVENT_COUNT"


class RegulationDirection(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    NONE = "NONE"


class RegulationResetLevel(StrEnum):
    ABNORMAL = "ABNORMAL"
    SERIOUS_ABNORMAL = "SERIOUS_ABNORMAL"


class CalculatedRegulationState(StrEnum):
    NORMAL = "NORMAL"
    ABNORMAL_TRIGGERED = "ABNORMAL_TRIGGERED"
    SERIOUS_TRIGGERED = "SERIOUS_TRIGGERED"


class AnnouncedRegulationState(StrEnum):
    NONE = "NONE"
    ABNORMAL = "ABNORMAL"
    SERIOUS_ABNORMAL = "SERIOUS_ABNORMAL"


class RegulationApplicability(StrEnum):
    APPLICABLE = "APPLICABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class RegulationEvaluationState(StrEnum):
    NOT_TRIGGERED = "NOT_TRIGGERED"
    TRIGGERED_CALCULATED = "TRIGGERED_CALCULATED"
    ANNOUNCED_BY_EXCHANGE = "ANNOUNCED_BY_EXCHANGE"


class RegulationReachability(StrEnum):
    CURRENT = "CURRENT"
    REACHABLE_NEXT_SESSION = "REACHABLE_NEXT_SESSION"
    NOT_REACHABLE_NEXT_SESSION = "NOT_REACHABLE_NEXT_SESSION"
    NOT_PRICE_CALCULABLE = "NOT_PRICE_CALCULABLE"


class RegulationScenarioCode(StrEnum):
    INDEX_DOWN_2 = "INDEX_DOWN_2"
    INDEX_FLAT = "INDEX_FLAT"
    INDEX_UP_2 = "INDEX_UP_2"
    CURRENT = "CURRENT"
    NONE = "NONE"


class RegulationRunStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class RegulationDataCompleteness(StrEnum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class RegulationEventType(StrEnum):
    ABNORMAL_VOLATILITY = "ABNORMAL_VOLATILITY"
    SERIOUS_ABNORMAL_VOLATILITY = "SERIOUS_ABNORMAL_VOLATILITY"


def _require_nonblank(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")


def _require_timezone_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _expected_exchange(segment: RegulationSegment) -> Exchange:
    if segment is RegulationSegment.SSE_MAIN:
        return Exchange.SSE
    return Exchange.SZSE


@dataclass(frozen=True, slots=True)
class RegulationRule:
    rule_code: str
    exchange: Exchange
    segment: RegulationSegment
    level: RegulationRuleLevel
    kind: RegulationRuleKind
    direction: RegulationDirection
    window_days: int | None
    threshold_pct: Decimal | None
    comparison_window_days: int | None
    ratio_threshold: Decimal | None
    secondary_threshold_pct: Decimal | None
    count_window_days: int | None
    required_count: int | None
    counted_event_kind: str | None
    reset_level: RegulationResetLevel
    benchmark_symbol: str | None
    rule_set_version: str
    effective_date: date
    expire_date: date | None
    source_document: str
    source_clause: str
    source_url: str
    enabled: bool

    def __post_init__(self) -> None:
        for field_name in (
            "rule_code",
            "rule_set_version",
            "source_document",
            "source_clause",
            "source_url",
        ):
            _require_nonblank(getattr(self, field_name), field_name.replace("_", " "))
        if self.exchange is not _expected_exchange(self.segment):
            raise ValueError("regulation rule exchange does not match segment")
        if self.effective_date < REGULATION_RULES_EFFECTIVE_FROM:
            raise ValueError("regulation rules cannot be effective before 2026-07-06")
        if self.expire_date is not None and self.expire_date < self.effective_date:
            raise ValueError("expire date must not precede effective date")

        if self.kind is RegulationRuleKind.CUMULATIVE_DEVIATION:
            self._validate_cumulative_deviation()
        elif self.kind is RegulationRuleKind.TURNOVER_COMPOSITE:
            self._validate_turnover_composite()
        else:
            self._validate_event_count()

    def _validate_cumulative_deviation(self) -> None:
        if self.direction is RegulationDirection.NONE:
            raise ValueError("cumulative-deviation rules cannot use Direction.NONE")
        if self.window_days is None or self.window_days <= 0 or self.threshold_pct is None:
            raise ValueError("cumulative-deviation requires a positive window and threshold")
        if self.direction is RegulationDirection.UP and self.threshold_pct <= 0:
            raise ValueError("UP threshold must be positive")
        if self.direction is RegulationDirection.DOWN and self.threshold_pct >= 0:
            raise ValueError("DOWN threshold must be negative")
        if self.benchmark_symbol is None or not self.benchmark_symbol.strip():
            raise ValueError("cumulative-deviation requires a benchmark symbol")
        if not _STANDARD_SYMBOL.fullmatch(self.benchmark_symbol):
            raise ValueError("benchmark symbol must use the standard symbol format")
        prohibited = (
            self.comparison_window_days,
            self.ratio_threshold,
            self.secondary_threshold_pct,
            self.count_window_days,
            self.required_count,
            self.counted_event_kind,
        )
        if any(value is not None for value in prohibited):
            raise ValueError("cumulative-deviation contains fields owned by another rule kind")

    def _validate_turnover_composite(self) -> None:
        if self.direction is not RegulationDirection.NONE:
            raise ValueError("turnover-composite rules require Direction.NONE")
        required_positive = (
            self.window_days,
            self.comparison_window_days,
            self.ratio_threshold,
            self.secondary_threshold_pct,
        )
        if any(value is None or value <= 0 for value in required_positive):
            raise ValueError("turnover-composite requires all four positive parameters")
        prohibited = (
            self.threshold_pct,
            self.count_window_days,
            self.required_count,
            self.counted_event_kind,
            self.benchmark_symbol,
        )
        if any(value is not None for value in prohibited):
            raise ValueError("turnover-composite contains fields owned by another rule kind")

    def _validate_event_count(self) -> None:
        if self.direction is RegulationDirection.NONE:
            raise ValueError("event-count rules cannot use Direction.NONE")
        if (
            self.count_window_days is None
            or self.count_window_days <= 0
            or self.required_count is None
            or self.required_count <= 0
            or self.counted_event_kind is None
            or not self.counted_event_kind.strip()
        ):
            raise ValueError("event-count requires a window, count, and event kind")
        prohibited = (
            self.window_days,
            self.threshold_pct,
            self.comparison_window_days,
            self.ratio_threshold,
            self.secondary_threshold_pct,
            self.benchmark_symbol,
        )
        if any(value is not None for value in prohibited):
            raise ValueError("event-count contains fields owned by another rule kind")


@dataclass(frozen=True, slots=True)
class RegulationEventRecord:
    symbol: str
    exchange: Exchange
    segment: RegulationSegment
    event_type: RegulationEventType
    event_level: RegulationRuleLevel
    direction: RegulationDirection | None
    period_start_date: date
    period_end_date: date
    published_at: datetime
    effective_reset_date: date | None
    source_event_id: str
    source_title: str
    source_url: str
    source_content_hash: str
    source_code: str
    explicit_rule_codes: tuple[str, ...]
    observed_at: datetime

    def __post_init__(self) -> None:
        if not _STANDARD_SYMBOL.fullmatch(self.symbol):
            raise ValueError("event symbol must use the standard symbol format")
        if self.exchange is not _expected_exchange(self.segment):
            raise ValueError("event exchange does not match segment")
        if not self.symbol.startswith(f"{self.exchange.value}:"):
            raise ValueError("event symbol exchange prefix does not match exchange")
        expected_level = {
            RegulationEventType.ABNORMAL_VOLATILITY: RegulationRuleLevel.ABNORMAL,
            RegulationEventType.SERIOUS_ABNORMAL_VOLATILITY: RegulationRuleLevel.SERIOUS_ABNORMAL,
        }[self.event_type]
        if self.event_level is not expected_level:
            raise ValueError("event type and level do not match")
        if self.direction is RegulationDirection.NONE:
            raise ValueError("event direction must be UP, DOWN, or missing")
        if self.period_end_date < self.period_start_date:
            raise ValueError("event period end must not precede its start")
        if (
            self.effective_reset_date is not None
            and self.effective_reset_date <= self.period_end_date
        ):
            raise ValueError("effective reset date must follow the event period")
        _require_timezone_aware(self.published_at, "published at")
        _require_timezone_aware(self.observed_at, "observed at")
        if self.observed_at < self.published_at:
            raise ValueError("observed at must not precede published at")
        for field_name in ("source_event_id", "source_title", "source_url", "source_code"):
            _require_nonblank(getattr(self, field_name), field_name.replace("_", " "))
        expected_source = {Exchange.SSE: "sse_official", Exchange.SZSE: "szse_official"}[
            self.exchange
        ]
        if self.source_code != expected_source:
            raise ValueError("official source code does not match event exchange")
        if not _LOWERCASE_SHA256.fullmatch(self.source_content_hash):
            raise ValueError("source content hash must be lowercase SHA-256")
        if len(set(self.explicit_rule_codes)) != len(self.explicit_rule_codes):
            raise ValueError("explicit rule codes must be unique")
        for rule_code in self.explicit_rule_codes:
            _require_nonblank(rule_code, "explicit rule code")


def regulation_event_natural_key(record: RegulationEventRecord) -> tuple[str, str]:
    return record.source_code, record.source_event_id


def validate_regulation_rules(
    rules: tuple[RegulationRule, ...], trade_date: date
) -> tuple[RegulationRule, ...]:
    codes: set[str] = set()
    for rule in rules:
        if rule.rule_code in codes:
            raise ValueError(f"duplicate rule code: {rule.rule_code}")
        codes.add(rule.rule_code)

    active = tuple(
        rule
        for rule in rules
        if rule.enabled
        and rule.effective_date <= trade_date
        and (rule.expire_date is None or trade_date <= rule.expire_date)
    )
    if not active:
        raise ValueError("no enabled regulation rule covers the requested date")
    versions = {rule.rule_set_version for rule in active}
    if len(versions) != 1:
        raise ValueError("active rules must share one rule-set version")
    dimensions: set[
        tuple[
            RegulationSegment,
            RegulationRuleLevel,
            RegulationRuleKind,
            RegulationDirection,
            int | None,
            int | None,
        ]
    ] = set()
    for rule in active:
        dimension = (
            rule.segment,
            rule.level,
            rule.kind,
            rule.direction,
            rule.window_days if rule.window_days is not None else rule.count_window_days,
            rule.comparison_window_days,
        )
        if dimension in dimensions:
            raise ValueError("duplicate active rule dimension")
        dimensions.add(dimension)
    return active


@dataclass(frozen=True, slots=True)
class RegulationDailyReturn:
    trade_date: date
    stock_close: Decimal | None
    stock_reference_previous_close: Decimal | None
    stock_return: Decimal | None
    benchmark_close: Decimal | None
    benchmark_previous_close: Decimal | None
    benchmark_return: Decimal | None
    turnover_rate_pct: Decimal | None

    def __post_init__(self) -> None:
        prices = (
            self.stock_close,
            self.stock_reference_previous_close,
            self.benchmark_close,
            self.benchmark_previous_close,
        )
        if any(value is not None and value <= 0 for value in prices):
            raise ValueError("regulation daily-return prices must be positive when present")
        factors = (self.stock_return, self.benchmark_return)
        if any(value is not None and value <= Decimal("-1") for value in factors):
            raise ValueError("regulation daily-return factors must remain positive")
        if self.turnover_rate_pct is not None and self.turnover_rate_pct < 0:
            raise ValueError("turnover rate must not be negative")


@dataclass(frozen=True, slots=True)
class RegulationCandidate:
    symbol: str
    exchange: Exchange
    segment: RegulationSegment
    applicability: RegulationApplicability
    applicability_reason: str | None
    daily_returns: tuple[RegulationDailyReturn, ...]
    events: tuple[RegulationEventRecord, ...]
    abnormal_reset_date: date | None
    serious_reset_date: date | None
    next_day_reference_price: Decimal | None
    next_day_price_limit: DailyPriceLimit | None

    def __post_init__(self) -> None:
        if not _STANDARD_SYMBOL.fullmatch(self.symbol):
            raise ValueError("candidate must use the standard symbol format")
        if self.exchange is not _expected_exchange(self.segment):
            raise ValueError("candidate exchange does not match segment")
        if not self.symbol.startswith(f"{self.exchange.value}:"):
            raise ValueError("candidate symbol exchange prefix does not match exchange")
        dates = tuple(item.trade_date for item in self.daily_returns)
        if dates != tuple(sorted(set(dates))):
            raise ValueError("candidate daily-return dates must be sorted and unique")
        if any(event.symbol != self.symbol for event in self.events):
            raise ValueError("candidate events must belong to the candidate symbol")
        if self.next_day_reference_price is not None and self.next_day_reference_price <= 0:
            raise ValueError("next-day reference price must be positive")
        if (
            self.next_day_price_limit is not None
            and self.next_day_price_limit.symbol != self.symbol
        ):
            raise ValueError("next-day price limit must belong to the candidate symbol")


@dataclass(frozen=True, slots=True)
class RegulationCalculationInput:
    trade_date: date
    next_trade_date: date
    algorithm_version: str
    scenario_config_version: str
    active_rules: tuple[RegulationRule, ...]
    trading_dates: tuple[date, ...]
    candidates: tuple[RegulationCandidate, ...]
    rule_set_hash: str
    market_watermark: str
    capital_watermark: str
    event_watermark: datetime

    def __post_init__(self) -> None:
        if self.next_trade_date <= self.trade_date:
            raise ValueError("next trade date must follow trade date")
        for field_name in (
            "algorithm_version",
            "scenario_config_version",
            "market_watermark",
            "capital_watermark",
        ):
            _require_nonblank(getattr(self, field_name), field_name.replace("_", " "))
        if not _LOWERCASE_SHA256.fullmatch(self.rule_set_hash):
            raise ValueError("rule set hash must be lowercase SHA-256")
        _require_timezone_aware(self.event_watermark, "event watermark")
        validate_regulation_rules(self.active_rules, self.trade_date)
        if self.trading_dates != tuple(sorted(set(self.trading_dates))):
            raise ValueError("trading dates must be sorted and unique")
        if self.trade_date not in self.trading_dates:
            raise ValueError("trading dates must include the calculation trade date")
        symbols = tuple(candidate.symbol for candidate in self.candidates)
        if len(symbols) != len(set(symbols)):
            raise ValueError("regulation candidates must have unique symbols")


@dataclass(frozen=True, slots=True)
class RegulationStatusResult:
    trade_date: date
    symbol: str
    exchange: Exchange
    segment: RegulationSegment
    applicability: RegulationApplicability
    applicability_reason: str | None
    data_completeness: RegulationDataCompleteness
    calculated_state: CalculatedRegulationState
    announced_state: AnnouncedRegulationState
    close: Decimal | None
    stock_daily_return_pct: Decimal | None
    benchmark_symbol: str | None
    benchmark_close: Decimal | None
    benchmark_daily_return_pct: Decimal | None
    daily_deviation_pct: Decimal | None
    abnormal_count_10d: int
    abnormal_count_10d_up: int
    abnormal_count_10d_down: int
    abnormal_reset_date: date | None
    serious_reset_date: date | None


@dataclass(frozen=True, slots=True)
class RegulationRuleResult:
    symbol: str
    rule_code: str
    evaluation_state: RegulationEvaluationState
    triggered: bool
    window_start_date: date | None
    window_end_date: date | None
    observed_window_days: int | None
    current_value: Decimal | None
    threshold: Decimal | None
    distance: Decimal | None
    secondary_current_value: Decimal | None
    secondary_threshold: Decimal | None
    event_count: int | None
    required_count: int | None
    selected_reset_date: date | None
    data_completeness: RegulationDataCompleteness
    incomplete_reason: str | None


@dataclass(frozen=True, slots=True)
class RegulationWarningResult:
    trade_date: date
    next_trade_date: date
    symbol: str
    rule_code: str
    level: RegulationRuleLevel
    direction: RegulationDirection
    current_value: Decimal | None
    threshold: Decimal | None
    distance: Decimal | None
    scenario_code: RegulationScenarioCode
    scenario_index_pct: Decimal | None
    next_day_reference_price: Decimal | None
    raw_trigger_price: Decimal | None
    next_day_trigger_price: Decimal | None
    next_day_trigger_pct: Decimal | None
    price_limit_ratio: Decimal | None
    lower_limit_price: Decimal | None
    upper_limit_price: Decimal | None
    reachability: RegulationReachability
    window_start_date: date | None
    window_end_date: date | None
    requires_official_event_confirmation: bool
    message_template_code: str
    message: str


@dataclass(frozen=True, slots=True)
class RegulationCoverage:
    expected_count: int
    complete_count: int
    incomplete_count: int
    not_applicable_count: int

    def __post_init__(self) -> None:
        counts = (
            self.expected_count,
            self.complete_count,
            self.incomplete_count,
            self.not_applicable_count,
        )
        if min(counts) < 0:
            raise ValueError("regulation coverage counts must not be negative")
        if sum(counts[1:]) != self.expected_count:
            raise ValueError("regulation coverage categories must equal expected count")


@dataclass(frozen=True, slots=True)
class RegulationCalculationOutput:
    trade_date: date
    next_trade_date: date
    statuses: tuple[RegulationStatusResult, ...]
    rule_results: tuple[RegulationRuleResult, ...]
    warnings: tuple[RegulationWarningResult, ...]
    coverage: RegulationCoverage
    quality_findings: tuple[str, ...]
