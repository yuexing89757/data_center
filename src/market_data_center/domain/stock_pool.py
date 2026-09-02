"""Provider-neutral price-limit facts and immutable stock-pool snapshots."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from market_data_center.domain.records import Exchange, SecurityStatus, SecurityType, TradeStatus

MAINBOARD_LIMIT_UP_POOL = "CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP"
MAINBOARD_LIMIT_DOWN_POOL = "CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN"
PRICE_LIMIT_ALGORITHM_VERSION = "1.0.0"
PRICE_LIMIT_RULE_VERSION = "CN_MAINBOARD_2026_07_06"
GEM_PRICE_LIMIT_RULE_VERSION = "CN_GEM_2026_07_06"
PRICE_LIMIT_RULE_EFFECTIVE_FROM = date(2026, 7, 6)


class PriceLimitDirection(StrEnum):
    UP = "up"
    DOWN = "down"


class StockPoolSnapshotStatus(StrEnum):
    READY = "ready"
    FAILED = "failed"


class StockPoolQualitySeverity(StrEnum):
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class PriceLimitRule:
    rule_version: str
    exchange: Exchange
    board: str
    effective_from: date
    effective_to: date | None
    regular_ratio: Decimal
    st_ratio: Decimal
    price_tick: Decimal
    initial_no_limit_trading_days: int


PRICE_LIMIT_RULES = (
    *(
        PriceLimitRule(
            PRICE_LIMIT_RULE_VERSION,
            exchange,
            "mainboard",
            PRICE_LIMIT_RULE_EFFECTIVE_FROM,
            None,
            Decimal("0.10"),
            Decimal("0.10"),
            Decimal("0.01"),
            5,
        )
        for exchange in (Exchange.SSE, Exchange.SZSE)
    ),
    PriceLimitRule(
        GEM_PRICE_LIMIT_RULE_VERSION,
        Exchange.SZSE,
        "gem",
        PRICE_LIMIT_RULE_EFFECTIVE_FROM,
        None,
        Decimal("0.20"),
        Decimal("0.20"),
        Decimal("0.01"),
        5,
    ),
)


def price_limit_rule(
    exchange: Exchange, trade_date: date, *, board: str = "mainboard"
) -> PriceLimitRule:
    try:
        return next(
            rule
            for rule in PRICE_LIMIT_RULES
            if rule.exchange is exchange
            and rule.board == board
            and rule.effective_from <= trade_date
            and (rule.effective_to is None or trade_date <= rule.effective_to)
        )
    except StopIteration as error:
        raise ValueError("no accepted price-limit rule covers this exchange/date") from error


@dataclass(frozen=True, slots=True)
class StockPoolDefinition:
    code: str
    display_name: str
    description: str
    direction: PriceLimitDirection

    def __post_init__(self) -> None:
        if not self.code.strip() or not self.display_name.strip() or not self.description.strip():
            raise ValueError("stock-pool definition fields must not be blank")


STOCK_POOL_DEFINITIONS = (
    StockPoolDefinition(
        MAINBOARD_LIMIT_UP_POOL,
        "沪深主板昨日涨停",
        "上一交易日收盘价等于版本化涨停价的沪深主板普通股票。",
        PriceLimitDirection.UP,
    ),
    StockPoolDefinition(
        MAINBOARD_LIMIT_DOWN_POOL,
        "沪深主板昨日跌停",
        "上一交易日收盘价等于版本化跌停价的沪深主板普通股票。",
        PriceLimitDirection.DOWN,
    ),
)


@dataclass(frozen=True, slots=True)
class StockPoolCandidate:
    symbol: str
    code: str
    exchange: Exchange
    security_type: SecurityType
    security_status: SecurityStatus
    ipo_date: date | None
    listing_trading_day_number: int | None
    prior_five_bar_count: int
    trade_status: TradeStatus | None
    previous_close: Decimal | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    is_st: bool | None
    daily_bar_ingestion_id: UUID | None
    indicator_ingestion_id: UUID | None
    free_float_turnover_rate_pct: Decimal | None
    free_float_shares: int | None
    circulating_market_value: Decimal | None


@dataclass(frozen=True, slots=True)
class StockPoolBuildInput:
    basis_trade_date: date
    effective_trade_date: date
    candidates: tuple[StockPoolCandidate, ...]

    def __post_init__(self) -> None:
        if self.effective_trade_date <= self.basis_trade_date:
            raise ValueError("effective trade date must follow basis trade date")


@dataclass(frozen=True, slots=True)
class DailyPriceLimit:
    symbol: str
    trade_date: date
    previous_close: Decimal
    upper_limit: Decimal
    lower_limit: Decimal
    limit_ratio: Decimal
    price_tick: Decimal
    is_st: bool | None
    rule_version: str
    algorithm_version: str

    def __post_init__(self) -> None:
        if not (self.upper_limit > self.previous_close >= self.lower_limit > 0):
            raise ValueError("invalid daily price-limit range")
        if self.limit_ratio <= 0 or self.price_tick <= 0:
            raise ValueError("price-limit ratio and tick must be positive")


@dataclass(frozen=True, slots=True)
class PriceLimitEvent:
    symbol: str
    trade_date: date
    direction: PriceLimitDirection
    close: Decimal
    limit_price: Decimal
    rule_version: str
    algorithm_version: str

    def __post_init__(self) -> None:
        if self.close <= 0 or self.close != self.limit_price:
            raise ValueError("price-limit event close must equal its positive limit price")


@dataclass(frozen=True, slots=True)
class PriceLimitSealSummary:
    """Future order-book summary; absent until an accepted quote source supplies it."""

    symbol: str
    trade_date: date
    direction: PriceLimitDirection
    observed_at: datetime
    seal_amount: Decimal
    source_code: str

    def __post_init__(self) -> None:
        if self.seal_amount < 0:
            raise ValueError("seal amount must not be negative")


@dataclass(frozen=True, slots=True)
class StockPoolMember:
    pool_code: str
    symbol: str
    direction: PriceLimitDirection

    def __post_init__(self) -> None:
        expected = {
            PriceLimitDirection.UP: MAINBOARD_LIMIT_UP_POOL,
            PriceLimitDirection.DOWN: MAINBOARD_LIMIT_DOWN_POOL,
        }[self.direction]
        if self.pool_code != expected:
            raise ValueError("stock-pool member direction does not match pool definition")


@dataclass(frozen=True, slots=True)
class StockPoolQualityFinding:
    rule_code: str
    severity: StockPoolQualitySeverity
    message: str
    symbol: str | None = None


@dataclass(frozen=True, slots=True)
class StockPoolCalculationOutput:
    basis_trade_date: date
    effective_trade_date: date
    daily_price_limits: tuple[DailyPriceLimit, ...]
    events: tuple[PriceLimitEvent, ...]
    members: tuple[StockPoolMember, ...]
    findings: tuple[StockPoolQualityFinding, ...]
    candidate_count: int
    rejected_count: int
    input_hash: str


@dataclass(frozen=True, slots=True)
class StockPoolBuildSummary:
    status: str
    calculation_id: UUID
    snapshot_ids: tuple[UUID, ...]
    basis_trade_date: date
    effective_trade_date: date
    candidate_count: int
    member_count: int
    rejected_count: int


@dataclass(frozen=True, slots=True)
class StockPoolSnapshot:
    snapshot_id: UUID
    calculation_id: UUID
    pool_code: str
    basis_trade_date: date
    effective_trade_date: date
    version: int
    status: StockPoolSnapshotStatus
    member_count: int
    candidate_count: int
    rejected_count: int
    content_hash: str
    input_hash: str
    rule_version: str
    algorithm_version: str
    generated_at: datetime

    def __post_init__(self) -> None:
        if self.effective_trade_date <= self.basis_trade_date or self.version < 1:
            raise ValueError("invalid stock-pool snapshot date/version")
        if min(self.member_count, self.candidate_count, self.rejected_count) < 0:
            raise ValueError("stock-pool snapshot counts must not be negative")
        if self.member_count > self.candidate_count or self.rejected_count > self.candidate_count:
            raise ValueError("stock-pool snapshot counts exceed candidates")
        for digest in (self.content_hash, self.input_hash):
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError("stock-pool snapshot hashes must be lowercase SHA-256")


def pool_definition(code: str) -> StockPoolDefinition:
    try:
        return next(item for item in STOCK_POOL_DEFINITIONS if item.code == code)
    except StopIteration as error:
        raise ValueError(f"unknown stock pool code: {code}") from error
