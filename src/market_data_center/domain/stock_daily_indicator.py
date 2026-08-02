"""Provider-neutral stock daily indicator snapshots and validation."""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum

from market_data_center.domain.records import Market


class PriceLimitStatus(StrEnum):
    FLAT = "flat"
    RISE = "rise"
    LIMIT_UP = "limit_up"
    ONE_PRICE_LIMIT_UP = "one_price_limit_up"
    FALL = "fall"
    LIMIT_DOWN = "limit_down"
    ONE_PRICE_LIMIT_DOWN = "one_price_limit_down"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class StockDailyIndicatorSnapshotRecord:
    symbol: str
    trade_date: date
    market: Market
    close: Decimal | None
    turnover_rate_pct: Decimal | None
    free_float_turnover_rate_pct: Decimal | None
    volume_ratio: Decimal | None
    pe: Decimal | None
    pe_ttm: Decimal | None
    pb: Decimal | None
    ps: Decimal | None
    ps_ttm: Decimal | None
    dividend_yield_pct: Decimal | None
    dividend_yield_ttm_pct: Decimal | None
    total_shares: int | None
    circulating_shares: int | None
    free_float_shares: int | None
    total_market_value: Decimal | None
    circulating_market_value: Decimal | None
    price_limit_status: PriceLimitStatus
    source_code: str

    def __post_init__(self) -> None:
        if self.market is not Market.CN_A_SHARE:
            raise ValueError("stock daily indicators only support CN_A_SHARE")
        exchange, separator, code = self.symbol.partition(":")
        if separator != ":" or exchange not in {"SSE", "SZSE", "BSE"}:
            raise ValueError("symbol must use a supported standard exchange prefix")
        if len(code) != 6 or not code.isdigit():
            raise ValueError("symbol code must contain six digits")
        if not self.source_code.strip():
            raise ValueError("source_code must not be blank")

        nonnegative_decimals = (
            self.close,
            self.turnover_rate_pct,
            self.free_float_turnover_rate_pct,
            self.volume_ratio,
            self.dividend_yield_pct,
            self.dividend_yield_ttm_pct,
            self.total_market_value,
            self.circulating_market_value,
        )
        if any(value is not None and value < 0 for value in nonnegative_decimals):
            raise ValueError("prices, rates, ratios and market values must not be negative")
        shares = (self.total_shares, self.circulating_shares, self.free_float_shares)
        if any(value is not None and value < 0 for value in shares):
            raise ValueError("share counts must not be negative")
        if self.total_shares is not None and self.total_shares == 0:
            raise ValueError("total_shares must be positive when present")


@dataclass(frozen=True, slots=True)
class StockDailyIndicatorFinding:
    rule_code: str
    message: str
    natural_key: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class StockDailyIndicatorValidationResult:
    accepted: tuple[StockDailyIndicatorSnapshotRecord, ...]
    findings: tuple[StockDailyIndicatorFinding, ...]
    rejected_rows: int


def stock_daily_indicator_natural_key(
    record: StockDailyIndicatorSnapshotRecord,
) -> tuple[str, date]:
    return record.symbol, record.trade_date


def stock_daily_indicator_natural_key_json(
    record: StockDailyIndicatorSnapshotRecord,
) -> dict[str, object]:
    return {"symbol": record.symbol, "trade_date": record.trade_date.isoformat()}


def validate_stock_daily_indicators(
    records: Sequence[StockDailyIndicatorSnapshotRecord],
    *,
    known_symbols: Collection[str],
    known_trading_dates: Collection[date],
    minimum_accepted_rows: int = 0,
) -> StockDailyIndicatorValidationResult:
    if minimum_accepted_rows < 0:
        raise ValueError("minimum_accepted_rows must not be negative")
    grouped: dict[tuple[str, date], list[StockDailyIndicatorSnapshotRecord]] = {}
    for record in records:
        grouped.setdefault(stock_daily_indicator_natural_key(record), []).append(record)

    accepted: list[StockDailyIndicatorSnapshotRecord] = []
    findings: list[StockDailyIndicatorFinding] = []
    rejected_rows = 0
    for grouped_records in grouped.values():
        record = grouped_records[0]
        natural_key = stock_daily_indicator_natural_key_json(record)
        if record.symbol not in known_symbols:
            rejected_rows += len(grouped_records)
            findings.append(
                StockDailyIndicatorFinding(
                    rule_code="stock_daily_indicator.unknown_symbol",
                    message="stock daily indicator references an unknown Security symbol",
                    natural_key=natural_key,
                )
            )
            continue
        if record.trade_date not in known_trading_dates:
            rejected_rows += len(grouped_records)
            findings.append(
                StockDailyIndicatorFinding(
                    rule_code="stock_daily_indicator.unknown_trading_date",
                    message="stock daily indicator references an unknown trading date",
                    natural_key=natural_key,
                )
            )
            continue
        if (
            record.circulating_shares is not None
            and record.total_shares is not None
            and record.circulating_shares > record.total_shares
        ) or (
            record.free_float_shares is not None
            and record.circulating_shares is not None
            and record.free_float_shares > record.circulating_shares
        ):
            rejected_rows += len(grouped_records)
            findings.append(
                StockDailyIndicatorFinding(
                    rule_code="stock_daily_indicator.invalid_share_order",
                    message="stock daily indicator share layers have an invalid order",
                    natural_key=natural_key,
                )
            )
            continue
        if (
            record.circulating_market_value is not None
            and record.total_market_value is not None
            and record.circulating_market_value > record.total_market_value
        ):
            rejected_rows += len(grouped_records)
            findings.append(
                StockDailyIndicatorFinding(
                    rule_code="stock_daily_indicator.invalid_market_value_order",
                    message="circulating market value exceeds total market value",
                    natural_key=natural_key,
                )
            )
            continue
        if any(candidate != record for candidate in grouped_records[1:]):
            rejected_rows += len(grouped_records)
            findings.append(
                StockDailyIndicatorFinding(
                    rule_code="stock_daily_indicator.conflicting_duplicate",
                    message="batch contains conflicting indicators for one natural key",
                    natural_key=natural_key,
                )
            )
            continue
        accepted.append(record)
    if len(accepted) < minimum_accepted_rows:
        findings.append(
            StockDailyIndicatorFinding(
                rule_code="stock_daily_indicator.incomplete_market_snapshot",
                message=(
                    "accepted stock daily indicator rows are below the historical coverage floor"
                ),
                natural_key={"minimum_accepted_rows": minimum_accepted_rows},
            )
        )
        return StockDailyIndicatorValidationResult(
            accepted=(),
            findings=tuple(findings),
            rejected_rows=len(records),
        )
    return StockDailyIndicatorValidationResult(
        accepted=tuple(accepted),
        findings=tuple(findings),
        rejected_rows=rejected_rows,
    )
