"""Pure, deterministic calculation of main-board price limits and stock pools."""

from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from json import dumps

from market_data_center.domain.records import Exchange, SecurityStatus, SecurityType, TradeStatus
from market_data_center.domain.stock_pool import (
    MAINBOARD_LIMIT_DOWN_POOL,
    MAINBOARD_LIMIT_UP_POOL,
    PRICE_LIMIT_ALGORITHM_VERSION,
    PRICE_LIMIT_RULE_EFFECTIVE_FROM,
    PRICE_LIMIT_RULE_VERSION,
    DailyPriceLimit,
    PriceLimitDirection,
    PriceLimitEvent,
    StockPoolBuildInput,
    StockPoolCalculationOutput,
    StockPoolCandidate,
    StockPoolMember,
    StockPoolQualityFinding,
    StockPoolQualitySeverity,
    price_limit_rule,
)


def calculate_mainboard_stock_pools(source: StockPoolBuildInput) -> StockPoolCalculationOutput:
    if source.basis_trade_date < PRICE_LIMIT_RULE_EFFECTIVE_FROM:
        raise ValueError("basis date predates the supported price-limit rule")
    if source.effective_trade_date <= source.basis_trade_date:
        raise ValueError("effective trade date must follow basis trade date")
    if len({item.symbol for item in source.candidates}) != len(source.candidates):
        raise ValueError("stock-pool candidates must be unique by symbol")

    limits: list[DailyPriceLimit] = []
    events: list[PriceLimitEvent] = []
    members: list[StockPoolMember] = []
    findings: list[StockPoolQualityFinding] = []
    rejected = 0
    for candidate in sorted(source.candidates, key=lambda item: item.symbol):
        reason = _rejection_reason(candidate)
        if reason is not None:
            rejected += 1
            findings.append(
                StockPoolQualityFinding(
                    rule_code=reason,
                    severity=StockPoolQualitySeverity.ERROR,
                    message=(
                        "candidate excluded because the applicable price-limit rule is not provable"
                    ),
                    symbol=candidate.symbol,
                )
            )
            continue
        previous_close = candidate.previous_close
        close = candidate.close
        assert previous_close is not None and close is not None
        rule = price_limit_rule(candidate.exchange, source.basis_trade_date)
        ratio = rule.st_ratio if candidate.is_st else rule.regular_ratio
        upper = _round_limit(
            previous_close * (Decimal(1) + ratio), previous_close, 1, rule.price_tick
        )
        lower = _round_limit(
            previous_close * (Decimal(1) - ratio), previous_close, -1, rule.price_tick
        )
        if (candidate.high is not None and candidate.high > upper) or (
            candidate.low is not None and candidate.low < lower
        ):
            rejected += 1
            findings.append(
                StockPoolQualityFinding(
                    rule_code="stock_pool.ohlc_outside_price_limit",
                    severity=StockPoolQualitySeverity.ERROR,
                    message="candidate OHLC contradicts the selected price-limit rule",
                    symbol=candidate.symbol,
                )
            )
            continue
        limits.append(
            DailyPriceLimit(
                symbol=candidate.symbol,
                trade_date=source.basis_trade_date,
                previous_close=previous_close,
                upper_limit=upper,
                lower_limit=lower,
                limit_ratio=ratio,
                price_tick=rule.price_tick,
                is_st=candidate.is_st,
                rule_version=PRICE_LIMIT_RULE_VERSION,
                algorithm_version=PRICE_LIMIT_ALGORITHM_VERSION,
            )
        )
        for direction, limit_price, pool_code in (
            (PriceLimitDirection.UP, upper, MAINBOARD_LIMIT_UP_POOL),
            (PriceLimitDirection.DOWN, lower, MAINBOARD_LIMIT_DOWN_POOL),
        ):
            if close != limit_price:
                continue
            events.append(
                PriceLimitEvent(
                    symbol=candidate.symbol,
                    trade_date=source.basis_trade_date,
                    direction=direction,
                    close=close,
                    limit_price=limit_price,
                    rule_version=PRICE_LIMIT_RULE_VERSION,
                    algorithm_version=PRICE_LIMIT_ALGORITHM_VERSION,
                )
            )
            members.append(StockPoolMember(pool_code, candidate.symbol, direction))
    input_hash = _input_hash(source)
    return StockPoolCalculationOutput(
        basis_trade_date=source.basis_trade_date,
        effective_trade_date=source.effective_trade_date,
        daily_price_limits=tuple(limits),
        events=tuple(events),
        members=tuple(members),
        findings=tuple(findings),
        candidate_count=len(source.candidates),
        rejected_count=rejected,
        input_hash=input_hash,
    )


def stock_pool_content_hash(pool_code: str, members: tuple[StockPoolMember, ...]) -> str:
    payload = [
        item.symbol
        for item in sorted(members, key=lambda item: item.symbol)
        if item.pool_code == pool_code
    ]
    return sha256(dumps(payload, separators=(",", ":")).encode()).hexdigest()


def _round_limit(
    value: Decimal, previous_close: Decimal, direction: int, price_tick: Decimal
) -> Decimal:
    rounded = value.quantize(price_tick, rounding=ROUND_HALF_UP)
    if abs(rounded - previous_close) < price_tick:
        rounded = previous_close + price_tick * direction
    return max(rounded, price_tick)


def _rejection_reason(candidate: StockPoolCandidate) -> str | None:
    if (
        candidate.security_type is not SecurityType.STOCK
        or candidate.security_status is not SecurityStatus.LISTED
    ):
        return "stock_pool.unsupported_security_status"
    if not _is_mainboard(candidate.exchange, candidate.code):
        return "stock_pool.unsupported_board"
    if candidate.ipo_date is None or candidate.listing_trading_day_number is None:
        return "stock_pool.unknown_listing_stage"
    if candidate.listing_trading_day_number <= 5:
        return "stock_pool.no_limit_initial_listing_stage"
    if candidate.prior_five_bar_count != 5:
        return "stock_pool.unproven_continuous_listing_stage"
    if candidate.trade_status is not TradeStatus.TRADING:
        return "stock_pool.not_trading"
    if candidate.previous_close is None or candidate.previous_close <= 0 or candidate.close is None:
        return "stock_pool.missing_price"
    if candidate.daily_bar_ingestion_id is None:
        return "stock_pool.missing_daily_bar_lineage"
    if candidate.indicator_ingestion_id is None:
        return "stock_pool.missing_daily_indicator"
    return None


def _is_mainboard(exchange: Exchange, code: str) -> bool:
    if len(code) != 6 or not code.isdigit():
        return False
    if exchange is Exchange.SSE:
        return 600000 <= int(code) <= 603999 or 605000 <= int(code) <= 605999
    if exchange is Exchange.SZSE:
        number = int(code)
        return 1 <= number <= 4999 and not 1001 <= number <= 1199
    return False


def _input_hash(source: StockPoolBuildInput) -> str:
    payload = {
        "basis_trade_date": source.basis_trade_date.isoformat(),
        "effective_trade_date": source.effective_trade_date.isoformat(),
        "algorithm_version": PRICE_LIMIT_ALGORITHM_VERSION,
        "rule_version": PRICE_LIMIT_RULE_VERSION,
        "candidates": [
            {
                "symbol": item.symbol,
                "code": item.code,
                "exchange": item.exchange.value,
                "security_type": item.security_type.value,
                "security_status": item.security_status.value,
                "ipo_date": item.ipo_date.isoformat() if item.ipo_date else None,
                "listing_day": item.listing_trading_day_number,
                "prior_five_bar_count": item.prior_five_bar_count,
                "trade_status": item.trade_status.value if item.trade_status else None,
                "previous_close": str(item.previous_close)
                if item.previous_close is not None
                else None,
                "open": str(item.open) if item.open is not None else None,
                "high": str(item.high) if item.high is not None else None,
                "low": str(item.low) if item.low is not None else None,
                "close": str(item.close) if item.close is not None else None,
                "daily_bar_ingestion_id": str(item.daily_bar_ingestion_id)
                if item.daily_bar_ingestion_id
                else None,
                "indicator_ingestion_id": str(item.indicator_ingestion_id)
                if item.indicator_ingestion_id
                else None,
            }
            for item in sorted(source.candidates, key=lambda value: value.symbol)
        ],
    }
    return sha256(dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
