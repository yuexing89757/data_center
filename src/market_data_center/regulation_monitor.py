"""Pure conditional intraday thresholds; never persists hypothetical close events."""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal

from market_data_center.domain.regulation import (
    REGULATION_RULES_EFFECTIVE_FROM,
    MonitorDay,
    MonitorState,
    RegulationApplicability,
    RegulationCalculationInput,
    RegulationCandidate,
    RegulationDailyReturn,
    RegulationDirection,
    RegulationRule,
    RegulationRuleKind,
    RegulationRuleLevel,
    RegulationSegment,
)
from market_data_center.domain.stock_pool import price_limit_rule
from market_data_center.providers.regulation_quote import MonitorQuote, quote_issue
from market_data_center.regulation_calculator import _round_to_tick, calculate_monitor_day
from market_data_center.stock_pool_calculator import calculate_price_limit_range


@dataclass(frozen=True, slots=True)
class MonitorThreshold:
    trade_date: date
    trigger_price: Decimal | None
    trigger_change_pct: Decimal | None
    reachability: str
    reference_price: Decimal | None
    scenario_index_pct: Decimal | None
    reset_branch: str
    window_start_date: date | None = None
    missing_reason: str | None = None


@dataclass(frozen=True, slots=True)
class MonitorCondition:
    symbol: str
    rule_code: str
    today: MonitorThreshold
    next_day: MonitorThreshold


@dataclass(frozen=True, slots=True)
class MonitorProjection:
    day: MonitorDay
    conditions: tuple[MonitorCondition, ...]
    hypothetical_close_at: datetime | None
    scenario_index_pct: Decimal
    missing_reasons: tuple[str, ...]


def round_trigger_price(value: Decimal, direction: RegulationDirection) -> Decimal:
    if not value.is_finite() or value <= 0 or direction is RegulationDirection.NONE:
        raise ValueError("a positive finite price and price direction are required")
    return _round_to_tick(value, Decimal("0.01"), direction)


def _threshold(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    rule: RegulationRule,
    state: MonitorState | None,
    target: date,
    reference: Decimal | None,
    index_pct: Decimal | None,
    branch: str,
    issue: str | None = None,
) -> MonitorThreshold:
    empty = MonitorThreshold(target, None, None, "INCOMPLETE", reference, index_pct, branch)
    if target < rule.effective_date or (rule.expire_date is not None and target > rule.expire_date):
        return replace(empty, reachability="NOT_APPLICABLE", missing_reason="rule_not_effective")
    if candidate.applicability is not RegulationApplicability.APPLICABLE:
        return replace(
            empty,
            reachability=candidate.applicability.value,
            missing_reason=candidate.applicability_reason or "unknown_applicability",
        )
    if rule.kind is RegulationRuleKind.TURNOVER_COMPOSITE:
        return replace(
            empty,
            reachability="NOT_PRICE_CALCULABLE",
            missing_reason="turnover_cannot_be_inferred_from_price",
        )
    if issue:
        return replace(empty, missing_reason=issue)
    if state is None and target != REGULATION_RULES_EFFECTIVE_FROM:
        return replace(empty, missing_reason="missing_continuous_checkpoint")
    if state is not None and not state.complete:
        return replace(
            empty, missing_reason=next(iter(state.missing_reasons), "incomplete_history")
        )
    if reference is None or index_pct is None:
        return replace(empty, missing_reason="unverified_next_day_reference")
    if rule.kind is RegulationRuleKind.EVENT_COUNT:
        dates = sorted({*source.trading_dates, target})
        dates = [d for d in dates if d <= target][-(rule.count_window_days or 10) :]
        count = sum(
            e.trade_date in dates
            and e.trade_date < target
            and e.level is RegulationRuleLevel.ABNORMAL
            and e.kind is RegulationRuleKind.CUMULATIVE_DEVIATION
            and e.direction is rule.direction
            and (
                state is None
                or state.serious_reset_date is None
                or e.trade_date >= state.serious_reset_date
            )
            for e in (state.events if state else ())
        )
        if count != (rule.required_count or 0) - 1:
            return replace(
                empty,
                reachability="NOT_PRICE_CALCULABLE",
                missing_reason="requires_more_than_one_close_event",
            )
        ordinary = next(
            (
                r
                for r in source.active_rules
                if r.segment is candidate.segment
                and r.level is RegulationRuleLevel.ABNORMAL
                and r.direction is rule.direction
                and r.kind is RegulationRuleKind.CUMULATIVE_DEVIATION
            ),
            None,
        )
        if ordinary is None:
            return replace(empty, missing_reason="missing_ordinary_price_rule")
        rule = ordinary
    assert rule.window_days is not None and rule.threshold_pct is not None
    reset = REGULATION_RULES_EFFECTIVE_FROM
    if state:
        reset = max(reset, state.serious_reset_date or reset)
        if rule.level is RegulationRuleLevel.ABNORMAL:
            reset = max(reset, state.abnormal_reset_date or reset)
    history = [d for d in source.trading_dates if d < target]
    history = history[-(rule.window_days - 1) :] if rule.window_days > 1 else []
    history = [d for d in history if d >= reset]
    rows = {row.trade_date: row for row in candidate.daily_returns}
    for day in history:
        row = rows.get(day)
        if row is None or row.stock_return is None or row.benchmark_return is None:
            return replace(empty, missing_reason=f"missing_daily_return:{day}")
    solutions: list[tuple[Decimal, date, Decimal, Decimal]] = []
    for start in range(len(history) + 1):
        stock_factor = benchmark_factor = Decimal(1)
        for day in history[start:]:
            row = rows[day]
            assert row.stock_return is not None and row.benchmark_return is not None
            stock_factor *= 1 + row.stock_return
            benchmark_factor *= 1 + row.benchmark_return
        projected_index = benchmark_factor * (1 + index_pct / 100)
        raw = reference * (rule.threshold_pct / 100 + projected_index) / stock_factor
        if raw > 0:
            solutions.append(
                (
                    raw,
                    history[start] if start < len(history) else target,
                    stock_factor,
                    projected_index,
                )
            )
    if not solutions:
        return replace(
            empty, reachability="NOT_REACHABLE", missing_reason="no_positive_trigger_price"
        )
    chooser = min if rule.direction is RegulationDirection.UP else max
    raw, start_day, stock_factor, index_factor = chooser(solutions, key=lambda x: x[0])
    price = round_trigger_price(raw, rule.direction)
    deviation = (stock_factor * price / reference - index_factor) * 100
    satisfies = (
        deviation >= rule.threshold_pct
        if rule.direction is RegulationDirection.UP
        else deviation <= rule.threshold_pct
    )
    if not satisfies:  # Decimal division at an exact tick boundary can consume its final digit.
        price += Decimal("0.01") if rule.direction is RegulationDirection.UP else Decimal("-0.01")
    if price <= 0:
        return replace(
            empty, reachability="NOT_REACHABLE", missing_reason="no_positive_trigger_price"
        )
    limit = price_limit_rule(
        candidate.exchange,
        target,
        board="gem" if candidate.segment is RegulationSegment.GEM else "mainboard",
    )
    lower, upper = calculate_price_limit_range(reference, limit.regular_ratio, limit.price_tick)
    reachable = price <= upper if rule.direction is RegulationDirection.UP else price >= lower
    return replace(
        empty,
        trigger_price=price,
        trigger_change_pct=(price / reference - 1) * 100,
        reachability="REACHABLE" if reachable else "NOT_REACHABLE",
        window_start_date=start_day,
    )


def project_monitor(
    source: RegulationCalculationInput,
    states: tuple[MonitorState, ...],
    quotes: tuple[MonitorQuote, ...],
    scenario_index_pct: Decimal,
    now: datetime,
    *,
    next_reference_safe_symbols: frozenset[str] = frozenset(),
    unrestricted_shares: Mapping[str, Decimal] | None = None,
) -> MonitorProjection:
    """Source dates are the target session and its real successor, supplied by the calendar.

    Next-reference safety must be explicitly established by the bounded input RPC;
    absence of that proof is unknown, never an assumed absence of corporate actions.
    """
    if scenario_index_pct not in (Decimal(-2), Decimal(0), Decimal(2)):
        raise ValueError("unsupported index scenario")
    by_symbol = {q.symbol: q for q in quotes}
    if len(by_symbol) != len(quotes):
        raise ValueError("duplicate quote identity")
    issues: dict[str, str] = {}
    candidates = []
    for candidate in source.candidates:
        symbol = candidate.symbol
        stock = by_symbol.get(symbol)
        benchmark_symbol = next(
            (
                r.benchmark_symbol
                for r in source.active_rules
                if r.segment is candidate.segment and r.benchmark_symbol
            ),
            None,
        )
        benchmark = by_symbol.get(benchmark_symbol or "")
        issue = (
            "missing_stock_quote"
            if stock is None
            else "missing_benchmark_quote"
            if benchmark is None
            else quote_issue(stock, benchmark, source.trade_date, now=now, active_session=False)
        )
        if issue:
            issues[symbol] = issue
        turnover = None
        shares = (unrestricted_shares or {}).get(symbol)
        if (
            not issue
            and stock
            and stock.volume_shares is not None
            and shares is not None
            and shares > 0
        ):
            turnover = stock.volume_shares / shares * 100
        row = RegulationDailyReturn(
            source.trade_date,
            stock.price if stock and not issue else None,
            stock.reference_price if stock and not issue else None,
            stock.price / stock.reference_price - 1 if stock and not issue else None,
            benchmark.price if benchmark and not issue else None,
            benchmark.reference_price if benchmark and not issue else None,
            benchmark.price / benchmark.reference_price - 1 if benchmark and not issue else None,
            turnover,
        )
        candidates.append(
            replace(
                candidate,
                daily_returns=(
                    *(r for r in candidate.daily_returns if r.trade_date < source.trade_date),
                    row,
                ),
            )
        )
    current = replace(source, candidates=tuple(candidates))
    day = calculate_monitor_day(current, states)
    references = {
        c.symbol: c.daily_returns[-1].stock_close
        for c in candidates
        if c.symbol in next_reference_safe_symbols
    }
    conditions = _conditions(
        current, states, day, scenario_index_pct, references, issues, "HYPOTHETICAL"
    )
    symbols = {c.symbol for c in candidates}
    timestamps = [q.observed_at for q in quotes if q.symbol in symbols and q.symbol not in issues]
    return MonitorProjection(
        day,
        conditions,
        min(timestamps, default=None),
        scenario_index_pct,
        tuple(sorted(set(issues.values()))),
    )


def project_monitor_close(
    source: RegulationCalculationInput,
    states: tuple[MonitorState, ...],
    scenario_index_pct: Decimal,
) -> MonitorProjection:
    """Replay a confirmed snapshot; no clock, network, or hypothetical future close."""
    if scenario_index_pct not in (Decimal(-2), Decimal(0), Decimal(2)):
        raise ValueError("unsupported index scenario")
    day = calculate_monitor_day(source, states)
    references = {c.symbol: c.next_day_reference_price for c in source.candidates}
    conditions = _conditions(source, states, day, scenario_index_pct, references, {}, "CONFIRMED")
    return MonitorProjection(day, conditions, None, scenario_index_pct, ())


def _conditions(
    current: RegulationCalculationInput,
    states: tuple[MonitorState, ...],
    day: MonitorDay,
    scenario_index_pct: Decimal,
    references: Mapping[str, Decimal | None],
    issues: Mapping[str, str],
    mode: str,
) -> tuple[MonitorCondition, ...]:
    previous = {s.symbol: s for s in states}
    hypothetical = {s.symbol: s for s in day.states}
    conditions = []
    for candidate in current.candidates:
        symbol = candidate.symbol
        row = next(
            (r for r in candidate.daily_returns if r.trade_date == current.trade_date),
            RegulationDailyReturn(current.trade_date, None, None, None, None, None, None, None),
        )
        branch = "NO_NEW_RESET"
        events = [e for e in day.events if e.symbol == symbol]
        if any(e.level is RegulationRuleLevel.SERIOUS_ABNORMAL for e in events):
            branch = f"{mode}_SERIOUS_RESET"
        elif any(e.kind is RegulationRuleKind.CUMULATIVE_DEVIATION for e in events):
            branch = f"{mode}_ABNORMAL_RESET"
        for rule in current.active_rules:
            if rule.segment is not candidate.segment:
                continue
            today = _threshold(
                current,
                candidate,
                rule,
                previous.get(symbol),
                current.trade_date,
                row.stock_reference_previous_close,
                row.benchmark_return * 100 if row.benchmark_return is not None else None,
                "PUBLISHED_PREVIOUS_CLOSE",
                issues.get(symbol),
            )
            tomorrow = _threshold(
                current,
                candidate,
                rule,
                hypothetical[symbol],
                current.next_trade_date,
                references.get(symbol),
                scenario_index_pct,
                branch,
                issues.get(symbol),
            )
            conditions.append(MonitorCondition(symbol, rule.rule_code, today, tomorrow))
    return tuple(conditions)
