"""Pure deterministic calculation for exchange regulation conditions."""

from dataclasses import dataclass, replace
from datetime import date
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from market_data_center.domain.regulation import (
    AnnouncedRegulationState,
    CalculatedRegulationState,
    RegulationApplicability,
    RegulationCalculationInput,
    RegulationCalculationOutput,
    RegulationCandidate,
    RegulationCoverage,
    RegulationDataCompleteness,
    RegulationDirection,
    RegulationEvaluationState,
    RegulationEventRecord,
    RegulationReachability,
    RegulationRule,
    RegulationRuleKind,
    RegulationRuleLevel,
    RegulationRuleResult,
    RegulationScenarioCode,
    RegulationStatusResult,
    RegulationWarningResult,
    regulation_event_natural_key,
)

_ONE = Decimal(1)
_HUNDRED = Decimal(100)
REGULATION_ALGORITHM_VERSION = "regulation-calculator.v1"
REGULATION_SCENARIO_CONFIG_VERSION = "regulation-scenarios.v1"
_SCENARIOS = {
    RegulationScenarioCode.INDEX_DOWN_2: Decimal("-0.02"),
    RegulationScenarioCode.INDEX_FLAT: Decimal(0),
    RegulationScenarioCode.INDEX_UP_2: Decimal("0.02"),
}
_DISCLAIMER = "本结果仅为公开规则条件测算,不构成价格预测;实际认定及监管措施以交易所公开信息为准。"


@dataclass(frozen=True, slots=True)
class _EvaluatedRule:
    result: RegulationRuleResult
    calculated_triggered: bool


def _selected_reset_date(candidate: RegulationCandidate, rule: RegulationRule) -> date | None:
    if rule.reset_level.value == "ABNORMAL":
        return candidate.abnormal_reset_date
    return candidate.serious_reset_date


def _eligible_dates(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    rule: RegulationRule,
) -> tuple[date, ...]:
    assert rule.window_days is not None
    dates = tuple(day for day in source.trading_dates if day <= source.trade_date)
    dates = dates[-rule.window_days :]
    reset_date = _selected_reset_date(candidate, rule)
    if reset_date is not None:
        dates = tuple(day for day in dates if day >= reset_date)
    return dates


def _incomplete_result(
    rule: RegulationRule,
    reason: str,
    *,
    selected_reset_date: date | None = None,
) -> _EvaluatedRule:
    return _EvaluatedRule(
        result=RegulationRuleResult(
            symbol="",
            rule_code=rule.rule_code,
            evaluation_state=RegulationEvaluationState.NOT_TRIGGERED,
            triggered=False,
            window_start_date=None,
            window_end_date=None,
            observed_window_days=None,
            current_value=None,
            threshold=rule.threshold_pct,
            distance=None,
            secondary_current_value=None,
            secondary_threshold=rule.secondary_threshold_pct,
            event_count=None,
            required_count=rule.required_count,
            selected_reset_date=selected_reset_date,
            data_completeness=RegulationDataCompleteness.INCOMPLETE,
            incomplete_reason=reason,
        ),
        calculated_triggered=False,
    )


def _evaluate_deviation(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    rule: RegulationRule,
) -> _EvaluatedRule:
    assert rule.threshold_pct is not None
    expected_dates = _eligible_dates(source, candidate, rule)
    rows = {row.trade_date: row for row in candidate.daily_returns}
    for expected_date in expected_dates:
        row = rows.get(expected_date)
        if row is None or row.stock_return is None or row.benchmark_return is None:
            evaluated = _incomplete_result(
                rule,
                f"missing_daily_return:{expected_date.isoformat()}",
                selected_reset_date=_selected_reset_date(candidate, rule),
            )
            return replace_symbol(evaluated, candidate.symbol)

    values: list[tuple[Decimal, date, int]] = []
    for length in range(1, len(expected_dates) + 1):
        window = expected_dates[-length:]
        stock_factor = _ONE
        benchmark_factor = _ONE
        for trade_date in window:
            row = rows[trade_date]
            assert row.stock_return is not None
            assert row.benchmark_return is not None
            stock_factor *= _ONE + row.stock_return
            benchmark_factor *= _ONE + row.benchmark_return
        deviation = (stock_factor - benchmark_factor) * _HUNDRED
        values.append((deviation, window[0], length))

    if rule.direction is RegulationDirection.UP:
        current_value, start_date, observed_days = max(values, key=lambda item: item[0])
        triggered = current_value >= rule.threshold_pct
        distance = max(rule.threshold_pct - current_value, Decimal(0))
    else:
        current_value, start_date, observed_days = min(values, key=lambda item: item[0])
        triggered = current_value <= rule.threshold_pct
        distance = max(current_value - rule.threshold_pct, Decimal(0))

    return _EvaluatedRule(
        result=RegulationRuleResult(
            symbol=candidate.symbol,
            rule_code=rule.rule_code,
            evaluation_state=(
                RegulationEvaluationState.TRIGGERED_CALCULATED
                if triggered
                else RegulationEvaluationState.NOT_TRIGGERED
            ),
            triggered=triggered,
            window_start_date=start_date,
            window_end_date=source.trade_date,
            observed_window_days=observed_days,
            current_value=current_value,
            threshold=rule.threshold_pct,
            distance=distance,
            secondary_current_value=None,
            secondary_threshold=None,
            event_count=None,
            required_count=None,
            selected_reset_date=_selected_reset_date(candidate, rule),
            data_completeness=RegulationDataCompleteness.COMPLETE,
            incomplete_reason=None,
        ),
        calculated_triggered=triggered,
    )


def _evaluate_turnover(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    rule: RegulationRule,
) -> _EvaluatedRule:
    assert rule.window_days is not None
    assert rule.comparison_window_days is not None
    assert rule.ratio_threshold is not None
    assert rule.secondary_threshold_pct is not None
    required_days = rule.window_days + rule.comparison_window_days
    dates = tuple(day for day in source.trading_dates if day <= source.trade_date)
    reset_date = _selected_reset_date(candidate, rule)
    if reset_date is not None:
        dates = tuple(day for day in dates if day >= reset_date)
    dates = dates[-required_days:]
    if len(dates) != required_days:
        return replace_symbol(
            _incomplete_result(
                rule,
                "insufficient_turnover_history",
                selected_reset_date=reset_date,
            ),
            candidate.symbol,
        )

    rows = {row.trade_date: row for row in candidate.daily_returns}
    observations: list[Decimal] = []
    for expected_date in dates:
        row = rows.get(expected_date)
        if row is None or row.turnover_rate_pct is None:
            return replace_symbol(
                _incomplete_result(
                    rule,
                    f"missing_turnover_rate:{expected_date.isoformat()}",
                    selected_reset_date=reset_date,
                ),
                candidate.symbol,
            )
        observations.append(row.turnover_rate_pct)

    prior = observations[: rule.comparison_window_days]
    latest = observations[rule.comparison_window_days :]
    prior_average = sum(prior, Decimal(0)) / Decimal(len(prior))
    if prior_average == 0:
        return replace_symbol(
            _incomplete_result(
                rule,
                "zero_prior_turnover_average",
                selected_reset_date=reset_date,
            ),
            candidate.symbol,
        )
    latest_sum = sum(latest, Decimal(0))
    latest_average = latest_sum / Decimal(len(latest))
    ratio = latest_average / prior_average
    triggered = ratio >= rule.ratio_threshold and latest_sum >= rule.secondary_threshold_pct
    distance = Decimal(0) if triggered else max(rule.ratio_threshold - ratio, Decimal(0))
    return _EvaluatedRule(
        result=RegulationRuleResult(
            symbol=candidate.symbol,
            rule_code=rule.rule_code,
            evaluation_state=(
                RegulationEvaluationState.TRIGGERED_CALCULATED
                if triggered
                else RegulationEvaluationState.NOT_TRIGGERED
            ),
            triggered=triggered,
            window_start_date=dates[-rule.window_days],
            window_end_date=source.trade_date,
            observed_window_days=rule.window_days,
            current_value=ratio,
            threshold=rule.ratio_threshold,
            distance=distance,
            secondary_current_value=latest_sum,
            secondary_threshold=rule.secondary_threshold_pct,
            event_count=None,
            required_count=None,
            selected_reset_date=reset_date,
            data_completeness=RegulationDataCompleteness.COMPLETE,
            incomplete_reason=None,
        ),
        calculated_triggered=triggered,
    )


def _is_price_deviation_event(event: RegulationEventRecord) -> bool:
    return any("_DEV_" in code for code in event.explicit_rule_codes)


def _events_in_count_window(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    *,
    window_days: int = 10,
) -> tuple[RegulationEventRecord, ...]:
    dates = tuple(day for day in source.trading_dates if day <= source.trade_date)
    valid_dates = set(dates[-window_days:])
    reset_date = candidate.serious_reset_date
    by_key: dict[tuple[str, str], RegulationEventRecord] = {}
    for event in candidate.events:
        if (
            event.observed_at <= source.event_watermark
            and event.period_end_date in valid_dates
            and (reset_date is None or event.period_end_date >= reset_date)
            and event.event_level is RegulationRuleLevel.ABNORMAL
            and _is_price_deviation_event(event)
        ):
            by_key[regulation_event_natural_key(event)] = event
    return tuple(by_key.values())


def _evaluate_event_count(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    rule: RegulationRule,
) -> _EvaluatedRule:
    assert rule.count_window_days is not None
    assert rule.required_count is not None
    events = _events_in_count_window(source, candidate, window_days=rule.count_window_days)
    count = sum(event.direction is rule.direction for event in events)
    triggered = count >= rule.required_count
    distance = Decimal(0) if triggered else Decimal(rule.required_count - count)
    return _EvaluatedRule(
        result=RegulationRuleResult(
            symbol=candidate.symbol,
            rule_code=rule.rule_code,
            evaluation_state=(
                RegulationEvaluationState.TRIGGERED_CALCULATED
                if triggered
                else RegulationEvaluationState.NOT_TRIGGERED
            ),
            triggered=triggered,
            window_start_date=None,
            window_end_date=None,
            observed_window_days=None,
            current_value=Decimal(count),
            threshold=Decimal(rule.required_count),
            distance=distance,
            secondary_current_value=None,
            secondary_threshold=None,
            event_count=count,
            required_count=rule.required_count,
            selected_reset_date=candidate.serious_reset_date,
            data_completeness=RegulationDataCompleteness.COMPLETE,
            incomplete_reason=None,
        ),
        calculated_triggered=triggered,
    )


def replace_symbol(evaluated: _EvaluatedRule, symbol: str) -> _EvaluatedRule:
    result = evaluated.result
    return _EvaluatedRule(
        result=RegulationRuleResult(
            symbol=symbol,
            rule_code=result.rule_code,
            evaluation_state=result.evaluation_state,
            triggered=result.triggered,
            window_start_date=result.window_start_date,
            window_end_date=result.window_end_date,
            observed_window_days=result.observed_window_days,
            current_value=result.current_value,
            threshold=result.threshold,
            distance=result.distance,
            secondary_current_value=result.secondary_current_value,
            secondary_threshold=result.secondary_threshold,
            event_count=result.event_count,
            required_count=result.required_count,
            selected_reset_date=result.selected_reset_date,
            data_completeness=result.data_completeness,
            incomplete_reason=result.incomplete_reason,
        ),
        calculated_triggered=evaluated.calculated_triggered,
    )


def _announcement_state(
    source: RegulationCalculationInput, candidate: RegulationCandidate
) -> AnnouncedRegulationState:
    events = tuple(
        event
        for event in candidate.events
        if event.observed_at <= source.event_watermark
        and event.period_end_date <= source.trade_date
    )
    if any(event.event_level is RegulationRuleLevel.SERIOUS_ABNORMAL for event in events):
        return AnnouncedRegulationState.SERIOUS_ABNORMAL
    if events:
        return AnnouncedRegulationState.ABNORMAL
    return AnnouncedRegulationState.NONE


def _status(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    evaluated: tuple[_EvaluatedRule, ...],
) -> RegulationStatusResult:
    target = next(
        (row for row in candidate.daily_returns if row.trade_date == source.trade_date), None
    )
    serious = any(
        item.calculated_triggered and rule.level is RegulationRuleLevel.SERIOUS_ABNORMAL
        for item, rule in zip(
            evaluated,
            tuple(rule for rule in source.active_rules if rule.segment is candidate.segment),
            strict=True,
        )
    )
    abnormal = any(item.calculated_triggered for item in evaluated)
    state = (
        CalculatedRegulationState.SERIOUS_TRIGGERED
        if serious
        else CalculatedRegulationState.ABNORMAL_TRIGGERED
        if abnormal
        else CalculatedRegulationState.NORMAL
    )
    status_applicability = candidate.applicability
    status_reason = candidate.applicability_reason
    completeness = {
        RegulationApplicability.APPLICABLE: RegulationDataCompleteness.COMPLETE,
        RegulationApplicability.INSUFFICIENT_DATA: RegulationDataCompleteness.INCOMPLETE,
        RegulationApplicability.NOT_APPLICABLE: RegulationDataCompleteness.NOT_APPLICABLE,
    }[candidate.applicability]
    incomplete_result = next(
        (
            item.result
            for item in evaluated
            if item.result.data_completeness is RegulationDataCompleteness.INCOMPLETE
        ),
        None,
    )
    if incomplete_result is not None:
        completeness = RegulationDataCompleteness.INCOMPLETE
        if candidate.applicability is RegulationApplicability.APPLICABLE:
            status_applicability = RegulationApplicability.INSUFFICIENT_DATA
            status_reason = incomplete_result.incomplete_reason
    stock_return_pct = (
        target.stock_return * _HUNDRED
        if target is not None and target.stock_return is not None
        else None
    )
    benchmark_return_pct = (
        target.benchmark_return * _HUNDRED
        if target is not None and target.benchmark_return is not None
        else None
    )
    daily_deviation = (
        stock_return_pct - benchmark_return_pct
        if stock_return_pct is not None and benchmark_return_pct is not None
        else None
    )
    counted_events = _events_in_count_window(source, candidate)
    up_count = sum(event.direction is RegulationDirection.UP for event in counted_events)
    down_count = sum(event.direction is RegulationDirection.DOWN for event in counted_events)
    return RegulationStatusResult(
        trade_date=source.trade_date,
        symbol=candidate.symbol,
        exchange=candidate.exchange,
        segment=candidate.segment,
        applicability=status_applicability,
        applicability_reason=status_reason,
        data_completeness=completeness,
        calculated_state=state,
        announced_state=_announcement_state(source, candidate),
        close=target.stock_close if target is not None else None,
        stock_daily_return_pct=stock_return_pct,
        benchmark_symbol=next(
            (
                rule.benchmark_symbol
                for rule in source.active_rules
                if rule.segment is candidate.segment and rule.benchmark_symbol is not None
            ),
            None,
        ),
        benchmark_close=target.benchmark_close if target is not None else None,
        benchmark_daily_return_pct=benchmark_return_pct,
        daily_deviation_pct=daily_deviation,
        abnormal_count_10d=up_count + down_count,
        abnormal_count_10d_up=up_count,
        abnormal_count_10d_down=down_count,
        abnormal_reset_date=candidate.abnormal_reset_date,
        serious_reset_date=candidate.serious_reset_date,
    )


def _current_warning(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    rule: RegulationRule,
    result: RegulationRuleResult,
) -> RegulationWarningResult:
    return RegulationWarningResult(
        trade_date=source.trade_date,
        next_trade_date=source.next_trade_date,
        symbol=candidate.symbol,
        rule_code=rule.rule_code,
        level=rule.level,
        direction=rule.direction,
        current_value=result.current_value,
        threshold=result.threshold,
        distance=result.distance,
        scenario_code=RegulationScenarioCode.CURRENT,
        scenario_index_pct=None,
        next_day_reference_price=candidate.next_day_reference_price,
        raw_trigger_price=None,
        next_day_trigger_price=None,
        next_day_trigger_pct=None,
        price_limit_ratio=(
            candidate.next_day_price_limit.limit_ratio
            if candidate.next_day_price_limit is not None
            else None
        ),
        lower_limit_price=(
            candidate.next_day_price_limit.lower_limit
            if candidate.next_day_price_limit is not None
            else None
        ),
        upper_limit_price=(
            candidate.next_day_price_limit.upper_limit
            if candidate.next_day_price_limit is not None
            else None
        ),
        reachability=RegulationReachability.CURRENT,
        window_start_date=result.window_start_date,
        window_end_date=result.window_end_date,
        requires_official_event_confirmation=False,
        message_template_code="REGULATION_CONDITION_CURRENT_V1",
        message=f"当前已满足{rule.rule_code}的计算条件。{_DISCLAIMER}",
    )


def _not_price_calculable_warning(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    rule: RegulationRule,
    result: RegulationRuleResult,
) -> RegulationWarningResult:
    return RegulationWarningResult(
        trade_date=source.trade_date,
        next_trade_date=source.next_trade_date,
        symbol=candidate.symbol,
        rule_code=rule.rule_code,
        level=rule.level,
        direction=rule.direction,
        current_value=result.current_value,
        threshold=result.threshold,
        distance=result.distance,
        scenario_code=RegulationScenarioCode.NONE,
        scenario_index_pct=None,
        next_day_reference_price=candidate.next_day_reference_price,
        raw_trigger_price=None,
        next_day_trigger_price=None,
        next_day_trigger_pct=None,
        price_limit_ratio=(
            candidate.next_day_price_limit.limit_ratio
            if candidate.next_day_price_limit is not None
            else None
        ),
        lower_limit_price=(
            candidate.next_day_price_limit.lower_limit
            if candidate.next_day_price_limit is not None
            else None
        ),
        upper_limit_price=(
            candidate.next_day_price_limit.upper_limit
            if candidate.next_day_price_limit is not None
            else None
        ),
        reachability=RegulationReachability.NOT_PRICE_CALCULABLE,
        window_start_date=result.window_start_date,
        window_end_date=result.window_end_date,
        requires_official_event_confirmation=False,
        message_template_code="REGULATION_NOT_PRICE_CALCULABLE_V1",
        message=f"{rule.rule_code}依赖成交数据,无法仅由收盘价反解。{_DISCLAIMER}",
    )


def _round_to_tick(value: Decimal, tick: Decimal, direction: RegulationDirection) -> Decimal:
    rounding = ROUND_CEILING if direction is RegulationDirection.UP else ROUND_FLOOR
    return (value / tick).to_integral_value(rounding=rounding) * tick


def _solve_deviation_scenarios(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    rule: RegulationRule,
    result: RegulationRuleResult,
) -> tuple[RegulationWarningResult, ...]:
    assert rule.window_days is not None
    assert rule.threshold_pct is not None
    price_limit = candidate.next_day_price_limit
    reference_price = candidate.next_day_reference_price
    if price_limit is None or reference_price is None:
        return ()

    historical_dates = tuple(day for day in source.trading_dates if day <= source.trade_date)[
        -(rule.window_days - 1) :
    ]
    reset_date = _selected_reset_date(candidate, rule)
    if reset_date is not None:
        historical_dates = tuple(day for day in historical_dates if day >= reset_date)
    tomorrow_dates = (*historical_dates, source.next_trade_date)
    rows = {row.trade_date: row for row in candidate.daily_returns}
    tau = rule.threshold_pct / _HUNDRED
    warnings: list[RegulationWarningResult] = []

    for scenario_code, scenario_return in _SCENARIOS.items():
        solutions: list[tuple[Decimal, date, Decimal, Decimal]] = []
        for start_index, start_date in enumerate(tomorrow_dates):
            stock_factor = _ONE
            benchmark_factor = _ONE
            complete = True
            for historical_date in tomorrow_dates[start_index:-1]:
                row = rows.get(historical_date)
                if row is None or row.stock_return is None or row.benchmark_return is None:
                    complete = False
                    break
                stock_factor *= _ONE + row.stock_return
                benchmark_factor *= _ONE + row.benchmark_return
            if not complete:
                continue
            projected_benchmark_factor = benchmark_factor * (_ONE + scenario_return)
            required_return = (tau + projected_benchmark_factor) / stock_factor - _ONE
            raw_price = reference_price * (_ONE + required_return)
            if raw_price > 0:
                solutions.append((required_return, start_date, raw_price, stock_factor))

        if not solutions:
            continue
        if rule.direction is RegulationDirection.UP:
            required_return, start_date, raw_price, _ = min(
                solutions, key=lambda item: (item[0], item[1])
            )
        else:
            required_return, start_date, raw_price, _ = max(
                solutions, key=lambda item: (item[0], item[1])
            )
        trigger_price = _round_to_tick(raw_price, price_limit.price_tick, rule.direction)
        rounded_return = trigger_price / reference_price - _ONE
        reachable = (
            trigger_price <= price_limit.upper_limit
            if rule.direction is RegulationDirection.UP
            else trigger_price >= price_limit.lower_limit
        )
        warnings.append(
            RegulationWarningResult(
                trade_date=source.trade_date,
                next_trade_date=source.next_trade_date,
                symbol=candidate.symbol,
                rule_code=rule.rule_code,
                level=rule.level,
                direction=rule.direction,
                current_value=result.current_value,
                threshold=result.threshold,
                distance=result.distance,
                scenario_code=scenario_code,
                scenario_index_pct=scenario_return * _HUNDRED,
                next_day_reference_price=reference_price,
                raw_trigger_price=raw_price,
                next_day_trigger_price=trigger_price,
                next_day_trigger_pct=rounded_return * _HUNDRED,
                price_limit_ratio=price_limit.limit_ratio,
                lower_limit_price=price_limit.lower_limit,
                upper_limit_price=price_limit.upper_limit,
                reachability=(
                    RegulationReachability.REACHABLE_NEXT_SESSION
                    if reachable
                    else RegulationReachability.NOT_REACHABLE_NEXT_SESSION
                ),
                window_start_date=start_date,
                window_end_date=source.next_trade_date,
                requires_official_event_confirmation=False,
                message_template_code="REGULATION_NEXT_SESSION_PRICE_V1",
                message=(
                    f"指数情景{scenario_code.value}下,下一交易日收盘价达到"
                    f"{trigger_price}元时可能满足{rule.rule_code}条件。{_DISCLAIMER}"
                ),
            )
        )
    return tuple(warnings)


def _candidate_warnings(
    source: RegulationCalculationInput,
    candidate: RegulationCandidate,
    rules: tuple[RegulationRule, ...],
    evaluated: tuple[_EvaluatedRule, ...],
) -> tuple[RegulationWarningResult, ...]:
    warnings: list[RegulationWarningResult] = []
    deviation_rules = {
        rule.direction: (rule, item.result)
        for rule, item in zip(rules, evaluated, strict=True)
        if rule.kind is RegulationRuleKind.CUMULATIVE_DEVIATION
        and rule.level is RegulationRuleLevel.ABNORMAL
        and item.result.data_completeness is RegulationDataCompleteness.COMPLETE
    }
    for rule, item in zip(rules, evaluated, strict=True):
        result = item.result
        if result.data_completeness is not RegulationDataCompleteness.COMPLETE:
            continue
        if item.calculated_triggered:
            warnings.append(_current_warning(source, candidate, rule, result))
        elif rule.kind is RegulationRuleKind.CUMULATIVE_DEVIATION:
            warnings.extend(_solve_deviation_scenarios(source, candidate, rule, result))
        elif rule.kind is RegulationRuleKind.TURNOVER_COMPOSITE:
            warnings.append(_not_price_calculable_warning(source, candidate, rule, result))
        elif (
            rule.kind is RegulationRuleKind.EVENT_COUNT
            and result.event_count is not None
            and result.required_count is not None
            and result.event_count == result.required_count - 1
            and rule.direction in deviation_rules
        ):
            abnormal_rule, abnormal_result = deviation_rules[rule.direction]
            for warning in _solve_deviation_scenarios(
                source, candidate, abnormal_rule, abnormal_result
            ):
                warnings.append(
                    replace(
                        warning,
                        rule_code=rule.rule_code,
                        level=rule.level,
                        current_value=result.current_value,
                        threshold=result.threshold,
                        distance=result.distance,
                        requires_official_event_confirmation=True,
                        message_template_code="REGULATION_EVENT_COUNT_PATH_V1",
                        message=(f"{warning.message} 该次数路径仍须交易所正式认定后方可计入。"),
                    )
                )

    current = tuple(
        warning for warning in warnings if warning.scenario_code is RegulationScenarioCode.CURRENT
    )
    projected = (
        warning
        for warning in warnings
        if warning.scenario_code is not RegulationScenarioCode.CURRENT
    )
    dominant: dict[
        tuple[RegulationDirection, RegulationRuleLevel, RegulationScenarioCode],
        RegulationWarningResult,
    ] = {}
    for warning in projected:
        key = (warning.direction, warning.level, warning.scenario_code)
        existing = dominant.get(key)
        candidate_key = (
            abs(warning.next_day_trigger_pct)
            if warning.next_day_trigger_pct is not None
            else Decimal("Infinity"),
            warning.rule_code,
        )
        existing_key = (
            abs(existing.next_day_trigger_pct)
            if existing is not None and existing.next_day_trigger_pct is not None
            else Decimal("Infinity"),
            existing.rule_code if existing is not None else "",
        )
        if existing is None or candidate_key < existing_key:
            dominant[key] = warning
    return current + tuple(
        dominant[key]
        for key in sorted(
            dominant,
            key=lambda item: (item[0].value, item[1].value, item[2].value),
        )
    )


def calculate_regulation(
    source: RegulationCalculationInput,
) -> RegulationCalculationOutput:
    """Evaluate all configured rules without I/O or ambient state."""

    statuses: list[RegulationStatusResult] = []
    rule_results: list[RegulationRuleResult] = []
    warnings: list[RegulationWarningResult] = []
    complete = incomplete = not_applicable = 0
    findings: list[str] = []

    for candidate in source.candidates:
        rules = tuple(rule for rule in source.active_rules if rule.segment is candidate.segment)
        evaluated: list[_EvaluatedRule] = []
        for rule in rules:
            if candidate.applicability is not RegulationApplicability.APPLICABLE:
                data_completeness = (
                    RegulationDataCompleteness.NOT_APPLICABLE
                    if candidate.applicability is RegulationApplicability.NOT_APPLICABLE
                    else RegulationDataCompleteness.INCOMPLETE
                )
                item = _incomplete_result(
                    rule,
                    candidate.applicability_reason or candidate.applicability.value,
                )
                item = replace_symbol(item, candidate.symbol)
                if data_completeness is RegulationDataCompleteness.NOT_APPLICABLE:
                    result = item.result
                    item = _EvaluatedRule(
                        result=RegulationRuleResult(
                            symbol=result.symbol,
                            rule_code=result.rule_code,
                            evaluation_state=result.evaluation_state,
                            triggered=result.triggered,
                            window_start_date=None,
                            window_end_date=None,
                            observed_window_days=None,
                            current_value=None,
                            threshold=result.threshold,
                            distance=None,
                            secondary_current_value=None,
                            secondary_threshold=result.secondary_threshold,
                            event_count=None,
                            required_count=result.required_count,
                            selected_reset_date=None,
                            data_completeness=data_completeness,
                            incomplete_reason=candidate.applicability_reason,
                        ),
                        calculated_triggered=False,
                    )
            elif rule.kind is RegulationRuleKind.CUMULATIVE_DEVIATION:
                item = _evaluate_deviation(source, candidate, rule)
            elif rule.kind is RegulationRuleKind.TURNOVER_COMPOSITE:
                item = _evaluate_turnover(source, candidate, rule)
            else:
                item = _evaluate_event_count(source, candidate, rule)
            evaluated.append(item)
            rule_results.append(item.result)

        status = _status(source, candidate, tuple(evaluated))
        statuses.append(status)
        if candidate.applicability is RegulationApplicability.APPLICABLE:
            warnings.extend(_candidate_warnings(source, candidate, rules, tuple(evaluated)))
        if status.data_completeness is RegulationDataCompleteness.COMPLETE:
            complete += 1
        elif status.data_completeness is RegulationDataCompleteness.INCOMPLETE:
            incomplete += 1
            findings.append(f"{candidate.symbol}:incomplete")
        else:
            not_applicable += 1

    coverage = RegulationCoverage(
        expected_count=len(source.candidates),
        complete_count=complete,
        incomplete_count=incomplete,
        not_applicable_count=not_applicable,
    )
    return RegulationCalculationOutput(
        trade_date=source.trade_date,
        next_trade_date=source.next_trade_date,
        statuses=tuple(statuses),
        rule_results=tuple(rule_results),
        warnings=tuple(warnings),
        coverage=coverage,
        quality_findings=tuple(findings),
    )
