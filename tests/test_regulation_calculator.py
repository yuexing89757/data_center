from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from market_data_center.domain.records import Exchange
from market_data_center.domain.regulation import (
    AnnouncedRegulationState,
    CalculatedRegulationState,
    RegulationApplicability,
    RegulationCalculationInput,
    RegulationCandidate,
    RegulationDailyReturn,
    RegulationDataCompleteness,
    RegulationDirection,
    RegulationEvaluationState,
    RegulationEventRecord,
    RegulationEventType,
    RegulationReachability,
    RegulationResetLevel,
    RegulationRule,
    RegulationRuleKind,
    RegulationRuleLevel,
    RegulationScenarioCode,
    RegulationSegment,
)
from market_data_center.domain.stock_pool import DailyPriceLimit
from market_data_center.regulation_calculator import calculate_regulation

D1 = date(2026, 8, 31)
D2 = date(2026, 9, 1)
D3 = date(2026, 9, 2)
D4 = date(2026, 9, 3)
DATES = (D1, D2, D3)
LONG_DATES = (
    date(2026, 8, 24),
    date(2026, 8, 25),
    date(2026, 8, 26),
    date(2026, 8, 27),
    date(2026, 8, 28),
    D1,
    D2,
    D3,
)
SHA256 = "a" * 64
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _rule(**changes: object) -> RegulationRule:
    values: dict[str, object] = {
        "rule_code": "SSE_MAIN_ABNORMAL_3D_DEV_UP",
        "exchange": Exchange.SSE,
        "segment": RegulationSegment.SSE_MAIN,
        "level": RegulationRuleLevel.ABNORMAL,
        "kind": RegulationRuleKind.CUMULATIVE_DEVIATION,
        "direction": RegulationDirection.UP,
        "window_days": 3,
        "threshold_pct": Decimal("20"),
        "comparison_window_days": None,
        "ratio_threshold": None,
        "secondary_threshold_pct": None,
        "count_window_days": None,
        "required_count": None,
        "counted_event_kind": None,
        "reset_level": RegulationResetLevel.ABNORMAL,
        "benchmark_symbol": "SSE:000002",
        "rule_set_version": "cn-a-share-regulation-2026-07-06.v1",
        "effective_date": date(2026, 7, 6),
        "expire_date": None,
        "source_document": "official",
        "source_clause": "5.4.2",
        "source_url": "https://www.sse.com.cn/rule",
        "enabled": True,
    }
    values.update(changes)
    return RegulationRule(**values)  # type: ignore[arg-type]


def _daily(
    trade_date: date,
    stock_return: str,
    benchmark_return: str,
    *,
    turnover: str = "1",
) -> RegulationDailyReturn:
    return RegulationDailyReturn(
        trade_date=trade_date,
        stock_close=Decimal("10"),
        stock_reference_previous_close=Decimal("9"),
        stock_return=Decimal(stock_return),
        benchmark_close=Decimal("100"),
        benchmark_previous_close=Decimal("99"),
        benchmark_return=Decimal(benchmark_return),
        turnover_rate_pct=Decimal(turnover),
    )


def _candidate(
    daily_returns: tuple[RegulationDailyReturn, ...],
    **changes: object,
) -> RegulationCandidate:
    values: dict[str, object] = {
        "symbol": "SSE:600000",
        "exchange": Exchange.SSE,
        "segment": RegulationSegment.SSE_MAIN,
        "applicability": RegulationApplicability.APPLICABLE,
        "applicability_reason": None,
        "daily_returns": daily_returns,
        "events": (),
        "abnormal_reset_date": None,
        "serious_reset_date": None,
        "next_day_reference_price": Decimal("10"),
        "next_day_price_limit": DailyPriceLimit(
            symbol="SSE:600000",
            trade_date=D4,
            previous_close=Decimal("10"),
            upper_limit=Decimal("11"),
            lower_limit=Decimal("9"),
            limit_ratio=Decimal("0.10"),
            price_tick=Decimal("0.01"),
            is_st=False,
            rule_version="cn-a-share-price-limit.v1",
            algorithm_version="price-limit.v1",
        ),
    }
    values.update(changes)
    return RegulationCandidate(**values)  # type: ignore[arg-type]


def _input(
    rules: tuple[RegulationRule, ...],
    candidate: RegulationCandidate,
    *,
    trading_dates: tuple[date, ...] = DATES,
) -> RegulationCalculationInput:
    return RegulationCalculationInput(
        trade_date=D3,
        next_trade_date=D4,
        algorithm_version="regulation-calculator.v1",
        scenario_config_version="regulation-scenarios.v1",
        active_rules=rules,
        trading_dates=trading_dates,
        candidates=(candidate,),
        rule_set_hash=SHA256,
        market_watermark="market-1",
        capital_watermark="capital-1",
        event_watermark=datetime(2026, 9, 2, 23, tzinfo=SHANGHAI),
    )


def _event(
    event_id: str,
    period_end_date: date,
    *,
    direction: RegulationDirection | None = RegulationDirection.UP,
    level: RegulationRuleLevel = RegulationRuleLevel.ABNORMAL,
    explicit_rule_codes: tuple[str, ...] = ("SSE_MAIN_ABNORMAL_3D_DEV_UP",),
) -> RegulationEventRecord:
    event_type = (
        RegulationEventType.ABNORMAL_VOLATILITY
        if level is RegulationRuleLevel.ABNORMAL
        else RegulationEventType.SERIOUS_ABNORMAL_VOLATILITY
    )
    return RegulationEventRecord(
        symbol="SSE:600000",
        exchange=Exchange.SSE,
        segment=RegulationSegment.SSE_MAIN,
        event_type=event_type,
        event_level=level,
        direction=direction,
        period_start_date=period_end_date,
        period_end_date=period_end_date,
        published_at=datetime(2026, 9, 2, 18, tzinfo=SHANGHAI),
        effective_reset_date=D4,
        source_event_id=event_id,
        source_title="official event",
        source_url=f"https://www.sse.com.cn/{event_id}",
        source_content_hash="a" * 64,
        source_code="sse_official",
        explicit_rule_codes=explicit_rule_codes,
        observed_at=datetime(2026, 9, 2, 19, tzinfo=SHANGHAI),
    )


def test_deviation_compounds_and_two_sessions_can_trigger_three_day_rule() -> None:
    candidate = _candidate(
        (
            _daily(D1, "0", "0"),
            _daily(D2, "0.10", "0"),
            _daily(D3, "0.10", "0"),
        )
    )

    output = calculate_regulation(_input((_rule(),), candidate))

    result = output.rule_results[0]
    assert result.current_value == Decimal("21.00")
    assert result.window_start_date == D2
    assert result.window_end_date == D3
    assert result.observed_window_days == 2
    assert result.triggered is True
    assert result.evaluation_state is RegulationEvaluationState.TRIGGERED_CALCULATED
    assert result.distance == Decimal("0")


def test_deviation_exact_equality_triggers_without_summing_daily_differences() -> None:
    candidate = _candidate(
        (
            _daily(D1, "0", "0"),
            _daily(D2, "0.21", "0.01"),
            _daily(D3, "0", "0"),
        )
    )

    result = calculate_regulation(_input((_rule(),), candidate)).rule_results[0]

    assert result.current_value == Decimal("20.00")
    assert result.triggered is True


def test_down_deviation_selects_minimum_window_and_respects_reset() -> None:
    rule = _rule(
        rule_code="SSE_MAIN_ABNORMAL_3D_DEV_DOWN",
        direction=RegulationDirection.DOWN,
        threshold_pct=Decimal("-20"),
    )
    candidate = _candidate(
        (
            _daily(D1, "-0.30", "0"),
            _daily(D2, "0.10", "0"),
            _daily(D3, "-0.20", "0"),
        ),
        abnormal_reset_date=D2,
    )

    result = calculate_regulation(_input((rule,), candidate)).rule_results[0]

    assert result.current_value == Decimal("-20.00")
    assert result.window_start_date == D3
    assert result.triggered is True
    assert result.selected_reset_date == D2


def test_deviation_gap_is_incomplete_instead_of_compressing_calendar() -> None:
    candidate = _candidate((_daily(D1, "0.30", "0"), _daily(D3, "0", "0")))

    result = calculate_regulation(_input((_rule(),), candidate)).rule_results[0]

    assert result.data_completeness is RegulationDataCompleteness.INCOMPLETE
    assert result.current_value is None
    assert result.triggered is False
    assert result.incomplete_reason == "missing_daily_return:2026-09-01"


def test_turnover_requires_both_ratio_and_latest_cumulative_thresholds() -> None:
    turnover_rule = _rule(
        rule_code="SSE_MAIN_ABNORMAL_TURNOVER",
        kind=RegulationRuleKind.TURNOVER_COMPOSITE,
        direction=RegulationDirection.NONE,
        threshold_pct=None,
        comparison_window_days=5,
        ratio_threshold=Decimal("30"),
        secondary_threshold_pct=Decimal("20"),
        benchmark_symbol=None,
    )
    turnovers = ("0.2", "0.2", "0.2", "0.2", "0.2", "7", "7", "7")
    rows = tuple(
        _daily(day, "0", "0", turnover=value)
        for day, value in zip(LONG_DATES, turnovers, strict=True)
    )

    result = calculate_regulation(
        _input((turnover_rule,), _candidate(rows), trading_dates=LONG_DATES)
    ).rule_results[0]

    assert result.current_value == Decimal("35")
    assert result.secondary_current_value == Decimal("21")
    assert result.triggered is True

    low_sum_rows = rows[:-3] + tuple(
        replace(row, turnover_rate_pct=Decimal("1")) for row in rows[-3:]
    )
    low_sum = calculate_regulation(
        _input((turnover_rule,), _candidate(low_sum_rows), trading_dates=LONG_DATES)
    ).rule_results[0]
    assert low_sum.current_value == Decimal("5")
    assert low_sum.secondary_current_value == Decimal("3")
    assert low_sum.triggered is False


def test_turnover_zero_prior_average_is_incomplete() -> None:
    turnover_rule = _rule(
        rule_code="SSE_MAIN_ABNORMAL_TURNOVER",
        kind=RegulationRuleKind.TURNOVER_COMPOSITE,
        direction=RegulationDirection.NONE,
        threshold_pct=None,
        comparison_window_days=5,
        ratio_threshold=Decimal("30"),
        secondary_threshold_pct=Decimal("20"),
        benchmark_symbol=None,
    )
    rows = tuple(
        _daily(day, "0", "0", turnover="0" if index < 5 else "7")
        for index, day in enumerate(LONG_DATES)
    )

    result = calculate_regulation(
        _input((turnover_rule,), _candidate(rows), trading_dates=LONG_DATES)
    ).rule_results[0]

    assert result.data_completeness is RegulationDataCompleteness.INCOMPLETE
    assert result.incomplete_reason == "zero_prior_turnover_average"


def test_event_count_uses_only_distinct_official_price_events_in_direction() -> None:
    count_rule = _rule(
        rule_code="SSE_MAIN_SERIOUS_10D_COUNT_UP",
        level=RegulationRuleLevel.SERIOUS_ABNORMAL,
        kind=RegulationRuleKind.EVENT_COUNT,
        direction=RegulationDirection.UP,
        window_days=None,
        threshold_pct=None,
        count_window_days=10,
        required_count=2,
        counted_event_kind="PRICE_DEVIATION_ABNORMAL",
        reset_level=RegulationResetLevel.SERIOUS_ABNORMAL,
        benchmark_symbol=None,
    )
    included = _event("a-event", D1)
    included_two = _event("b-event", D2)
    turnover = _event(
        "c-event",
        D2,
        explicit_rule_codes=("SSE_MAIN_ABNORMAL_TURNOVER",),
    )
    unknown = _event("d-event", D2, direction=None)
    down = _event("e-event", D2, direction=RegulationDirection.DOWN)
    candidate = _candidate(
        tuple(_daily(day, "0", "0") for day in LONG_DATES),
        events=(included, included, included_two, turnover, unknown, down),
        serious_reset_date=date(2026, 8, 28),
    )

    output = calculate_regulation(_input((count_rule,), candidate, trading_dates=LONG_DATES))
    result = output.rule_results[0]

    assert result.event_count == 2
    assert result.required_count == 2
    assert result.triggered is True
    assert output.statuses[0].abnormal_count_10d_up == 2
    assert output.statuses[0].abnormal_count_10d_down == 1


def test_calculated_and_announced_states_are_independent() -> None:
    serious_rule = _rule(
        rule_code="SSE_MAIN_SERIOUS_10D_DEV_UP",
        level=RegulationRuleLevel.SERIOUS_ABNORMAL,
        window_days=10,
        threshold_pct=Decimal("100"),
        reset_level=RegulationResetLevel.SERIOUS_ABNORMAL,
    )
    serious_candidate = _candidate(
        (
            _daily(D1, "0", "0"),
            _daily(D2, "1", "0"),
            _daily(D3, "0", "0"),
        )
    )
    serious_status = calculate_regulation(_input((serious_rule,), serious_candidate)).statuses[0]
    assert serious_status.calculated_state is CalculatedRegulationState.SERIOUS_TRIGGERED
    assert serious_status.announced_state is AnnouncedRegulationState.NONE

    announced_candidate = _candidate(
        tuple(_daily(day, "0", "0") for day in DATES),
        events=(_event("f-event", D3),),
    )
    announced_status = calculate_regulation(_input((_rule(),), announced_candidate)).statuses[0]
    assert announced_status.calculated_state is CalculatedRegulationState.NORMAL
    assert announced_status.announced_state is AnnouncedRegulationState.ABNORMAL


def test_t_plus_one_solver_reenumerates_windows_for_all_index_scenarios() -> None:
    candidate = _candidate(
        (
            _daily(D1, "0", "0"),
            _daily(D2, "0", "0"),
            _daily(D3, "0.10", "0"),
        )
    )

    output = calculate_regulation(_input((_rule(),), candidate))

    warnings = {warning.scenario_code: warning for warning in output.warnings}
    assert set(warnings) == {
        RegulationScenarioCode.INDEX_DOWN_2,
        RegulationScenarioCode.INDEX_FLAT,
        RegulationScenarioCode.INDEX_UP_2,
    }
    assert warnings[RegulationScenarioCode.INDEX_DOWN_2].next_day_trigger_price == Decimal("10.73")
    assert warnings[RegulationScenarioCode.INDEX_FLAT].next_day_trigger_price == Decimal("10.91")
    assert warnings[RegulationScenarioCode.INDEX_UP_2].next_day_trigger_price == Decimal("11.10")
    assert (
        warnings[RegulationScenarioCode.INDEX_FLAT].reachability
        is RegulationReachability.REACHABLE_NEXT_SESSION
    )
    assert (
        warnings[RegulationScenarioCode.INDEX_UP_2].reachability
        is RegulationReachability.NOT_REACHABLE_NEXT_SESSION
    )
    assert warnings[RegulationScenarioCode.INDEX_FLAT].window_end_date == D4


def test_down_trigger_price_rounds_down_and_checks_lower_limit() -> None:
    rule = _rule(
        rule_code="SSE_MAIN_ABNORMAL_3D_DEV_DOWN",
        direction=RegulationDirection.DOWN,
        threshold_pct=Decimal("-20"),
    )
    candidate = _candidate(
        (
            _daily(D1, "0", "0"),
            _daily(D2, "0", "0"),
            _daily(D3, "-0.10", "0"),
        )
    )

    output = calculate_regulation(_input((rule,), candidate))
    flat = next(
        warning
        for warning in output.warnings
        if warning.scenario_code is RegulationScenarioCode.INDEX_FLAT
    )

    assert flat.raw_trigger_price == Decimal("8.888888888888888888888888889")
    assert flat.next_day_trigger_price == Decimal("8.88")
    assert flat.next_day_trigger_pct == Decimal("-11.200")
    assert flat.reachability is RegulationReachability.NOT_REACHABLE_NEXT_SESSION


def test_current_trigger_uses_current_scenario_without_projected_price() -> None:
    candidate = _candidate(
        (
            _daily(D1, "0", "0"),
            _daily(D2, "0.10", "0"),
            _daily(D3, "0.10", "0"),
        )
    )

    warnings = calculate_regulation(_input((_rule(),), candidate)).warnings

    assert len(warnings) == 1
    assert warnings[0].scenario_code is RegulationScenarioCode.CURRENT
    assert warnings[0].reachability is RegulationReachability.CURRENT
    assert warnings[0].next_day_trigger_price is None


def test_turnover_warning_is_explicitly_not_price_calculable() -> None:
    turnover_rule = _rule(
        rule_code="SSE_MAIN_ABNORMAL_TURNOVER",
        kind=RegulationRuleKind.TURNOVER_COMPOSITE,
        direction=RegulationDirection.NONE,
        threshold_pct=None,
        comparison_window_days=5,
        ratio_threshold=Decimal("30"),
        secondary_threshold_pct=Decimal("20"),
        benchmark_symbol=None,
    )
    rows = tuple(_daily(day, "0", "0", turnover="1") for day in LONG_DATES)

    warning = calculate_regulation(
        _input((turnover_rule,), _candidate(rows), trading_dates=LONG_DATES)
    ).warnings[0]

    assert warning.scenario_code is RegulationScenarioCode.NONE
    assert warning.reachability is RegulationReachability.NOT_PRICE_CALCULABLE
    assert warning.next_day_trigger_price is None


def test_one_short_event_count_warning_requires_official_confirmation() -> None:
    abnormal_rule = _rule()
    count_rule = _rule(
        rule_code="SSE_MAIN_SERIOUS_10D_COUNT_UP",
        level=RegulationRuleLevel.SERIOUS_ABNORMAL,
        kind=RegulationRuleKind.EVENT_COUNT,
        direction=RegulationDirection.UP,
        window_days=None,
        threshold_pct=None,
        count_window_days=10,
        required_count=2,
        counted_event_kind="PRICE_DEVIATION_ABNORMAL",
        reset_level=RegulationResetLevel.SERIOUS_ABNORMAL,
        benchmark_symbol=None,
    )
    candidate = _candidate(
        tuple(_daily(day, "0", "0") for day in DATES),
        events=(_event("g-event", D1),),
    )

    warnings = calculate_regulation(_input((abnormal_rule, count_rule), candidate)).warnings
    count_warnings = tuple(
        warning for warning in warnings if warning.rule_code == count_rule.rule_code
    )

    assert len(count_warnings) == 3
    assert all(warning.requires_official_event_confirmation for warning in count_warnings)
    assert all("交易所正式认定" in warning.message for warning in count_warnings)
