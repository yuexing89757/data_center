"""Calculated close events are independent of official announcement records."""

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

import pytest
from test_regulation_calculator import _candidate, _daily, _event, _input, _rule

from market_data_center.domain.records import Exchange
from market_data_center.domain.regulation import (
    CalculatedEvent,
    MonitorState,
    RegulationDataCompleteness,
)
from market_data_center.domain.regulation import (
    RegulationDirection as Direction,
)
from market_data_center.domain.regulation import (
    RegulationResetLevel as Reset,
)
from market_data_center.domain.regulation import (
    RegulationRuleKind as Kind,
)
from market_data_center.domain.regulation import (
    RegulationRuleLevel as Level,
)
from market_data_center.domain.regulation import (
    RegulationSegment as Segment,
)
from market_data_center.regulation_calculator import calculate_monitor_day

START = date(2026, 7, 6)
DAYS = tuple(
    START + timedelta(days=n) for n in range(45) if (START + timedelta(days=n)).weekday() < 5
)


def source(returns, rules=None, *, days=None, turnovers=None, gem=False):
    days = days or DAYS[: len(returns)]
    rules = rules or (_rule(),)
    rows = tuple(
        _daily(day, str(value), "0", turnover=str(turnovers[i] if turnovers else 1))
        for i, (day, value) in enumerate(zip(days, returns, strict=True))
    )
    candidate = _candidate(rows)
    if gem:
        candidate = replace(
            candidate,
            symbol="SZSE:300001",
            exchange=Exchange.SZSE,
            segment=Segment.GEM,
            next_day_price_limit=None,
        )
        rules = tuple(
            replace(
                rule,
                exchange=Exchange.SZSE,
                segment=Segment.GEM,
                benchmark_symbol="SZSE:399102" if rule.benchmark_symbol else None,
            )
            for rule in rules
        )
    return replace(
        _input(rules, candidate),
        trade_date=days[-1],
        next_trade_date=next(day for day in DAYS if day > days[-1]),
        trading_dates=tuple(day for day in DAYS if day <= days[-1]),
        algorithm_version="regulation-monitor.v1",
    )


def checkpoint(src, events=(), **changes):
    previous = max(day for day in src.trading_dates if day < src.trade_date)
    return MonitorState(src.candidates[0].symbol, previous, True, None, None, events, **changes)


def price_event(day, direction=Direction.UP, level=Level.ABNORMAL, kind=Kind.CUMULATIVE_DEVIATION):
    return CalculatedEvent("SSE:600000", day, level, direction, kind, ("rule",), ((day, day),))


def count_rule(required=4, direction=Direction.UP):
    return _rule(
        rule_code="SSE_MAIN_SERIOUS_10D_COUNT_" + direction.value,
        level=Level.SERIOUS_ABNORMAL,
        kind=Kind.EVENT_COUNT,
        direction=direction,
        window_days=None,
        threshold_pct=None,
        count_window_days=10,
        required_count=required,
        counted_event_kind="PRICE_DEVIATION_ABNORMAL",
        reset_level=Reset.SERIOUS_ABNORMAL,
        benchmark_symbol=None,
    )


@pytest.mark.parametrize(
    "threshold,window,gem",
    [
        ("20", 3, False),
        ("-20", 3, False),
        ("30", 3, True),
        ("-30", 3, True),
        ("100", 10, False),
        ("-50", 10, False),
        ("200", 30, False),
        ("-70", 30, False),
    ],
)
@pytest.mark.parametrize("delta", ["-0.000001", "0", "0.000001"])
def test_threshold_boundary(threshold, window, gem, delta):
    target = Decimal(threshold)
    direction = Direction.UP if target > 0 else Direction.DOWN
    rule = _rule(
        direction=direction,
        threshold_pct=target,
        window_days=window,
        level=Level.ABNORMAL if window == 3 else Level.SERIOUS_ABNORMAL,
    )
    src = source([Decimal(0), (target + Decimal(delta)) / 100], (rule,), gem=gem)
    result = calculate_monitor_day(src, (checkpoint(src, missing_reasons=()),))
    expected = Decimal(delta) >= 0 if target > 0 else Decimal(delta) <= 0
    assert result.output.rule_results[0].triggered is expected
    assert len(result.events) == int(expected)


def test_missing_checkpoint_never_becomes_zero_or_generates_events():
    src = source([0, ".1", ".1"])
    result = calculate_monitor_day(src, ())
    assert not result.states[0].complete
    assert "missing_continuous_checkpoint" in result.states[0].missing_reasons
    assert result.events == ()
    assert result.output.rule_results[0].data_completeness == RegulationDataCompleteness.INCOMPLETE


def test_first_effective_session_can_start_a_complete_chain():
    result = calculate_monitor_day(source([0]), ())
    assert result.states[0].complete
    assert result.events == ()


def test_compound_window_deduplicates_and_resets_both_price_directions():
    src = source([0, ".1", ".1"])
    result = calculate_monitor_day(src, (checkpoint(src, missing_reasons=()),))
    assert result.output.rule_results[0].current_value == Decimal("21")
    assert len(result.events) == 1
    assert result.states[0].abnormal_reset_date == DAYS[3]
    tomorrow = calculate_monitor_day(source([0, ".1", ".1", ".1"]), result.states)
    assert tomorrow.events == ()
    assert tomorrow.output.rule_results[0].current_value == Decimal("10")
    assert len(tomorrow.states[0].events) == 1


@pytest.mark.parametrize("required,gem", [(4, False), (3, True)])
def test_todays_event_is_counted_before_serious_reset(required, gem):
    rules = (_rule(threshold_pct=Decimal("30") if gem else Decimal("20")), count_rule(required))
    src = source([0] * 5 + [".3"], rules, gem=gem)
    events = tuple(
        replace(price_event(day), symbol=src.candidates[0].symbol) for day in DAYS[1:required]
    )
    result = calculate_monitor_day(src, (checkpoint(src, events, missing_reasons=()),))
    assert result.output.rule_results[1].event_count == required
    assert result.output.rule_results[1].triggered
    assert result.states[0].serious_reset_date == DAYS[6]
    assert result.states[0].abnormal_reset_date == DAYS[6]
    assert len([e for e in result.states[0].events if e.level == Level.ABNORMAL]) == required


def test_count_excludes_opposite_turnover_expired_and_pre_reset_events():
    src = source([0] * 11, (count_rule(),))
    events = (
        price_event(DAYS[0]),
        price_event(DAYS[1]),
        price_event(DAYS[2], Direction.DOWN),
        price_event(DAYS[3], Direction.NONE, kind=Kind.TURNOVER_COMPOSITE),
        price_event(DAYS[9]),
    )
    state = replace(checkpoint(src, events, missing_reasons=()), serious_reset_date=DAYS[5])
    result = calculate_monitor_day(src, (state,))
    assert result.output.rule_results[0].event_count == 1
    assert len(result.states[0].events) == 4  # historical counts retain pre-reset events
    assert DAYS[0] not in [e.trade_date for e in result.states[0].events]


def test_unknown_history_and_wrong_checkpoint_cannot_heal_themselves():
    src = source([0, ".3"])
    bad = replace(
        checkpoint(src, missing_reasons=()), complete=False, missing_reasons=("history_gap",)
    )
    assert not calculate_monitor_day(src, (bad,)).states[0].complete
    with pytest.raises(ValueError, match="checkpoint"):
        calculate_monitor_day(src, (replace(bad, through_date=src.trade_date),))


def test_official_events_do_not_enter_computed_counts_or_resets():
    src = source([0, 0], (count_rule(),))
    original = src.candidates[0]
    src = replace(src, candidates=(replace(original, events=(_event("official", DAYS[0]),)),))
    result = calculate_monitor_day(src, (checkpoint(src, missing_reasons=()),))
    assert result.output.rule_results[0].event_count == 0
    assert src.candidates[0].events[0].source_event_id == "official"
    assert result.events == ()


@pytest.mark.parametrize(
    "prior,latest,expected",
    [
        (".3", (9, 9, 9), True),
        (".2", (6, 7, 7), True),
        (".31", (9, 9, 9), False),
        (".1", (6, 6, 6), False),
        ("0", (9, 9, 9), False),
    ],
)
def test_turnover_requires_both_conditions_and_does_not_reset_prices(prior, latest, expected):
    rule = _rule(
        rule_code="SSE_MAIN_ABNORMAL_TURNOVER",
        kind=Kind.TURNOVER_COMPOSITE,
        direction=Direction.NONE,
        threshold_pct=None,
        window_days=3,
        comparison_window_days=5,
        ratio_threshold=Decimal(30),
        secondary_threshold_pct=Decimal(20),
        benchmark_symbol=None,
    )
    src = source([0] * 8, (rule,), turnovers=[prior] * 5 + list(latest))
    result = calculate_monitor_day(src, (checkpoint(src, missing_reasons=()),))
    assert result.output.rule_results[0].triggered is expected
    assert result.states[0].abnormal_reset_date is None
    assert all(e.kind == Kind.TURNOVER_COMPOSITE for e in result.events)


def test_price_gap_poisoning_prevents_unreliable_continuation():
    src = source([0, ".1", ".1"])
    rows = src.candidates[0].daily_returns
    src = replace(src, candidates=(replace(src.candidates[0], daily_returns=(rows[0], rows[2])),))
    result = calculate_monitor_day(src, (checkpoint(src, missing_reasons=()),))
    assert not result.states[0].complete
    assert not result.events


def test_serious_price_and_count_rules_merge_then_next_session_resets():
    serious = _rule(
        rule_code="SSE_MAIN_SERIOUS_10D_DEV_UP",
        window_days=10,
        threshold_pct=Decimal(100),
        level=Level.SERIOUS_ABNORMAL,
        reset_level=Reset.SERIOUS_ABNORMAL,
    )
    rules = (_rule(), serious, count_rule())
    src = source([0] * 5 + [1], rules)
    prior = checkpoint(src, tuple(price_event(d) for d in DAYS[1:4]), missing_reasons=())
    result = calculate_monitor_day(src, (prior,))
    assert len(result.events) == 2
    assert result.events[1].rule_codes == (
        "SSE_MAIN_SERIOUS_10D_COUNT_UP",
        "SSE_MAIN_SERIOUS_10D_DEV_UP",
    )
    tomorrow = calculate_monitor_day(source([0] * 5 + [1, 0], rules), result.states)
    assert tomorrow.output.rule_results[2].event_count == 0
    assert len([e for e in tomorrow.states[0].events if e.level == Level.ABNORMAL]) == 4
    assert tomorrow.events == ()


def test_repeat_is_deterministic_and_does_not_mutate_inputs():
    src = source([0, ".1", ".1"])
    prior = (checkpoint(src, missing_reasons=()),)
    first = calculate_monitor_day(src, prior)
    assert calculate_monitor_day(src, prior) == first
    assert prior[0].events == ()
    assert src.candidates[0].abnormal_reset_date is None
