from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from test_regulation_calculator import _rule
from test_regulation_monitor_calculator import checkpoint, count_rule, price_event, source

from market_data_center.domain.regulation import (
    RegulationDirection,
    RegulationRuleKind,
    RegulationRuleLevel,
)
from market_data_center.providers.regulation_quote import MonitorQuote
from market_data_center.regulation_monitor import project_monitor, round_trigger_price


def fixture(price="10.50"):
    src = source([".10", "0"])
    now = datetime.combine(src.trade_date, datetime.min.time(), ZoneInfo("Asia/Shanghai")).replace(
        hour=10
    )
    quotes = (
        MonitorQuote("SSE:600000", now, Decimal(price), Decimal("10"), None),
        MonitorQuote("SSE:000002", now, Decimal("100"), Decimal("100"), None),
    )
    return src, (checkpoint(src, missing_reasons=()),), quotes, now


def test_rounding_and_today_next_reference_are_not_mixed():
    assert round_trigger_price(Decimal("10.001"), RegulationDirection.UP) == Decimal("10.01")
    assert round_trigger_price(Decimal("10.009"), RegulationDirection.DOWN) == Decimal("10.00")
    src, states, quotes, now = fixture()
    result = project_monitor(
        src, states, quotes, Decimal(0), now, next_reference_safe_symbols=frozenset({"SSE:600000"})
    )
    row = result.conditions[0]
    assert row.today.trigger_price == Decimal("10.91")
    assert row.today.trigger_change_pct == Decimal("9.1")
    assert row.today.reference_price == Decimal("10")
    assert row.next_day.reference_price == Decimal("10.50")
    assert row.next_day.trigger_price == Decimal("10.91")
    assert row.next_day.trigger_change_pct == (Decimal("10.91") / Decimal("10.50") - 1) * 100
    assert states[0].events == ()  # hypothetical changes never mutate published counts


def test_hypothetical_trigger_resets_next_day_window():
    src, states, quotes, now = fixture("11.00")
    result = project_monitor(
        src, states, quotes, Decimal(0), now, next_reference_safe_symbols=frozenset({"SSE:600000"})
    )
    assert len(result.day.events) == 1
    assert result.conditions[0].next_day.trigger_price == Decimal("13.20")
    assert result.conditions[0].next_day.reachability == "NOT_REACHABLE"
    assert result.conditions[0].next_day.reset_branch == "HYPOTHETICAL_ABNORMAL_RESET"


def test_unknown_next_reference_and_missing_index_are_explicit():
    src, states, quotes, now = fixture()
    result = project_monitor(src, states, quotes, Decimal(0), now)
    assert result.conditions[0].next_day.trigger_price is None
    assert result.conditions[0].next_day.missing_reason == "unverified_next_day_reference"
    missing = project_monitor(src, states, quotes[:1], Decimal(0), now)
    assert missing.conditions[0].today.trigger_price is None
    assert missing.conditions[0].today.missing_reason == "missing_benchmark_quote"
    with pytest.raises(ValueError):
        project_monitor(src, states, quotes, Decimal(3), now)


def test_incomplete_history_never_becomes_a_shorter_complete_window():
    src, states, quotes, now = fixture()
    src = replace(src, candidates=(replace(src.candidates[0], daily_returns=()),))
    result = project_monitor(src, states, quotes, Decimal(0), now)
    assert result.conditions[0].today.trigger_price is None
    assert "missing_daily_return" in result.conditions[0].today.missing_reason


@pytest.mark.parametrize("scenario,expected", [("-2", "10.73"), ("0", "10.91"), ("2", "11.10")])
def test_next_day_index_scenarios(scenario, expected):
    src, states, quotes, now = fixture()
    result = project_monitor(
        src,
        states,
        quotes,
        Decimal(scenario),
        now,
        next_reference_safe_symbols=frozenset({"SSE:600000"}),
    )
    assert result.conditions[0].next_day.trigger_price == Decimal(expected)


def test_downward_threshold_uses_floor_and_live_index_affects_today():
    src, states, quotes, now = fixture("9.50")
    down = replace(
        src.active_rules[0], direction=RegulationDirection.DOWN, threshold_pct=Decimal(-20)
    )
    prior = replace(src.candidates[0].daily_returns[0], stock_return=Decimal("-.1"))
    src = replace(
        src, active_rules=(down,), candidates=(replace(src.candidates[0], daily_returns=(prior,)),)
    )
    result = project_monitor(src, states, quotes, Decimal(0), now)
    assert result.conditions[0].today.trigger_price == Decimal("8.88")
    higher_index = (*quotes[:1], replace(quotes[1], price=Decimal("102")))
    result = project_monitor(src, states, higher_index, Decimal(0), now)
    assert result.conditions[0].today.trigger_price == Decimal("9.11")


def test_expired_rule_cannot_project_a_next_day_threshold():
    src, states, quotes, now = fixture()
    src = replace(src, active_rules=(replace(src.active_rules[0], expire_date=src.trade_date),))
    result = project_monitor(
        src, states, quotes, Decimal(0), now, next_reference_safe_symbols=frozenset({"SSE:600000"})
    )
    assert result.conditions[0].next_day.reachability == "NOT_APPLICABLE"
    assert result.conditions[0].next_day.trigger_price is None


def test_stale_quote_cannot_supply_either_threshold():
    src, states, quotes, now = fixture()
    result = project_monitor(src, states, quotes, Decimal(0), now + timedelta(seconds=61))
    assert result.conditions[0].today.trigger_price is None
    assert result.conditions[0].next_day.trigger_price is None
    assert not result.day.events


def test_count_path_requires_exactly_one_more_event_and_serious_reset_is_simulated():
    src = source([0, 0, 0, ".1", 0], (_rule(), count_rule()))
    now = datetime.combine(src.trade_date, datetime.min.time(), ZoneInfo("Asia/Shanghai")).replace(
        hour=10
    )
    quotes = (
        MonitorQuote("SSE:600000", now, Decimal(11), Decimal(10), None),
        MonitorQuote("SSE:000002", now, Decimal(100), Decimal(100), None),
    )
    events = tuple(price_event(d) for d in src.trading_dates[:3])
    states = (checkpoint(src, events, missing_reasons=()),)
    result = project_monitor(
        src, states, quotes, Decimal(0), now, next_reference_safe_symbols=frozenset({"SSE:600000"})
    )
    count = next(c for c in result.conditions if c.rule_code == count_rule().rule_code)
    assert count.today.trigger_price == Decimal("10.91")
    assert count.next_day.trigger_price is None
    assert count.next_day.reset_branch == "HYPOTHETICAL_SERIOUS_RESET"
    assert any(e.level is RegulationRuleLevel.SERIOUS_ABNORMAL for e in result.day.events)
    fewer = project_monitor(src, (replace(states[0], events=events[:2]),), quotes, Decimal(0), now)
    count = next(c for c in fewer.conditions if c.rule_code == count_rule().rule_code)
    assert count.today.trigger_price is None


def test_turnover_requires_unrestricted_shares_and_never_has_a_price_threshold():
    turnover = _rule(
        rule_code="SSE_MAIN_ABNORMAL_TURNOVER",
        kind=RegulationRuleKind.TURNOVER_COMPOSITE,
        direction=RegulationDirection.NONE,
        threshold_pct=None,
        comparison_window_days=5,
        ratio_threshold=Decimal(30),
        secondary_threshold_pct=Decimal(20),
        benchmark_symbol=None,
    )
    src = source([0] * 8, (_rule(), turnover), turnovers=[".3"] * 5 + [9, 9, 0])
    now = datetime.combine(src.trade_date, datetime.min.time(), ZoneInfo("Asia/Shanghai")).replace(
        hour=10
    )
    quotes = (
        MonitorQuote("SSE:600000", now, Decimal(10), Decimal(10), Decimal(900)),
        MonitorQuote("SSE:000002", now, Decimal(100), Decimal(100), None),
    )
    states = (checkpoint(src, missing_reasons=()),)
    unknown = project_monitor(src, states, quotes, Decimal(0), now)
    rr = next(r for r in unknown.day.output.rule_results if r.rule_code == turnover.rule_code)
    assert rr.current_value is None
    known = project_monitor(
        src, states, quotes, Decimal(0), now, unrestricted_shares={"SSE:600000": Decimal(10000)}
    )
    rr = next(r for r in known.day.output.rule_results if r.rule_code == turnover.rule_code)
    assert rr.triggered
    condition = next(c for c in known.conditions if c.rule_code == turnover.rule_code)
    assert condition.today.trigger_price is None
    assert condition.today.reachability == "NOT_PRICE_CALCULABLE"


def test_confirmed_close_uses_published_reference_and_never_calls_live_quotes():
    from market_data_center.regulation_monitor import project_monitor_close

    src, states, _, _ = fixture()
    result = project_monitor_close(src, states, Decimal(0))
    assert result.hypothetical_close_at is None
    assert result.conditions[0].next_day.reference_price == Decimal("10")
    assert result.conditions[0].next_day.trigger_price == Decimal("10.91")
    assert result.conditions[0].next_day.reset_branch == "NO_NEW_RESET"
