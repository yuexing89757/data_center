"""An explicit period can start independently without pretending to cover earlier events."""

from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from json import loads
from zoneinfo import ZoneInfo

import pytest
from test_regulation_calculator import _candidate, _daily, _input, _rule
from test_regulation_monitor_service import Persistence

from market_data_center.regulation_calculator import calculate_monitor_day
from market_data_center.regulation_monitor import project_monitor_close
from market_data_center.regulation_monitor_codec import encode_monitor, monitor_input_hash
from market_data_center.regulation_service import RegulationMonitorService

START = date(2026, 9, 1)
NEXT = date(2026, 9, 2)


def first_day_source():
    previous = date(2026, 8, 31)
    return replace(
        _input(
            (_rule(),),
            _candidate((_daily(previous, ".3", "0"), _daily(START, "0", "0"))),
            trading_dates=(previous, START, NEXT),
        ),
        trade_date=START,
        next_trade_date=NEXT,
        trading_dates=(previous, START),
    )


def test_september_origin_starts_without_july_checkpoint_and_excludes_august_return():
    p = Persistence()
    p.sources = {START: first_day_source()}
    service = RegulationMonitorService(
        p, clock=lambda: datetime(2026, 9, 29, 22, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    )

    report = service.preflight(START, START)
    assert report["can_execute"] is True
    summary = service.calculate(START)
    assert summary.coverage.complete_count == 1
    assert p.published[0][1].algorithm_version == "regulation-monitor.v3"
    assert p.published[0][1].monitor_start_date == START
    result = p.published[0][2]
    assert result.events == ()
    assert result.output.rule_results[0].current_value == Decimal(0)
    assert p.published[0][3]["parent_calculation_id"] is None


def test_scoped_price_projection_uses_the_same_period_start():
    src = replace(
        first_day_source(), algorithm_version="regulation-monitor.v3", monitor_start_date=START
    )
    result = project_monitor_close(src, (), Decimal(0))
    assert result.conditions[0].today.trigger_price == Decimal("10.80")
    assert result.conditions[0].today.window_start_date == START


def test_later_day_cannot_restart_without_checkpoint():
    p = Persistence()
    p.sources = {
        NEXT: replace(
            first_day_source(),
            trade_date=NEXT,
            next_trade_date=date(2026, 9, 3),
            trading_dates=(START, NEXT),
        )
    }
    service = RegulationMonitorService(
        p, clock=lambda: datetime(2026, 9, 29, tzinfo=ZoneInfo("Asia/Shanghai"))
    )
    assert service.preflight(NEXT, NEXT)["can_execute"] is False
    with pytest.raises(ValueError, match="checkpoint"):
        service.calculate(NEXT)
    assert not p.started


def test_period_start_changes_v3_identity_but_keeps_v2_hash_unchanged():
    src = replace(
        first_day_source(), algorithm_version="regulation-monitor.v3", monitor_start_date=START
    )
    previous_period = replace(src, monitor_start_date=date(2026, 8, 31))
    assert monitor_input_hash(src, (), None) != monitor_input_hash(previous_period, (), None)
    legacy = replace(
        src, algorithm_version="regulation-monitor.v2", monitor_start_date=date(2026, 7, 6)
    )
    payload = loads(encode_monitor(legacy))
    payload.pop("monitor_start_date")
    expected = sha256(
        encode_monitor({"source": payload, "states": (), "parent_calculation_id": None}).encode()
    ).hexdigest()
    assert monitor_input_hash(legacy, (), None) == expected


def test_preperiod_events_do_not_enter_scoped_count():
    from test_regulation_monitor_calculator import count_rule, price_event

    from market_data_center.domain.regulation import MonitorState

    src = replace(
        first_day_source(),
        algorithm_version="regulation-monitor.v3",
        monitor_start_date=START,
        active_rules=(count_rule(),),
    )
    prior = MonitorState(
        "SSE:600000", date(2026, 8, 31), True, None, None, (price_event(date(2026, 8, 31)),), (), ()
    )
    result = calculate_monitor_day(src, (prior,))
    assert result.output.rule_results[0].event_count == 0
    assert result.states[0].events == ()
