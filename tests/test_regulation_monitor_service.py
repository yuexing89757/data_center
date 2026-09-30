import json
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from pydantic import SecretStr
from test_regulation_calculator import _rule
from test_regulation_monitor_calculator import DAYS, checkpoint, source

from market_data_center.domain.regulation import RegulationDirection, RegulationRuleKind
from market_data_center.regulation_service import RegulationMonitorService


class Persistence:
    def __init__(self):
        self.sources = {DAYS[0]: source([".1"]), DAYS[1]: source([".1", ".1"])}
        self.runs = {}
        self.checkpoints = {}
        self.published = []
        self.started = []
        self.failed = []
        self.fail_publish = False

    def load_monitor_trading_dates(self, start, end):
        return tuple(day for day in self.sources if start <= day <= end)

    def load_calculation_source(self, day):
        return self.sources[day]

    def load_monitor_checkpoint(self, day, **kwargs):
        return self.checkpoints.get(day, (None, ()))

    def find_calculation(self, day, input_hash):
        return self.runs.get((day, input_hash))

    def start_calculation(self, run, events):
        self.started.append(run)
        return run.calculation_id

    def publish_monitor_day(self, run, src, result, **kwargs):
        if self.fail_publish:
            raise ValueError("publish failed")
        self.published.append((run, src, result, kwargs))
        self.checkpoints[run.next_trade_date] = (run.calculation_id, result.states)
        self.runs[(run.trade_date, run.input_hash)] = run.calculation_id

    def mark_calculation_failed(self, batch, at):
        self.failed.append(batch)


def service(p):
    return RegulationMonitorService(
        p,
        clock=lambda: datetime(2026, 9, 29, 22, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        monitor_start_date=DAYS[0],
    )


def test_preflight_simulates_sequence_without_any_writes():
    p = Persistence()
    report = service(p).preflight(DAYS[0], DAYS[1])
    assert report["can_execute"]
    assert report["trading_dates"] == [DAYS[0], DAYS[1]]
    assert report["symbol_count"] == 1
    assert not p.started and not p.published and not p.failed
    assert report["planned_writes"]["calculated_events"] == 1


def test_publishes_versioned_sequence_and_idempotently_reuses_same_inputs():
    p = Persistence()
    s = service(p)
    first = s.calculate(DAYS[0])
    assert not first.reused
    second = s.calculate(DAYS[1])
    assert second.warning_count > 0
    assert p.published[1][3]["parent_calculation_id"] == first.calculation_id
    assert len(p.published[1][2].events) == 1
    assert s.calculate(DAYS[1]).calculation_id == second.calculation_id
    assert len(p.published) == 2
    # A new predecessor identity must prevent accidental reuse of the old successor.
    p.checkpoints[DAYS[1]] = (uuid4(), p.checkpoints[DAYS[1]][1])
    assert not s.calculate(DAYS[1]).reused


def test_missing_origin_blocks_execution_and_failure_never_publishes_later_days():
    p = Persistence()
    s = service(p)
    assert not s.preflight(DAYS[1], DAYS[1])["can_execute"]
    with pytest.raises(ValueError, match="checkpoint"):
        s.calculate(DAYS[1])
    assert not p.started
    p.fail_publish = True
    with pytest.raises(ValueError, match="publish failed"):
        s.calculate(DAYS[0])
    assert len(p.failed) == 1 and not p.published


def test_preflight_reports_partial_stock_history_without_zero_filling():
    p = Persistence()
    src = p.sources[DAYS[0]]
    missing = replace(
        src.candidates[0], symbol="SSE:600001", daily_returns=(), next_day_price_limit=None
    )
    p.sources[DAYS[0]] = replace(src, candidates=(*src.candidates, missing))
    report = service(p).preflight(DAYS[0], DAYS[0])
    assert report["can_execute"] and report["missing_dependencies"]
    assert service(p).calculate(DAYS[0]).status.value == "PARTIAL"
    assert not p.published[0][2].states[1].complete


def test_preflight_identifies_the_missing_stock_without_writing():
    p = Persistence()
    src = p.sources[DAYS[0]]
    missing = replace(
        src.candidates[0], symbol="SSE:600001", daily_returns=(), next_day_price_limit=None
    )
    p.sources[DAYS[0]] = replace(src, candidates=(*src.candidates, missing))

    report = service(p).preflight(DAYS[0], DAYS[0])

    assert any(
        item["symbol"] == "SSE:600001" and item["reason"]
        for item in report["missing_dependency_details"]
    )
    assert report["symbol_count"] == 2
    assert report["can_execute"]
    assert report["missing_detail_count"] == 2
    assert not report["missing_details_truncated"]
    assert not p.started and not p.published and not p.failed


def test_preflight_reports_independent_turnover_gap_with_rule_identity():
    turnover_rule = _rule(
        rule_code="SSE_MAIN_ABNORMAL_TURNOVER",
        kind=RegulationRuleKind.TURNOVER_COMPOSITE,
        direction=RegulationDirection.NONE,
        threshold_pct=None,
        window_days=3,
        comparison_window_days=5,
        ratio_threshold=Decimal(30),
        secondary_threshold_pct=Decimal(20),
        benchmark_symbol=None,
    )
    src = source([0] * 8, (_rule(), turnover_rule))
    candidate = src.candidates[0]
    rows = candidate.daily_returns
    src = replace(
        src,
        candidates=(
            replace(
                candidate, daily_returns=(*rows[:-1], replace(rows[-1], turnover_rate_pct=None))
            ),
        ),
    )
    p = Persistence()
    p.sources = {DAYS[7]: src}
    p.checkpoints[DAYS[7]] = (uuid4(), (checkpoint(src, missing_reasons=()),))

    report = service(p).preflight(DAYS[7], DAYS[7])

    assert report["can_execute"]
    assert report["missing_dependencies"][0]["reasons"] == [
        f"missing_turnover_rate:{DAYS[7].isoformat()}"
    ]
    assert report["missing_dependency_details"] == [
        {
            "trade_date": DAYS[7].isoformat(),
            "symbol": candidate.symbol,
            "rule_code": turnover_rule.rule_code,
            "reason": f"missing_turnover_rate:{DAYS[7].isoformat()}",
        }
    ]
    assert not p.started and not p.published and not p.failed


def test_preflight_caps_details_but_counts_all_distinct_gaps():
    p = Persistence()
    src = p.sources[DAYS[0]]
    p.sources[DAYS[0]] = replace(
        src,
        candidates=tuple(
            replace(
                src.candidates[0],
                symbol=f"SSE:{code:06d}",
                daily_returns=(),
                next_day_price_limit=None,
            )
            for code in range(600001, 600206)
        ),
    )

    report = service(p).preflight(DAYS[0], DAYS[0])

    assert report["missing_detail_count"] == 410
    assert len(report["missing_dependency_details"]) == 200
    assert report["missing_details_truncated"] is True
    assert report["missing_dependency_details"][0] == {
        "trade_date": DAYS[0].isoformat(),
        "symbol": "SSE:600001",
        "rule_code": None,
        "reason": "missing_current_daily_return",
    }
    json.dumps(report["missing_dependency_details"])
    assert not p.started and not p.published and not p.failed


def test_reverting_inputs_does_not_report_a_superseded_batch_as_current():
    p = Persistence()
    s = service(p)
    first = s.calculate(DAYS[0])
    p.checkpoints[DAYS[1]] = (uuid4(), p.checkpoints[DAYS[1]][1])
    assert p.checkpoints[DAYS[1]][0] != first.calculation_id
    with pytest.raises(ValueError, match="superseded"):
        s.calculate(DAYS[0])


@pytest.mark.parametrize("execute", [False, True])
def test_cli_default_is_database_enforced_readonly_and_execute_is_sequential(
    monkeypatch, capsys, execute
):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from market_data_center import cli

    p = Persistence()
    engine = MagicMock()
    factory = MagicMock(return_value=engine)
    monkeypatch.setattr(cli, "create_engine", factory)
    monkeypatch.setattr(
        cli,
        "WorkerSettings",
        lambda: SimpleNamespace(database_url=SecretStr("postgresql://test@localhost/test")),
    )
    monkeypatch.setattr(cli, "PostgreSQLRegulationPersistence", lambda _: p)
    monkeypatch.setattr(cli, "RegulationMonitorService", lambda *a, **k: service(p))
    args = [
        "market-data-center",
        "regulation-monitor",
        "--start-date",
        str(DAYS[0]),
        "--end-date",
        str(DAYS[1]),
    ]
    monkeypatch.setattr("sys.argv", args + (["--execute"] if execute else []))
    cli.main()
    if execute:
        assert [run.trade_date for run, *_ in p.published] == list(DAYS[:2])
    else:
        assert not p.started and not p.failed
        assert (
            "default_transaction_read_only=on"
            in factory.call_args.kwargs["connect_args"]["options"]
        )
    assert "can_execute" in capsys.readouterr().out
    engine.dispose.assert_called_once()
