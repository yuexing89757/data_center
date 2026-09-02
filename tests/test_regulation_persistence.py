from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from market_data_center.domain.records import Exchange
from market_data_center.domain.regulation import (
    RegulationCalculationOutput,
    RegulationCalculationRun,
    RegulationCoverage,
    RegulationDirection,
    RegulationRuleKind,
    RegulationRunStatus,
    RegulationSegment,
)
from market_data_center.persistence.regulation_postgres import (
    PostgreSQLRegulationPersistence,
)

TRADE_DATE = date(2026, 9, 2)
NEXT_DATE = date(2026, 9, 3)
NOW = datetime(2026, 9, 2, 22, 30, tzinfo=ZoneInfo("Asia/Shanghai"))


def _run(**changes: object) -> RegulationCalculationRun:
    values: dict[str, object] = {
        "calculation_id": uuid4(),
        "trade_date": TRADE_DATE,
        "next_trade_date": NEXT_DATE,
        "status": RegulationRunStatus.SUCCEEDED,
        "algorithm_version": "regulation-calculator.v1",
        "rule_set_version": "cn-a-share-regulation-2026-07-06.v1",
        "rule_set_hash": "a" * 64,
        "scenario_config_version": "regulation-scenarios.v1",
        "input_hash": "b" * 64,
        "market_watermark": "market-1",
        "capital_watermark": "capital-1",
        "event_watermark": NOW,
        "coverage": RegulationCoverage(0, 0, 0, 0),
        "started_at": NOW,
        "completed_at": NOW,
    }
    values.update(changes)
    return RegulationCalculationRun(**values)  # type: ignore[arg-type]


def _rule_row() -> dict[str, object]:
    return {
        "rule_id": uuid4(),
        "rule_code": "SSE_MAIN_ABNORMAL_3D_DEV_UP",
        "exchange": "SSE",
        "segment": "SSE_MAIN",
        "level": "ABNORMAL",
        "kind": "CUMULATIVE_DEVIATION",
        "direction": "UP",
        "window_days": 3,
        "threshold_pct": Decimal("20"),
        "comparison_window_days": None,
        "ratio_threshold": None,
        "secondary_threshold_pct": None,
        "count_window_days": None,
        "required_count": None,
        "counted_event_kind": None,
        "reset_level": "ABNORMAL",
        "benchmark_symbol": "SSE:000002",
        "rule_set_version": "cn-a-share-regulation-2026-07-06.v1",
        "effective_date": date(2026, 7, 6),
        "expire_date": None,
        "source_document": "official",
        "source_clause": "5.4.2(1)",
        "source_url": "https://www.sse.com.cn/rule",
        "enabled": True,
    }


def _engine_with_result(result: object) -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    connection = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    connection.execute.return_value = result
    return engine, connection


def test_load_active_rules_maps_decimal_domain_without_database_identity() -> None:
    result = MagicMock()
    result.mappings.return_value = (_rule_row(),)
    engine, connection = _engine_with_result(result)
    persistence = PostgreSQLRegulationPersistence(engine)

    rules = persistence.load_active_rules(TRADE_DATE)

    assert len(rules) == 1
    assert rules[0].exchange is Exchange.SSE
    assert rules[0].segment is RegulationSegment.SSE_MAIN
    assert rules[0].kind is RegulationRuleKind.CUMULATIVE_DEVIATION
    assert rules[0].direction is RegulationDirection.UP
    assert rules[0].threshold_pct == Decimal("20")
    assert not hasattr(rules[0], "rule_id")
    assert connection.execute.call_count == 1


def _mapping_result(rows: tuple[dict[str, object], ...]) -> MagicMock:
    result = MagicMock()
    result.mappings.return_value = rows
    return result


def test_load_calculation_source_assembles_exact_calendar_returns_and_price_limit() -> None:
    engine = MagicMock()
    connection = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    connection.execution_options.return_value = connection
    calendar_rows = (
        {"trade_date": date(2026, 8, 31)},
        {"trade_date": date(2026, 9, 1)},
        {"trade_date": TRADE_DATE},
    )
    security_rows = (
        {
            "symbol": "SSE:600000",
            "code": "600000",
            "exchange": "SSE",
            "status": "listed",
            "ipo_date": date(1999, 11, 10),
            "name": "浦发银行",
        },
    )
    bars = []
    for day in (date(2026, 8, 31), date(2026, 9, 1), TRADE_DATE):
        bars.extend(
            (
                {
                    "symbol": "SSE:600000",
                    "trade_date": day,
                    "close": Decimal("10"),
                    "previous_close": Decimal("10"),
                    "is_st": False,
                    "ingestion_id": uuid4(),
                },
                {
                    "symbol": "SSE:000002",
                    "trade_date": day,
                    "close": Decimal("100"),
                    "previous_close": Decimal("100"),
                    "is_st": None,
                    "ingestion_id": uuid4(),
                },
            )
        )
    indicators = tuple(
        {
            "symbol": "SSE:600000",
            "trade_date": day,
            "turnover_rate_pct": Decimal("1"),
            "ingestion_id": uuid4(),
        }
        for day in (date(2026, 8, 31), date(2026, 9, 1), TRADE_DATE)
    )
    next_day_result = MagicMock()
    next_day_result.scalar_one.return_value = NEXT_DATE
    connection.execute.side_effect = (
        _mapping_result((_rule_row(),)),
        _mapping_result(calendar_rows),
        next_day_result,
        _mapping_result(security_rows),
        _mapping_result(tuple(bars)),
        _mapping_result(indicators),
        _mapping_result(()),
        _mapping_result(()),
    )
    persistence = PostgreSQLRegulationPersistence(engine)

    source = persistence.load_calculation_source(TRADE_DATE)

    assert source.trade_date == TRADE_DATE
    assert source.next_trade_date == NEXT_DATE
    assert source.trading_dates == tuple(row["trade_date"] for row in calendar_rows)
    assert len(source.candidates) == 1
    candidate = source.candidates[0]
    assert candidate.segment is RegulationSegment.SSE_MAIN
    assert candidate.daily_returns[-1].stock_return == Decimal("0")
    assert candidate.daily_returns[-1].benchmark_return == Decimal("0")
    assert candidate.next_day_price_limit is not None
    assert candidate.next_day_price_limit.upper_limit == Decimal("11.00")
    assert candidate.next_day_price_limit.lower_limit == Decimal("9.00")


def test_find_calculation_returns_only_published_run_identity() -> None:
    calculation_id = uuid4()
    result = MagicMock()
    result.scalar_one_or_none.return_value = calculation_id
    engine, connection = _engine_with_result(result)
    persistence = PostgreSQLRegulationPersistence(engine)

    found = persistence.find_calculation(TRADE_DATE, "b" * 64)

    assert found == calculation_id
    sql = str(connection.execute.call_args.args[0])
    assert "status in ('SUCCEEDED', 'PARTIAL')" in sql


def test_publish_calculation_rejects_output_identity_mismatch_before_transaction() -> None:
    engine = MagicMock()
    persistence = PostgreSQLRegulationPersistence(engine)
    output = RegulationCalculationOutput(
        trade_date=date(2026, 9, 1),
        next_trade_date=NEXT_DATE,
        statuses=(),
        rule_results=(),
        warnings=(),
        coverage=RegulationCoverage(0, 0, 0, 0),
        quality_findings=(),
    )

    with pytest.raises(ValueError, match="trade date"):
        persistence.publish_calculation(_run(), output)

    engine.begin.assert_not_called()


def test_publish_empty_calculation_uses_one_atomic_transaction() -> None:
    engine = MagicMock()
    connection = MagicMock()
    connection.execute.return_value.rowcount = 1
    engine.begin.return_value.__enter__.return_value = connection
    persistence = PostgreSQLRegulationPersistence(engine)
    output = RegulationCalculationOutput(
        trade_date=TRADE_DATE,
        next_trade_date=NEXT_DATE,
        statuses=(),
        rule_results=(),
        warnings=(),
        coverage=RegulationCoverage(0, 0, 0, 0),
        quality_findings=(),
    )

    persistence.publish_calculation(_run(), output)

    engine.begin.assert_called_once_with()
    assert connection.execute.call_count == 1
    assert "update regulation.calculation_run" in str(connection.execute.call_args.args[0])


def test_start_calculation_inserts_running_run_and_can_retry_failed_input() -> None:
    engine = MagicMock()
    connection = MagicMock()
    result = MagicMock()
    calculation_id = uuid4()
    result.scalar_one_or_none.return_value = calculation_id
    connection.execute.return_value = result
    engine.begin.return_value.__enter__.return_value = connection
    persistence = PostgreSQLRegulationPersistence(engine)
    run = _run(
        calculation_id=calculation_id,
        status=RegulationRunStatus.RUNNING,
        completed_at=None,
    )

    started_id = persistence.start_calculation(run)

    assert started_id == calculation_id
    sql = str(connection.execute.call_args.args[0])
    assert "insert into regulation.calculation_run" in sql
    assert "on conflict (trade_date, input_hash)" in sql
    assert "where calculation_run.status = 'FAILED'" in sql


def test_terminal_and_running_runs_enforce_completion_time() -> None:
    with pytest.raises(ValueError, match="completed at"):
        _run(status=RegulationRunStatus.FAILED, completed_at=None)
    with pytest.raises(ValueError, match="must not have completed"):
        _run(status=RegulationRunStatus.RUNNING)
