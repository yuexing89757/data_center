from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest

from market_data_center.domain.records import Exchange
from market_data_center.domain.regulation import (
    RegulationCalculationInput,
    RegulationCalculationOutput,
    RegulationCalculationRun,
    RegulationCoverage,
    RegulationDirection,
    RegulationResetLevel,
    RegulationRule,
    RegulationRuleKind,
    RegulationRuleLevel,
    RegulationRunStatus,
    RegulationSegment,
)
from market_data_center.regulation_service import (
    RegulationService,
    regulation_input_hash,
)

TRADE_DATE = date(2026, 9, 2)
NEXT_DATE = date(2026, 9, 3)
NOW = datetime(2026, 9, 2, 22, 30, tzinfo=ZoneInfo("Asia/Shanghai"))


def _rule(code: str = "SSE_MAIN_ABNORMAL_3D_DEV_UP") -> RegulationRule:
    return RegulationRule(
        rule_code=code,
        exchange=Exchange.SSE,
        segment=RegulationSegment.SSE_MAIN,
        level=RegulationRuleLevel.ABNORMAL,
        kind=RegulationRuleKind.CUMULATIVE_DEVIATION,
        direction=RegulationDirection.UP,
        window_days=3,
        threshold_pct=Decimal("20"),
        comparison_window_days=None,
        ratio_threshold=None,
        secondary_threshold_pct=None,
        count_window_days=None,
        required_count=None,
        counted_event_kind=None,
        reset_level=RegulationResetLevel.ABNORMAL,
        benchmark_symbol="SSE:000002",
        rule_set_version="cn-a-share-regulation-2026-07-06.v1",
        effective_date=date(2026, 7, 6),
        expire_date=None,
        source_document="official",
        source_clause="5.4.2",
        source_url="https://www.sse.com.cn/rule",
        enabled=True,
    )


def _source() -> RegulationCalculationInput:
    return RegulationCalculationInput(
        trade_date=TRADE_DATE,
        next_trade_date=NEXT_DATE,
        algorithm_version="regulation-calculator.v1",
        scenario_config_version="regulation-scenarios.v1",
        active_rules=(_rule(),),
        trading_dates=(TRADE_DATE,),
        candidates=(),
        rule_set_hash="a" * 64,
        market_watermark="market-1",
        capital_watermark="capital-1",
        event_watermark=NOW,
    )


class FakePersistence:
    def __init__(self, source: RegulationCalculationInput) -> None:
        self.source = source
        self.existing: UUID | None = None
        self.started: list[RegulationCalculationRun] = []
        self.published: list[tuple[RegulationCalculationRun, RegulationCalculationOutput]] = []
        self.failed: list[tuple[UUID, datetime]] = []

    def load_calculation_source(self, trade_date: date) -> RegulationCalculationInput:
        assert trade_date == self.source.trade_date
        return self.source

    def find_calculation(self, trade_date: date, input_hash: str) -> UUID | None:
        assert trade_date == self.source.trade_date
        assert len(input_hash) == 64
        return self.existing

    def start_calculation(self, run: RegulationCalculationRun) -> UUID:
        self.started.append(run)
        return run.calculation_id

    def publish_calculation(
        self, run: RegulationCalculationRun, output: RegulationCalculationOutput
    ) -> None:
        self.published.append((run, output))

    def mark_calculation_failed(self, calculation_id: UUID, completed_at: datetime) -> None:
        self.failed.append((calculation_id, completed_at))


def test_service_publishes_one_versioned_daily_calculation() -> None:
    persistence = FakePersistence(_source())
    service = RegulationService(persistence, clock=lambda: NOW)

    summary = service.calculate(TRADE_DATE)

    assert summary.reused is False
    assert summary.status is RegulationRunStatus.SUCCEEDED
    assert summary.trade_date == TRADE_DATE
    assert summary.next_trade_date == NEXT_DATE
    assert len(persistence.started) == 1
    assert persistence.started[0].status is RegulationRunStatus.RUNNING
    assert persistence.started[0].completed_at is None
    assert len(persistence.published) == 1
    run, output = persistence.published[0]
    assert run.input_hash == regulation_input_hash(_source())
    assert run.coverage == output.coverage == RegulationCoverage(0, 0, 0, 0)
    assert run.completed_at == NOW


def test_service_reuses_identical_input_without_republishing() -> None:
    persistence = FakePersistence(_source())
    persistence.existing = uuid4()
    service = RegulationService(persistence, clock=lambda: NOW)

    summary = service.calculate(TRADE_DATE)

    assert summary.calculation_id == persistence.existing
    assert summary.reused is True
    assert persistence.started == []
    assert persistence.published == []


def test_service_records_failed_run_when_calculation_raises() -> None:
    persistence = FakePersistence(_source())
    calculation_id = uuid4()

    def fail_calculation(source: RegulationCalculationInput) -> RegulationCalculationOutput:
        raise RuntimeError("calculation failed")

    service = RegulationService(
        persistence,
        calculator=fail_calculation,
        clock=lambda: NOW,
        uuid_factory=lambda: calculation_id,
    )

    with pytest.raises(RuntimeError, match="calculation failed"):
        service.calculate(TRADE_DATE)

    assert len(persistence.started) == 1
    assert persistence.started[0].calculation_id == calculation_id
    assert persistence.published == []
    assert persistence.failed == [(calculation_id, NOW)]


def test_input_hash_is_stable_for_rule_order_and_changes_with_watermark() -> None:
    first = _source()
    second_rule = replace(
        _rule("SSE_MAIN_SERIOUS_10D_DEV_UP"),
        level=RegulationRuleLevel.SERIOUS_ABNORMAL,
        window_days=10,
        threshold_pct=Decimal("100"),
        reset_level=RegulationResetLevel.SERIOUS_ABNORMAL,
    )
    ordered = replace(first, active_rules=(first.active_rules[0], second_rule))
    reversed_rules = replace(first, active_rules=(second_rule, first.active_rules[0]))

    assert regulation_input_hash(ordered) == regulation_input_hash(reversed_rules)
    assert regulation_input_hash(first) != regulation_input_hash(
        replace(first, market_watermark="market-2")
    )
