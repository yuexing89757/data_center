"""Application service for exact-date Regulation calculations."""

import hashlib
import json
from collections.abc import Callable
from dataclasses import fields, is_dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Protocol, cast
from uuid import UUID, uuid4

from market_data_center.domain.regulation import (
    RegulationCalculationInput,
    RegulationCalculationOutput,
    RegulationCalculationRun,
    RegulationCalculationSummary,
    RegulationCoverage,
    RegulationRunStatus,
)
from market_data_center.regulation_calculator import calculate_regulation


class RegulationPersistencePort(Protocol):
    def load_calculation_source(self, trade_date: date) -> RegulationCalculationInput: ...

    def find_calculation(self, trade_date: date, input_hash: str) -> UUID | None: ...

    def start_calculation(self, run: RegulationCalculationRun) -> UUID: ...

    def publish_calculation(
        self, run: RegulationCalculationRun, output: RegulationCalculationOutput
    ) -> None: ...

    def mark_calculation_failed(self, calculation_id: UUID, completed_at: datetime) -> None: ...


def _canonical(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _canonical(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    return value


def regulation_input_hash(source: RegulationCalculationInput) -> str:
    """Hash a logical input snapshot independently of source row ordering."""

    payload = cast(dict[str, object], _canonical(source))
    rules = cast(list[dict[str, object]], payload["active_rules"])
    rules.sort(key=lambda item: str(item["rule_code"]))
    candidates = cast(list[dict[str, object]], payload["candidates"])
    candidates.sort(key=lambda item: str(item["symbol"]))
    for candidate in candidates:
        cast(list[dict[str, object]], candidate["daily_returns"]).sort(
            key=lambda item: str(item["trade_date"])
        )
        cast(list[dict[str, object]], candidate["events"]).sort(
            key=lambda item: (str(item["source_code"]), str(item["source_event_id"]))
        )
    encoded = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class RegulationService:
    def __init__(
        self,
        persistence: RegulationPersistencePort,
        *,
        calculator: Callable[
            [RegulationCalculationInput], RegulationCalculationOutput
        ] = calculate_regulation,
        clock: Callable[[], datetime],
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._persistence = persistence
        self._calculator = calculator
        self._clock = clock
        self._uuid_factory = uuid_factory

    def calculate(self, trade_date: date) -> RegulationCalculationSummary:
        source = self._persistence.load_calculation_source(trade_date)
        input_hash = regulation_input_hash(source)
        existing = self._persistence.find_calculation(trade_date, input_hash)
        if existing is not None:
            output = self._calculator(source)
            status = (
                RegulationRunStatus.PARTIAL
                if output.coverage.incomplete_count
                else RegulationRunStatus.SUCCEEDED
            )
            return RegulationCalculationSummary(
                calculation_id=existing,
                trade_date=source.trade_date,
                next_trade_date=source.next_trade_date,
                status=status,
                coverage=output.coverage,
                warning_count=len(output.warnings),
                reused=True,
            )

        started_at = self._clock()
        expected_count = len(source.candidates)
        running = RegulationCalculationRun(
            calculation_id=self._uuid_factory(),
            trade_date=source.trade_date,
            next_trade_date=source.next_trade_date,
            status=RegulationRunStatus.RUNNING,
            algorithm_version=source.algorithm_version,
            rule_set_version=source.active_rules[0].rule_set_version,
            rule_set_hash=source.rule_set_hash,
            scenario_config_version=source.scenario_config_version,
            input_hash=input_hash,
            market_watermark=source.market_watermark,
            capital_watermark=source.capital_watermark,
            event_watermark=source.event_watermark,
            coverage=RegulationCoverage(
                expected_count=expected_count,
                complete_count=0,
                incomplete_count=expected_count,
                not_applicable_count=0,
            ),
            started_at=started_at,
            completed_at=None,
        )
        calculation_id = self._persistence.start_calculation(running)
        running = replace(running, calculation_id=calculation_id)
        try:
            output = self._calculator(source)
            status = (
                RegulationRunStatus.PARTIAL
                if output.coverage.incomplete_count
                else RegulationRunStatus.SUCCEEDED
            )
            run = replace(
                running,
                status=status,
                coverage=output.coverage,
                completed_at=self._clock(),
            )
            self._persistence.publish_calculation(run, output)
        except Exception:
            self._persistence.mark_calculation_failed(calculation_id, self._clock())
            raise
        return RegulationCalculationSummary(
            calculation_id=run.calculation_id,
            trade_date=run.trade_date,
            next_trade_date=run.next_trade_date,
            status=run.status,
            coverage=run.coverage,
            warning_count=len(output.warnings),
            reused=False,
        )
