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
from zoneinfo import ZoneInfo

from market_data_center.domain.regulation import (
    REGULATION_RULES_EFFECTIVE_FROM,
    MonitorDay,
    MonitorState,
    RegulationCalculationInput,
    RegulationCalculationOutput,
    RegulationCalculationRun,
    RegulationCalculationSummary,
    RegulationCoverage,
    RegulationDataCompleteness,
    RegulationEventRecord,
    RegulationReachability,
    RegulationRunStatus,
    RegulationScenarioCode,
    RegulationWarningResult,
)
from market_data_center.persistence.regulation_postgres import PostgreSQLRegulationPersistence
from market_data_center.regulation_calculator import (
    REGULATION_MONITOR_VERSION,
    calculate_regulation,
)
from market_data_center.regulation_monitor import project_monitor_close
from market_data_center.regulation_monitor_codec import monitor_input_hash


class RegulationPersistencePort(Protocol):
    def load_calculation_source(self, trade_date: date) -> RegulationCalculationInput: ...

    def find_calculation(self, trade_date: date, input_hash: str) -> UUID | None: ...

    def start_calculation(
        self, run: RegulationCalculationRun, input_events: tuple[RegulationEventRecord, ...]
    ) -> UUID: ...

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
    if source.algorithm_version in {"regulation-calculator.v1", "regulation-calculator.v2"}:
        payload.pop("st_watermark")
    if source.algorithm_version == "regulation-calculator.v1":
        payload.pop("reset_trading_dates")
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
        input_events = tuple(event for candidate in source.candidates for event in candidate.events)
        calculation_id = self._persistence.start_calculation(running, input_events)
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


def _monitor_day(
    source: RegulationCalculationInput, states: tuple[MonitorState, ...]
) -> MonitorDay:
    """Reuse conditional price math for the published three-scenario observation list."""
    warnings = []
    candidates = {c.symbol: c for c in source.candidates}
    rules = {r.rule_code: r for r in source.active_rules}
    result = None
    for pct, code in (
        (-2, RegulationScenarioCode.INDEX_DOWN_2),
        (0, RegulationScenarioCode.INDEX_FLAT),
        (2, RegulationScenarioCode.INDEX_UP_2),
    ):
        projection = project_monitor_close(source, states, Decimal(pct))
        result = projection.day
        values = {(r.symbol, r.rule_code): r for r in result.output.rule_results}
        for condition in projection.conditions:
            threshold = condition.next_day
            if threshold.trigger_price is None:
                continue
            rule = rules[condition.rule_code]
            value = values[(condition.symbol, condition.rule_code)]
            limit = candidates[condition.symbol].next_day_price_limit
            warnings.append(
                RegulationWarningResult(
                    trade_date=source.trade_date,
                    next_trade_date=source.next_trade_date,
                    symbol=condition.symbol,
                    rule_code=rule.rule_code,
                    level=rule.level,
                    direction=rule.direction,
                    current_value=value.current_value,
                    threshold=value.threshold,
                    distance=value.distance,
                    scenario_code=code,
                    scenario_index_pct=Decimal(pct),
                    next_day_reference_price=threshold.reference_price,
                    raw_trigger_price=None,
                    next_day_trigger_price=threshold.trigger_price,
                    next_day_trigger_pct=threshold.trigger_change_pct,
                    price_limit_ratio=limit.limit_ratio if limit else None,
                    lower_limit_price=limit.lower_limit if limit else None,
                    upper_limit_price=limit.upper_limit if limit else None,
                    reachability=(
                        RegulationReachability.REACHABLE_NEXT_SESSION
                        if threshold.reachability == "REACHABLE"
                        else RegulationReachability.NOT_REACHABLE_NEXT_SESSION
                    ),
                    window_start_date=threshold.window_start_date,
                    window_end_date=source.next_trade_date,
                    requires_official_event_confirmation=False,
                    message_template_code="REGULATION_MONITOR_CONDITION_V1",
                    message="Conditional system calculation, not an exchange determination.",
                )
            )
    assert result is not None
    return replace(result, output=replace(result.output, warnings=tuple(warnings)))


class RegulationMonitorService:
    def __init__(
        self, persistence: PostgreSQLRegulationPersistence, *, clock: Callable[[], datetime]
    ) -> None:
        self._persistence = persistence
        self._clock = clock

    def calculate(self, trade_date: date) -> RegulationCalculationSummary:
        source = replace(
            self._persistence.load_calculation_source(trade_date),
            algorithm_version=REGULATION_MONITOR_VERSION,
        )
        parent, states = self._persistence.load_monitor_checkpoint(trade_date)
        if trade_date != REGULATION_RULES_EFFECTIVE_FROM and parent is None:
            raise ValueError("monitor requires a verified preceding checkpoint")
        input_hash = monitor_input_hash(source, states, parent)
        result = _monitor_day(source, states)
        status = (
            RegulationRunStatus.PARTIAL
            if result.output.coverage.incomplete_count
            else RegulationRunStatus.SUCCEEDED
        )
        existing = self._persistence.find_calculation(trade_date, input_hash)
        if existing is not None:
            current, _ = self._persistence.load_monitor_checkpoint(source.next_trade_date)
            if current != existing:
                raise ValueError("matching monitor inputs belong to a superseded publication")
            return RegulationCalculationSummary(
                existing,
                trade_date,
                source.next_trade_date,
                status,
                result.output.coverage,
                len(result.output.warnings),
                True,
            )
        running = RegulationCalculationRun(
            calculation_id=uuid4(),
            trade_date=trade_date,
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
            coverage=result.output.coverage,
            started_at=self._clock(),
            completed_at=None,
        )
        batch = self._persistence.start_calculation(
            running, tuple(e for c in source.candidates for e in c.events)
        )
        completed = replace(
            running, calculation_id=batch, status=status, completed_at=self._clock()
        )
        try:
            self._persistence.publish_monitor_day(
                completed, source, result, parent_calculation_id=parent, previous_states=states
            )
        except Exception:
            self._persistence.mark_calculation_failed(batch, self._clock())
            raise
        return RegulationCalculationSummary(
            batch,
            trade_date,
            source.next_trade_date,
            status,
            result.output.coverage,
            len(result.output.warnings),
            False,
        )

    def preflight(self, start_date: date, end_date: date) -> dict[str, object]:
        today = self._clock().astimezone(ZoneInfo("Asia/Shanghai")).date()
        if start_date < REGULATION_RULES_EFFECTIVE_FROM or not start_date <= end_date <= today:
            raise ValueError("monitor requires an ordered nonfuture range from 2026-07-06")
        if (end_date - start_date).days > 366:
            raise ValueError("monitor preflight is limited to 366 calendar days")
        dates = tuple(sorted(self._persistence.load_monitor_trading_dates(start_date, end_date)))
        missing: list[dict[str, object]] = []
        details: list[dict[str, object]] = []
        detail_count = 0
        symbols: set[str] = set()
        rows = events = 0
        can_execute = bool(dates)
        states: tuple[MonitorState, ...] = ()
        if dates:
            try:
                parent, states = self._persistence.load_monitor_checkpoint(dates[0])
                if dates[0] != REGULATION_RULES_EFFECTIVE_FROM and parent is None:
                    missing.append(
                        {"trade_date": dates[0], "reason": "missing_continuous_checkpoint"}
                    )
                    can_execute = False
            except ValueError:
                missing.append({"trade_date": dates[0], "reason": "superseded_checkpoint"})
                can_execute = False
        for day in dates:
            try:
                source = replace(
                    self._persistence.load_calculation_source(day),
                    algorithm_version=REGULATION_MONITOR_VERSION,
                )
                result = _monitor_day(source, states)
            except ValueError:
                missing.append(
                    {"trade_date": day, "reason": "invalid_or_missing_calculation_inputs"}
                )
                can_execute = False
                break
            symbols.update(c.symbol for c in source.candidates)
            rows += len(result.states)
            events += len(result.events)
            states = result.states
            incomplete_symbols = {
                status.symbol
                for status in result.output.statuses
                if status.data_completeness is RegulationDataCompleteness.INCOMPLETE
            }
            day_gaps: set[tuple[str, str | None, str]] = {
                (state.symbol, None, reason)
                for state in states
                if state.symbol in incomplete_symbols
                for reason in state.missing_reasons
            }
            day_gaps.update(
                (rule.symbol, rule.rule_code, rule.incomplete_reason)
                for rule in result.output.rule_results
                if rule.symbol in incomplete_symbols and rule.incomplete_reason
            )
            detail_count += len(day_gaps)
            for symbol, rule_code, reason in sorted(
                day_gaps, key=lambda gap: (gap[0], gap[1] or "", gap[2])
            ):
                if len(details) == 200:
                    break
                details.append(
                    {
                        "trade_date": day.isoformat(),
                        "symbol": symbol,
                        "rule_code": rule_code,
                        "reason": reason,
                    }
                )
            if result.output.coverage.incomplete_count:
                reasons = sorted({reason for _, _, reason in day_gaps})
                missing.append(
                    {
                        "trade_date": day,
                        "count": result.output.coverage.incomplete_count,
                        "reasons": reasons,
                        "status": "PARTIAL",
                    }
                )
            if not any(state.complete for state in states):
                can_execute = False
        return {
            "start_date": start_date,
            "end_date": end_date,
            "trading_dates": list(dates),
            "symbol_count": len(symbols),
            "missing_dependencies": missing,
            "missing_dependency_details": details,
            "missing_detail_count": detail_count,
            "missing_details_truncated": detail_count > len(details),
            "affected_dates": list(dates),
            "can_execute": can_execute,
            "planned_writes": {
                "batches": len(dates),
                "monitor_inputs": rows,
                "calculated_events": events,
            },
        }
