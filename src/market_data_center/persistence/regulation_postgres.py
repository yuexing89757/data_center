"""PostgreSQL access for versioned Regulation calculations."""

import hashlib
from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import Connection, Engine, RowMapping, bindparam, text

from market_data_center.domain.records import Exchange
from market_data_center.domain.regulation import (
    RegulationApplicability,
    RegulationCalculationInput,
    RegulationCalculationOutput,
    RegulationCalculationRun,
    RegulationCandidate,
    RegulationDailyReturn,
    RegulationDirection,
    RegulationEventRecord,
    RegulationEventType,
    RegulationResetLevel,
    RegulationRule,
    RegulationRuleKind,
    RegulationRuleLevel,
    RegulationRunStatus,
    RegulationSegment,
    validate_regulation_rules,
)
from market_data_center.domain.stock_pool import DailyPriceLimit, price_limit_rule
from market_data_center.regulation_calculator import (
    REGULATION_ALGORITHM_VERSION,
    REGULATION_SCENARIO_CONFIG_VERSION,
)
from market_data_center.stock_pool_calculator import calculate_price_limit_range


class PostgreSQLRegulationPersistence:
    """Load configured rules and atomically publish calculation results."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def load_active_rules(self, trade_date: date) -> tuple[RegulationRule, ...]:
        with self._engine.connect() as connection:
            rules = _load_active_rules(connection, trade_date)
        return validate_regulation_rules(rules, trade_date)

    def load_calculation_source(self, trade_date: date) -> RegulationCalculationInput:
        with self._engine.connect() as raw_connection:
            connection = raw_connection.execution_options(isolation_level="REPEATABLE READ")
            with connection.begin():
                rules = validate_regulation_rules(
                    _load_active_rules(connection, trade_date), trade_date
                )
                calendar_rows = tuple(
                    connection.execute(
                        text("""
select trade_date
from core.trading_calendar
where market = 'CN_A_SHARE' and is_trading_day and trade_date <= :trade_date
order by trade_date desc
limit 30
"""),
                        {"trade_date": trade_date},
                    ).mappings()
                )
                trading_dates = tuple(sorted(row["trade_date"] for row in calendar_rows))
                if not trading_dates or trading_dates[-1] != trade_date:
                    raise ValueError("requested regulation date is not an available trading day")
                next_trade_date = connection.execute(
                    text("""
select min(trade_date)
from core.trading_calendar
where market = 'CN_A_SHARE' and is_trading_day and trade_date > :trade_date
"""),
                    {"trade_date": trade_date},
                ).scalar_one()
                if next_trade_date is None:
                    raise ValueError("next trading day is unavailable")

                securities = tuple(
                    connection.execute(
                        text("""
select security.symbol, security.code, security.exchange, security.status,
       security.ipo_date, history.name
from core.security security
left join core.security_name_history history
  on history.symbol = security.symbol
 and history.effective_from <= :trade_date
 and (history.effective_to is null or history.effective_to >= :trade_date)
where security.security_type = 'stock'
  and security.exchange in ('SSE', 'SZSE')
  and (security.ipo_date is null or security.ipo_date <= :trade_date)
  and (security.delisting_date is null or security.delisting_date >= :trade_date)
order by security.symbol
"""),
                        {"trade_date": trade_date},
                    ).mappings()
                )
                supported = tuple(
                    row for row in securities if _segment(str(row["exchange"]), str(row["code"]))
                )
                stock_symbols = tuple(str(row["symbol"]) for row in supported)
                benchmark_by_segment = {
                    rule.segment: rule.benchmark_symbol
                    for rule in rules
                    if rule.benchmark_symbol is not None
                }
                all_symbols = tuple(sorted({*stock_symbols, *benchmark_by_segment.values()}))
                bars = tuple(
                    connection.execute(
                        _SELECT_BARS,
                        {
                            "symbols": all_symbols,
                            "start_date": trading_dates[0],
                            "trade_date": trade_date,
                        },
                    ).mappings()
                )
                indicators = tuple(
                    connection.execute(
                        _SELECT_INDICATORS,
                        {
                            "symbols": stock_symbols,
                            "start_date": trading_dates[0],
                            "trade_date": trade_date,
                        },
                    ).mappings()
                )
                event_rows = tuple(
                    connection.execute(
                        _SELECT_EVENTS,
                        {"symbols": stock_symbols, "trade_date": trade_date},
                    ).mappings()
                )
                capital_rows = tuple(
                    connection.execute(
                        _SELECT_CAPITAL_EVENTS,
                        {
                            "symbols": stock_symbols,
                            "start_date": trading_dates[0],
                            "next_trade_date": next_trade_date,
                        },
                    ).mappings()
                )

        return _assemble_source(
            trade_date=trade_date,
            next_trade_date=next_trade_date,
            rules=rules,
            trading_dates=trading_dates,
            securities=supported,
            bars=bars,
            indicators=indicators,
            event_rows=event_rows,
            capital_rows=capital_rows,
            benchmark_by_segment=benchmark_by_segment,
        )

    def find_calculation(self, trade_date: date, input_hash: str) -> UUID | None:
        with self._engine.connect() as connection:
            return connection.execute(
                text("""
select calculation_id
from regulation.calculation_run
where trade_date = :trade_date
  and input_hash = :input_hash
  and status in ('SUCCEEDED', 'PARTIAL')
  and completed_at is not null
order by completed_at desc
limit 1
"""),
                {"trade_date": trade_date, "input_hash": input_hash},
            ).scalar_one_or_none()

    def start_calculation(self, run: RegulationCalculationRun) -> UUID:
        if run.status is not RegulationRunStatus.RUNNING:
            raise ValueError("only a running regulation calculation can be started")
        with self._engine.begin() as connection:
            calculation_id = connection.execute(
                _INSERT_RUNNING_RUN, _run_parameters(run)
            ).scalar_one_or_none()
        if calculation_id is None:
            raise ValueError("regulation calculation is already running or published")
        if not isinstance(calculation_id, UUID):
            raise TypeError("regulation calculation id must be a UUID")
        return calculation_id

    def publish_calculation(
        self, run: RegulationCalculationRun, output: RegulationCalculationOutput
    ) -> None:
        _validate_publish(run, output)
        with self._engine.begin() as connection:
            if not output.statuses:
                _complete_run(connection, run)
                return

            rule_codes = sorted({result.rule_code for result in output.rule_results})
            rule_ids = {
                row["rule_code"]: row["rule_id"]
                for row in connection.execute(
                    text("""
select rule_id, rule_code
from regulation.rule
where enabled
  and effective_date <= :trade_date
  and (expire_date is null or expire_date >= :trade_date)
order by rule_code
"""),
                    {"trade_date": run.trade_date},
                ).mappings()
                if row["rule_code"] in rule_codes
            }
            missing = set(rule_codes) - set(rule_ids)
            if missing:
                raise ValueError(f"missing active rule ids: {sorted(missing)}")

            connection.execute(
                _INSERT_STATUS,
                [
                    {
                        "calculation_id": run.calculation_id,
                        "trade_date": status.trade_date,
                        "symbol": status.symbol,
                        "exchange": status.exchange.value,
                        "segment": status.segment.value,
                        "applicability": status.applicability.value,
                        "applicability_reason": status.applicability_reason,
                        "data_completeness": status.data_completeness.value,
                        "calculated_state": status.calculated_state.value,
                        "announced_state": status.announced_state.value,
                        "close": status.close,
                        "stock_daily_return_pct": status.stock_daily_return_pct,
                        "benchmark_symbol": status.benchmark_symbol,
                        "benchmark_close": status.benchmark_close,
                        "benchmark_daily_return_pct": status.benchmark_daily_return_pct,
                        "daily_deviation_pct": status.daily_deviation_pct,
                        "abnormal_count_10d": status.abnormal_count_10d,
                        "abnormal_count_10d_up": status.abnormal_count_10d_up,
                        "abnormal_count_10d_down": status.abnormal_count_10d_down,
                        "abnormal_reset_date": status.abnormal_reset_date,
                        "serious_reset_date": status.serious_reset_date,
                    }
                    for status in sorted(output.statuses, key=lambda item: item.symbol)
                ],
            )
            if output.rule_results:
                connection.execute(
                    _INSERT_RULE_RESULT,
                    [
                        {
                            "calculation_id": run.calculation_id,
                            "symbol": result.symbol,
                            "rule_id": rule_ids[result.rule_code],
                            "evaluation_state": result.evaluation_state.value,
                            "triggered": result.triggered,
                            "window_start_date": result.window_start_date,
                            "window_end_date": result.window_end_date,
                            "observed_window_days": result.observed_window_days,
                            "current_value": result.current_value,
                            "threshold": result.threshold,
                            "distance": result.distance,
                            "secondary_current_value": result.secondary_current_value,
                            "secondary_threshold": result.secondary_threshold,
                            "event_count": result.event_count,
                            "required_count": result.required_count,
                            "selected_reset_date": result.selected_reset_date,
                            "data_completeness": result.data_completeness.value,
                            "incomplete_reason": result.incomplete_reason,
                        }
                        for result in sorted(
                            output.rule_results,
                            key=lambda item: (item.symbol, item.rule_code),
                        )
                    ],
                )
            if output.warnings:
                connection.execute(
                    _INSERT_WARNING,
                    [
                        {
                            "calculation_id": run.calculation_id,
                            "trade_date": warning.trade_date,
                            "next_trade_date": warning.next_trade_date,
                            "symbol": warning.symbol,
                            "rule_id": rule_ids[warning.rule_code],
                            "warning_type": "REGULATION_CONDITION",
                            "level": warning.level.value,
                            "direction": warning.direction.value,
                            "current_value": warning.current_value,
                            "threshold": warning.threshold,
                            "distance": warning.distance,
                            "scenario_code": warning.scenario_code.value,
                            "scenario_index_pct": warning.scenario_index_pct,
                            "next_day_reference_price": warning.next_day_reference_price,
                            "raw_trigger_price": warning.raw_trigger_price,
                            "next_day_trigger_price": warning.next_day_trigger_price,
                            "next_day_trigger_pct": warning.next_day_trigger_pct,
                            "price_limit_ratio": warning.price_limit_ratio,
                            "lower_limit_price": warning.lower_limit_price,
                            "upper_limit_price": warning.upper_limit_price,
                            "reachability": warning.reachability.value,
                            "window_start_date": warning.window_start_date,
                            "window_end_date": warning.window_end_date,
                            "requires_official_event_confirmation": (
                                warning.requires_official_event_confirmation
                            ),
                            "message_template_code": warning.message_template_code,
                            "message": warning.message,
                        }
                        for warning in sorted(
                            output.warnings,
                            key=lambda item: (
                                item.symbol,
                                item.rule_code,
                                item.scenario_code.value,
                            ),
                        )
                    ],
                )
            _complete_run(connection, run)

    def mark_calculation_failed(self, calculation_id: UUID, completed_at: datetime) -> None:
        with self._engine.begin() as connection:
            result = connection.execute(
                text("""
update regulation.calculation_run
set status = 'FAILED', completed_at = :completed_at
where calculation_id = :calculation_id and status = 'RUNNING'
"""),
                {"calculation_id": calculation_id, "completed_at": completed_at},
            )
            if result.rowcount != 1:
                raise ValueError("running regulation calculation was not found")


def _load_active_rules(connection: Connection, trade_date: date) -> tuple[RegulationRule, ...]:
    rows = connection.execute(
        text("""
select rule_id, rule_code, exchange, segment, level, kind, direction,
       window_days, threshold_pct, comparison_window_days, ratio_threshold,
       secondary_threshold_pct, count_window_days, required_count,
       counted_event_kind, reset_level, benchmark_symbol, rule_set_version,
       effective_date, expire_date, source_document, source_clause, source_url,
       enabled
from regulation.rule
where enabled
  and effective_date <= :trade_date
  and (expire_date is null or expire_date >= :trade_date)
order by segment, level, kind, direction, rule_code
"""),
        {"trade_date": trade_date},
    ).mappings()
    return tuple(_rule_from_row(row) for row in rows)


def _segment(exchange: str, code: str) -> RegulationSegment | None:
    if len(code) != 6 or not code.isdigit():
        return None
    number = int(code)
    if exchange == "SSE" and (600000 <= number <= 603999 or 605000 <= number <= 605999):
        return RegulationSegment.SSE_MAIN
    if exchange == "SZSE" and (300000 <= number <= 301999):
        return RegulationSegment.GEM
    if exchange == "SZSE" and 1 <= number <= 4999 and not 1001 <= number <= 1199:
        return RegulationSegment.SZSE_MAIN
    return None


def _decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _event_from_row(row: RowMapping | dict[str, object]) -> RegulationEventRecord:
    direction = row["direction"]
    return RegulationEventRecord(
        symbol=str(row["symbol"]),
        exchange=Exchange(str(row["exchange"])),
        segment=RegulationSegment(str(row["segment"])),
        event_type=RegulationEventType(str(row["event_type"])),
        event_level=RegulationRuleLevel(str(row["event_level"])),
        direction=RegulationDirection(str(direction)) if direction is not None else None,
        period_start_date=row["period_start_date"],  # type: ignore[arg-type]
        period_end_date=row["period_end_date"],  # type: ignore[arg-type]
        published_at=row["published_at"],  # type: ignore[arg-type]
        effective_reset_date=row["effective_reset_date"],  # type: ignore[arg-type]
        source_event_id=str(row["source_event_id"]),
        source_title=str(row["source_title"]),
        source_url=str(row["source_url"]),
        source_content_hash=str(row["source_content_hash"]),
        source_code=str(row["source_code"]),
        explicit_rule_codes=tuple(row["explicit_rule_codes"]),  # type: ignore[arg-type]
        observed_at=row["observed_at"],  # type: ignore[arg-type]
    )


def _reference_price(
    base_close: Decimal | None, capital_rows: tuple[RowMapping | dict[str, object], ...]
) -> Decimal | None:
    if base_close is None or base_close <= 0:
        return None
    cash = bonus = rights_ratio = rights_value = Decimal(0)
    for row in capital_rows:
        if row["event_kind"] == "distribution":
            status = str(row["status"])
            if status == "cancelled":
                continue
            if status != "implemented":
                return None
            cash += _decimal(row["cash_dividend_per_share"]) or Decimal(0)
            bonus += (_decimal(row["bonus_share_ratio"]) or Decimal(0)) + (
                _decimal(row["transfer_share_ratio"]) or Decimal(0)
            )
        else:
            ratio = _decimal(row["rights_ratio"])
            price = _decimal(row["rights_price"])
            if ratio is None or price is None:
                return None
            rights_ratio += ratio
            rights_value += ratio * price
    result = (base_close - cash + rights_value) / (Decimal(1) + bonus + rights_ratio)
    return result if result > 0 else None


def _watermark(rows: tuple[RowMapping | dict[str, object], ...]) -> str:
    values = sorted(
        {str(row["ingestion_id"]) for row in rows if row.get("ingestion_id") is not None}
    )
    return hashlib.sha256("|".join(values).encode()).hexdigest() if values else "none"


def _assemble_source(
    *,
    trade_date: date,
    next_trade_date: date,
    rules: tuple[RegulationRule, ...],
    trading_dates: tuple[date, ...],
    securities: tuple[RowMapping | dict[str, object], ...],
    bars: tuple[RowMapping | dict[str, object], ...],
    indicators: tuple[RowMapping | dict[str, object], ...],
    event_rows: tuple[RowMapping | dict[str, object], ...],
    capital_rows: tuple[RowMapping | dict[str, object], ...],
    benchmark_by_segment: dict[RegulationSegment, str],
) -> RegulationCalculationInput:
    bars_by_key = {(str(row["symbol"]), row["trade_date"]): row for row in bars}
    indicators_by_key = {(str(row["symbol"]), row["trade_date"]): row for row in indicators}
    events_by_symbol: dict[str, list[RegulationEventRecord]] = defaultdict(list)
    for row in event_rows:
        event = _event_from_row(row)
        events_by_symbol[event.symbol].append(event)
    capital_by_key: dict[tuple[str, date], list[RowMapping | dict[str, object]]] = defaultdict(list)
    for row in capital_rows:
        capital_by_key[(str(row["symbol"]), row["ex_date"])].append(row)  # type: ignore[index]

    candidates: list[RegulationCandidate] = []
    for security in securities:
        symbol = str(security["symbol"])
        exchange = Exchange(str(security["exchange"]))
        segment = _segment(exchange.value, str(security["code"]))
        assert segment is not None
        benchmark_symbol = benchmark_by_segment[segment]
        target_bar = bars_by_key.get((symbol, trade_date))
        target_close = _decimal(target_bar["close"]) if target_bar is not None else None
        next_reference = _reference_price(
            target_close,
            tuple(capital_by_key.get((symbol, next_trade_date), ())),
        )
        name = security["name"]
        is_st = bool(target_bar["is_st"]) if target_bar is not None else False
        normalized_name = str(name).upper().replace(" ", "") if name is not None else ""
        ipo_date = security["ipo_date"]
        listing_days = (
            sum(ipo_date <= day <= next_trade_date for day in trading_dates)  # type: ignore[operator]
            if ipo_date is not None and ipo_date >= trading_dates[0]  # type: ignore[operator]
            else 6
        )
        if str(security["status"]) != "listed":
            applicability = RegulationApplicability.NOT_APPLICABLE
            reason = "security_not_listed"
        elif name is None:
            applicability = RegulationApplicability.INSUFFICIENT_DATA
            reason = "missing_security_name_history"
        elif is_st or normalized_name.startswith(("ST", "*ST")):
            applicability = RegulationApplicability.NOT_APPLICABLE
            reason = "st_security_excluded"
        elif listing_days <= 5:
            applicability = RegulationApplicability.NOT_APPLICABLE
            reason = "no_limit_initial_listing_stage"
        elif next_reference is None:
            applicability = RegulationApplicability.INSUFFICIENT_DATA
            reason = "missing_next_day_reference_price"
        else:
            applicability = RegulationApplicability.APPLICABLE
            reason = None

        daily_returns: list[RegulationDailyReturn] = []
        previous_stock_close: Decimal | None = None
        previous_benchmark_close: Decimal | None = None
        for current_date in trading_dates:
            stock_bar = bars_by_key.get((symbol, current_date))
            benchmark_bar = bars_by_key.get((benchmark_symbol, current_date))
            stock_close = _decimal(stock_bar["close"]) if stock_bar is not None else None
            benchmark_close = (
                _decimal(benchmark_bar["close"]) if benchmark_bar is not None else None
            )
            stock_previous = (
                _decimal(stock_bar["previous_close"]) if stock_bar is not None else None
            )
            if stock_previous is None:
                stock_previous = _reference_price(
                    previous_stock_close,
                    tuple(capital_by_key.get((symbol, current_date), ())),
                )
            benchmark_previous = (
                _decimal(benchmark_bar["previous_close"]) if benchmark_bar is not None else None
            )
            if benchmark_previous is None:
                benchmark_previous = previous_benchmark_close
            indicator = indicators_by_key.get((symbol, current_date))
            daily_returns.append(
                RegulationDailyReturn(
                    trade_date=current_date,
                    stock_close=stock_close,
                    stock_reference_previous_close=stock_previous,
                    stock_return=(
                        stock_close / stock_previous - Decimal(1)
                        if stock_close is not None and stock_previous is not None
                        else None
                    ),
                    benchmark_close=benchmark_close,
                    benchmark_previous_close=benchmark_previous,
                    benchmark_return=(
                        benchmark_close / benchmark_previous - Decimal(1)
                        if benchmark_close is not None and benchmark_previous is not None
                        else None
                    ),
                    turnover_rate_pct=(
                        _decimal(indicator["turnover_rate_pct"]) if indicator is not None else None
                    ),
                )
            )
            if stock_close is not None:
                previous_stock_close = stock_close
            if benchmark_close is not None:
                previous_benchmark_close = benchmark_close

        price_limit: DailyPriceLimit | None = None
        if applicability is RegulationApplicability.APPLICABLE and next_reference is not None:
            limit_rule = price_limit_rule(
                exchange,
                next_trade_date,
                board="gem" if segment is RegulationSegment.GEM else "mainboard",
            )
            lower, upper = calculate_price_limit_range(
                next_reference, limit_rule.regular_ratio, limit_rule.price_tick
            )
            price_limit = DailyPriceLimit(
                symbol=symbol,
                trade_date=next_trade_date,
                previous_close=next_reference,
                upper_limit=upper,
                lower_limit=lower,
                limit_ratio=limit_rule.regular_ratio,
                price_tick=limit_rule.price_tick,
                is_st=False,
                rule_version=limit_rule.rule_version,
                algorithm_version="price-limit.v1",
            )
        symbol_events = tuple(sorted(events_by_symbol[symbol], key=lambda item: item.observed_at))
        candidates.append(
            RegulationCandidate(
                symbol=symbol,
                exchange=exchange,
                segment=segment,
                applicability=applicability,
                applicability_reason=reason,
                daily_returns=tuple(daily_returns),
                events=symbol_events,
                abnormal_reset_date=max(
                    (
                        event.effective_reset_date
                        for event in symbol_events
                        if event.event_level is RegulationRuleLevel.ABNORMAL
                        and event.effective_reset_date is not None
                    ),
                    default=None,
                ),
                serious_reset_date=max(
                    (
                        event.effective_reset_date
                        for event in symbol_events
                        if event.event_level is RegulationRuleLevel.SERIOUS_ABNORMAL
                        and event.effective_reset_date is not None
                    ),
                    default=None,
                ),
                next_day_reference_price=next_reference,
                next_day_price_limit=price_limit,
            )
        )

    observed = tuple(event.observed_at for values in events_by_symbol.values() for event in values)
    event_watermark = max(
        observed,
        default=datetime.combine(trade_date, time.min, ZoneInfo("Asia/Shanghai")),
    )
    rule_payload = "|".join(repr(rule) for rule in sorted(rules, key=lambda item: item.rule_code))
    return RegulationCalculationInput(
        trade_date=trade_date,
        next_trade_date=next_trade_date,
        algorithm_version=REGULATION_ALGORITHM_VERSION,
        scenario_config_version=REGULATION_SCENARIO_CONFIG_VERSION,
        active_rules=rules,
        trading_dates=trading_dates,
        candidates=tuple(sorted(candidates, key=lambda item: item.symbol)),
        rule_set_hash=hashlib.sha256(rule_payload.encode()).hexdigest(),
        market_watermark=_watermark((*bars, *indicators)),
        capital_watermark=_watermark(capital_rows),
        event_watermark=event_watermark,
    )


def _optional_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _rule_from_row(row: RowMapping | dict[str, object]) -> RegulationRule:
    return RegulationRule(
        rule_code=str(row["rule_code"]),
        exchange=Exchange(str(row["exchange"])),
        segment=RegulationSegment(str(row["segment"])),
        level=RegulationRuleLevel(str(row["level"])),
        kind=RegulationRuleKind(str(row["kind"])),
        direction=RegulationDirection(str(row["direction"])),
        window_days=row["window_days"],  # type: ignore[arg-type]
        threshold_pct=_optional_decimal(row["threshold_pct"]),
        comparison_window_days=row["comparison_window_days"],  # type: ignore[arg-type]
        ratio_threshold=_optional_decimal(row["ratio_threshold"]),
        secondary_threshold_pct=_optional_decimal(row["secondary_threshold_pct"]),
        count_window_days=row["count_window_days"],  # type: ignore[arg-type]
        required_count=row["required_count"],  # type: ignore[arg-type]
        counted_event_kind=(
            str(row["counted_event_kind"]) if row["counted_event_kind"] is not None else None
        ),
        reset_level=RegulationResetLevel(str(row["reset_level"])),
        benchmark_symbol=(
            str(row["benchmark_symbol"]) if row["benchmark_symbol"] is not None else None
        ),
        rule_set_version=str(row["rule_set_version"]),
        effective_date=row["effective_date"],  # type: ignore[arg-type]
        expire_date=row["expire_date"],  # type: ignore[arg-type]
        source_document=str(row["source_document"]),
        source_clause=str(row["source_clause"]),
        source_url=str(row["source_url"]),
        enabled=bool(row["enabled"]),
    )


def _validate_publish(run: RegulationCalculationRun, output: RegulationCalculationOutput) -> None:
    if run.trade_date != output.trade_date:
        raise ValueError("calculation output trade date does not match run")
    if run.next_trade_date != output.next_trade_date:
        raise ValueError("calculation output next trade date does not match run")
    if run.coverage != output.coverage:
        raise ValueError("calculation output coverage does not match run")
    if run.status not in (RegulationRunStatus.SUCCEEDED, RegulationRunStatus.PARTIAL):
        raise ValueError("only a successful or partial calculation can be published")
    expected_status = (
        RegulationRunStatus.PARTIAL
        if output.coverage.incomplete_count
        else RegulationRunStatus.SUCCEEDED
    )
    if run.status is not expected_status:
        raise ValueError("calculation run status does not match output coverage")
    status_symbols = {status.symbol for status in output.statuses}
    if len(status_symbols) != len(output.statuses):
        raise ValueError("calculation statuses must have unique symbols")
    if any(result.symbol not in status_symbols for result in output.rule_results):
        raise ValueError("rule result has no matching status")
    if any(warning.symbol not in status_symbols for warning in output.warnings):
        raise ValueError("warning has no matching status")


def _run_parameters(run: RegulationCalculationRun) -> dict[str, object]:
    return {
        "calculation_id": run.calculation_id,
        "trade_date": run.trade_date,
        "next_trade_date": run.next_trade_date,
        "status": run.status.value,
        "algorithm_version": run.algorithm_version,
        "rule_set_version": run.rule_set_version,
        "rule_set_hash": run.rule_set_hash,
        "scenario_config_version": run.scenario_config_version,
        "input_hash": run.input_hash,
        "market_watermark": run.market_watermark,
        "capital_watermark": run.capital_watermark,
        "event_watermark": run.event_watermark,
        "expected_count": run.coverage.expected_count,
        "complete_count": run.coverage.complete_count,
        "incomplete_count": run.coverage.incomplete_count,
        "not_applicable_count": run.coverage.not_applicable_count,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
    }


def _complete_run(connection: Connection, run: RegulationCalculationRun) -> None:
    result = connection.execute(_UPDATE_RUN_COMPLETE, _run_parameters(run))
    if result.rowcount != 1:
        raise ValueError("running regulation calculation was not found")


_SELECT_BARS = text("""
select symbol, trade_date, close, previous_close, is_st, ingestion_id
from core.daily_bar
where symbol in :symbols and trade_date between :start_date and :trade_date
order by symbol, trade_date
""").bindparams(bindparam("symbols", expanding=True))

_SELECT_INDICATORS = text("""
select symbol, trade_date, turnover_rate_pct, ingestion_id
from core.stock_daily_indicator
where symbol in :symbols and trade_date between :start_date and :trade_date
order by symbol, trade_date
""").bindparams(bindparam("symbols", expanding=True))

_SELECT_EVENTS = text("""
select symbol, exchange, segment, event_type, event_level, direction,
       period_start_date, period_end_date, published_at, effective_reset_date,
       source_event_id, source_title, source_url, source_content_hash,
       source_code, explicit_rule_codes, observed_at
from regulation.event
where symbol in :symbols and period_end_date <= :trade_date
order by symbol, observed_at, source_code, source_event_id
""").bindparams(bindparam("symbols", expanding=True))

_SELECT_CAPITAL_EVENTS = text("""
select symbol, ex_date, 'distribution' as event_kind, status,
       cash_dividend_per_share, bonus_share_ratio, transfer_share_ratio,
       null::numeric as rights_ratio, null::numeric as rights_price, ingestion_id
from capital.distribution
where symbol in :symbols and ex_date between :start_date and :next_trade_date
union all
select symbol, ex_date, 'rights_issue' as event_kind, null::text as status,
       null::numeric, null::numeric, null::numeric,
       rights_ratio, rights_price, ingestion_id
from capital.rights_issue
where symbol in :symbols and ex_date between :start_date and :next_trade_date
order by symbol, ex_date, event_kind
""").bindparams(bindparam("symbols", expanding=True))


_INSERT_RUNNING_RUN = text("""
insert into regulation.calculation_run (
    calculation_id, trade_date, next_trade_date, status, algorithm_version,
    rule_set_version, rule_set_hash, scenario_config_version, input_hash,
    market_watermark, capital_watermark, event_watermark, expected_count,
    complete_count, incomplete_count, not_applicable_count, started_at, completed_at
) values (
    :calculation_id, :trade_date, :next_trade_date, :status, :algorithm_version,
    :rule_set_version, :rule_set_hash, :scenario_config_version, :input_hash,
    :market_watermark, :capital_watermark, :event_watermark, :expected_count,
    :complete_count, :incomplete_count, :not_applicable_count, :started_at, :completed_at
)
on conflict (trade_date, input_hash) do update set
    status = 'RUNNING',
    algorithm_version = excluded.algorithm_version,
    rule_set_version = excluded.rule_set_version,
    rule_set_hash = excluded.rule_set_hash,
    scenario_config_version = excluded.scenario_config_version,
    market_watermark = excluded.market_watermark,
    capital_watermark = excluded.capital_watermark,
    event_watermark = excluded.event_watermark,
    expected_count = excluded.expected_count,
    complete_count = excluded.complete_count,
    incomplete_count = excluded.incomplete_count,
    not_applicable_count = excluded.not_applicable_count,
    started_at = excluded.started_at,
    completed_at = null
where calculation_run.status = 'FAILED'
returning calculation_id
""")

_UPDATE_RUN_COMPLETE = text("""
update regulation.calculation_run
set status = :status,
    expected_count = :expected_count,
    complete_count = :complete_count,
    incomplete_count = :incomplete_count,
    not_applicable_count = :not_applicable_count,
    completed_at = :completed_at
where calculation_id = :calculation_id and status = 'RUNNING'
""")

_INSERT_STATUS = text("""
insert into regulation.status (
    calculation_id, trade_date, symbol, exchange, segment, applicability,
    applicability_reason, data_completeness, calculated_state, announced_state,
    close, stock_daily_return_pct, benchmark_symbol, benchmark_close,
    benchmark_daily_return_pct, daily_deviation_pct, abnormal_count_10d,
    abnormal_count_10d_up, abnormal_count_10d_down, abnormal_reset_date,
    serious_reset_date
) values (
    :calculation_id, :trade_date, :symbol, :exchange, :segment, :applicability,
    :applicability_reason, :data_completeness, :calculated_state, :announced_state,
    :close, :stock_daily_return_pct, :benchmark_symbol, :benchmark_close,
    :benchmark_daily_return_pct, :daily_deviation_pct, :abnormal_count_10d,
    :abnormal_count_10d_up, :abnormal_count_10d_down, :abnormal_reset_date,
    :serious_reset_date
)
""")

_INSERT_RULE_RESULT = text("""
insert into regulation.rule_result (
    calculation_id, symbol, rule_id, evaluation_state, triggered,
    window_start_date, window_end_date, observed_window_days, current_value,
    threshold, distance, secondary_current_value, secondary_threshold,
    event_count, required_count, selected_reset_date, data_completeness,
    incomplete_reason
) values (
    :calculation_id, :symbol, :rule_id, :evaluation_state, :triggered,
    :window_start_date, :window_end_date, :observed_window_days, :current_value,
    :threshold, :distance, :secondary_current_value, :secondary_threshold,
    :event_count, :required_count, :selected_reset_date, :data_completeness,
    :incomplete_reason
)
""")

_INSERT_WARNING = text("""
insert into regulation.warning (
    calculation_id, trade_date, next_trade_date, symbol, rule_id, warning_type,
    level, direction, current_value, threshold, distance, scenario_code,
    scenario_index_pct, next_day_reference_price, raw_trigger_price,
    next_day_trigger_price, next_day_trigger_pct, price_limit_ratio,
    lower_limit_price, upper_limit_price, reachability, window_start_date,
    window_end_date, requires_official_event_confirmation,
    message_template_code, message
) values (
    :calculation_id, :trade_date, :next_trade_date, :symbol, :rule_id, :warning_type,
    :level, :direction, :current_value, :threshold, :distance, :scenario_code,
    :scenario_index_pct, :next_day_reference_price, :raw_trigger_price,
    :next_day_trigger_price, :next_day_trigger_pct, :price_limit_ratio,
    :lower_limit_price, :upper_limit_price, :reachability, :window_start_date,
    :window_end_date, :requires_official_event_confirmation,
    :message_template_code, :message
)
""")
