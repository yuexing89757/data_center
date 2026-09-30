"""Bounded monitor reads: frozen close counts plus explicitly conditional live prices."""

from dataclasses import asdict, replace
from datetime import date, datetime, time
from decimal import Decimal
from time import monotonic
from typing import Annotated, Any, Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import Field

from market_data_center.domain.regulation import (
    RegulationApplicability,
    RegulationDirection,
    RegulationRule,
    RegulationRuleKind,
)
from market_data_center.providers.regulation_quote import fetch_monitor_quotes
from market_data_center.public_api.models import (
    ApiModel,
    ApiTimestamp,
    ErrorResponse,
    RegulationCoverage,
)
from market_data_center.public_api.queries import (
    PublicQueryAmbiguous,
    PublicQueryConflict,
    PublicQueryError,
    PublicQueryInvalid,
    PublicQueryNotFound,
    PublicQueryService,
)
from market_data_center.regulation_monitor import project_monitor, project_monitor_close
from market_data_center.regulation_monitor_codec import decode_monitor_source, decode_monitor_state

SHANGHAI = ZoneInfo("Asia/Shanghai")
Code = Annotated[str, Field(pattern=r"^[0-9]{6}$")]
MonitorDate = Annotated[date, Field(ge=date(2026, 7, 6))]


class MonitorQueryRequest(ApiModel):
    trade_date: MonitorDate
    calculation_id: UUID
    codes: list[Code] = Field(min_length=1, max_length=50)
    scenario_index_pct: Literal["-2", "0", "2"] = "0"


class MonitorMetadata(ApiModel):
    schema_version: Literal["regulation-monitor.v1"]
    trade_date: date
    base_trade_date: date
    next_trade_date: date
    calculation_id: UUID
    algorithm_version: str
    rule_set_version: str
    generated_at: ApiTimestamp
    count_cutoff_date: date
    coverage: RegulationCoverage
    requested_count: int = Field(ge=0)
    found_count: int = Field(ge=0)
    missing_count: int = Field(ge=0)


class MonitorCandidate(ApiModel):
    symbol: str | None
    code: Code
    name: str | None
    candidate_basis: list[str]


class MonitorCandidatesResponse(MonitorMetadata):
    total: int = Field(ge=0)
    next_cursor: str | None
    candidate_basis: str
    items: list[MonitorCandidate]


class MonitorThresholdView(ApiModel):
    trade_date: date
    trigger_price: Decimal | None
    trigger_change_pct: Decimal | None
    reachability: str
    reference_price: Decimal | None
    scenario_index_pct: Decimal | None
    reset_branch: str
    window_start_date: date | None
    missing_reason: str | None


class MonitorRule(ApiModel):
    rule_code: str
    benchmark_symbol: str | None
    name: str
    level: str
    direction: str
    kind: str
    window_start_date: date | None
    window_end_date: date | None
    current_value: Decimal | None
    threshold: Decimal | None
    secondary_current_value: Decimal | None
    secondary_threshold: Decimal | None
    state: Literal[
        "CURRENT_REACHED",
        "TODAY_REACHABLE",
        "NEXT_REACHABLE",
        "NOT_REACHED",
        "INCOMPLETE",
        "NOT_APPLICABLE",
    ]
    today: MonitorThresholdView
    next_day: MonitorThresholdView
    eligible_count: int | None
    required_count: int | None
    missing_reasons: list[str]


class MonitorEvent(ApiModel):
    symbol: str
    trade_date: date
    level: str
    direction: str
    kind: str
    rule_codes: list[str]
    windows: list[tuple[date, date]]


class MonitorStock(ApiModel):
    symbol: str | None
    code: Code
    name: str | None
    segment: str | None = None
    applicability: str = "INSUFFICIENT_DATA"
    last_price: Decimal | None = None
    change_pct: Decimal | None = None
    calculated_price_abnormal_count_10d_up: int | None = None
    calculated_price_abnormal_count_10d_down: int | None = None
    calculated_turnover_count_10d: int | None = None
    official_price_abnormal_count_10d_up: int | None = None
    official_price_abnormal_count_10d_down: int | None = None
    official_coverage: str = "UNKNOWN"
    official_watermark: ApiTimestamp | None = None
    events: list[MonitorEvent] = Field(default_factory=list)
    rules: list[MonitorRule] = Field(default_factory=list)
    missing_reasons: list[str] = Field(default_factory=list)


class MonitorQueryResponse(MonitorMetadata):
    status: Literal["COMPLETE", "PARTIAL"]
    observation_mode: Literal["LIVE", "MIDDAY_PAUSED", "CLOSE_ESTIMATE", "CONFIRMED_CLOSE"]
    quote_observed_at: ApiTimestamp | None
    hypothetical_close_at: ApiTimestamp | None
    scenario_index_pct: Literal["-2", "0", "2"]
    reference_checked_at: ApiTimestamp | None = None
    items: list[MonitorStock]


router = APIRouter(
    prefix="/api/v1/regulation/monitor",
    tags=["市场数据"],
    responses={code: {"model": ErrorResponse} for code in (401, 404, 409, 422, 502, 503)},
)


def _metadata(raw: dict[str, Any], now: datetime) -> dict[str, Any]:
    return {
        **{key: raw[key] for key in MonitorMetadata.model_fields if key != "generated_at"},
        "generated_at": now,
    }


def _query_error(error: PublicQueryError) -> HTTPException:
    if isinstance(error, PublicQueryNotFound):
        return HTTPException(404, "monitor base batch is not published")
    if isinstance(error, PublicQueryConflict):
        return HTTPException(409, "monitor batch changed; reload candidates")
    if isinstance(error, (PublicQueryInvalid, PublicQueryAmbiguous)):
        return HTTPException(422, "monitor parameters or stock identity are invalid")
    return HTTPException(503, "monitor data service is unavailable")


@router.get(
    "/candidates",
    response_model=MonitorCandidatesResponse,
    summary="查询异动测算观察名单",
    description="读取固定收盘批次的观察名单。游标绑定日期、搜索及批次。不触发采集或写入。",
)
def candidates(
    request: Request,
    trade_date: Annotated[date, Query(ge=date(2026, 7, 6))],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
    query: Annotated[str | None, Query(max_length=80)] = None,
) -> MonitorCandidatesResponse:
    if trade_date > datetime.now(SHANGHAI).date():
        raise HTTPException(422, "future monitoring date is not supported")
    try:
        service = cast(PublicQueryService, request.app.state.query_service)
        raw = cast(
            dict[str, Any],
            service.query_regulation_monitor_candidates(trade_date, limit, cursor, query),
        )
        return MonitorCandidatesResponse(
            **_metadata(raw, datetime.now(SHANGHAI)),
            total=raw["total"],
            next_cursor=raw["next_cursor"],
            candidate_basis=raw["candidate_basis"],
            items=raw["items"],
        )
    except PublicQueryError as error:
        raise _query_error(error) from error
    except (ValueError, KeyError, TypeError) as error:
        raise HTTPException(503, "monitor candidate snapshot is invalid") from error


def _rule_name(rule: RegulationRule) -> str:
    direction = "上涨" if rule.direction is RegulationDirection.UP else "下跌"
    if rule.kind is RegulationRuleKind.TURNOVER_COMPOSITE:
        return "3日换手率放大且累计换手达到门槛"
    if rule.kind is RegulationRuleKind.EVENT_COUNT:
        return f"{rule.count_window_days}个交易日内{rule.required_count}次同向{direction}异动"
    return f"{rule.window_days}个交易日{direction}偏离达到{rule.threshold_pct}%"


@router.post(
    "/query",
    response_model=MonitorQueryResponse,
    summary="查询当前页规则、动态门槛及测算次数",
    description="有界查询最多50只股票。已确认次数冻结于收盘批次。实时门槛是条件测算而非预测。",
)
def query_monitor(request: Request, body: MonitorQueryRequest) -> MonitorQueryResponse:
    deadline = monotonic() + 8
    now = datetime.now(SHANGHAI)
    if body.trade_date > now.date():
        raise HTTPException(422, "future monitoring date is not supported")
    try:
        service = cast(PublicQueryService, request.app.state.query_service)
        raw = cast(
            dict[str, Any],
            service.query_regulation_monitor_inputs(
                body.trade_date, body.calculation_id, tuple(dict.fromkeys(body.codes))
            ),
        )
        return _project_response(raw, body, now, deadline)
    except PublicQueryError as error:
        raise _query_error(error) from error
    except (ValueError, KeyError, TypeError) as error:
        raise HTTPException(503, "monitor input snapshot is invalid") from error


def _project_response(
    raw: dict[str, Any], body: MonitorQueryRequest, now: datetime, deadline: float
) -> MonitorQueryResponse:
    if str(raw["calculation_id"]) != str(body.calculation_id) or str(raw["trade_date"]) != str(
        body.trade_date
    ):
        raise HTTPException(409, "monitor batch/date conflict")
    confirmed = bool(raw["is_confirmed_close"])
    if body.trade_date < now.date() and not confirmed:
        raise HTTPException(404, "historical close batch is not published")
    records = {i["code"]: i for i in raw["items"]}
    available = [i for i in raw["items"] if i.get("payload") is not None]
    source = decode_monitor_source(
        {**raw["source"], "candidates": [i["payload"]["candidate"] for i in available]}
    )
    published = {i["symbol"]: decode_monitor_state(i["payload"]["state"]) for i in available}
    mode: Literal["LIVE", "MIDDAY_PAUSED", "CLOSE_ESTIMATE", "CONFIRMED_CLOSE"]
    if confirmed:
        if source.trade_date != body.trade_date:
            raise HTTPException(409, "confirmed close date mismatch")
        states = tuple(
            decode_monitor_state(i["payload"]["previous_state"])
            for i in available
            if i["payload"].get("previous_state") is not None
        )
        source = replace(
            source,
            candidates=tuple(
                replace(c, next_day_reference_price=None)
                if records[c.symbol.split(":")[1]].get("next_day_reference_safe") is not True
                else c
                for c in source.candidates
            ),
        )
        projection = project_monitor_close(source, states, Decimal(body.scenario_index_pct))
        mode = "CONFIRMED_CLOSE"
    else:
        if source.next_trade_date != body.trade_date:
            raise HTTPException(409, "previous close does not precede requested trading day")
        states = tuple(published.values())
        source = replace(
            source,
            trade_date=body.trade_date,
            next_trade_date=date.fromisoformat(str(raw["next_trade_date"])),
            trading_dates=(*source.trading_dates, body.trade_date)[-30:],
            candidates=tuple(
                replace(
                    c,
                    applicability=RegulationApplicability(
                        records[c.symbol.split(":")[1]].get(
                            "target_applicability", "INSUFFICIENT_DATA"
                        )
                    ),
                    applicability_reason=records[c.symbol.split(":")[1]].get(
                        "target_applicability_reason"
                    )
                    or (
                        None
                        if records[c.symbol.split(":")[1]].get("target_applicability")
                        == "APPLICABLE"
                        else "target_applicability_unverified"
                    ),
                )
                for c in source.candidates
            ),
        )
        stocks = tuple(
            c.symbol
            for c in source.candidates
            if c.applicability is RegulationApplicability.APPLICABLE
        )
        segments = {c.segment for c in source.candidates if c.symbol in stocks}
        benchmarks = tuple(
            sorted(
                {
                    r.benchmark_symbol
                    for r in source.active_rules
                    if r.segment in segments and r.benchmark_symbol
                }
            )
        )
        quotes = fetch_monitor_quotes(stocks, benchmarks, deadline) if stocks else ()
        if stocks and not quotes:
            raise HTTPException(502, "monitor upstream quotes are unavailable")
        now = datetime.now(SHANGHAI)
        if now.date() != body.trade_date:
            raise HTTPException(409, "monitor trading date changed; reload candidates")
        projection = project_monitor(
            source,
            states,
            quotes,
            Decimal(body.scenario_index_pct),
            now,
            next_reference_safe_symbols=frozenset(
                i["symbol"] for i in available if i.get("next_day_reference_safe") is True
            ),
            unrestricted_shares={
                i["symbol"]: Decimal(str(i["unrestricted_shares"]))
                for i in available
                if i.get("unrestricted_shares") is not None
            },
        )
        mode = (
            "MIDDAY_PAUSED"
            if time(11, 30) < now.time() < time(13)
            else "CLOSE_ESTIMATE"
            if now.time() >= time(15)
            else "LIVE"
        )
    values = {(r.symbol, r.rule_code): r for r in projection.day.output.rule_results}
    statuses = {s.symbol: s for s in projection.day.output.statuses}
    rules = {r.rule_code: r for r in source.active_rules}
    by_symbol: dict[str, list[MonitorRule]] = {}
    for condition in projection.conditions:
        value = values[(condition.symbol, condition.rule_code)]
        rule = rules[condition.rule_code]
        state: Any = "NOT_REACHED"
        if condition.today.reachability == "NOT_APPLICABLE":
            state = "NOT_APPLICABLE"
        elif value.incomplete_reason or condition.today.reachability in (
            "INCOMPLETE",
            "INSUFFICIENT_DATA",
        ):
            state = "INCOMPLETE"
        elif value.triggered:
            state = "CURRENT_REACHED"
        elif condition.today.reachability == "REACHABLE":
            state = "TODAY_REACHABLE"
        elif condition.next_day.reachability == "REACHABLE":
            state = "NEXT_REACHABLE"
        reasons = sorted(
            {
                r
                for r in (
                    value.incomplete_reason,
                    condition.today.missing_reason,
                    condition.next_day.missing_reason,
                )
                if r
            }
        )
        by_symbol.setdefault(condition.symbol, []).append(
            MonitorRule(
                rule_code=rule.rule_code,
                benchmark_symbol=rule.benchmark_symbol,
                name=_rule_name(rule),
                level=rule.level,
                direction=rule.direction,
                kind=rule.kind,
                window_start_date=value.window_start_date,
                window_end_date=value.window_end_date,
                current_value=value.current_value,
                threshold=value.threshold,
                secondary_current_value=value.secondary_current_value,
                secondary_threshold=value.secondary_threshold,
                state=state,
                today=MonitorThresholdView(**asdict(condition.today)),
                next_day=MonitorThresholdView(**asdict(condition.next_day)),
                eligible_count=value.event_count,
                required_count=value.required_count,
                missing_reasons=reasons,
            )
        )
    items = []
    cutoff = date.fromisoformat(str(raw["count_cutoff_date"]))
    recent = set(tuple(day for day in source.trading_dates if day <= cutoff)[-10:])
    for code in dict.fromkeys(body.codes):
        record = records.get(code, {})
        symbol = record.get("symbol")
        stock = MonitorStock(
            code=code,
            symbol=symbol,
            name=record.get("name"),
            official_coverage=raw["official_coverage"],
            official_watermark=raw.get("official_watermark"),
        )
        if symbol not in published:
            stock.missing_reasons = ["outside_published_coverage"]
        else:
            published_state = published[symbol]
            current = statuses[symbol]
            stock.segment, stock.applicability = current.segment, current.applicability
            stock.last_price, stock.change_pct = current.close, current.stock_daily_return_pct
            stock.rules = by_symbol.get(symbol, [])
            stock.events = [
                MonitorEvent(**asdict(e)) for e in published_state.events if e.trade_date in recent
            ]
            stock.missing_reasons = list(published_state.missing_reasons)
            if current.applicability_reason:
                stock.missing_reasons.append(current.applicability_reason)
            if record.get("next_day_st_verified") is False and any(
                rule.next_day.trigger_price is not None for rule in stock.rules
            ):
                stock.missing_reasons.append("next_day_st_unverified")
            if published_state.complete:
                ordinary = [e for e in stock.events if e.level == "ABNORMAL"]
                stock.calculated_price_abnormal_count_10d_up = sum(
                    e.kind == "CUMULATIVE_DEVIATION" and e.direction == "UP" for e in ordinary
                )
                stock.calculated_price_abnormal_count_10d_down = sum(
                    e.kind == "CUMULATIVE_DEVIATION" and e.direction == "DOWN" for e in ordinary
                )
                if published_state.turnover_missing_dates is not None and not recent.intersection(
                    published_state.turnover_missing_dates
                ):
                    stock.calculated_turnover_count_10d = sum(
                        e.kind == "TURNOVER_COMPOSITE" for e in ordinary
                    )
                else:
                    stock.missing_reasons.append("incomplete_turnover_history")
        items.append(stock)
    missing_count = sum(i.symbol not in published for i in items)
    partial = any(
        i.missing_reasons
        or any(r.state == "INCOMPLETE" or r.next_day.reachability == "INCOMPLETE" for r in i.rules)
        for i in items
    )
    if monotonic() > deadline:
        raise HTTPException(503, "monitor request budget exceeded")
    meta = _metadata(raw, now)
    meta.update(
        requested_count=len(body.codes),
        found_count=len(items) - missing_count,
        missing_count=missing_count,
    )
    return MonitorQueryResponse(
        **meta,
        status="PARTIAL" if partial else "COMPLETE",
        observation_mode=mode,
        quote_observed_at=projection.hypothetical_close_at,
        hypothetical_close_at=projection.hypothetical_close_at,
        scenario_index_pct=body.scenario_index_pct,
        reference_checked_at=raw.get("reference_checked_at"),
        items=items,
    )
