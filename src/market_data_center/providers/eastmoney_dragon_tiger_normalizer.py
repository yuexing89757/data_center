"""Normalize immutable EastMoney DragonTiger Raw into provider-neutral facts."""

import json
import re
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from hashlib import sha256

from market_data_center.domain.dragon_tiger import (
    DragonTigerAmountPeriod,
    DragonTigerAmountPeriodBasis,
    DragonTigerEventDraft,
    DragonTigerNormalizationResult,
    DragonTigerReason,
    DragonTigerReasonType,
    DragonTigerSourceFinding,
    DragonTigerTriggerWindow,
    DragonTigerWindowBasis,
    SeatTradeRecord,
)
from market_data_center.domain.ingestion import QualitySeverity
from market_data_center.providers.contracts import ProviderError, RawRow

SCHEMA_VERSION = "eastmoney.dragon_tiger.v3"
SUPPORTED_REPLAY_SCHEMAS = frozenset(
    {
        "eastmoney.trading_billboard.v1",
        "eastmoney.dragon_tiger.v2",
        SCHEMA_VERSION,
    }
)

type SourceRow = Mapping[str, object]


def normalize_eastmoney_dragon_tiger_raw(
    rows: Sequence[RawRow], schema_version: str
) -> DragonTigerNormalizationResult:
    if schema_version not in SUPPORTED_REPLAY_SCHEMAS:
        raise ProviderError("unsupported EastMoney DragonTiger Raw schema")
    grouped: dict[str, list[SourceRow]] = {
        "summary": [],
        "buy_seat": [],
        "sell_seat": [],
    }
    for raw in rows:
        kind = raw.get("record_kind")
        if kind not in grouped:
            raise ProviderError("EastMoney DragonTiger Raw record kind is invalid")
        try:
            payload = json.loads(raw["payload_json"], parse_float=Decimal)
        except (KeyError, json.JSONDecodeError, InvalidOperation) as error:
            raise ProviderError("EastMoney DragonTiger Raw payload is invalid") from error
        if not isinstance(payload, Mapping):
            raise ProviderError("EastMoney DragonTiger Raw payload is not an object")
        grouped[kind].append(payload)
    return _normalize(grouped["summary"], grouped["buy_seat"], grouped["sell_seat"])


def map_eastmoney_trigger_window(reason: str, trade_date: date) -> DragonTigerTriggerWindow:
    if re.search(r"最近3个有成交的交易日", reason):
        return DragonTigerTriggerWindow(
            basis=DragonTigerWindowBasis.SECURITY_TRADED_SESSIONS,
            session_count=3,
            occurrence_count=None,
            start_date=None,
            end_date=trade_date,
        )
    three_day = re.search(r"连续(?:三个|3个)交易日", reason)
    if three_day is not None:
        return _market_window(trade_date, 3, None)
    multi_day = re.search(r"连续(10|30)个交易日", reason)
    if multi_day is not None:
        occurrences_match = re.search(r"交易日内(\d+)次", reason)
        occurrences = int(occurrences_match.group(1)) if occurrences_match else None
        return _market_window(trade_date, int(multi_day.group(1)), occurrences)
    if "交易日" in reason or re.search(
        r"(?:最近|连续).{0,16}日|[二两三四五六七八九十2-9]+个?日内", reason
    ):
        raise ProviderError("DT_PERIOD_MAPPING_UNSUPPORTED")
    return _market_window(trade_date, 1, None)


def _market_window(
    trade_date: date, sessions: int, occurrences: int | None
) -> DragonTigerTriggerWindow:
    return DragonTigerTriggerWindow(
        basis=DragonTigerWindowBasis.MARKET_SESSIONS,
        session_count=sessions,
        occurrence_count=occurrences,
        start_date=trade_date if sessions == 1 else None,
        end_date=trade_date,
    )


def _amount_period(trigger: DragonTigerTriggerWindow) -> DragonTigerAmountPeriod:
    if (
        trigger.basis is DragonTigerWindowBasis.MARKET_SESSIONS
        and trigger.session_count in {1, 3}
        and trigger.occurrence_count is None
    ):
        return DragonTigerAmountPeriod(
            basis=DragonTigerAmountPeriodBasis.MARKET_SESSIONS,
            session_count=trigger.session_count,
            start_date=trigger.end_date if trigger.session_count == 1 else None,
            end_date=trigger.end_date,
        )
    return DragonTigerAmountPeriod(
        basis=DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED,
        session_count=None,
        start_date=None,
        end_date=None,
    )


def _normalize(
    summaries: Sequence[SourceRow],
    buy_rows: Sequence[SourceRow],
    sell_rows: Sequence[SourceRow],
) -> DragonTigerNormalizationResult:
    if not summaries:
        raise ProviderError("EastMoney DragonTiger contains no stock summaries")
    findings: list[DragonTigerSourceFinding] = []
    summaries = _deduplicate_rows(summaries, "summary", findings)
    buy_rows = _deduplicate_rows(buy_rows, "buy_seat", findings)
    sell_rows = _deduplicate_rows(sell_rows, "sell_seat", findings)
    buy_rows = _filter_zero_placeholders(buy_rows, "buy_seat", findings)
    sell_rows = _filter_zero_placeholders(sell_rows, "sell_seat", findings)
    by_side = {"buy": _group_details(buy_rows), "sell": _group_details(sell_rows)}
    events: list[DragonTigerEventDraft] = []
    seen_events: set[str] = set()
    for summary in summaries:
        event_id = _required_text(summary, "TRADE_ID")
        if event_id in seen_events:
            raise ProviderError("EastMoney DragonTiger has duplicate event identity")
        seen_events.add(event_id)
        symbol = _stock_symbol(summary)
        if symbol is None:
            continue
        trade_date = _source_date(summary)
        buy = by_side["buy"].get(event_id, ())
        sell = by_side["sell"].get(event_id, ())
        if not buy and not sell:
            raise ProviderError("EastMoney DragonTiger contains no seat disclosure")
        if not buy or not sell:
            findings.append(
                DragonTigerSourceFinding(
                    rule_code="DT_DISCLOSURE_SIDE_MISSING",
                    severity=QualitySeverity.WARNING,
                    source_event_id=event_id,
                    report_kind="sell_seat" if not sell else "buy_seat",
                    occurrence_count=1,
                    filtered_count=0,
                )
            )
        ordered_buy = tuple(sorted(buy, key=lambda row: _seat_sort_key(row, "buy")))
        ordered_sell = tuple(sorted(sell, key=lambda row: _seat_sort_key(row, "sell")))
        if len(ordered_buy) > 5 or len(ordered_sell) > 5:
            raise ProviderError("EastMoney DragonTiger allows at most five seats per side")
        reason_name = _required_text(summary, "EXPLANATION")
        trigger = map_eastmoney_trigger_window(reason_name, trade_date)
        reason_type = _reason_type(reason_name)
        reason = DragonTigerReason(
            reason_code=_reason_code(reason_type, trigger, reason_name),
            reason_name=reason_name,
            reason_type=reason_type,
            source_code="eastmoney",
            source_reason_code=_required_text(summary, "CHANGE_TYPE"),
            source_reason_name=reason_name,
        )
        events.append(
            DragonTigerEventDraft(
                source_record_id=event_id,
                symbol=symbol,
                trade_date=trade_date,
                trigger_window=trigger,
                amount_period=_amount_period(trigger),
                reason=reason,
                reason_name_raw=reason_name,
                close_price=_decimal(summary, "CLOSE_PRICE"),
                change_pct=_decimal(summary, "CHANGE_RATE"),
                turnover_amount=_decimal(summary, "ACCUM_AMOUNT"),
                turnover_rate=_decimal(summary, "TURNOVERRATE"),
                amplitude=_decimal(summary, "AMPLITUDE"),
                lhb_buy_amount=_decimal(summary, "BILLBOARD_BUY_AMT"),
                lhb_sell_amount=_decimal(summary, "BILLBOARD_SELL_AMT"),
                buy_disclosure_present=bool(buy),
                sell_disclosure_present=bool(sell),
                seat_trades=_merge_seat_rows(
                    event_id, symbol, trade_date, ordered_buy, ordered_sell
                ),
                source_code="eastmoney",
            )
        )
    if not events:
        raise ProviderError("EastMoney DragonTiger contains no accepted stock summaries")
    return DragonTigerNormalizationResult(tuple(events), tuple(findings))


def _deduplicate_rows(
    rows: Sequence[SourceRow],
    report_kind: str,
    findings: list[DragonTigerSourceFinding],
) -> tuple[SourceRow, ...]:
    unique: list[SourceRow] = []
    fingerprints: set[str] = set()
    duplicate_counts: dict[str, int] = {}
    for row in rows:
        fingerprint = sha256(_canonical_json(row).encode()).hexdigest()
        if fingerprint in fingerprints:
            event_id = _required_text(row, "TRADE_ID")
            duplicate_counts[event_id] = duplicate_counts.get(event_id, 0) + 1
            continue
        fingerprints.add(fingerprint)
        unique.append(row)
    findings.extend(
        DragonTigerSourceFinding(
            rule_code="DT_SOURCE_DUPLICATE_FILTERED",
            severity=QualitySeverity.WARNING,
            source_event_id=event_id,
            report_kind=report_kind,
            occurrence_count=count,
            filtered_count=count,
        )
        for event_id, count in sorted(duplicate_counts.items())
    )
    return tuple(unique)


def _filter_zero_placeholders(
    rows: Sequence[SourceRow],
    report_kind: str,
    findings: list[DragonTigerSourceFinding],
) -> tuple[SourceRow, ...]:
    accepted: list[SourceRow] = []
    filtered_counts: dict[str, int] = {}
    for row in rows:
        name = _required_text(row, "OPERATEDEPT_NAME")
        values = tuple(_decimal(row, field) for field in ("BUY", "SELL", "NET"))
        if name == "深股通投资者" and all(value == 0 for value in values):
            event_id = _required_text(row, "TRADE_ID")
            filtered_counts[event_id] = filtered_counts.get(event_id, 0) + 1
            continue
        accepted.append(row)
    findings.extend(
        DragonTigerSourceFinding(
            rule_code="DT_ZERO_ACTIVITY_PLACEHOLDER_FILTERED",
            severity=QualitySeverity.WARNING,
            source_event_id=event_id,
            report_kind=report_kind,
            occurrence_count=count,
            filtered_count=count,
        )
        for event_id, count in sorted(filtered_counts.items())
    )
    return tuple(accepted)


def _merge_seat_rows(
    event_id: str,
    symbol: str,
    trade_date: date,
    buy_rows: tuple[SourceRow, ...],
    sell_rows: tuple[SourceRow, ...],
) -> tuple[SeatTradeRecord, ...]:
    builders: dict[str, dict[str, object]] = {}
    order: list[str] = []
    for side, rows in (("buy", buy_rows), ("sell", sell_rows)):
        for rank, row in enumerate(rows, start=1):
            if _source_date(row) != trade_date or _stock_symbol(row) != symbol:
                raise ProviderError("EastMoney DragonTiger seat parent identity mismatch")
            name = _required_text(row, "OPERATEDEPT_NAME")
            code = _optional_text(row.get("OPERATEDEPT_CODE"))
            reliable_code = (
                code if code not in {None, "0"} and not _is_placeholder_seat_name(name) else None
            )
            fingerprint = sha256(_canonical_json(row).encode()).hexdigest()[:16]
            key = (
                f"seat:{reliable_code}"
                if reliable_code is not None
                else f"anonymous:{side}:{rank}:{fingerprint}"
            )
            if key not in builders:
                builders[key] = {
                    "name": name,
                    "code": reliable_code,
                    "buy": None,
                    "sell": None,
                    "buy_rank": None,
                    "sell_rank": None,
                }
                order.append(key)
            builder = builders[key]
            builder["buy"] = _coalesce_amount(builder["buy"], _decimal(row, "BUY"))
            builder["sell"] = _coalesce_amount(builder["sell"], _decimal(row, "SELL"))
            builder[f"{side}_rank"] = rank
    trades: list[SeatTradeRecord] = []
    for key in order:
        value = builders[key]
        name = str(value["name"])
        raw_code = value["code"]
        seat_source_key = str(raw_code) if raw_code is not None else None
        source_id = (
            f"{event_id}:seat:{seat_source_key}"
            if seat_source_key is not None
            else f"{event_id}:{key}"
        )
        trades.append(
            SeatTradeRecord(
                source_record_id=source_id,
                source_event_id=event_id,
                symbol=symbol,
                trade_date=trade_date,
                seat_id=None,
                seat_source_key=seat_source_key,
                seat_name_raw=name,
                buy_amount=value["buy"] if isinstance(value["buy"], Decimal) else None,
                sell_amount=value["sell"] if isinstance(value["sell"], Decimal) else None,
                buy_rank=value["buy_rank"] if isinstance(value["buy_rank"], int) else None,
                sell_rank=value["sell_rank"] if isinstance(value["sell_rank"], int) else None,
                is_institution=name == "机构专用",
                is_northbound=name in {"沪股通专用", "深股通专用", "北向资金专用"},
                source_code="eastmoney",
            )
        )
    return tuple(trades)


def _coalesce_amount(current: object, incoming: Decimal | None) -> Decimal | None:
    if current is None:
        return incoming
    if incoming is None:
        return current if isinstance(current, Decimal) else None
    if not isinstance(current, Decimal) or current != incoming:
        raise ProviderError("EastMoney DragonTiger seat amount conflicts across sides")
    return current


def _reason_type(reason: str) -> DragonTigerReasonType:
    if "换手率" in reason:
        return DragonTigerReasonType.TURNOVER
    if "振幅" in reason:
        return DragonTigerReasonType.AMPLITUDE
    if "ST" in reason.upper():
        return DragonTigerReasonType.ST
    if "连续涨停" in reason:
        return DragonTigerReasonType.CONTINUOUS_LIMIT
    if "偏离值" in reason:
        return DragonTigerReasonType.PRICE_DEVIATION
    return DragonTigerReasonType.OTHER


def _reason_code(
    reason_type: DragonTigerReasonType,
    trigger: DragonTigerTriggerWindow,
    reason_name: str,
) -> str:
    digest = sha256(reason_name.strip().encode()).hexdigest()[:12].upper()
    return (
        f"{reason_type.value}_{trigger.basis.value}_{trigger.session_count}_"
        f"{trigger.occurrence_count or 0}_{digest}"
    )


def _group_details(rows: Sequence[SourceRow]) -> dict[str, tuple[SourceRow, ...]]:
    grouped: dict[str, list[SourceRow]] = {}
    for row in rows:
        grouped.setdefault(_required_text(row, "TRADE_ID"), []).append(row)
    return {key: tuple(value) for key, value in grouped.items()}


def _seat_sort_key(row: SourceRow, side: str) -> tuple[object, ...]:
    amount = _decimal(row, "BUY" if side == "buy" else "SELL")
    return (
        amount is None,
        -(amount or Decimal(0)),
        _optional_text(row.get("OPERATEDEPT_CODE")) or "",
        _required_text(row, "OPERATEDEPT_NAME"),
        sha256(_canonical_json(row).encode()).hexdigest(),
    )


def _is_placeholder_seat_name(name: str) -> bool:
    return name in {
        "机构专用",
        "沪股通专用",
        "沪股通",
        "深股通专用",
        "深股通投资者",
        "北向资金专用",
        "机构",
        "机构投资者",
        "自然人",
        "中小投资者",
        "其他自然人",
    }


def _stock_symbol(row: SourceRow) -> str | None:
    secucode = _required_text(row, "SECUCODE").upper()
    code, separator, suffix = secucode.partition(".")
    if separator != "." or len(code) != 6 or not code.isdigit():
        raise ProviderError("EastMoney DragonTiger security identifier is invalid")
    if suffix == "SH" and code.startswith("6"):
        return f"SSE:{code}"
    if suffix == "SZ" and code.startswith(("0", "3")):
        return f"SZSE:{code}"
    if suffix == "BJ" and code.startswith(("4", "8", "9")):
        return f"BSE:{code}"
    return None


def _source_date(row: SourceRow) -> date:
    value = row.get("TRADE_DATE")
    if not isinstance(value, str) or len(value) < 10:
        raise ProviderError("EastMoney DragonTiger required TRADE_DATE is missing")
    try:
        return date.fromisoformat(value[:10])
    except ValueError as error:
        raise ProviderError("EastMoney DragonTiger response date is invalid") from error


def _required_text(row: SourceRow, field: str) -> str:
    value = _optional_text(row.get(field))
    if value is None:
        raise ProviderError(f"EastMoney DragonTiger required {field} is missing")
    return value


def _optional_text(value: object) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (str, int, Decimal)):
        text = str(value).strip()
        return text or None
    return None


def _decimal(row: SourceRow, field: str) -> Decimal | None:
    value = row.get(field)
    if value is None or value == "":
        return None
    if isinstance(value, (bool, float)):
        raise ProviderError(f"EastMoney DragonTiger {field} decimal is invalid")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ProviderError(f"EastMoney DragonTiger {field} decimal is invalid") from error
    if not parsed.is_finite():
        raise ProviderError(f"EastMoney DragonTiger {field} decimal is invalid")
    return parsed


def _canonical_json(value: object) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        raise ProviderError("EastMoney response contains binary floating-point data")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ProviderError("EastMoney response object key is invalid")
        return (
            "{"
            + ",".join(
                f"{_canonical_json(key)}:{_canonical_json(value[key])}" for key in sorted(value)
            )
            + "}"
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return "[" + ",".join(_canonical_json(item) for item in value) + "]"
    raise ProviderError("EastMoney response contains an unsupported JSON value")
