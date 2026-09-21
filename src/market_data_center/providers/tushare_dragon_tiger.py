"""Tushare Adapter for provider-neutral DragonTiger facts."""

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
from market_data_center.providers.contracts import DragonTigerProviderBatch, ProviderError, RawRow
from market_data_center.providers.tushare import TushareClient

TOP_LIST_FIELDS = (
    "trade_date",
    "ts_code",
    "name",
    "close",
    "pct_change",
    "turnover_rate",
    "amount",
    "l_sell",
    "l_buy",
    "l_amount",
    "net_amount",
    "net_rate",
    "amount_rate",
    "float_values",
    "reason",
)
TOP_INST_FIELDS = (
    "trade_date",
    "ts_code",
    "exalter",
    "side",
    "buy",
    "buy_rate",
    "sell",
    "sell_rate",
    "net_buy",
    "reason",
)
SCHEMA_VERSION = "tushare.dragon_tiger.v1"
MAX_DETAIL_REPAIR_SYMBOLS = 100

type SourceRow = Mapping[str, object]


class TushareDragonTigerAdapter:
    source_code = "tushare"

    def __init__(self, client: TushareClient) -> None:
        self._client = client

    def fetch_dragon_tiger(self, trade_date: date) -> DragonTigerProviderBatch:
        params = {"trade_date": trade_date.strftime("%Y%m%d")}
        try:
            summaries = tuple(self._client.query("top_list", params=params, fields=TOP_LIST_FIELDS))
            details = list(self._client.query("top_inst", params=params, fields=TOP_INST_FIELDS))
            summary_symbols = {
                str(row.get("ts_code", ""))
                for row in summaries
                if not str(row.get("ts_code", "")).upper().endswith(".BJ")
                and not _is_non_stock_row(row)
            }
            detail_symbols = {str(row.get("ts_code", "")) for row in details}
            missing_symbols = sorted(summary_symbols - detail_symbols)
            if len(missing_symbols) > MAX_DETAIL_REPAIR_SYMBOLS:
                raise ProviderError("Tushare DragonTiger bulk detail response is incomplete")
            for symbol in missing_symbols:
                details.extend(
                    self._client.query(
                        "top_inst",
                        params={**params, "ts_code": symbol},
                        fields=TOP_INST_FIELDS,
                    )
                )
        except Exception as error:
            raise ProviderError("Tushare DragonTiger request failed") from error
        detail_rows = tuple(details)
        raw_rows = tuple(
            [_raw_row("summary", index, row) for index, row in enumerate(summaries)]
            + [_raw_row("seat", index, row) for index, row in enumerate(detail_rows)]
        )
        return DragonTigerProviderBatch(
            raw_rows=raw_rows,
            request_params={
                "trade_date": trade_date.isoformat(),
                "apis": ["top_list", "top_inst"],
                "source_counts": {
                    "top_list": len(summaries),
                    "top_inst": len(detail_rows),
                },
            },
            schema_version=SCHEMA_VERSION,
            normalization_factory=lambda: _normalize(summaries, detail_rows, trade_date),
        )


def normalize_tushare_dragon_tiger_raw(
    rows: Sequence[RawRow], schema_version: str
) -> DragonTigerNormalizationResult:
    if schema_version != SCHEMA_VERSION:
        raise ProviderError("unsupported Tushare DragonTiger Raw schema")
    grouped: dict[str, list[SourceRow]] = {"summary": [], "seat": []}
    for raw in rows:
        kind = raw.get("record_kind")
        if kind not in grouped:
            raise ProviderError("Tushare DragonTiger Raw record kind is invalid")
        try:
            payload = json.loads(raw["payload_json"], parse_float=Decimal)
        except (KeyError, json.JSONDecodeError, InvalidOperation) as error:
            raise ProviderError("Tushare DragonTiger Raw payload is invalid") from error
        if not isinstance(payload, Mapping):
            raise ProviderError("Tushare DragonTiger Raw payload is not an object")
        grouped[kind].append(payload)
    if not grouped["summary"]:
        raise ProviderError("Tushare DragonTiger Raw contains no summary rows")
    requested_date = _source_date(grouped["summary"][0])
    return _normalize(tuple(grouped["summary"]), tuple(grouped["seat"]), requested_date)


def _normalize(
    summaries: tuple[SourceRow, ...],
    details: tuple[SourceRow, ...],
    requested_date: date,
) -> DragonTigerNormalizationResult:
    if not summaries or not details:
        raise ProviderError("Tushare DragonTiger requires both summary and seat rows")
    detail_groups: dict[tuple[date, str, str], list[SourceRow]] = {}
    findings: list[DragonTigerSourceFinding] = []
    filtered_counts: dict[tuple[tuple[date, str, str], str, str], int] = {}
    for row in details:
        key = (_source_date(row), _symbol(row), _required_text(row, "reason"))
        if key[1].startswith("BSE:") or _is_non_stock_row(row):
            rule_code = (
                "DT_BSE_SECURITY_FILTERED"
                if key[1].startswith("BSE:")
                else "DT_NON_STOCK_SECURITY_FILTERED"
            )
            filter_key = (key, "top_inst", rule_code)
            filtered_counts[filter_key] = filtered_counts.get(filter_key, 0) + 1
            continue
        buy_amount = _decimal(row, "buy")
        sell_amount = _decimal(row, "sell")
        if (buy_amount is None or buy_amount == 0) and (sell_amount is None or sell_amount == 0):
            filter_key = (key, "top_inst", "DT_ZERO_ACTIVITY_PLACEHOLDER_FILTERED")
            filtered_counts[filter_key] = filtered_counts.get(filter_key, 0) + 1
            continue
        detail_groups.setdefault(key, []).append(row)
    for key, rows in tuple(detail_groups.items()):
        deduplicated, filtered = _deduplicate_truncated_seat_aliases(rows)
        detail_groups[key] = deduplicated
        if filtered:
            filter_key = (key, "top_inst", "DT_SOURCE_DUPLICATE_FILTERED")
            filtered_counts[filter_key] = filtered_counts.get(filter_key, 0) + filtered
    summary_rows: dict[tuple[date, str, str], SourceRow] = {}
    for summary in summaries:
        trade_date = _source_date(summary)
        if trade_date != requested_date:
            raise ProviderError("Tushare DragonTiger response date mismatch")
        key = (trade_date, _symbol(summary), _required_text(summary, "reason"))
        if key[1].startswith("BSE:") or _is_non_stock_row(summary):
            rule_code = (
                "DT_BSE_SECURITY_FILTERED"
                if key[1].startswith("BSE:")
                else "DT_NON_STOCK_SECURITY_FILTERED"
            )
            filter_key = (key, "top_list", rule_code)
            filtered_counts[filter_key] = filtered_counts.get(filter_key, 0) + 1
            continue
        previous = summary_rows.get(key)
        if previous is None:
            summary_rows[key] = summary
            continue
        previous_without_name = {
            field: value for field, value in previous.items() if field != "name"
        }
        candidate_without_name = {
            field: value for field, value in summary.items() if field != "name"
        }
        if _canonical_json(previous_without_name) == _canonical_json(candidate_without_name):
            rule_code = "DT_SOURCE_DUPLICATE_FILTERED"
        else:
            preferred = _prefer_precise_summary(previous, summary)
            if preferred is None:
                raise ProviderError("Tushare DragonTiger conflicting duplicate summary")
            summary_rows[key] = preferred
            rule_code = "DT_SOURCE_ROUNDED_DUPLICATE_FILTERED"
        filter_key = (key, "top_list", rule_code)
        filtered_counts[filter_key] = filtered_counts.get(filter_key, 0) + 1
    distinct_summary_keys = set(summary_rows)
    matched_detail_keys: set[tuple[date, str, str]] = set()
    events: list[DragonTigerEventDraft] = []
    for key, summary in summary_rows.items():
        trade_date, symbol, reason_name = key
        source_record_id = _event_id(key)
        source_details = detail_groups.get(key)
        if not source_details:
            candidates = [
                (detail_key, rows)
                for detail_key, rows in detail_groups.items()
                if detail_key[:2] == key[:2]
            ]
            matching_aliases = [
                candidate
                for candidate in candidates
                if _normalized_reason_alias(candidate[0][2])
                == _normalized_reason_alias(reason_name)
            ]
            if not matching_aliases:
                matching_aliases = [
                    candidate
                    for candidate in candidates
                    if _reason_alias_signature(candidate[0][2], trade_date)
                    == _reason_alias_signature(reason_name, trade_date)
                ]
            if len(matching_aliases) == 1:
                candidates = matching_aliases
            symbol_summary_keys = {
                summary_key for summary_key in distinct_summary_keys if summary_key[:2] == key[:2]
            }
            if len(candidates) != 1 or (len(symbol_summary_keys) != 1 and not matching_aliases):
                raise ProviderError("Tushare DragonTiger detail cannot join a summary event")
            detail_key, source_details = candidates[0]
            findings.append(
                DragonTigerSourceFinding(
                    rule_code="DT_TUSHARE_REASON_ALIAS_JOINED",
                    severity=QualitySeverity.WARNING,
                    source_event_id=source_record_id,
                    report_kind="top_inst",
                    occurrence_count=len(source_details),
                    filtered_count=0,
                )
            )
            matched_detail_keys.add(detail_key)
        else:
            matched_detail_keys.add(key)
        trigger = _trigger_window(reason_name, trade_date)
        reason_type = _reason_type(reason_name)
        reason = DragonTigerReason(
            reason_code=_reason_code(reason_type, trigger, reason_name),
            reason_name=reason_name,
            reason_type=reason_type,
            source_code="tushare",
            source_reason_code=sha256(reason_name.encode("utf-8")).hexdigest()[:16],
            source_reason_name=reason_name,
        )
        events.append(
            DragonTigerEventDraft(
                source_record_id=source_record_id,
                symbol=symbol,
                trade_date=trade_date,
                trigger_window=trigger,
                amount_period=_amount_period(trigger),
                reason=reason,
                reason_name_raw=reason_name,
                close_price=_decimal(summary, "close"),
                change_pct=_decimal(summary, "pct_change"),
                turnover_amount=_decimal(summary, "amount"),
                turnover_rate=_decimal(summary, "turnover_rate"),
                amplitude=None,
                lhb_buy_amount=_decimal(summary, "l_buy"),
                lhb_sell_amount=_decimal(summary, "l_sell"),
                buy_disclosure_present=True,
                sell_disclosure_present=True,
                seat_trades=_seat_trades(source_record_id, symbol, trade_date, source_details),
                source_code="tushare",
            )
        )
    unmatched = set(detail_groups) - matched_detail_keys
    for key in sorted(unmatched):
        findings.append(
            DragonTigerSourceFinding(
                rule_code="DT_TUSHARE_UNMATCHED_DETAIL_FILTERED",
                severity=QualitySeverity.WARNING,
                source_event_id=_event_id(key),
                report_kind="top_inst",
                occurrence_count=len(detail_groups[key]),
                filtered_count=len(detail_groups[key]),
            )
        )
    for (key, report_kind, rule_code), count in sorted(filtered_counts.items()):
        findings.append(
            DragonTigerSourceFinding(
                rule_code=rule_code,
                severity=QualitySeverity.WARNING,
                source_event_id=_event_id(key),
                report_kind=report_kind,
                occurrence_count=count,
                filtered_count=count,
            )
        )
    return DragonTigerNormalizationResult(tuple(events), tuple(findings))


def _seat_trades(
    event_id: str,
    symbol: str,
    trade_date: date,
    rows: Sequence[SourceRow],
) -> tuple[SeatTradeRecord, ...]:
    by_side: dict[str, list[SourceRow]] = {"0": [], "1": []}
    for row in rows:
        side = _required_text(row, "side")
        if side not in by_side:
            raise ProviderError("Tushare DragonTiger seat side is invalid")
        by_side[side].append(row)
    ordered = {
        "0": sorted(by_side["0"], key=lambda row: _seat_sort_key(row, "buy")),
        "1": sorted(by_side["1"], key=lambda row: _seat_sort_key(row, "sell")),
    }
    if len(ordered["0"]) > 5 or len(ordered["1"]) > 5:
        raise ProviderError("Tushare DragonTiger allows at most five seats per side")
    result: list[SeatTradeRecord] = []
    for side, side_rows in ordered.items():
        for rank, row in enumerate(side_rows, start=1):
            name = _required_text(row, "exalter")
            digest = sha256(_canonical_json(row).encode("utf-8")).hexdigest()[:16]
            result.append(
                SeatTradeRecord(
                    source_record_id=f"{event_id}:{side}:{rank}:{digest}",
                    source_event_id=event_id,
                    symbol=symbol,
                    trade_date=trade_date,
                    seat_id=None,
                    seat_source_key=None,
                    seat_name_raw=name,
                    buy_amount=_decimal(row, "buy"),
                    sell_amount=_decimal(row, "sell"),
                    buy_rank=rank if side == "0" else None,
                    sell_rank=rank if side == "1" else None,
                    is_institution=name == "机构专用",
                    is_northbound=name in {"沪股通专用", "深股通专用", "北向资金专用"},
                    source_code="tushare",
                )
            )
    return tuple(result)


def _deduplicate_truncated_seat_aliases(
    rows: Sequence[SourceRow],
) -> tuple[list[SourceRow], int]:
    groups: dict[str, list[SourceRow]] = {}
    for row in rows:
        facts = {field: value for field, value in row.items() if field != "exalter"}
        groups.setdefault(_canonical_json(facts), []).append(row)
    result: list[SourceRow] = []
    filtered = 0
    for same_facts in groups.values():
        names = [_required_text(row, "exalter") for row in same_facts]
        if len(same_facts) == 2 and _is_truncated_seat_alias(*names):
            result.append(max(same_facts, key=lambda row: len(_required_text(row, "exalter"))))
            filtered += 1
        else:
            result.extend(same_facts)
    return result, filtered


def _is_truncated_seat_alias(first: str, second: str) -> bool:
    shorter, longer = sorted((first, second), key=len)
    return longer == f"{shorter}营业部"


def _prefer_precise_summary(first: SourceRow, second: SourceRow) -> SourceRow | None:
    amount_fields = {"l_buy", "l_sell", "l_amount", "net_amount"}

    def is_rounded(row: SourceRow) -> bool:
        values = [_decimal(row, field) for field in amount_fields]
        return all(value is not None and value % Decimal(100) == 0 for value in values)

    first_rounded = is_rounded(first)
    second_rounded = is_rounded(second)
    amendment_fields = {
        "name",
        "float_values",
        "turnover_rate",
        "l_buy",
        "l_sell",
        "l_amount",
        "net_amount",
        "net_rate",
        "amount_rate",
    }
    first_stable = {field: value for field, value in first.items() if field not in amendment_fields}
    second_stable = {
        field: value for field, value in second.items() if field not in amendment_fields
    }
    if _canonical_json(first_stable) == _canonical_json(second_stable):

        def amendment_rank(row: SourceRow) -> tuple[int, Decimal]:
            return (
                sum(row.get(field) is not None for field in amendment_fields),
                _decimal(row, "l_amount") or Decimal(-1),
            )

        if first_rounded == second_rounded:
            first_rank = amendment_rank(first)
            second_rank = amendment_rank(second)
            if first_rank != second_rank:
                return first if first_rank > second_rank else second
            differing_fields = {
                field for field in amendment_fields if first.get(field) != second.get(field)
            }
            if differing_fields <= {"name", "float_values", "turnover_rate"}:
                merged = dict(second)
                for field in differing_fields & {"float_values", "turnover_rate"}:
                    merged[field] = None
                return merged

    ignored = amount_fields | {"name"}
    first_other = {field: value for field, value in first.items() if field not in ignored}
    second_other = {field: value for field, value in second.items() if field not in ignored}
    if _canonical_json(first_other) != _canonical_json(second_other):
        return None

    if first_rounded == second_rounded:
        return None
    rounded, precise = (first, second) if first_rounded else (second, first)
    tolerances = {
        "l_buy": Decimal(500),
        "l_sell": Decimal(500),
        "l_amount": Decimal(1000),
        "net_amount": Decimal(1000),
    }
    if any(
        abs((_decimal(rounded, field) or Decimal(0)) - (_decimal(precise, field) or Decimal(0)))
        >= tolerance
        for field, tolerance in tolerances.items()
    ):
        return None
    return precise


def _event_id(key: tuple[date, str, str]) -> str:
    value = "|".join((key[0].isoformat(), key[1], key[2]))
    return sha256(value.encode("utf-8")).hexdigest()


def _trigger_window(reason: str, trade_date: date) -> DragonTigerTriggerWindow:
    window_match = re.search(r"连续(三个|3个|十个|10个|三十个|30个)交易日", reason)
    sessions = {
        "三个": 3,
        "3个": 3,
        "十个": 10,
        "10个": 10,
        "三十个": 30,
        "30个": 30,
    }.get(window_match.group(1) if window_match else "")
    if sessions is not None:
        occurrence_match = re.search(r"交易日内([三四34])次出现", reason)
        occurrences = {
            "三": 3,
            "3": 3,
            "四": 4,
            "4": 4,
        }.get(occurrence_match.group(1) if occurrence_match else "")
        return DragonTigerTriggerWindow(
            basis=DragonTigerWindowBasis.MARKET_SESSIONS,
            session_count=sessions,
            occurrence_count=occurrences,
            start_date=None,
            end_date=trade_date,
        )
    if "交易日" in reason or re.search(
        r"(?:最近|连续).{0,16}日|[二两三四五六七八九十2-9]+个?日内", reason
    ):
        raise ProviderError("DT_PERIOD_MAPPING_UNSUPPORTED")
    return DragonTigerTriggerWindow(
        basis=DragonTigerWindowBasis.MARKET_SESSIONS,
        session_count=1,
        occurrence_count=None,
        start_date=trade_date,
        end_date=trade_date,
    )


def _amount_period(trigger: DragonTigerTriggerWindow) -> DragonTigerAmountPeriod:
    if trigger.session_count not in {1, 3}:
        return DragonTigerAmountPeriod(
            basis=DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED,
            session_count=None,
            start_date=None,
            end_date=None,
        )
    return DragonTigerAmountPeriod(
        basis=DragonTigerAmountPeriodBasis.MARKET_SESSIONS,
        session_count=trigger.session_count,
        start_date=trigger.start_date,
        end_date=trigger.end_date,
    )


def _reason_type(reason: str) -> DragonTigerReasonType:
    if "换手率" in reason:
        return DragonTigerReasonType.TURNOVER
    if "振幅" in reason:
        return DragonTigerReasonType.AMPLITUDE
    if "ST" in reason.upper() and not reason.upper().startswith("非ST"):
        return DragonTigerReasonType.ST
    if "连续涨停" in reason:
        return DragonTigerReasonType.CONTINUOUS_LIMIT
    if "偏离值" in reason:
        return DragonTigerReasonType.PRICE_DEVIATION
    return DragonTigerReasonType.OTHER


def _reason_alias_signature(
    reason: str, trade_date: date
) -> tuple[DragonTigerReasonType, int, int | None]:
    trigger = _trigger_window(reason, trade_date)
    return _reason_type(reason), trigger.session_count, trigger.occurrence_count


def _normalized_reason_alias(reason: str) -> str:
    normalized = re.sub(r"[\s\uff0c,\u3002\uff1b;\uff1a:\uff08\uff09()\u3001]", "", reason)
    return normalized.replace("退市整理的证券", "退市整理期")


def _is_non_stock_row(row: SourceRow) -> bool:
    return "可转债" in str(row.get("reason", ""))


def _reason_code(
    reason_type: DragonTigerReasonType,
    trigger: DragonTigerTriggerWindow,
    reason_name: str,
) -> str:
    digest = sha256(reason_name.strip().encode("utf-8")).hexdigest()[:12].upper()
    return f"{reason_type.value}_{trigger.basis.value}_{trigger.session_count}_0_{digest}"


def _seat_sort_key(row: SourceRow, side: str) -> tuple[object, ...]:
    amount = _decimal(row, "buy" if side == "buy" else "sell")
    return (
        amount is None,
        -(amount or Decimal(0)),
        _required_text(row, "exalter"),
        sha256(_canonical_json(row).encode("utf-8")).hexdigest(),
    )


def _source_date(row: SourceRow) -> date:
    value = _required_text(row, "trade_date")
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
    except (ValueError, IndexError) as error:
        raise ProviderError("Tushare DragonTiger date is invalid") from error


def _symbol(row: SourceRow) -> str:
    value = _required_text(row, "ts_code").upper()
    code, separator, suffix = value.partition(".")
    if separator != "." or len(code) != 6 or not code.isdigit():
        raise ProviderError("Tushare DragonTiger security identifier is invalid")
    if suffix == "SH":
        return f"SSE:{code}"
    if suffix == "SZ":
        return f"SZSE:{code}"
    if suffix == "BJ":
        return f"BSE:{code}"
    raise ProviderError("Tushare DragonTiger exchange is unsupported")


def _required_text(row: SourceRow, field: str) -> str:
    value = row.get(field)
    if value is None or isinstance(value, bool):
        raise ProviderError(f"Tushare DragonTiger required {field} is missing")
    text = str(value).strip()
    if not text:
        raise ProviderError(f"Tushare DragonTiger required {field} is missing")
    return text


def _decimal(row: SourceRow, field: str) -> Decimal | None:
    value = row.get(field)
    if value is None or value == "":
        return None
    if isinstance(value, (bool, float)):
        raise ProviderError(f"Tushare DragonTiger {field} decimal is invalid")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ProviderError(f"Tushare DragonTiger {field} decimal is invalid") from error
    if not parsed.is_finite():
        raise ProviderError(f"Tushare DragonTiger {field} decimal is invalid")
    return parsed


def _raw_row(kind: str, index: int, row: SourceRow) -> RawRow:
    return {
        "record_kind": kind,
        "source_index": str(index),
        "payload_json": _canonical_json(row),
    }


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
        raise ProviderError("Tushare response contains binary floating-point data")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ProviderError("Tushare response object key is invalid")
        return (
            "{"
            + ",".join(
                f"{_canonical_json(key)}:{_canonical_json(value[key])}" for key in sorted(value)
            )
            + "}"
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return "[" + ",".join(_canonical_json(item) for item in value) + "]"
    raise ProviderError("Tushare response contains an unsupported JSON value")
