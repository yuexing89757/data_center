"""Tushare Adapter for provider-neutral DragonTiger facts."""

import json
import re
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from hashlib import sha256

from market_data_center.domain.dragon_tiger import (
    DragonTigerEventDraft,
    DragonTigerPeriodType,
    DragonTigerReason,
    DragonTigerReasonType,
    SeatTradeRecord,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError, RawRow
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

type SourceRow = Mapping[str, object]


class TushareDragonTigerAdapter:
    source_code = "tushare"

    def __init__(self, client: TushareClient) -> None:
        self._client = client

    def fetch_dragon_tiger(self, trade_date: date) -> ProviderBatch[DragonTigerEventDraft]:
        params = {"trade_date": trade_date.strftime("%Y%m%d")}
        try:
            summaries = tuple(self._client.query("top_list", params=params, fields=TOP_LIST_FIELDS))
            details = tuple(self._client.query("top_inst", params=params, fields=TOP_INST_FIELDS))
        except Exception as error:
            raise ProviderError("Tushare DragonTiger request failed") from error
        raw_rows = tuple(
            [_raw_row("summary", index, row) for index, row in enumerate(summaries)]
            + [_raw_row("seat", index, row) for index, row in enumerate(details)]
        )
        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={
                "trade_date": trade_date.isoformat(),
                "apis": ["top_list", "top_inst"],
                "source_counts": {
                    "top_list": len(summaries),
                    "top_inst": len(details),
                },
            },
            schema_version=SCHEMA_VERSION,
            record_factory=lambda: _normalize(summaries, details, trade_date),
        )


def normalize_tushare_dragon_tiger_raw(
    rows: Sequence[RawRow], schema_version: str
) -> tuple[DragonTigerEventDraft, ...]:
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
) -> tuple[DragonTigerEventDraft, ...]:
    if not summaries or not details:
        raise ProviderError("Tushare DragonTiger requires both summary and seat rows")
    detail_groups: dict[tuple[date, str, str], list[SourceRow]] = {}
    for row in details:
        key = (_source_date(row), _symbol(row), _required_text(row, "reason"))
        detail_groups.setdefault(key, []).append(row)
    summary_keys: set[tuple[date, str, str]] = set()
    events: list[DragonTigerEventDraft] = []
    for summary in summaries:
        trade_date = _source_date(summary)
        if trade_date != requested_date:
            raise ProviderError("Tushare DragonTiger response date mismatch")
        symbol = _symbol(summary)
        reason_name = _required_text(summary, "reason")
        key = (trade_date, symbol, reason_name)
        if key in summary_keys:
            raise ProviderError("Tushare DragonTiger duplicate summary identity")
        summary_keys.add(key)
        source_record_id = _event_id(key)
        source_details = detail_groups.get(key)
        if not source_details:
            raise ProviderError("Tushare DragonTiger detail cannot join a summary event")
        period = _period_type(reason_name)
        reason_type = _reason_type(reason_name)
        reason = DragonTigerReason(
            reason_code=_reason_code(reason_type, period, reason_name),
            reason_name=reason_name,
            reason_type=reason_type,
            period_type=period,
            source_code="tushare",
            source_reason_code=sha256(reason_name.encode("utf-8")).hexdigest()[:16],
            source_reason_name=reason_name,
        )
        events.append(
            DragonTigerEventDraft(
                source_record_id=source_record_id,
                symbol=symbol,
                trade_date=trade_date,
                period_type=period,
                period_start_date=trade_date if period is DragonTigerPeriodType.DAY else None,
                period_end_date=trade_date,
                reason=reason,
                reason_name_raw=reason_name,
                close_price=_decimal(summary, "close"),
                change_pct=_decimal(summary, "pct_change"),
                turnover_amount=_decimal(summary, "amount"),
                turnover_rate=_decimal(summary, "turnover_rate"),
                amplitude=None,
                lhb_buy_amount=_decimal(summary, "l_buy"),
                lhb_sell_amount=_decimal(summary, "l_sell"),
                seat_trades=_seat_trades(source_record_id, symbol, trade_date, source_details),
                source_code="tushare",
            )
        )
    unmatched = set(detail_groups) - summary_keys
    if unmatched:
        raise ProviderError("Tushare DragonTiger detail cannot join a summary event")
    return tuple(events)


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


def _event_id(key: tuple[date, str, str]) -> str:
    value = "|".join((key[0].isoformat(), key[1], key[2]))
    return sha256(value.encode("utf-8")).hexdigest()


def _period_type(reason: str) -> DragonTigerPeriodType:
    if "连续三个交易日" in reason:
        return DragonTigerPeriodType.THREE_DAY
    if "交易日" in reason or re.search(
        r"(?:最近|连续).{0,16}日|[二两三四五六七八九十2-9]+个?日内", reason
    ):
        raise ProviderError("Tushare DragonTiger period is unsupported")
    return DragonTigerPeriodType.DAY


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
    period: DragonTigerPeriodType,
    reason_name: str,
) -> str:
    digest = sha256(reason_name.strip().encode("utf-8")).hexdigest()[:12].upper()
    return f"{reason_type.value}_{period.value}_{digest}"


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
