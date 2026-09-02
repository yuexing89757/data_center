"""Provider-neutral A-share DragonTiger facts and pure validation."""

import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from uuid import UUID

_SUPPORTED_EXCHANGES = frozenset({"SSE", "SZSE", "BSE"})


class DragonTigerPeriodType(StrEnum):
    DAY = "DAY"
    THREE_DAY = "THREE_DAY"


class DragonTigerReasonType(StrEnum):
    PRICE_DEVIATION = "PRICE_DEVIATION"
    TURNOVER = "TURNOVER"
    AMPLITUDE = "AMPLITUDE"
    CONTINUOUS_LIMIT = "CONTINUOUS_LIMIT"
    ST = "ST"
    OTHER = "OTHER"


class TradingSeatType(StrEnum):
    BROKER = "BROKER"
    INSTITUTION = "INSTITUTION"
    NORTHBOUND = "NORTHBOUND"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class DragonTigerReason:
    reason_code: str
    reason_name: str
    reason_type: DragonTigerReasonType
    period_type: DragonTigerPeriodType
    source_code: str
    source_reason_code: str
    source_reason_name: str

    def __post_init__(self) -> None:
        _require_nonblank(
            reason_code=self.reason_code,
            reason_name=self.reason_name,
            source_code=self.source_code,
            source_reason_code=self.source_reason_code,
            source_reason_name=self.source_reason_name,
        )


@dataclass(frozen=True, slots=True)
class TradingSeat:
    seat_id: UUID
    canonical_name: str
    broker_name: str | None
    branch_name: str | None
    seat_type: TradingSeatType
    province: str | None
    city: str | None
    first_seen_date: date
    last_seen_date: date
    is_active: bool

    def __post_init__(self) -> None:
        _require_nonblank(canonical_name=self.canonical_name)
        if self.first_seen_date > self.last_seen_date:
            raise ValueError("seat first_seen_date must not follow last_seen_date")


@dataclass(frozen=True, slots=True)
class TradingSeatAlias:
    seat_id: UUID
    source_code: str
    source_seat_key: str | None
    alias_name: str

    def __post_init__(self) -> None:
        _require_nonblank(source_code=self.source_code, alias_name=self.alias_name)
        if self.source_seat_key is not None and not self.source_seat_key.strip():
            raise ValueError("source_seat_key must be None or non-blank")


@dataclass(frozen=True, slots=True)
class SeatTradeRecord:
    source_record_id: str
    source_event_id: str
    symbol: str
    trade_date: date
    seat_id: UUID | None
    seat_source_key: str | None
    seat_name_raw: str
    buy_amount: Decimal | None
    sell_amount: Decimal | None
    buy_rank: int | None
    sell_rank: int | None
    is_institution: bool
    is_northbound: bool
    source_code: str

    def __post_init__(self) -> None:
        _require_nonblank(
            source_record_id=self.source_record_id,
            source_event_id=self.source_event_id,
            seat_name_raw=self.seat_name_raw,
            source_code=self.source_code,
        )
        _require_symbol(self.symbol)
        _require_nonnegative(self.buy_amount, self.sell_amount)
        if self.buy_amount is None and self.sell_amount is None:
            raise ValueError("seat trade requires at least one disclosed amount")
        if self.buy_amount == 0 and self.sell_amount == 0:
            raise ValueError("seat trade amounts cannot both be zero")
        if self.buy_rank is None and self.sell_rank is None:
            raise ValueError("seat trade requires at least one disclosed rank")
        for rank in (self.buy_rank, self.sell_rank):
            if rank is not None and not 1 <= rank <= 5:
                raise ValueError("seat trade rank must be between 1 and 5")
        if self.seat_source_key is not None and not self.seat_source_key.strip():
            raise ValueError("seat_source_key must be None or non-blank")

    @property
    def net_amount(self) -> Decimal | None:
        if self.buy_amount is None or self.sell_amount is None:
            return None
        return self.buy_amount - self.sell_amount

    @property
    def is_pure_buy(self) -> bool:
        return self.buy_amount is not None and self.buy_amount > 0 and self.sell_amount == 0

    @property
    def is_pure_sell(self) -> bool:
        return self.sell_amount is not None and self.sell_amount > 0 and self.buy_amount == 0

    @property
    def is_buy_and_sell(self) -> bool:
        return (
            self.buy_amount is not None
            and self.buy_amount > 0
            and self.sell_amount is not None
            and self.sell_amount > 0
        )


@dataclass(frozen=True, slots=True)
class DragonTigerEventDraft:
    source_record_id: str
    symbol: str
    trade_date: date
    period_type: DragonTigerPeriodType
    period_start_date: date | None
    period_end_date: date
    reason: DragonTigerReason
    reason_name_raw: str
    close_price: Decimal | None
    change_pct: Decimal | None
    turnover_amount: Decimal | None
    turnover_rate: Decimal | None
    amplitude: Decimal | None
    lhb_buy_amount: Decimal | None
    lhb_sell_amount: Decimal | None
    seat_trades: tuple[SeatTradeRecord, ...]
    source_code: str

    def __post_init__(self) -> None:
        if self.period_end_date != self.trade_date:
            raise ValueError("draft period_end_date must equal trade_date")
        if self.period_type is DragonTigerPeriodType.DAY:
            if self.period_start_date != self.trade_date:
                raise ValueError("day draft period must equal trade_date")
        elif self.period_start_date is not None:
            raise ValueError("three-day draft start must be resolved by the trading calendar")
        if self.reason.period_type is not self.period_type:
            raise ValueError("draft and reason period_type must match")

    def resolve_period(self, period_start_date: date) -> "DragonTigerEventRecord":
        return DragonTigerEventRecord(
            source_record_id=self.source_record_id,
            symbol=self.symbol,
            trade_date=self.trade_date,
            period_type=self.period_type,
            period_start_date=period_start_date,
            period_end_date=self.period_end_date,
            reason=self.reason,
            reason_name_raw=self.reason_name_raw,
            close_price=self.close_price,
            change_pct=self.change_pct,
            turnover_amount=self.turnover_amount,
            turnover_rate=self.turnover_rate,
            amplitude=self.amplitude,
            lhb_buy_amount=self.lhb_buy_amount,
            lhb_sell_amount=self.lhb_sell_amount,
            seat_trades=self.seat_trades,
            source_code=self.source_code,
        )


@dataclass(frozen=True, slots=True)
class DragonTigerEventRecord:
    source_record_id: str
    symbol: str
    trade_date: date
    period_type: DragonTigerPeriodType
    period_start_date: date
    period_end_date: date
    reason: DragonTigerReason
    reason_name_raw: str
    close_price: Decimal | None
    change_pct: Decimal | None
    turnover_amount: Decimal | None
    turnover_rate: Decimal | None
    amplitude: Decimal | None
    lhb_buy_amount: Decimal | None
    lhb_sell_amount: Decimal | None
    seat_trades: tuple[SeatTradeRecord, ...]
    source_code: str

    def __post_init__(self) -> None:
        _require_nonblank(
            source_record_id=self.source_record_id,
            reason_name_raw=self.reason_name_raw,
            source_code=self.source_code,
        )
        _require_symbol(self.symbol)
        _require_nonnegative(
            self.close_price,
            self.turnover_amount,
            self.turnover_rate,
            self.amplitude,
            self.lhb_buy_amount,
            self.lhb_sell_amount,
        )
        if self.period_end_date != self.trade_date:
            raise ValueError("event period_end_date must equal trade_date")
        if self.period_type is DragonTigerPeriodType.DAY:
            if self.period_start_date != self.trade_date:
                raise ValueError("day event period must equal trade_date")
        elif self.period_start_date >= self.period_end_date:
            raise ValueError("three-day event requires an earlier period_start_date")
        if self.reason.period_type is not self.period_type:
            raise ValueError("event and reason period_type must match")
        if self.reason.source_code != self.source_code:
            raise ValueError("event and reason source_code must match")
        if not self.seat_trades:
            raise ValueError("event requires at least one seat trade")


@dataclass(frozen=True, slots=True)
class DragonTigerFinding:
    rule_code: str
    message: str
    natural_key: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class DragonTigerValidationResult:
    accepted: tuple[DragonTigerEventRecord, ...]
    findings: tuple[DragonTigerFinding, ...]
    rejected_rows: int


def dragon_tiger_natural_key(record: DragonTigerEventRecord) -> tuple[str, str]:
    return record.source_code, record.source_record_id


def dragon_tiger_content_hash(record: DragonTigerEventRecord) -> str:
    canonical = json.dumps(
        _event_payload(record),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(canonical).hexdigest()


def validate_dragon_tiger_events(
    records: Sequence[DragonTigerEventRecord],
    *,
    known_symbols: Collection[str],
    known_trading_dates: Collection[date],
) -> DragonTigerValidationResult:
    source_keys: set[tuple[str, str]] = set()
    semantic_keys: set[tuple[str, date, DragonTigerPeriodType, str, str]] = set()
    accepted: list[DragonTigerEventRecord] = []
    findings: list[DragonTigerFinding] = []
    for record in records:
        finding = _validate_event(record, known_symbols, known_trading_dates)
        source_key = dragon_tiger_natural_key(record)
        semantic_key = (
            record.symbol,
            record.trade_date,
            record.period_type,
            record.reason.reason_code,
            record.source_code,
        )
        if finding is None and source_key in source_keys:
            finding = ("duplicate_source_key", "duplicate source event identity")
        if finding is None and semantic_key in semantic_keys:
            finding = ("conflicting_semantic_key", "duplicate source semantic event")
        if finding is not None:
            findings.append(
                DragonTigerFinding(
                    rule_code=f"dragon_tiger.{finding[0]}",
                    message=finding[1],
                    natural_key=_natural_key_json(record),
                )
            )
            continue
        source_keys.add(source_key)
        semantic_keys.add(semantic_key)
        accepted.append(record)
    return DragonTigerValidationResult(
        accepted=tuple(accepted),
        findings=tuple(findings),
        rejected_rows=len(records) - len(accepted),
    )


def _validate_event(
    record: DragonTigerEventRecord,
    known_symbols: Collection[str],
    known_trading_dates: Collection[date],
) -> tuple[str, str] | None:
    if record.symbol not in known_symbols:
        return "unknown_security", "event security is not known for the trade date"
    if record.trade_date not in known_trading_dates:
        return "unknown_trading_date", "event date is not a known trading date"
    if record.period_start_date not in known_trading_dates:
        return "unknown_period_start", "event period start is not a known trading date"
    trade_source_ids: set[str] = set()
    buy_ranks: set[int] = set()
    sell_ranks: set[int] = set()
    for trade in record.seat_trades:
        if (
            trade.source_event_id != record.source_record_id
            or trade.symbol != record.symbol
            or trade.trade_date != record.trade_date
            or trade.source_code != record.source_code
        ):
            return "seat_parent_mismatch", "seat trade does not match its parent event"
        if trade.source_record_id in trade_source_ids:
            return "duplicate_seat_source_key", "seat source identity is duplicated"
        trade_source_ids.add(trade.source_record_id)
        if trade.buy_rank is not None:
            if trade.buy_rank in buy_ranks:
                return "duplicate_buy_rank", "event buy rank is duplicated"
            buy_ranks.add(trade.buy_rank)
        if trade.sell_rank is not None:
            if trade.sell_rank in sell_ranks:
                return "duplicate_sell_rank", "event sell rank is duplicated"
            sell_ranks.add(trade.sell_rank)
    return None


def _require_nonblank(**values: str) -> None:
    blank = next((name for name, value in values.items() if not value.strip()), None)
    if blank is not None:
        raise ValueError(f"{blank} must not be blank")


def _require_symbol(symbol: str) -> None:
    exchange, separator, code = symbol.partition(":")
    if (
        separator != ":"
        or exchange not in _SUPPORTED_EXCHANGES
        or len(code) != 6
        or not code.isdigit()
    ):
        raise ValueError("symbol must be a standard A-share symbol")


def _require_nonnegative(*values: Decimal | None) -> None:
    if any(value is not None and value < 0 for value in values):
        raise ValueError("non-net amounts and percentages must not be negative")


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def _trade_payload(trade: SeatTradeRecord) -> dict[str, object]:
    return {
        "source_record_id": trade.source_record_id,
        "source_event_id": trade.source_event_id,
        "symbol": trade.symbol,
        "trade_date": trade.trade_date.isoformat(),
        "seat_id": str(trade.seat_id) if trade.seat_id is not None else None,
        "seat_source_key": trade.seat_source_key,
        "seat_name_raw": trade.seat_name_raw,
        "buy_amount": _decimal_text(trade.buy_amount),
        "sell_amount": _decimal_text(trade.sell_amount),
        "buy_rank": trade.buy_rank,
        "sell_rank": trade.sell_rank,
        "is_institution": trade.is_institution,
        "is_northbound": trade.is_northbound,
        "source_code": trade.source_code,
    }


def _event_payload(record: DragonTigerEventRecord) -> dict[str, object]:
    return {
        "source_record_id": record.source_record_id,
        "symbol": record.symbol,
        "trade_date": record.trade_date.isoformat(),
        "period_type": record.period_type.value,
        "period_start_date": record.period_start_date.isoformat(),
        "period_end_date": record.period_end_date.isoformat(),
        "reason": {
            "reason_code": record.reason.reason_code,
            "reason_name": record.reason.reason_name,
            "reason_type": record.reason.reason_type.value,
            "period_type": record.reason.period_type.value,
            "source_code": record.reason.source_code,
            "source_reason_code": record.reason.source_reason_code,
            "source_reason_name": record.reason.source_reason_name,
        },
        "reason_name_raw": record.reason_name_raw,
        "close_price": _decimal_text(record.close_price),
        "change_pct": _decimal_text(record.change_pct),
        "turnover_amount": _decimal_text(record.turnover_amount),
        "turnover_rate": _decimal_text(record.turnover_rate),
        "amplitude": _decimal_text(record.amplitude),
        "lhb_buy_amount": _decimal_text(record.lhb_buy_amount),
        "lhb_sell_amount": _decimal_text(record.lhb_sell_amount),
        "seat_trades": [_trade_payload(trade) for trade in record.seat_trades],
        "source_code": record.source_code,
    }


def _natural_key_json(record: DragonTigerEventRecord) -> dict[str, object]:
    return {
        "source_code": record.source_code,
        "source_record_id": record.source_record_id,
        "symbol": record.symbol,
        "trade_date": record.trade_date.isoformat(),
        "period_type": record.period_type.value,
        "reason_code": record.reason.reason_code,
    }
