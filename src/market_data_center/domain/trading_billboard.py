"""Immutable A-share trading billboard facts and pure validation."""

import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from hashlib import sha256

_CENT = Decimal("0.01")
_SUPPORTED_EXCHANGES = frozenset({"SSE", "SZSE", "BSE"})


class TradingBillboardSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class TradingBillboardSeatRecord:
    source_event_id: str
    symbol: str
    trade_date: date
    side: TradingBillboardSide
    rank: int
    seat_code: str | None
    seat_name: str
    buy_amount: Decimal | None
    sell_amount: Decimal | None
    net_amount: Decimal | None
    buy_to_market_pct: Decimal | None
    sell_to_market_pct: Decimal | None
    source_code: str = "eastmoney"

    def __post_init__(self) -> None:
        _require_identity(self.source_code, self.source_event_id, self.symbol)
        if not 1 <= self.rank <= 5:
            raise ValueError("seat rank must be between 1 and 5")
        if self.seat_code is not None and not self.seat_code.strip():
            raise ValueError("seat_code must be None or non-blank")
        if not self.seat_name.strip():
            raise ValueError("seat_name must not be blank")
        _require_nonnegative(
            self.buy_amount,
            self.sell_amount,
            self.buy_to_market_pct,
            self.sell_to_market_pct,
        )


@dataclass(frozen=True, slots=True)
class TradingBillboardRecord:
    symbol: str
    trade_date: date
    source_event_id: str
    reason_code: str
    reason_text: str
    close_price: Decimal | None
    change_rate_pct: Decimal | None
    turnover_rate_pct: Decimal | None
    market_amount: Decimal | None
    buy_amount: Decimal
    sell_amount: Decimal
    net_amount: Decimal
    deal_amount: Decimal
    deal_to_market_pct: Decimal | None
    net_to_market_pct: Decimal | None
    free_float_market_value: Decimal | None
    buy_seats: tuple[TradingBillboardSeatRecord, ...]
    sell_seats: tuple[TradingBillboardSeatRecord, ...]
    source_code: str = "eastmoney"

    def __post_init__(self) -> None:
        _require_identity(self.source_code, self.source_event_id, self.symbol)
        if not self.reason_code.strip() or not self.reason_text.strip():
            raise ValueError("reason_code and reason_text must not be blank")
        _require_nonnegative(
            self.close_price,
            self.turnover_rate_pct,
            self.market_amount,
            self.buy_amount,
            self.sell_amount,
            self.deal_amount,
            self.deal_to_market_pct,
            self.free_float_market_value,
        )


@dataclass(frozen=True, slots=True)
class TradingBillboardFinding:
    rule_code: str
    message: str
    natural_key: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class TradingBillboardValidationResult:
    accepted: tuple[TradingBillboardRecord, ...]
    findings: tuple[TradingBillboardFinding, ...]
    rejected_rows: int


def trading_billboard_natural_key(record: TradingBillboardRecord) -> tuple[str, str]:
    return record.source_code, record.source_event_id


def trading_billboard_content_hash(record: TradingBillboardRecord) -> str:
    payload = {
        "symbol": record.symbol,
        "trade_date": record.trade_date.isoformat(),
        "source_event_id": record.source_event_id,
        "reason_code": record.reason_code,
        "reason_text": record.reason_text,
        "close_price": _decimal_text(record.close_price),
        "change_rate_pct": _decimal_text(record.change_rate_pct),
        "turnover_rate_pct": _decimal_text(record.turnover_rate_pct),
        "market_amount": _decimal_text(record.market_amount),
        "buy_amount": _decimal_text(record.buy_amount),
        "sell_amount": _decimal_text(record.sell_amount),
        "net_amount": _decimal_text(record.net_amount),
        "deal_amount": _decimal_text(record.deal_amount),
        "deal_to_market_pct": _decimal_text(record.deal_to_market_pct),
        "net_to_market_pct": _decimal_text(record.net_to_market_pct),
        "free_float_market_value": _decimal_text(record.free_float_market_value),
        "buy_seats": [_seat_payload(seat) for seat in record.buy_seats],
        "sell_seats": [_seat_payload(seat) for seat in record.sell_seats],
        "source_code": record.source_code,
    }
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return sha256(canonical).hexdigest()


def validate_trading_billboards(
    records: Sequence[TradingBillboardRecord],
    *,
    known_symbols: Collection[str],
    known_trading_dates: Collection[date],
) -> TradingBillboardValidationResult:
    accepted_universe = tuple(
        record
        for record in records
        if not record.symbol.startswith("BSE:") and record.symbol in known_symbols
    )
    source_groups: dict[tuple[str, str], list[TradingBillboardRecord]] = {}
    semantic_groups: dict[tuple[str, date, str], list[TradingBillboardRecord]] = {}
    for record in accepted_universe:
        source_groups.setdefault(trading_billboard_natural_key(record), []).append(record)
        semantic_groups.setdefault(
            (record.symbol, record.trade_date, record.reason_code), []
        ).append(record)

    conflicting_source_keys = {
        key for key, group in source_groups.items() if any(item != group[0] for item in group[1:])
    }
    duplicate_source_keys = {
        key
        for key, group in source_groups.items()
        if len(group) > 1 and key not in conflicting_source_keys
    }
    conflicting_semantic_keys = {
        key
        for key, group in semantic_groups.items()
        if len({trading_billboard_natural_key(item) for item in group}) > 1
    }

    accepted: list[TradingBillboardRecord] = []
    findings: list[TradingBillboardFinding] = []
    rejected_rows = len(records) - len(accepted_universe)
    if records and not accepted_universe:
        findings.append(
            _finding(
                "no_known_security",
                "trading billboard contains no known supported securities",
                {"source_record_count": len(records)},
            )
        )
    for source_key, group in source_groups.items():
        record = group[0]
        natural_key = _natural_key_json(record)
        semantic_key = (record.symbol, record.trade_date, record.reason_code)
        if source_key in conflicting_source_keys:
            rejected_rows += len(group)
            findings.append(
                _finding(
                    "conflicting_source_key",
                    "batch contains conflicting facts for one source event",
                    natural_key,
                )
            )
            continue
        if source_key in duplicate_source_keys:
            rejected_rows += len(group)
            findings.append(
                _finding(
                    "duplicate_source_key",
                    "batch contains duplicate facts for one source event",
                    natural_key,
                )
            )
            continue
        if semantic_key in conflicting_semantic_keys:
            rejected_rows += len(group)
            findings.append(
                _finding(
                    "conflicting_semantic_key",
                    "batch contains multiple source events for one semantic key",
                    natural_key,
                )
            )
            continue
        rule = _validate_record(record, known_trading_dates)
        if rule is not None:
            rejected_rows += len(group)
            findings.append(_finding(rule[0], rule[1], natural_key))
            continue
        accepted.append(record)

    return TradingBillboardValidationResult(
        accepted=tuple(accepted),
        findings=tuple(findings),
        rejected_rows=rejected_rows,
    )


def _validate_record(
    record: TradingBillboardRecord,
    known_trading_dates: Collection[date],
) -> tuple[str, str] | None:
    if record.trade_date not in known_trading_dates:
        return "unknown_trading_date", "trading billboard date is not a known trading date"
    if not _equal_at_cent(record.deal_amount, record.buy_amount + record.sell_amount):
        return "invalid_deal_amount", "deal amount must equal buy amount plus sell amount"
    if not _equal_at_cent(record.net_amount, record.buy_amount - record.sell_amount):
        return "invalid_net_amount", "net amount must equal buy amount minus sell amount"
    for expected_side, seats in (
        (TradingBillboardSide.BUY, record.buy_seats),
        (TradingBillboardSide.SELL, record.sell_seats),
    ):
        if not 1 <= len(seats) <= 5:
            return "seat_count", "each side must contain between one and five seats"
        if any(seat.side is not expected_side for seat in seats):
            return "seat_side_mismatch", "seat side does not match its aggregate collection"
        if [seat.rank for seat in seats] != list(range(1, len(seats) + 1)):
            return "seat_rank_sequence", "seat ranks must be unique, ordered and contiguous"
        for seat in seats:
            if (
                seat.source_event_id != record.source_event_id
                or seat.symbol != record.symbol
                or seat.trade_date != record.trade_date
                or seat.source_code != record.source_code
            ):
                return "seat_parent_mismatch", "seat identity does not match its parent event"
            if (
                seat.buy_amount is not None
                and seat.sell_amount is not None
                and (
                    seat.net_amount is None
                    or not _equal_at_cent(seat.net_amount, seat.buy_amount - seat.sell_amount)
                )
            ):
                return "invalid_seat_net_amount", "seat net amount is inconsistent"
    return None


def _require_identity(source_code: str, source_event_id: str, symbol: str) -> None:
    if source_code != "eastmoney":
        raise ValueError("trading billboard source_code must be eastmoney")
    if not source_event_id.strip():
        raise ValueError("source_event_id must not be blank")
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


def _equal_at_cent(left: Decimal, right: Decimal) -> bool:
    return left.quantize(_CENT, rounding=ROUND_HALF_UP) == right.quantize(
        _CENT, rounding=ROUND_HALF_UP
    )


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def _seat_payload(seat: TradingBillboardSeatRecord) -> dict[str, object]:
    return {
        "source_event_id": seat.source_event_id,
        "symbol": seat.symbol,
        "trade_date": seat.trade_date.isoformat(),
        "side": seat.side.value,
        "rank": seat.rank,
        "seat_code": seat.seat_code,
        "seat_name": seat.seat_name,
        "buy_amount": _decimal_text(seat.buy_amount),
        "sell_amount": _decimal_text(seat.sell_amount),
        "net_amount": _decimal_text(seat.net_amount),
        "buy_to_market_pct": _decimal_text(seat.buy_to_market_pct),
        "sell_to_market_pct": _decimal_text(seat.sell_to_market_pct),
        "source_code": seat.source_code,
    }


def _natural_key_json(record: TradingBillboardRecord) -> dict[str, object]:
    return {
        "source_code": record.source_code,
        "source_event_id": record.source_event_id,
        "symbol": record.symbol,
        "trade_date": record.trade_date.isoformat(),
        "reason_code": record.reason_code,
    }


def _finding(
    suffix: str, message: str, natural_key: Mapping[str, object]
) -> TradingBillboardFinding:
    return TradingBillboardFinding(
        rule_code=f"trading_billboard.{suffix}",
        message=message,
        natural_key=natural_key,
    )
