"""Convertible bond domain records, natural keys and validation."""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum


class ConvertibleBondLifecycle(StrEnum):
    PENDING_LIST = "pending_list"
    LISTED = "listed"
    IN_CONVERSION = "in_conversion"
    CALLED = "called"
    MATURED = "matured"
    DELISTED = "delisted"


class ConvertibleBondTradeStatus(StrEnum):
    TRADING = "trading"
    SUSPENDED = "suspended"
    HALTED_LIMIT = "halted_limit"
    UNKNOWN = "unknown"


class ConvertPriceRevisionReason(StrEnum):
    DIVIDEND = "dividend"
    BONUS_SHARE = "bonus_share"
    RIGHTS_ISSUE = "rights_issue"
    DOWNWARD_REVISION = "downward_revision"
    OTHER = "other"


class ConvertibleBondCallEventType(StrEnum):
    FORCED_REDEMPTION = "forced_redemption"
    SELL_BACK = "sell_back"
    MATURITY_REDEMPTION = "maturity_redemption"


class ConvertibleBondCallStatus(StrEnum):
    ANNOUNCED = "announced"
    EXECUTED = "executed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class ConvertibleBondBasicRecord:
    symbol: str
    bond_code: str
    bond_short_name: str
    bond_full_name: str
    underlying_symbol: str
    exchange: str
    par_value: Decimal
    lifecycle_status: str
    source_code: str
    issue_size: Decimal | None = None
    issue_date: date | None = None
    value_date: date | None = None
    maturity_years: int | None = None
    maturity_date: date | None = None
    convert_price_initial: Decimal | None = None
    convert_price: Decimal | None = None
    convert_start_date: date | None = None
    convert_end_date: date | None = None
    coupon_rate: Decimal | None = None
    redeem_clause: str | None = None
    sell_back_clause: str | None = None

    def __post_init__(self) -> None:
        if self.par_value <= 0:
            raise ValueError("par_value must be positive")
        if self.issue_size is not None and self.issue_size < 0:
            raise ValueError("issue_size must be non-negative")
        if self.maturity_years is not None and self.maturity_years <= 0:
            raise ValueError("maturity_years must be positive")
        if self.convert_price_initial is not None and self.convert_price_initial <= 0:
            raise ValueError("convert_price_initial must be positive")
        if self.convert_price is not None and self.convert_price <= 0:
            raise ValueError("convert_price must be positive")
        if (
            self.convert_start_date is not None
            and self.convert_end_date is not None
            and self.convert_end_date < self.convert_start_date
        ):
            raise ValueError("convert_end_date must not precede convert_start_date")


@dataclass(frozen=True, slots=True)
class ConvertibleBondDailyBarRecord:
    symbol: str
    trade_date: date
    market: str
    trade_status: str
    source_code: str
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    previous_close: Decimal | None = None
    volume: int | None = None
    amount: Decimal | None = None
    pct_chg: Decimal | None = None
    convert_value: Decimal | None = None
    convert_premium_pct: Decimal | None = None
    convert_price: Decimal | None = None
    remain_size: Decimal | None = None

    def __post_init__(self) -> None:
        for name in ("open", "high", "low", "close", "previous_close"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.volume is not None and self.volume < 0:
            raise ValueError("volume must be non-negative")
        if self.amount is not None and self.amount < 0:
            raise ValueError("amount must be non-negative")
        if self.remain_size is not None and self.remain_size < 0:
            raise ValueError("remain_size must be non-negative")
        if self.low is not None and self.high is not None and self.low > self.high:
            raise ValueError("low must not exceed high")


@dataclass(frozen=True, slots=True)
class ConvertibleBondConvertPriceRevisionRecord:
    symbol: str
    effective_date: date
    convert_price_after: Decimal
    revision_reason: str
    source_code: str
    convert_price_before: Decimal | None = None
    announcement_date: date | None = None

    def __post_init__(self) -> None:
        if self.convert_price_after <= 0:
            raise ValueError("convert_price_after must be positive")
        if self.convert_price_before is not None and self.convert_price_before <= 0:
            raise ValueError("convert_price_before must be positive when present")


@dataclass(frozen=True, slots=True)
class ConvertibleBondCallEventRecord:
    symbol: str
    event_type: str
    announcement_date: date
    status: str
    source_code: str
    trigger_date: date | None = None
    record_date: date | None = None
    call_price: Decimal | None = None

    def __post_init__(self) -> None:
        if self.call_price is not None and self.call_price < 0:
            raise ValueError("call_price must be non-negative")


type ConvertibleBondRecord = (
    ConvertibleBondBasicRecord
    | ConvertibleBondDailyBarRecord
    | ConvertibleBondConvertPriceRevisionRecord
    | ConvertibleBondCallEventRecord
)


def convertible_bond_natural_key(record: ConvertibleBondRecord) -> tuple[str, ...]:
    if isinstance(record, ConvertibleBondBasicRecord):
        return ("bond", record.symbol)
    if isinstance(record, ConvertibleBondDailyBarRecord):
        return ("daily_bar", record.symbol, record.trade_date.isoformat())
    if isinstance(record, ConvertibleBondConvertPriceRevisionRecord):
        return ("convert_price_revision", record.symbol, record.effective_date.isoformat())
    return ("call_event", record.symbol, record.event_type, record.announcement_date.isoformat())


def convertible_bond_natural_key_json(record: ConvertibleBondRecord) -> dict[str, object]:
    key = convertible_bond_natural_key(record)
    return {"record_type": key[0], "identity": list(key[1:])}


@dataclass(frozen=True, slots=True)
class ConvertibleBondFinding:
    rule_code: str
    message: str
    natural_key: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class ConvertibleBondValidationResult:
    accepted: tuple[ConvertibleBondRecord, ...]
    findings: tuple[ConvertibleBondFinding, ...]
    rejected_records: tuple[ConvertibleBondRecord, ...]


def validate_convertible_bond(
    records: Sequence[ConvertibleBondRecord],
    *,
    known_symbols: Collection[str],
) -> ConvertibleBondValidationResult:
    """Validate natural-key uniqueness and known-symbol membership."""
    groups: dict[tuple[str, ...], list[ConvertibleBondRecord]] = {}
    for record in records:
        groups.setdefault(convertible_bond_natural_key(record), []).append(record)

    accepted: list[ConvertibleBondRecord] = []
    findings: list[ConvertibleBondFinding] = []
    rejected: list[ConvertibleBondRecord] = []

    for key, group in groups.items():
        symbol = key[1]
        if symbol not in known_symbols:
            for record in group:
                rejected.append(record)
            findings.append(
                ConvertibleBondFinding(
                    rule_code="convertible_bond.unknown_symbol",
                    message=f"symbol {symbol} is not a known security",
                    natural_key={"record_type": key[0], "symbol": symbol},
                )
            )
            continue
        fingerprints = {_record_fingerprint(record) for record in group}
        if len(fingerprints) > 1:
            for record in group:
                rejected.append(record)
            findings.append(
                ConvertibleBondFinding(
                    rule_code="convertible_bond.conflicting_duplicate",
                    message=f"conflicting records share natural key {key}",
                    natural_key={"record_type": key[0], "symbol": symbol},
                )
            )
            continue
        accepted.extend(group)

    return ConvertibleBondValidationResult(tuple(accepted), tuple(findings), tuple(rejected))


def _record_fingerprint(record: ConvertibleBondRecord) -> str:
    """Stable identity for duplicate detection (excludes lineage fields)."""
    from dataclasses import asdict

    payload = asdict(record)
    payload.pop("source_code", None)
    return repr(sorted(payload.items(), key=lambda item: item[0]))
