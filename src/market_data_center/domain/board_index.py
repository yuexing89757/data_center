"""Provider-neutral third-party board-index facts and validation."""

from collections import Counter
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum

from market_data_center.domain.records import Market


class BoardIndexType(StrEnum):
    DYNAMIC_THEME = "dynamic_theme"


class BoardIndexStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class BoardIndexRecord:
    board_id: str
    board_code: str
    namespace: str
    name: str
    board_type: BoardIndexType
    market: Market
    status: BoardIndexStatus
    source_code: str

    def __post_init__(self) -> None:
        expected_id = f"{self.namespace}:{self.board_code}"
        if self.board_id != expected_id:
            raise ValueError(f"board_id must equal {expected_id}")
        if not self.namespace.strip() or not self.board_code.strip() or not self.name.strip():
            raise ValueError("board-index identity and name must not be blank")
        if not self.source_code.strip():
            raise ValueError("source_code must not be blank")


@dataclass(frozen=True, slots=True)
class BoardIndexDailyBarRecord:
    board_id: str
    trade_date: date
    market: Market
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    amount: Decimal
    source_code: str

    def __post_init__(self) -> None:
        if not self.board_id.strip() or not self.source_code.strip():
            raise ValueError("board_id and source_code must not be blank")
        numeric_values = (self.open, self.high, self.low, self.close, self.amount)
        if any(not isinstance(value, Decimal) for value in numeric_values):
            raise TypeError("board-index prices and amount must use Decimal")
        if any(value < 0 for value in numeric_values):
            raise ValueError("board-index prices and amount must not be negative")
        if self.volume < 0:
            raise ValueError("board-index volume must not be negative")
        if self.low > self.high:
            raise ValueError("board-index low must not exceed high")
        if not self.low <= self.open <= self.high:
            raise ValueError("board-index open must be within [low, high]")
        if not self.low <= self.close <= self.high:
            raise ValueError("board-index close must be within [low, high]")


@dataclass(frozen=True, slots=True)
class BoardIndexConstituentSnapshotRecord:
    board_id: str
    trade_date: date
    members: tuple[str, ...]
    source_code: str

    def __post_init__(self) -> None:
        if not self.board_id.strip() or not self.source_code.strip():
            raise ValueError("board_id and source_code must not be blank")


type BoardIndexProviderRecord = (
    BoardIndexRecord | BoardIndexDailyBarRecord | BoardIndexConstituentSnapshotRecord
)


@dataclass(frozen=True, slots=True)
class BoardIndexFinding:
    rule_code: str
    message: str
    natural_key: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class BoardIndexDailyBarValidationResult:
    accepted: tuple[BoardIndexDailyBarRecord, ...]
    findings: tuple[BoardIndexFinding, ...]
    rejected_rows: int


def validate_board_index_daily_bars(
    records: Sequence[BoardIndexDailyBarRecord],
    *,
    known_board_ids: Collection[str],
    known_trading_dates: Collection[date],
) -> BoardIndexDailyBarValidationResult:
    grouped: dict[tuple[str, date], list[BoardIndexDailyBarRecord]] = {}
    for record in records:
        grouped.setdefault((record.board_id, record.trade_date), []).append(record)

    accepted: list[BoardIndexDailyBarRecord] = []
    findings: list[BoardIndexFinding] = []
    rejected_rows = 0
    for (board_id, trade_date), grouped_records in grouped.items():
        natural_key = {"board_id": board_id, "trade_date": trade_date.isoformat()}
        record = grouped_records[0]
        if any(candidate != record for candidate in grouped_records[1:]):
            rejected_rows += len(grouped_records)
            findings.append(
                BoardIndexFinding(
                    "board_index_daily_bar.conflicting_duplicate",
                    "batch contains conflicting board-index bars for one natural key",
                    natural_key,
                )
            )
            continue
        if board_id not in known_board_ids:
            rejected_rows += len(grouped_records)
            findings.append(
                BoardIndexFinding(
                    "board_index_daily_bar.unknown_board",
                    "board-index bar references an unknown BoardIndex",
                    natural_key,
                )
            )
            continue
        if trade_date not in known_trading_dates:
            rejected_rows += len(grouped_records)
            findings.append(
                BoardIndexFinding(
                    "board_index_daily_bar.non_trading_date",
                    "board-index bar date is not a known trading day",
                    natural_key,
                )
            )
            continue
        accepted.append(record)
    return BoardIndexDailyBarValidationResult(tuple(accepted), tuple(findings), rejected_rows)


def validate_board_index_constituent_snapshot(
    record: BoardIndexConstituentSnapshotRecord,
    *,
    known_board_ids: Collection[str],
    known_symbols: Collection[str],
    known_trading_dates: Collection[date],
) -> tuple[BoardIndexFinding, ...]:
    natural_key: dict[str, object] = {
        "board_id": record.board_id,
        "trade_date": record.trade_date.isoformat(),
    }
    findings: list[BoardIndexFinding] = []
    if record.board_id not in known_board_ids:
        findings.append(
            BoardIndexFinding(
                "board_index_constituent.unknown_board",
                "constituent snapshot references an unknown BoardIndex",
                natural_key,
            )
        )
    if record.trade_date not in known_trading_dates:
        findings.append(
            BoardIndexFinding(
                "board_index_constituent.non_trading_date",
                "constituent snapshot date is not a known trading day",
                natural_key,
            )
        )
    duplicates = sorted(symbol for symbol, count in Counter(record.members).items() if count > 1)
    if duplicates:
        findings.append(
            BoardIndexFinding(
                "board_index_constituent.duplicate_member",
                "constituent snapshot contains duplicate Security symbols",
                {**natural_key, "symbols": duplicates},
            )
        )
    unknown = sorted(set(record.members).difference(known_symbols))
    if unknown:
        findings.append(
            BoardIndexFinding(
                "board_index_constituent.unknown_security",
                "constituent snapshot references unknown Security symbols",
                {**natural_key, "symbols": unknown},
            )
        )
    return tuple(findings)
