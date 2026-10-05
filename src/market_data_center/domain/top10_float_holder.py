"""Provider-neutral top-10 float-holder point-in-time facts."""

from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class Top10FloatHolderRecord:
    symbol: str
    end_date: date
    announcement_date: date
    holder_rank: int
    holder_name: str
    hold_amount: int
    hold_ratio: Decimal | None
    hold_float_ratio: Decimal | None
    hold_change: int | None
    holder_type: str | None
    source_code: str


@dataclass(frozen=True, slots=True)
class Top10FloatHolderValidation:
    accepted: tuple[Top10FloatHolderRecord, ...]
    rejected: tuple[tuple[Top10FloatHolderRecord, str], ...]


def validate_top10_float_holders(
    records: tuple[Top10FloatHolderRecord, ...],
    *,
    known_symbols: set[str],
) -> tuple[Top10FloatHolderRecord, ...]:
    for record in records:
        if record.symbol not in known_symbols:
            raise ValueError(f"unknown top10 float holder symbol: {record.symbol}")
        if not 1 <= record.holder_rank <= 10:
            raise ValueError("top10 float holder rank must be between 1 and 10")
        if record.hold_amount <= 0:
            raise ValueError("top10 float holder amount must be positive")
        if record.end_date > record.announcement_date:
            raise ValueError("top10 float holder end date follows announcement date")
        if not record.holder_name.strip():
            raise ValueError("top10 float holder name is required")
        if record.hold_ratio is not None and record.hold_ratio < 0:
            raise ValueError("top10 float holder ratio cannot be negative")
        if record.hold_float_ratio is not None and record.hold_float_ratio < 0:
            raise ValueError("top10 float holder float ratio cannot be negative")
        if record.source_code != "tushare":
            raise ValueError("unsupported top10 float holder source")
    return records


def partition_top10_float_holders(
    records: tuple[Top10FloatHolderRecord, ...],
    *,
    known_symbols: set[str],
) -> Top10FloatHolderValidation:
    """Reject invalid facts without weakening the strict record invariants."""
    keys = Counter(
        (record.symbol, record.end_date, record.announcement_date, record.holder_rank)
        for record in records
    )
    names = Counter(
        (record.symbol, record.end_date, record.announcement_date, record.holder_name)
        for record in records
    )
    accepted: list[Top10FloatHolderRecord] = []
    rejected: list[tuple[Top10FloatHolderRecord, str]] = []
    for record in records:
        try:
            if (
                keys[(record.symbol, record.end_date, record.announcement_date, record.holder_rank)]
                > 1
            ):
                raise ValueError("duplicate top10 float holder rank")
            if (
                names[
                    (record.symbol, record.end_date, record.announcement_date, record.holder_name)
                ]
                > 1
            ):
                raise ValueError("duplicate top10 float holder name")
            validate_top10_float_holders((record,), known_symbols=known_symbols)
        except ValueError as error:
            rejected.append((record, str(error)))
        else:
            accepted.append(record)
    return Top10FloatHolderValidation(tuple(accepted), tuple(rejected))
