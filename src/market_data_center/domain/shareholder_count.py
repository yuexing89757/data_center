"""Provider-neutral shareholder-count point-in-time facts."""

from dataclasses import dataclass
from datetime import date
from hashlib import sha256


@dataclass(frozen=True, slots=True)
class ShareholderCountRecord:
    symbol: str
    statistics_date: date
    announcement_date: date
    shareholder_count: int
    revision_key: str
    source_code: str


def shareholder_count_revision_key(
    *,
    symbol: str,
    statistics_date: date,
    announcement_date: date,
    shareholder_count: int,
) -> str:
    values = (
        symbol,
        statistics_date.isoformat(),
        announcement_date.isoformat(),
        str(shareholder_count),
    )
    return sha256("\x1f".join(values).encode()).hexdigest()


def validate_shareholder_counts(
    records: tuple[ShareholderCountRecord, ...],
    *,
    known_symbols: set[str],
) -> tuple[ShareholderCountRecord, ...]:
    seen: set[tuple[str, date, str]] = set()
    for record in records:
        if record.symbol not in known_symbols:
            raise ValueError(f"unknown shareholder-count symbol: {record.symbol}")
        if record.statistics_date > record.announcement_date:
            raise ValueError("shareholder-count announcement precedes statistics date")
        if record.shareholder_count <= 0:
            raise ValueError("shareholder count must be positive")
        if record.source_code != "tushare":
            raise ValueError("unsupported shareholder-count source")
        expected = shareholder_count_revision_key(
            symbol=record.symbol,
            statistics_date=record.statistics_date,
            announcement_date=record.announcement_date,
            shareholder_count=record.shareholder_count,
        )
        if record.revision_key != expected:
            raise ValueError("shareholder-count revision key mismatch")
        natural_key = (record.symbol, record.statistics_date, record.revision_key)
        if natural_key in seen:
            raise ValueError("duplicate shareholder-count revision")
        seen.add(natural_key)
    return records
