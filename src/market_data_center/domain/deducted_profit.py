"""Point-in-time deducted-profit financial facts."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from hashlib import sha256


@dataclass(frozen=True, slots=True)
class DeductedProfitRecord:
    symbol: str
    report_period: date
    announcement_date: date
    actual_announcement_date: date | None
    cumulative_deducted_profit: Decimal | None
    quarterly_deducted_profit: Decimal | None
    update_flag: str | None
    revision_key: str
    source_code: str

    @property
    def effective_announcement_date(self) -> date:
        return self.actual_announcement_date or self.announcement_date


def deducted_profit_revision_key(
    *,
    symbol: str,
    report_period: date,
    announcement_date: date,
    actual_announcement_date: date | None,
    cumulative_deducted_profit: Decimal | None,
    quarterly_deducted_profit: Decimal | None,
    update_flag: str | None,
) -> str:
    values = (
        symbol,
        report_period.isoformat(),
        announcement_date.isoformat(),
        actual_announcement_date.isoformat() if actual_announcement_date else "",
        str(cumulative_deducted_profit) if cumulative_deducted_profit is not None else "",
        str(quarterly_deducted_profit) if quarterly_deducted_profit is not None else "",
        update_flag or "",
    )
    return sha256("\x1f".join(values).encode()).hexdigest()


def validate_deducted_profits(
    records: tuple[DeductedProfitRecord, ...],
    *,
    known_symbols: set[str],
) -> tuple[DeductedProfitRecord, ...]:
    seen: set[tuple[str, date, str]] = set()
    for record in records:
        if record.symbol not in known_symbols:
            raise ValueError(f"unknown deducted-profit symbol: {record.symbol}")
        if record.announcement_date < record.report_period or (
            record.actual_announcement_date is not None
            and record.actual_announcement_date < record.report_period
        ):
            raise ValueError("deducted-profit announcement precedes report period")
        if record.cumulative_deducted_profit is None and record.quarterly_deducted_profit is None:
            raise ValueError("deducted-profit record has no profit values")
        expected = deducted_profit_revision_key(
            symbol=record.symbol,
            report_period=record.report_period,
            announcement_date=record.announcement_date,
            actual_announcement_date=record.actual_announcement_date,
            cumulative_deducted_profit=record.cumulative_deducted_profit,
            quarterly_deducted_profit=record.quarterly_deducted_profit,
            update_flag=record.update_flag,
        )
        if record.revision_key != expected:
            raise ValueError("deducted-profit revision key mismatch")
        natural_key = (record.symbol, record.report_period, record.revision_key)
        if natural_key in seen:
            raise ValueError("duplicate deducted-profit revision")
        seen.add(natural_key)
    return records
