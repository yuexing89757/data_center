"""Fail-closed retention cleanup for call-auction series detail facts."""

from dataclasses import dataclass
from datetime import date
from typing import Protocol

RETAINED_COMPLETED_TRADING_DAYS = 3


class DataCleanupPersistence(Protocol):
    def latest_completed_trading_dates(
        self, reference_date: date, limit: int
    ) -> tuple[date, ...]: ...

    def delete_call_auction_market_series_snapshots_before(self, cutoff_date: date) -> int: ...


@dataclass(frozen=True, slots=True)
class DataCleanupSummary:
    cutoff_date: date
    retained_trading_days: int
    deleted_rows: int

    def __post_init__(self) -> None:
        if self.deleted_rows < 0:
            raise ValueError("deleted_rows must be nonnegative")


def retention_cutoff(reference_date: date, completed_dates: tuple[date, ...]) -> date:
    if len(completed_dates) != RETAINED_COMPLETED_TRADING_DAYS:
        raise RuntimeError("cleanup requires three completed trading dates")
    if len(set(completed_dates)) != len(completed_dates):
        raise RuntimeError("cleanup trading dates must be distinct")
    if any(item >= reference_date for item in completed_dates):
        raise RuntimeError("cleanup trading dates must precede reference date")
    return min(completed_dates)


class DataCleanupService:
    def __init__(self, persistence: DataCleanupPersistence) -> None:
        self._persistence = persistence

    def run(self, reference_date: date) -> DataCleanupSummary:
        dates = self._persistence.latest_completed_trading_dates(
            reference_date,
            RETAINED_COMPLETED_TRADING_DAYS,
        )
        cutoff_date = retention_cutoff(reference_date, dates)
        deleted_rows = self._persistence.delete_call_auction_market_series_snapshots_before(
            cutoff_date
        )
        return DataCleanupSummary(
            cutoff_date=cutoff_date,
            retained_trading_days=len(dates),
            deleted_rows=deleted_rows,
        )
