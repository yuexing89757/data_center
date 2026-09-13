"""Fail-closed retention cleanup for call-auction series detail facts."""

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from typing import Protocol

RETAINED_COMPLETED_TRADING_DAYS = 3


class DataCleanupPersistence(Protocol):
    def latest_completed_trading_dates(
        self, reference_date: date, limit: int
    ) -> tuple[date, ...]: ...

    def verify_and_delete_archived_call_auction_market_series_snapshots_before(
        self, cutoff_date: date
    ) -> tuple[int, int]: ...

    def delete_call_auction_market_series_snapshot_history_before(
        self, cutoff_date: date
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class DataCleanupSummary:
    cutoff_date: date
    retained_trading_days: int
    verified_rows: int
    deleted_rows: int
    history_cutoff_date: date
    history_deleted_rows: int

    def __post_init__(self) -> None:
        if min(self.verified_rows, self.deleted_rows, self.history_deleted_rows) < 0:
            raise ValueError(
                "verified_rows, deleted_rows, and history_deleted_rows must be nonnegative"
            )
        if self.verified_rows != self.deleted_rows:
            raise ValueError("verified_rows must equal deleted_rows")


def retention_cutoff(reference_date: date, completed_dates: tuple[date, ...]) -> date:
    if len(completed_dates) != RETAINED_COMPLETED_TRADING_DAYS:
        raise RuntimeError("cleanup requires three completed trading dates")
    if len(set(completed_dates)) != len(completed_dates):
        raise RuntimeError("cleanup trading dates must be distinct")
    if any(item >= reference_date for item in completed_dates):
        raise RuntimeError("cleanup trading dates must precede reference date")
    return min(completed_dates)


def six_calendar_months_before(reference_date: date) -> date:
    month_index = reference_date.year * 12 + reference_date.month - 1 - 6
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    day = min(reference_date.day, monthrange(year, month)[1])
    return date(year, month, day)


class DataCleanupService:
    def __init__(self, persistence: DataCleanupPersistence) -> None:
        self._persistence = persistence

    def run(self, reference_date: date) -> DataCleanupSummary:
        dates = self._persistence.latest_completed_trading_dates(
            reference_date,
            RETAINED_COMPLETED_TRADING_DAYS,
        )
        cutoff_date = retention_cutoff(reference_date, dates)
        verified_rows, deleted_rows = (
            self._persistence.verify_and_delete_archived_call_auction_market_series_snapshots_before(
                cutoff_date
            )
        )
        history_cutoff_date = six_calendar_months_before(reference_date)
        history_deleted_rows = (
            self._persistence.delete_call_auction_market_series_snapshot_history_before(
                history_cutoff_date
            )
        )
        return DataCleanupSummary(
            cutoff_date=cutoff_date,
            retained_trading_days=len(dates),
            verified_rows=verified_rows,
            deleted_rows=deleted_rows,
            history_cutoff_date=history_cutoff_date,
            history_deleted_rows=history_deleted_rows,
        )
