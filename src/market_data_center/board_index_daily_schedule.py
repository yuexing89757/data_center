"""Bounded retry and gap selection for the fixed THS board daily-bar job."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

from market_data_center.providers.contracts import ProviderError

BOOTSTRAP_LOOKBACK_DAYS = 90
MAX_PROVIDER_ATTEMPTS = 3


@dataclass(frozen=True, slots=True)
class BoardIndexDailyBarCollectionSummary:
    expected_date: date
    start_date: date | None
    attempts: int
    skipped: bool


def collect_board_index_daily_bar_gap(
    *,
    expected_date: date,
    latest_stored_date: date | None,
    ingest: Callable[[date, date], object],
    before_retry: Callable[[int], None] = lambda _attempt: None,
) -> BoardIndexDailyBarCollectionSummary:
    """Collect the missing tail, retrying only Provider failures."""
    if latest_stored_date is not None and latest_stored_date >= expected_date:
        return BoardIndexDailyBarCollectionSummary(expected_date, None, 0, True)

    start_date = (
        latest_stored_date + timedelta(days=1)
        if latest_stored_date is not None
        else expected_date - timedelta(days=BOOTSTRAP_LOOKBACK_DAYS)
    )
    for attempt in range(1, MAX_PROVIDER_ATTEMPTS + 1):
        try:
            ingest(start_date, expected_date)
        except ProviderError:
            if attempt == MAX_PROVIDER_ATTEMPTS:
                raise
            before_retry(attempt)
        else:
            return BoardIndexDailyBarCollectionSummary(
                expected_date,
                start_date,
                attempt,
                False,
            )
    raise AssertionError("bounded retry loop exited unexpectedly")
