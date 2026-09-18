from datetime import date

import pytest

from market_data_center.data_cleanup_service import (
    DataCleanupService,
    DataCleanupSummary,
    retention_cutoff,
    six_calendar_months_before,
)


class FakeCleanupPersistence:
    def __init__(
        self,
        dates: tuple[date, ...],
        verified_and_deleted: tuple[int, int] = (0, 0),
        history_deleted_rows: int = 0,
        quality_result_deleted_rows: int = 0,
    ) -> None:
        self.dates = dates
        self.verified_and_deleted = verified_and_deleted
        self.history_deleted_rows = history_deleted_rows
        self.quality_result_deleted_rows = quality_result_deleted_rows
        self.requested_dates: tuple[date, int] | None = None
        self.deleted_before: date | None = None
        self.history_deleted_before: date | None = None
        self.quality_results_deleted_before: date | None = None

    def latest_completed_trading_dates(self, reference_date: date, limit: int) -> tuple[date, ...]:
        self.requested_dates = (reference_date, limit)
        return self.dates

    def verify_and_delete_archived_call_auction_market_series_snapshots_before(
        self, cutoff_date: date
    ) -> tuple[int, int]:
        self.deleted_before = cutoff_date
        return self.verified_and_deleted

    def delete_call_auction_market_series_snapshot_history_before(self, cutoff_date: date) -> int:
        self.history_deleted_before = cutoff_date
        return self.history_deleted_rows

    def delete_quality_results_before(self, cutoff_date: date) -> int:
        self.quality_results_deleted_before = cutoff_date
        return self.quality_result_deleted_rows


def test_retention_cutoff_keeps_latest_three_completed_trading_days() -> None:
    completed = (date(2026, 9, 2), date(2026, 9, 1), date(2026, 8, 31))

    assert retention_cutoff(date(2026, 9, 3), completed) == date(2026, 8, 31)


def test_retention_cutoff_uses_completed_dates_on_weekend() -> None:
    completed = (date(2026, 9, 4), date(2026, 9, 3), date(2026, 9, 2))

    assert retention_cutoff(date(2026, 9, 6), completed) == date(2026, 9, 2)


def test_retention_cutoff_fails_closed_without_three_dates() -> None:
    with pytest.raises(RuntimeError, match="three completed trading dates"):
        retention_cutoff(date(2026, 9, 3), (date(2026, 9, 2), date(2026, 9, 1)))


def test_retention_cutoff_rejects_duplicate_dates() -> None:
    with pytest.raises(RuntimeError, match="distinct"):
        retention_cutoff(
            date(2026, 9, 3),
            (date(2026, 9, 2), date(2026, 9, 2), date(2026, 9, 1)),
        )


def test_retention_cutoff_rejects_dates_not_before_reference() -> None:
    with pytest.raises(RuntimeError, match="precede reference date"):
        retention_cutoff(
            date(2026, 9, 3),
            (date(2026, 9, 3), date(2026, 9, 2), date(2026, 9, 1)),
        )


def test_cleanup_service_deletes_only_after_cutoff_is_resolved() -> None:
    persistence = FakeCleanupPersistence(
        dates=(date(2026, 9, 2), date(2026, 9, 1), date(2026, 8, 31)),
        verified_and_deleted=(123, 123),
        history_deleted_rows=45,
        quality_result_deleted_rows=7,
    )

    result = DataCleanupService(persistence).run(date(2026, 9, 3))

    assert result == DataCleanupSummary(
        cutoff_date=date(2026, 8, 31),
        retained_trading_days=3,
        verified_rows=123,
        deleted_rows=123,
        history_cutoff_date=date(2026, 3, 3),
        history_deleted_rows=45,
        quality_result_cutoff_date=date(2026, 8, 4),
        quality_result_deleted_rows=7,
    )
    assert persistence.requested_dates == (date(2026, 9, 3), 3)
    assert persistence.deleted_before == date(2026, 8, 31)
    assert persistence.history_deleted_before == date(2026, 3, 3)
    assert persistence.quality_results_deleted_before == date(2026, 8, 4)


def test_cleanup_service_does_not_delete_when_calendar_history_is_incomplete() -> None:
    persistence = FakeCleanupPersistence(
        dates=(date(2026, 9, 2), date(2026, 9, 1)),
    )

    with pytest.raises(RuntimeError, match="three completed trading dates"):
        DataCleanupService(persistence).run(date(2026, 9, 3))

    assert persistence.deleted_before is None


def test_cleanup_summary_rejects_negative_deleted_count() -> None:
    with pytest.raises(ValueError, match="deleted_rows"):
        DataCleanupSummary(
            cutoff_date=date(2026, 8, 31),
            retained_trading_days=3,
            verified_rows=0,
            deleted_rows=-1,
            history_cutoff_date=date(2026, 3, 3),
            history_deleted_rows=0,
            quality_result_cutoff_date=date(2026, 8, 4),
            quality_result_deleted_rows=0,
        )


@pytest.mark.parametrize(
    ("reference_date", "expected"),
    [
        (date(2026, 9, 13), date(2026, 3, 13)),
        (date(2026, 8, 31), date(2026, 2, 28)),
        (date(2028, 8, 31), date(2028, 2, 29)),
        (date(2026, 1, 31), date(2025, 7, 31)),
    ],
)
def test_six_calendar_months_before_clamps_month_end(reference_date: date, expected: date) -> None:
    assert six_calendar_months_before(reference_date) == expected
