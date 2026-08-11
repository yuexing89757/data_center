import pytest

from market_data_center.daily_bar_batch import DailyBarBulkSummary


def test_daily_bar_bulk_summary_statuses() -> None:
    assert DailyBarBulkSummary(10, 10, 0, 0).status == "succeeded"
    assert DailyBarBulkSummary(10, 8, 1, 1).status == "partial"
    assert DailyBarBulkSummary(10, 0, 9, 1).status == "failed"
    assert DailyBarBulkSummary(0, 0, 0, 0).status == "succeeded"


def test_daily_bar_bulk_summary_requires_complete_accounting() -> None:
    with pytest.raises(ValueError, match="account for every requested symbol"):
        DailyBarBulkSummary(10, 8, 1, 0)
