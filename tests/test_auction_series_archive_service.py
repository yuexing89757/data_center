from datetime import date

import pytest

from market_data_center.auction_series_archive_service import (
    AuctionSeriesArchiveService,
    AuctionSeriesArchiveSummary,
)


class FakeArchivePersistence:
    def __init__(self, result: tuple[int, int]) -> None:
        self.result = result
        self.reference_date: date | None = None

    def archive_call_auction_market_series_snapshots(self, reference_date: date) -> tuple[int, int]:
        self.reference_date = reference_date
        return self.result


def test_archive_service_reports_inserted_and_existing_rows() -> None:
    persistence = FakeArchivePersistence((100, 70))

    result = AuctionSeriesArchiveService(persistence).run(date(2026, 9, 13))

    assert result == AuctionSeriesArchiveSummary(
        reference_date=date(2026, 9, 13),
        scanned_rows=100,
        inserted_rows=70,
        existing_rows=30,
    )
    assert persistence.reference_date == date(2026, 9, 13)


@pytest.mark.parametrize("scanned,inserted", [(-1, 0), (1, -1), (1, 2)])
def test_archive_summary_rejects_impossible_counts(scanned: int, inserted: int) -> None:
    with pytest.raises(ValueError):
        AuctionSeriesArchiveSummary(
            reference_date=date(2026, 9, 13),
            scanned_rows=scanned,
            inserted_rows=inserted,
            existing_rows=scanned - inserted,
        )
