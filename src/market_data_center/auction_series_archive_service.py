"""Idempotent history archive for call-auction market-series facts."""

from dataclasses import dataclass
from datetime import date
from typing import Protocol


class AuctionSeriesArchivePersistence(Protocol):
    def archive_call_auction_market_series_snapshots(
        self, reference_date: date
    ) -> tuple[int, int]: ...


@dataclass(frozen=True, slots=True)
class AuctionSeriesArchiveSummary:
    reference_date: date
    scanned_rows: int
    inserted_rows: int
    existing_rows: int

    def __post_init__(self) -> None:
        if min(self.scanned_rows, self.inserted_rows, self.existing_rows) < 0:
            raise ValueError("archive row counts must be nonnegative")
        if self.inserted_rows + self.existing_rows != self.scanned_rows:
            raise ValueError("archive row counts must balance")


class AuctionSeriesArchiveService:
    def __init__(self, persistence: AuctionSeriesArchivePersistence) -> None:
        self._persistence = persistence

    def run(self, reference_date: date) -> AuctionSeriesArchiveSummary:
        scanned_rows, inserted_rows = (
            self._persistence.archive_call_auction_market_series_snapshots(reference_date)
        )
        return AuctionSeriesArchiveSummary(
            reference_date=reference_date,
            scanned_rows=scanned_rows,
            inserted_rows=inserted_rows,
            existing_rows=scanned_rows - inserted_rows,
        )
