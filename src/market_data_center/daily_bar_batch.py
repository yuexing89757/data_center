"""Typed hand-off between Daily Bar preparation and bounded persistence."""

from dataclasses import dataclass

from market_data_center.domain import (
    DailyBarRecord,
    IngestionEnvelope,
    IngestionRun,
    QualityResult,
    RawManifest,
)


@dataclass(frozen=True, slots=True)
class PreparedDailyBarBatch:
    """One provider/run lineage unit prepared for a database commit."""

    run: IngestionRun
    manifest: RawManifest
    records: tuple[IngestionEnvelope[DailyBarRecord], ...]
    quality_results: tuple[QualityResult, ...]


@dataclass(frozen=True, slots=True)
class DailyBarBulkSummary:
    """Bounded full-market outcome measured in requested securities."""

    expected_symbols: int
    accepted_symbols: int
    failed_symbols: int
    unavailable_symbols: int

    def __post_init__(self) -> None:
        counts = (
            self.expected_symbols,
            self.accepted_symbols,
            self.failed_symbols,
            self.unavailable_symbols,
        )
        if min(counts) < 0:
            raise ValueError("daily-bar bulk counts cannot be negative")
        if (
            self.accepted_symbols + self.failed_symbols + self.unavailable_symbols
            != self.expected_symbols
        ):
            raise ValueError("daily-bar bulk outcome must account for every requested symbol")

    @property
    def rejected_symbols(self) -> int:
        return self.failed_symbols + self.unavailable_symbols

    @property
    def status(self) -> str:
        if self.expected_symbols and not self.accepted_symbols and self.failed_symbols:
            return "failed"
        if self.rejected_symbols:
            return "partial"
        return "succeeded"
