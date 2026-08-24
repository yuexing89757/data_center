"""Typed hand-offs for shareholder-count request preparation and publication."""

from dataclasses import dataclass

from market_data_center.domain import (
    IngestionEnvelope,
    IngestionRun,
    QualityResult,
    RawManifest,
    ShareholderCountRecord,
)


@dataclass(frozen=True, slots=True)
class PreparedShareholderCountBatch:
    """One Tushare request captured and validated before aggregate publication."""

    run: IngestionRun
    manifest: RawManifest | None
    records: tuple[IngestionEnvelope[ShareholderCountRecord], ...]
    quality_results: tuple[QualityResult, ...] = ()


@dataclass(frozen=True, slots=True)
class ShareholderCountSyncSummary:
    """Outcome of one controlled daily or backfill synchronization."""

    request_count: int
    fetched_rows: int
    accepted_rows: int
    superseded_request_count: int

    def __post_init__(self) -> None:
        counts = (
            self.request_count,
            self.fetched_rows,
            self.accepted_rows,
            self.superseded_request_count,
        )
        if min(counts) < 0:
            raise ValueError("shareholder-count synchronization counts cannot be negative")
        if self.accepted_rows > self.fetched_rows:
            raise ValueError("accepted shareholder-count rows cannot exceed fetched rows")
        if self.superseded_request_count > self.request_count:
            raise ValueError("superseded requests cannot exceed total requests")
