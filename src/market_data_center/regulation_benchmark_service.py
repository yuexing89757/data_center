"""Allowlisted exact-day collection of Regulation benchmark indices."""

from dataclasses import dataclass
from datetime import date
from typing import Protocol

from market_data_center.domain.ingestion import IngestionRun, IngestionStatus

REGULATION_BENCHMARK_SYMBOLS = (
    "SSE:000002",
    "SZSE:399107",
    "SZSE:399102",
)

_SOURCE_SYMBOLS = {
    "SSE:000002": "sh.000002",
    "SZSE:399107": "sz.399107",
    "SZSE:399102": "sz.399102",
}


class DailyBarPipeline(Protocol):
    def ingest_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> IngestionRun: ...


@dataclass(frozen=True, slots=True)
class RegulationBenchmarkCollectionSummary:
    trade_date: date
    expected_count: int
    accepted_count: int


class RegulationBenchmarkService:
    def __init__(self, pipeline: DailyBarPipeline) -> None:
        self._pipeline = pipeline

    def collect(self, trade_date: date) -> RegulationBenchmarkCollectionSummary:
        accepted = 0
        for symbol in REGULATION_BENCHMARK_SYMBOLS:
            run = self._pipeline.ingest_daily_bars(_SOURCE_SYMBOLS[symbol], trade_date, trade_date)
            if run.status is not IngestionStatus.SUCCEEDED or run.accepted_rows != 1:
                raise RuntimeError(f"exact benchmark bar is missing: {symbol}")
            accepted += 1
        return RegulationBenchmarkCollectionSummary(
            trade_date=trade_date,
            expected_count=len(REGULATION_BENCHMARK_SYMBOLS),
            accepted_count=accepted,
        )
