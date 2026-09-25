"""Allowlisted collection of missing Regulation benchmark history."""

from dataclasses import dataclass
from datetime import date
from logging import getLogger
from typing import Protocol

from market_data_center.domain.ingestion import IngestionRun, IngestionStatus
from market_data_center.providers.contracts import ProviderError

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


class BenchmarkPersistence(Protocol):
    def benchmark_gaps(self, trade_date: date) -> dict[str, tuple[date, ...]]: ...


@dataclass(frozen=True, slots=True)
class RegulationBenchmarkCollectionSummary:
    trade_date: date
    expected_count: int
    accepted_count: int
    missing_symbols: tuple[str, ...] = ()


class RegulationBenchmarkService:
    def __init__(self, pipeline: DailyBarPipeline, persistence: BenchmarkPersistence) -> None:
        self._pipeline = pipeline
        self._persistence = persistence

    def collect(self, trade_date: date) -> RegulationBenchmarkCollectionSummary:
        gaps = self._persistence.benchmark_gaps(trade_date)
        for symbol in REGULATION_BENCHMARK_SYMBOLS:
            missing = gaps[symbol]
            if not missing:
                continue
            try:
                run = self._pipeline.ingest_daily_bars(
                    _SOURCE_SYMBOLS[symbol], min(missing), max(missing)
                )
                if run.status is not IngestionStatus.SUCCEEDED:
                    getLogger(__name__).warning("Benchmark collection incomplete: %s", symbol)
            except ProviderError as error:
                getLogger(__name__).warning(
                    "Benchmark collection failed: %s (%s)", symbol, type(error).__name__
                )
        remaining = self._persistence.benchmark_gaps(trade_date)
        missing_symbols = tuple(
            symbol for symbol in REGULATION_BENCHMARK_SYMBOLS if remaining[symbol]
        )
        return RegulationBenchmarkCollectionSummary(
            trade_date=trade_date,
            expected_count=len(REGULATION_BENCHMARK_SYMBOLS),
            accepted_count=3 - len(missing_symbols),
            missing_symbols=missing_symbols,
        )
