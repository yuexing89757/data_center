from datetime import date
from types import SimpleNamespace

import pytest

from market_data_center.domain.ingestion import IngestionStatus
from market_data_center.regulation_benchmark_service import (
    REGULATION_BENCHMARK_SYMBOLS,
    RegulationBenchmarkService,
)

TRADE_DATE = date(2026, 9, 2)


class FakePipeline:
    def __init__(self, *, accepted_rows: int = 1) -> None:
        self.accepted_rows = accepted_rows
        self.calls: list[tuple[str, date, date]] = []

    def ingest_daily_bars(self, source_symbol: str, start: date, end: date):  # type: ignore[no-untyped-def]
        self.calls.append((source_symbol, start, end))
        return SimpleNamespace(
            status=IngestionStatus.SUCCEEDED,
            accepted_rows=self.accepted_rows,
        )


def test_benchmark_collection_requests_exact_three_official_indices() -> None:
    pipeline = FakePipeline()

    summary = RegulationBenchmarkService(pipeline).collect(TRADE_DATE)

    assert REGULATION_BENCHMARK_SYMBOLS == (
        "SSE:000002",
        "SZSE:399107",
        "SZSE:399102",
    )
    assert pipeline.calls == [
        ("sh.000002", TRADE_DATE, TRADE_DATE),
        ("sz.399107", TRADE_DATE, TRADE_DATE),
        ("sz.399102", TRADE_DATE, TRADE_DATE),
    ]
    assert summary.expected_count == 3
    assert summary.accepted_count == 3


def test_benchmark_collection_fails_when_an_exact_index_bar_is_missing() -> None:
    with pytest.raises(RuntimeError, match="exact benchmark bar"):
        RegulationBenchmarkService(FakePipeline(accepted_rows=0)).collect(TRADE_DATE)
