from datetime import date
from types import SimpleNamespace

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


class GapPersistence:
    def __init__(self, pipeline: FakePipeline, *, stays_missing: bool = False) -> None:
        self.pipeline = pipeline
        self.stays_missing = stays_missing

    def benchmark_gaps(self, trade_date: date) -> dict[str, tuple[date, ...]]:
        if self.pipeline.calls and not self.stays_missing:
            return {symbol: () for symbol in REGULATION_BENCHMARK_SYMBOLS}
        return {symbol: (date(2026, 8, 11), trade_date) for symbol in REGULATION_BENCHMARK_SYMBOLS}


def test_benchmark_collection_requests_exact_three_official_indices() -> None:
    pipeline = FakePipeline()

    summary = RegulationBenchmarkService(pipeline, GapPersistence(pipeline)).collect(TRADE_DATE)

    assert REGULATION_BENCHMARK_SYMBOLS == (
        "SSE:000002",
        "SZSE:399107",
        "SZSE:399102",
    )
    assert pipeline.calls == [
        ("sh.000002", date(2026, 8, 11), TRADE_DATE),
        ("sz.399107", date(2026, 8, 11), TRADE_DATE),
        ("sz.399102", date(2026, 8, 11), TRADE_DATE),
    ]
    assert summary.expected_count == 3
    assert summary.accepted_count == 3


def test_benchmark_collection_reports_when_index_history_is_missing() -> None:
    pipeline = FakePipeline(accepted_rows=0)
    summary = RegulationBenchmarkService(
        pipeline, GapPersistence(pipeline, stays_missing=True)
    ).collect(TRADE_DATE)
    assert summary.accepted_count == 0
    assert summary.missing_symbols == REGULATION_BENCHMARK_SYMBOLS


def test_benchmark_complete_history_skips_network_and_checks_persisted_coverage() -> None:
    pipeline = FakePipeline()

    class CompletePersistence:
        def benchmark_gaps(self, trade_date):
            return {symbol: () for symbol in REGULATION_BENCHMARK_SYMBOLS}

    summary = RegulationBenchmarkService(pipeline, CompletePersistence()).collect(TRADE_DATE)
    assert summary.accepted_count == 3
    assert pipeline.calls == []


def test_benchmark_successful_request_does_not_hide_missing_history() -> None:
    pipeline = FakePipeline(accepted_rows=30)
    summary = RegulationBenchmarkService(
        pipeline, GapPersistence(pipeline, stays_missing=True)
    ).collect(TRADE_DATE)
    assert summary.accepted_count == 0


def test_one_missing_index_is_partial_and_does_not_block_other_segments() -> None:
    from market_data_center.operations_service import _result_statistics
    from market_data_center.providers.contracts import ProviderError

    class PartialPersistence:
        def benchmark_gaps(self, trade_date):
            return {"SSE:000002": (), "SZSE:399107": (date(2026, 8, 11),), "SZSE:399102": ()}

    class FailedPipeline(FakePipeline):
        def ingest_daily_bars(self, source_symbol, start, end):
            self.calls.append((source_symbol, start, end))
            raise ProviderError("upstream unavailable")

    summary = RegulationBenchmarkService(FailedPipeline(), PartialPersistence()).collect(TRADE_DATE)
    assert summary.accepted_count == 2
    assert summary.missing_symbols == ("SZSE:399107",)
    fetched, accepted, rejected, status = _result_statistics(summary)
    assert (fetched, accepted, rejected, status.value) == (3, 2, 1, "partial")
