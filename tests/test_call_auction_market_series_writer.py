from __future__ import annotations

from datetime import date, timedelta
from threading import current_thread
from threading import enumerate as enumerate_threads
from uuid import uuid4

from market_data_center.call_auction_market_series_writer import (
    CallAuctionMarketSeriesWriter,
    CapturedRound,
    WriterOutcome,
)
from market_data_center.domain.call_auction_market_series import (
    MarketSeriesRound,
    MarketSeriesStatus,
    series_slots,
)

TRADE_DATE = date(2026, 8, 17)
SLOTS = series_slots(TRADE_DATE)


class RecordingPersistence:
    def __init__(self, fail_sequences: set[int] | None = None) -> None:
        self.fail_sequences = fail_sequences or set()
        self.calls: list[tuple[int, str, int | None]] = []

    def persist_captured_round(self, captured: CapturedRound) -> None:
        sample_seq = captured.completed_round.sample_seq
        thread = current_thread()
        self.calls.append((sample_seq, thread.name, thread.ident))
        if sample_seq in self.fail_sequences:
            raise RuntimeError("database unavailable")


def captured_round(sample_seq: int) -> CapturedRound:
    running = MarketSeriesRound(
        session_id=uuid4(),
        sample_seq=sample_seq,
        scheduled_at=SLOTS[sample_seq],
        collected_at=None,
        status=MarketSeriesStatus.RUNNING,
        attempt_count=0,
        expected_quotes=2,
        successful_quotes=0,
        failed_quotes=0,
        selected_ingestion_id=None,
    )
    completed = MarketSeriesRound(
        session_id=running.session_id,
        sample_seq=sample_seq,
        scheduled_at=running.scheduled_at,
        collected_at=running.scheduled_at + timedelta(seconds=1),
        status=MarketSeriesStatus.FAILED,
        attempt_count=0,
        expected_quotes=2,
        successful_quotes=0,
        failed_quotes=2,
        selected_ingestion_id=None,
        error_summary="missed_sampling_round",
    )
    return CapturedRound(running, completed, ())


def test_writer_persists_rounds_in_fifo_on_one_named_thread() -> None:
    persistence = RecordingPersistence()
    writer = CallAuctionMarketSeriesWriter(persistence)

    writer.submit(captured_round(sample_seq=0))
    writer.submit(captured_round(sample_seq=1))
    outcome = writer.close_and_wait()

    assert [item[0] for item in persistence.calls] == [0, 1]
    assert {item[1] for item in persistence.calls} == {"call-auction-series-writer"}
    assert len({item[2] for item in persistence.calls}) == 1
    assert outcome == WriterOutcome((0, 1), (), None)
    assert all(thread.name != "call-auction-series-writer" for thread in enumerate_threads())


def test_writer_records_failure_and_continues_with_later_rounds() -> None:
    persistence = RecordingPersistence(fail_sequences={1})
    writer = CallAuctionMarketSeriesWriter(persistence)

    for sample_seq in range(3):
        writer.submit(captured_round(sample_seq=sample_seq))
    outcome = writer.close_and_wait()

    assert [item[0] for item in persistence.calls] == [0, 1, 2]
    assert outcome == WriterOutcome((0, 2), (1,), "RuntimeError")
