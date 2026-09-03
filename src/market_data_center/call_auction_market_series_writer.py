"""Bounded single-writer persistence for opening-auction series captures."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from logging import getLogger
from queue import Queue
from threading import Thread
from typing import Protocol, cast

from market_data_center.domain.call_auction_market_series import (
    SERIES_ROUND_COUNT,
    MarketSeriesRound,
    MarketSeriesSnapshotRecord,
    MarketSeriesStatus,
)
from market_data_center.domain.ingestion import (
    IngestionRun,
    IngestionStatus,
    QualityResult,
    RawManifest,
)

LOGGER = getLogger(__name__)
WRITER_THREAD_NAME = "call-auction-series-writer"


@dataclass(frozen=True, slots=True)
class CapturedAttempt:
    run: IngestionRun
    records: tuple[MarketSeriesSnapshotRecord, ...]
    manifest: RawManifest
    quality_results: tuple[QualityResult, ...]
    elapsed: timedelta
    succeeded: bool

    def __post_init__(self) -> None:
        if self.run.status not in {IngestionStatus.SUCCEEDED, IngestionStatus.PARTIAL}:
            raise ValueError("captured attempt requires a supported terminal ingestion")
        if self.manifest.ingestion_id != self.run.ingestion_id:
            raise ValueError("captured attempt manifest does not match its ingestion")
        if self.manifest.row_count != self.run.fetched_rows:
            raise ValueError("captured attempt Raw count does not match its ingestion")
        if len(self.records) != self.run.accepted_rows:
            raise ValueError("captured attempt facts do not match accepted rows")
        if any(result.ingestion_id != self.run.ingestion_id for result in self.quality_results):
            raise ValueError("captured attempt quality does not match its ingestion")
        if self.elapsed < timedelta(0):
            raise ValueError("captured attempt elapsed time must not be negative")
        if self.succeeded is not (self.run.status is IngestionStatus.SUCCEEDED):
            raise ValueError("captured attempt success flag does not match its ingestion")


@dataclass(frozen=True, slots=True)
class CapturedRound:
    running_round: MarketSeriesRound
    completed_round: MarketSeriesRound
    attempts: tuple[CapturedAttempt, ...]

    def __post_init__(self) -> None:
        running_identity = (
            self.running_round.session_id,
            self.running_round.sample_seq,
            self.running_round.scheduled_at,
            self.running_round.expected_quotes,
        )
        completed_identity = (
            self.completed_round.session_id,
            self.completed_round.sample_seq,
            self.completed_round.scheduled_at,
            self.completed_round.expected_quotes,
        )
        if running_identity != completed_identity:
            raise ValueError("captured round identity changed")
        if self.running_round.status is not MarketSeriesStatus.RUNNING:
            raise ValueError("captured round must retain its running state")
        if self.completed_round.status is MarketSeriesStatus.RUNNING:
            raise ValueError("captured round requires a terminal result")
        if self.completed_round.attempt_count != len(self.attempts):
            raise ValueError("captured round attempt count does not match its attempts")

        ingestion_ids = tuple(attempt.run.ingestion_id for attempt in self.attempts)
        if len(set(ingestion_ids)) != len(ingestion_ids):
            raise ValueError("captured round ingestion IDs must be unique")
        selected = self.completed_round.selected_ingestion_id
        if selected is not None and selected not in ingestion_ids:
            raise ValueError("captured round selected ingestion is not an attempt")
        for attempt in self.attempts:
            request = attempt.run.request_params
            if (
                request.get("session_id") != str(self.running_round.session_id)
                or request.get("sample_seq") != self.running_round.sample_seq
            ):
                raise ValueError("captured attempt request does not match its round")
            if any(
                record.session_id != self.running_round.session_id
                or record.sample_seq != self.running_round.sample_seq
                for record in attempt.records
            ):
                raise ValueError("captured attempt facts do not match their round")


@dataclass(frozen=True, slots=True)
class WriterOutcome:
    persisted_sequences: tuple[int, ...]
    failed_sequences: tuple[int, ...]
    first_error_type: str | None


class CapturedRoundPersistence(Protocol):
    def persist_captured_round(self, captured: CapturedRound) -> None: ...


class _StopToken:
    pass


class CallAuctionMarketSeriesWriter:
    """Persist captured rounds serially without blocking the sampling producer."""

    def __init__(self, persistence: CapturedRoundPersistence) -> None:
        self._persistence = persistence
        self._stop = _StopToken()
        self._queue: Queue[CapturedRound | _StopToken] = Queue(maxsize=SERIES_ROUND_COUNT)
        self._persisted_sequences: list[int] = []
        self._failed_sequences: list[int] = []
        self._first_error_type: str | None = None
        self._closed = False
        self._thread = Thread(target=self._run, name=WRITER_THREAD_NAME, daemon=False)
        self._thread.start()

    def submit(self, captured: CapturedRound) -> None:
        if self._closed:
            raise RuntimeError("call-auction series writer is closed")
        self._queue.put(captured)

    def close_and_wait(self) -> WriterOutcome:
        if self._closed:
            raise RuntimeError("call-auction series writer is already closed")
        self._closed = True
        self._queue.put(self._stop)
        self._queue.join()
        self._thread.join()
        if self._thread.is_alive():
            raise RuntimeError("call-auction series writer did not stop")
        return WriterOutcome(
            tuple(self._persisted_sequences),
            tuple(self._failed_sequences),
            self._first_error_type,
        )

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is self._stop:
                    return
                captured = cast(CapturedRound, item)
                sample_seq = captured.completed_round.sample_seq
                try:
                    self._persistence.persist_captured_round(captured)
                except Exception as error:
                    error_type = type(error).__name__
                    self._failed_sequences.append(sample_seq)
                    if self._first_error_type is None:
                        self._first_error_type = error_type
                    LOGGER.error(
                        "call-auction series persistence failed for sample_seq=%d error_type=%s",
                        sample_seq,
                        error_type,
                    )
                else:
                    self._persisted_sequences.append(sample_seq)
            finally:
                self._queue.task_done()
