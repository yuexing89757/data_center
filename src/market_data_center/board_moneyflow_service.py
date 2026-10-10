"""Controlled daily synchronization for board money-flow facts."""

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Protocol

from market_data_center.board_moneyflow_batch import (
    BoardMoneyflowSyncSummary,
    PreparedBoardMoneyflowBatch,
)

DAILY_WINDOW_DAYS = 5


class BoardMoneyflowPipeline(Protocol):
    def prepare_board_moneyflow_request(self, trade_date: date) -> PreparedBoardMoneyflowBatch: ...


class BoardMoneyflowPersistence(Protocol):
    def commit_board_moneyflow_batches(
        self, batches: Sequence[PreparedBoardMoneyflowBatch]
    ) -> None: ...

    def abort_board_moneyflow_batches(
        self,
        batches: Sequence[PreparedBoardMoneyflowBatch],
        *,
        error_type: str,
    ) -> None: ...

    def board_moneyflow_completed_days(self, start_date: date, end_date: date) -> set[date]: ...


class BoardMoneyflowService:
    def __init__(
        self,
        pipeline: BoardMoneyflowPipeline,
        persistence: BoardMoneyflowPersistence,
    ) -> None:
        self._pipeline = pipeline
        self._persistence = persistence

    def sync_daily(self, as_of: date) -> BoardMoneyflowSyncSummary:
        """Rolling five-day window; already-published days are skipped."""
        return self.sync_range(as_of - timedelta(days=DAILY_WINDOW_DAYS - 1), as_of)

    def sync_range(self, start_date: date, end_date: date) -> BoardMoneyflowSyncSummary:
        """Publish each trade day in its own transaction, resumably.

        Days already present in ``core.board_moneyflow`` are skipped so a
        rerun after a mid-range failure only fetches the missing days.
        """
        if end_date < start_date:
            raise ValueError("end_date must not precede start_date")
        completed = self._persistence.board_moneyflow_completed_days(start_date, end_date)
        batches: list[PreparedBoardMoneyflowBatch] = []
        day = start_date
        while day <= end_date:
            if day not in completed:
                batches.append(self._pipeline.prepare_board_moneyflow_request(day))
                self._persistence.commit_board_moneyflow_batches((batches[-1],))
            day += timedelta(days=1)
        return BoardMoneyflowSyncSummary(
            request_count=len(batches),
            fetched_rows=sum(batch.run.fetched_rows for batch in batches),
            accepted_rows=sum(len(batch.records) for batch in batches),
            rejected_rows=sum(batch.run.rejected_rows for batch in batches),
            trade_days=len(batches),
        )
