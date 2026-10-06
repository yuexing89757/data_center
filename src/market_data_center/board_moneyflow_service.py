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


class BoardMoneyflowService:
    def __init__(
        self,
        pipeline: BoardMoneyflowPipeline,
        persistence: BoardMoneyflowPersistence,
    ) -> None:
        self._pipeline = pipeline
        self._persistence = persistence

    def sync_daily(self, as_of: date) -> BoardMoneyflowSyncSummary:
        """Re-read the latest days so late source corrections stay visible."""
        return self.sync_range(as_of - timedelta(days=DAILY_WINDOW_DAYS - 1), as_of)

    def sync_range(self, start_date: date, end_date: date) -> BoardMoneyflowSyncSummary:
        if end_date < start_date:
            raise ValueError("end_date must not precede start_date")
        batches: list[PreparedBoardMoneyflowBatch] = []
        try:
            day = start_date
            while day <= end_date:
                batches.append(self._pipeline.prepare_board_moneyflow_request(day))
                day += timedelta(days=1)
        except Exception as error:
            if batches:
                self._persistence.abort_board_moneyflow_batches(
                    tuple(batches), error_type=type(error).__name__
                )
            raise
        self._persistence.commit_board_moneyflow_batches(tuple(batches))
        return BoardMoneyflowSyncSummary(
            request_count=len(batches),
            fetched_rows=sum(batch.run.fetched_rows for batch in batches),
            accepted_rows=sum(len(batch.records) for batch in batches),
            rejected_rows=sum(batch.run.rejected_rows for batch in batches),
            trade_days=(end_date - start_date).days + 1,
        )
