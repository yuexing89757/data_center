"""Strictly bounded live fallback for the fixed THS:883423 bias endpoint."""

from collections.abc import Callable
from datetime import UTC, datetime
from logging import getLogger
from threading import Lock

from market_data_center.board_index_bias import calculate_board_index_bias
from market_data_center.domain.auction_indicative import SHANGHAI
from market_data_center.providers.akshare_ths import THS_BOARD_ID, AKShareTHSProvider
from market_data_center.providers.contracts import ProviderError
from market_data_center.public_api.board_index_bias_write import (
    BoardIndexPersistenceError,
    BoardIndexPersistenceQueue,
    BoardIndexPersistenceQueueFull,
)
from market_data_center.public_api.models import BoardIndexBiasResponse

LOGGER = getLogger(__name__)


class BoardIndexBiasLiveError(RuntimeError):
    pass


class BoardIndexBiasLiveBusy(BoardIndexBiasLiveError):
    pass


class BoardIndexBiasLiveUpstream(BoardIndexBiasLiveError):
    pass


class BoardIndexBiasLivePersistence(BoardIndexBiasLiveError):
    pass


class BoardIndexBiasLiveService:
    """One-process provider gate with synchronous Raw capture and queued DB registration."""

    def __init__(
        self,
        provider: AKShareTHSProvider,
        persistence: BoardIndexPersistenceQueue,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._provider = provider
        self._persistence = persistence
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = Lock()

    def fetch_current(self) -> BoardIndexBiasResponse:
        if not self._lock.acquire(blocking=False):
            raise BoardIndexBiasLiveBusy("the live board-index provider slot is busy")
        try:
            as_of_date = self._clock().astimezone(SHANGHAI).date()
            try:
                batch = self._provider.fetch_live_board_index_daily_bars(
                    THS_BOARD_ID,
                    as_of_date=as_of_date,
                    minimum_records=34,
                )
                response = calculate_board_index_bias(
                    batch.records,
                    fetched_at=batch.fetched_at,
                    data_origin="ths_live",
                    persistence_status="queued",
                )
            except (ProviderError, ValueError) as error:
                LOGGER.warning("live THS board-index fetch failed: %s", error)
                raise BoardIndexBiasLiveUpstream(
                    "the external board-index provider request failed"
                ) from error

            try:
                prepared = self._persistence.prepare(batch)
            except BoardIndexPersistenceError as error:
                raise BoardIndexBiasLivePersistence(
                    "live board-index source could not be captured"
                ) from error
            try:
                self._persistence.submit(prepared)
            except BoardIndexPersistenceQueueFull as error:
                self._persistence.discard(prepared)
                raise BoardIndexBiasLiveBusy("the board-index persistence queue is full") from error
            except BoardIndexPersistenceError as error:
                self._persistence.discard(prepared)
                raise BoardIndexBiasLivePersistence(
                    "live board-index source could not be queued"
                ) from error
            return response
        finally:
            self._lock.release()
