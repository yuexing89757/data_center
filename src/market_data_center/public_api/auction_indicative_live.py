"""Strictly bounded live provider access for one current-day auction detail request."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from logging import getLogger
from threading import Lock
from time import monotonic

from market_data_center.domain.auction_indicative import (
    SHANGHAI,
    CallAuctionIndicativeDetailRecord,
)
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.eastmoney_auction import EastmoneyAuctionIndicativeProvider
from market_data_center.public_api.auction_indicative_write import (
    AuctionIndicativePersistenceError,
    AuctionIndicativePersistenceQueue,
    AuctionIndicativePersistenceQueueFull,
    PreparedAuctionIndicativePersistence,
)
from market_data_center.public_api.models import (
    AuctionIndicativeDetailItem,
    AuctionIndicativeDetailResponse,
    AuctionIndicativeQuality,
)

LOGGER = getLogger(__name__)


class AuctionIndicativeLiveError(RuntimeError):
    pass


class AuctionIndicativeLiveInvalid(AuctionIndicativeLiveError):
    pass


class AuctionIndicativeLiveBusy(AuctionIndicativeLiveError):
    pass


class AuctionIndicativeLiveUpstream(AuctionIndicativeLiveError):
    pass


class AuctionIndicativeLiveUnavailable(AuctionIndicativeLiveError):
    pass


class AuctionIndicativeLivePersistence(AuctionIndicativeLiveError):
    pass


@dataclass(frozen=True, slots=True)
class _CachedFetch:
    fetched_at: datetime
    stored_at: float
    records: tuple[CallAuctionIndicativeDetailRecord, ...]
    source_row_count: int
    persistence: PreparedAuctionIndicativePersistence


class LiveAuctionIndicativeService:
    """One-process, one-request-at-a-time provider boundary with a tiny private cache."""

    def __init__(
        self,
        provider: EastmoneyAuctionIndicativeProvider,
        persistence: AuctionIndicativePersistenceQueue,
        *,
        cache_seconds: float = 3.0,
        minimum_interval_seconds: float = 1.0,
        clock: Callable[[], datetime] | None = None,
        timer: Callable[[], float] = monotonic,
    ) -> None:
        if not 0 <= cache_seconds <= 5 or not 0.5 <= minimum_interval_seconds <= 10:
            raise ValueError("live auction resource bounds are invalid")
        self._provider = provider
        self._persistence = persistence
        self._cache_seconds = cache_seconds
        self._minimum_interval_seconds = minimum_interval_seconds
        self._clock = clock or (lambda: datetime.now(UTC))
        self._timer = timer
        self._lock = Lock()
        self._last_provider_request = float("-inf")
        self._cache: dict[tuple[str, date], _CachedFetch] = {}

    def fetch(
        self, symbol: str, trade_date: date, offset: int, limit: int
    ) -> AuctionIndicativeDetailResponse:
        return self._fetch(symbol, trade_date, offset, limit, now=self._clock())

    def fetch_current(
        self, symbol: str, offset: int, limit: int
    ) -> AuctionIndicativeDetailResponse:
        now = self._clock()
        trade_date = now.astimezone(SHANGHAI).date()
        return self._fetch(symbol, trade_date, offset, limit, now=now)

    def _fetch(
        self, symbol: str, trade_date: date, offset: int, limit: int, *, now: datetime
    ) -> AuctionIndicativeDetailResponse:
        local_now = now.astimezone(SHANGHAI)
        if trade_date != local_now.date():
            raise AuctionIndicativeLiveInvalid("only the current Shanghai date is supported")
        if local_now.time() < time(9, 26):
            raise AuctionIndicativeLiveInvalid("auction detail is unavailable before 09:26")
        if not self._lock.acquire(blocking=False):
            raise AuctionIndicativeLiveBusy("the live provider request slot is busy")
        try:
            timer_now = self._timer()
            key = (symbol, trade_date)
            cached = self._cache.get(key)
            cache_hit = cached is not None and timer_now - cached.stored_at <= self._cache_seconds
            if not cache_hit:
                if timer_now - self._last_provider_request < self._minimum_interval_seconds:
                    raise AuctionIndicativeLiveBusy("live provider request rate is limited")
                self._last_provider_request = timer_now
                try:
                    batch = self._provider.fetch_current_day(symbol, trade_date, now=now)
                    records = tuple(
                        sorted(
                            batch.records,
                            key=lambda record: (record.observed_at, record.source_sequence),
                        )
                    )
                except ProviderError as error:
                    root_cause: BaseException = error
                    while root_cause.__cause__ is not None:
                        root_cause = root_cause.__cause__
                    LOGGER.warning(
                        "live auction provider failed for %s: %s: %s",
                        symbol,
                        type(root_cause).__name__,
                        root_cause,
                    )
                    raise AuctionIndicativeLiveUpstream(
                        "the external auction provider request failed"
                    ) from error
                if not records:
                    raise AuctionIndicativeLiveUnavailable(
                        "the external provider returned no auction-window observations"
                    )
                try:
                    persistence = self._persistence.prepare(
                        symbol=symbol,
                        trade_date=trade_date,
                        fetched_at=now,
                        raw_rows=batch.raw_rows,
                        records=records,
                    )
                except AuctionIndicativePersistenceError as error:
                    raise AuctionIndicativeLivePersistence(
                        "live observations could not be captured"
                    ) from error
                try:
                    self._persistence.submit(persistence)
                except AuctionIndicativePersistenceQueueFull as error:
                    self._persistence.discard(persistence)
                    raise AuctionIndicativeLiveBusy(
                        "the asynchronous persistence queue is full"
                    ) from error
                except AuctionIndicativePersistenceError as error:
                    self._persistence.discard(persistence)
                    raise AuctionIndicativeLivePersistence(
                        "live observations could not be queued for persistence"
                    ) from error
                cached = _CachedFetch(
                    fetched_at=now,
                    stored_at=timer_now,
                    records=records,
                    source_row_count=len(batch.raw_rows),
                    persistence=persistence,
                )
                self._cache = {key: cached}
            assert cached is not None
            records = cached.records
            page = records[offset : offset + limit]
            return AuctionIndicativeDetailResponse(
                symbol=symbol,
                trade_date=trade_date,
                fetched_at=cached.fetched_at,
                source="eastmoney",
                live_provider_derived=True,
                data_origin="eastmoney_live",
                cache_hit=cache_hit,
                persistence_status="queued",
                version=None,
                ingestion_status=None,
                ingestion_id=cached.persistence.ingestion_id,
                raw_id=cached.persistence.raw_id,
                input_hash=cached.persistence.input_hash,
                semantics="auction_virtual_indicative_matching_detail",
                is_exchange_trade_tick=False,
                is_order_by_order=False,
                total_count=len(records),
                offset=offset,
                returned_count=len(page),
                has_more=offset + len(page) < len(records),
                quality=AuctionIndicativeQuality(
                    status="complete",
                    source_row_count=cached.source_row_count,
                    accepted_auction_row_count=len(records),
                    source_display_classification_trusted=False,
                    raw_captured=True,
                    database_persistence="queued",
                ),
                items=[
                    AuctionIndicativeDetailItem(
                        observed_at=record.observed_at,
                        source_sequence=record.source_sequence,
                        indicative_price=record.indicative_price,
                        displayed_volume_shares=record.displayed_volume_shares,
                        source_display_classification=record.source_display_classification.value,
                    )
                    for record in page
                ],
            )
        finally:
            self._lock.release()
