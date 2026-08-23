"""Bounded request-time Tencent quotes for the external FastAPI surface."""

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Protocol

from market_data_center.domain.realtime_quote import FiveLevelQuoteSnapshotRecord
from market_data_center.providers.contracts import ProviderError, RealtimeQuoteProvider
from market_data_center.providers.tencent_quote import TencentQuoteProvider
from market_data_center.public_api.models import (
    LatestStockQuoteItem,
    LatestStockQuoteResponse,
    StockQuoteLevel,
)
from market_data_center.settings import ApiSettings


class TencentQuoteLiveUpstream(RuntimeError):
    """The bounded Tencent request did not produce any usable response."""


class TencentQuoteLiveService(Protocol):
    def fetch_current(
        self, codes: tuple[str, ...], max_age_seconds: int
    ) -> LatestStockQuoteResponse: ...


class DirectTencentQuoteLiveService:
    """Fetch current quotes without database, Raw, ingestion, or cache side effects."""

    def __init__(
        self,
        provider: RealtimeQuoteProvider | None = None,
        *,
        deadline_seconds: float = 8.0,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._provider = provider or TencentQuoteProvider()
        self._deadline_seconds = deadline_seconds
        self._clock = clock

    @classmethod
    def from_settings(cls, settings: ApiSettings) -> "DirectTencentQuoteLiveService":
        return cls(deadline_seconds=settings.fastapi_tencent_quote_deadline_seconds)

    def fetch_current(
        self, codes: tuple[str, ...], max_age_seconds: int
    ) -> LatestStockQuoteResponse:
        symbols_by_code = {
            code: symbol for code in codes if (symbol := _stock_symbol(code)) is not None
        }
        requested_symbols = tuple(symbols_by_code.values())
        if not requested_symbols:
            return _response(codes, max_age_seconds, ())

        deadline = self._clock().astimezone(UTC) + timedelta(seconds=self._deadline_seconds)
        try:
            fetched = self._provider.fetch_five_level_quotes(requested_symbols, deadline=deadline)
        except ProviderError as error:
            raise TencentQuoteLiveUpstream("Tencent quote request failed") from error
        if not fetched.records and set(fetched.failed_symbols) == set(requested_symbols):
            raise TencentQuoteLiveUpstream("Tencent quote request returned no usable rows")
        return _response(codes, max_age_seconds, fetched.records)


def _response(
    requested_codes: Sequence[str],
    max_age_seconds: int,
    records: Sequence[FiveLevelQuoteSnapshotRecord],
) -> LatestStockQuoteResponse:
    items_by_code: dict[str, LatestStockQuoteItem] = {}
    for record in records:
        symbol = record.symbol
        code = symbol.partition(":")[2]
        name = record.name
        source_timestamp = record.source_timestamp
        if code not in requested_codes or not name or source_timestamp is None:
            continue
        items_by_code[code] = LatestStockQuoteItem(
            symbol=symbol,
            code=code,
            name=name,
            observed_at=record.observed_at,
            source_timestamp=source_timestamp,
            quote_status=record.quote_status.value,
            last_price=record.last_price,
            previous_close=record.previous_close,
            open=record.open,
            high=record.high,
            low=record.low,
            cumulative_volume_shares=record.cumulative_volume,
            cumulative_amount_cny=record.cumulative_amount,
            bid_levels=[
                StockQuoteLevel(
                    level=level.level,
                    price=level.price,
                    volume_shares=level.volume,
                )
                for level in record.bid_levels
            ],
            ask_levels=[
                StockQuoteLevel(
                    level=level.level,
                    price=level.price,
                    volume_shares=level.volume,
                )
                for level in record.ask_levels
            ],
        )
    items = [items_by_code[code] for code in requested_codes if code in items_by_code]
    missing = [code for code in requested_codes if code not in items_by_code]
    return LatestStockQuoteResponse(
        max_age_seconds=max_age_seconds,
        requested_count=len(requested_codes),
        found_count=len(items),
        missing_codes=missing,
        items=items,
    )


def _stock_symbol(code: str) -> str | None:
    if code.startswith("6"):
        return f"SSE:{code}"
    if code.startswith(("0", "3")):
        return f"SZSE:{code}"
    return None
