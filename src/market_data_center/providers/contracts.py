"""Provider boundary contracts shared by pipeline code."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from types import TracebackType
from typing import Protocol, Self

from market_data_center.domain.records import DailyBarRecord, SecurityRecord, TradingDayRecord

type ProviderRecord = SecurityRecord | TradingDayRecord | DailyBarRecord
type RawRow = Mapping[str, str]


class ProviderError(RuntimeError):
    """Raised when a provider request or response is unsuccessful."""


class MarketDataProvider(Protocol):
    """Provider-neutral capability contract consumed by the pipeline."""

    source_code: str

    def fetch_securities(self) -> "ProviderBatch[SecurityRecord]": ...

    def fetch_trading_calendar(
        self, start_date: date, end_date: date
    ) -> "ProviderBatch[TradingDayRecord]": ...

    def fetch_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> "ProviderBatch[DailyBarRecord]": ...


class ManagedMarketDataProvider(MarketDataProvider, Protocol):
    """Provider adapter that owns optional client-session resources."""

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class ProviderBatch[RecordT: ProviderRecord]:
    records: Sequence[RecordT]
    raw_rows: Sequence[RawRow]
    request_params: Mapping[str, object]
    schema_version: str
