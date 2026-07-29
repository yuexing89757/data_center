"""Provider boundary contracts shared by pipeline code."""

from collections.abc import Callable, Mapping, Sequence
from datetime import date
from types import TracebackType
from typing import Protocol, Self

from market_data_center.domain.board_index import BoardIndexProviderRecord
from market_data_center.domain.classification import ClassificationRecord
from market_data_center.domain.records import (
    CapitalRecord,
    DailyBarRecord,
    SecurityRecord,
    TradingDayRecord,
)

type ProviderRecord = (
    SecurityRecord
    | TradingDayRecord
    | DailyBarRecord
    | CapitalRecord
    | ClassificationRecord
    | BoardIndexProviderRecord
)
type RawRow = Mapping[str, str]


class ProviderError(RuntimeError):
    """Raised when a provider request or response is unsuccessful."""


class ProviderRequestUnavailable(ProviderError):
    """The provider is healthy but cannot serve this specific dataset request."""


class MarketDataProvider(Protocol):
    """Provider-neutral capability contract consumed by the pipeline."""

    source_code: str

    def source_symbol(self, symbol: str) -> str: ...

    def fetch_securities(self) -> "ProviderBatch[SecurityRecord]": ...

    def fetch_trading_calendar(
        self, start_date: date, end_date: date
    ) -> "ProviderBatch[TradingDayRecord]": ...

    def fetch_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> "ProviderBatch[DailyBarRecord]": ...

    def fetch_capital(self, source_symbol: str) -> "ProviderBatch[CapitalRecord]": ...

    def fetch_classification_catalog(
        self, classification_type: str, snapshot_date: date
    ) -> "ProviderBatch[ClassificationRecord]": ...

    def fetch_classification_members(
        self, classification_type: str, classification_code: str, snapshot_date: date
    ) -> "ProviderBatch[ClassificationRecord]": ...


class ManagedMarketDataProvider(MarketDataProvider, Protocol):
    """Provider adapter that owns optional client-session resources."""

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class BoardIndexProvider(Protocol):
    """Dedicated capability boundary for third-party board indices."""

    source_code: str

    def fetch_board_indexes(self) -> "ProviderBatch[BoardIndexProviderRecord]": ...

    def fetch_board_index_daily_bars(
        self, board_id: str, start_date: date, end_date: date
    ) -> "ProviderBatch[BoardIndexProviderRecord]": ...

    def fetch_board_index_constituents(
        self, board_id: str, snapshot_date: date
    ) -> "ProviderBatch[BoardIndexProviderRecord]": ...


class ManagedBoardIndexProvider(BoardIndexProvider, Protocol):
    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class ProviderBatch[RecordT: ProviderRecord]:
    """Fetched raw payload with optional lazy provider-boundary normalization."""

    def __init__(
        self,
        *,
        raw_rows: Sequence[RawRow],
        request_params: Mapping[str, object],
        schema_version: str,
        records: Sequence[RecordT] | None = None,
        record_factory: Callable[[], Sequence[RecordT]] | None = None,
    ) -> None:
        if (records is None) == (record_factory is None):
            raise ValueError("ProviderBatch requires exactly one record source")
        self.raw_rows = raw_rows
        self.request_params = request_params
        self.schema_version = schema_version
        self._records = records
        self._record_factory = record_factory

    @property
    def records(self) -> Sequence[RecordT]:
        if self._records is None:
            if self._record_factory is None:  # pragma: no cover - constructor invariant
                raise RuntimeError("ProviderBatch record factory is missing")
            try:
                self._records = tuple(self._record_factory())
            except ProviderError:
                raise
            except Exception as error:
                raise ProviderError("provider response normalization failed") from error
        return self._records
