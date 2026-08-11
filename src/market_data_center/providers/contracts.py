"""Provider boundary contracts shared by pipeline code."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from types import TracebackType
from typing import Protocol, Self

from market_data_center.domain.board_index import BoardIndexProviderRecord
from market_data_center.domain.classification import ClassificationRecord
from market_data_center.domain.convertible_bond import ConvertibleBondRecord
from market_data_center.domain.deducted_profit import DeductedProfitRecord
from market_data_center.domain.realtime_quote import FiveLevelQuoteSnapshotRecord
from market_data_center.domain.records import (
    CapitalRecord,
    DailyBarRecord,
    SecurityRecord,
    TradingDayRecord,
)
from market_data_center.domain.stock_daily_indicator import StockDailyIndicatorSnapshotRecord

type ProviderRecord = (
    SecurityRecord
    | TradingDayRecord
    | DailyBarRecord
    | CapitalRecord
    | ClassificationRecord
    | BoardIndexProviderRecord
    | StockDailyIndicatorSnapshotRecord
    | DeductedProfitRecord
    | FiveLevelQuoteSnapshotRecord
    | ConvertibleBondRecord
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

    def fetch_stock_daily_indicators(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> "ProviderBatch[StockDailyIndicatorSnapshotRecord]": ...

    def fetch_stock_daily_indicator_snapshot(
        self, trade_date: date
    ) -> "ProviderBatch[StockDailyIndicatorSnapshotRecord]": ...

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


class DeductedProfitProvider(Protocol):
    source_code: str

    def fetch_deducted_profit_updates(
        self, as_of_date: date
    ) -> "ProviderBatch[DeductedProfitRecord]": ...


class ConvertibleBondProvider(Protocol):
    """Dedicated capability boundary for convertible bond facts."""

    source_code: str

    def fetch_convertible_bonds(self) -> "ProviderBatch[ConvertibleBondRecord]": ...

    def fetch_convertible_bond_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> "ProviderBatch[ConvertibleBondRecord]": ...


@dataclass(frozen=True, slots=True)
class RealtimeQuoteNormalizationError:
    raw_row_index: int
    symbol: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class RealtimeQuoteFetch:
    raw_rows: tuple[RawRow, ...]
    records: tuple[FiveLevelQuoteSnapshotRecord, ...]
    requested_symbols: tuple[str, ...]
    failed_symbols: tuple[str, ...]
    schema_version: str
    raw_observed_at: tuple[datetime, ...]
    normalization_errors: tuple[RealtimeQuoteNormalizationError, ...] = ()

    def __post_init__(self) -> None:
        if len(self.records) > len(self.raw_rows):
            raise ValueError("normalized quote records cannot outnumber provider Raw rows")
        if len(self.raw_observed_at) != len(self.raw_rows):
            raise ValueError("quote Raw rows and observation timestamps must have equal counts")
        if any(
            observed_at.tzinfo is None or observed_at.utcoffset() != timedelta()
            for observed_at in self.raw_observed_at
        ):
            raise ValueError("quote Raw observation timestamps must be aware UTC datetimes")
        if any(
            error.raw_row_index < 0 or error.raw_row_index >= len(self.raw_rows)
            for error in self.normalization_errors
        ):
            raise ValueError("quote normalization error references an invalid Raw row")


class RealtimeQuoteProvider(Protocol):
    source_code: str

    def fetch_five_level_quotes(
        self, symbols: Sequence[str], *, deadline: datetime | None = None
    ) -> RealtimeQuoteFetch: ...


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
