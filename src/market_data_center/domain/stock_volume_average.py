"""Provider-neutral inputs and results for ADR-0076."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from uuid import UUID

from market_data_center.domain.records import TradeStatus

ALGORITHM_VERSION = "1.0.0"
CALCULATION_CODE = "cn_a_stock_volume_average_5d"


@dataclass(frozen=True, slots=True)
class VolumeDay:
    trade_date: date
    volume: int | None
    present: bool = True
    certified: bool = True
    trade_status: TradeStatus = TradeStatus.UNKNOWN


@dataclass(frozen=True, slots=True)
class StockVolumeInput:
    symbol: str
    ipo_date: date
    delisting_date: date | None
    days: tuple[VolumeDay, ...]


@dataclass(frozen=True, slots=True)
class VolumeWindowInput:
    trade_date: date
    trading_dates: tuple[date, ...]
    stocks: tuple[StockVolumeInput, ...]


@dataclass(frozen=True, slots=True)
class VolumeAverageRecord:
    symbol: str
    trade_date: date
    window_start_date: date
    volume_average_5d: Decimal | None
    covered_days: int
    quality_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class VolumeCalculation:
    records: tuple[VolumeAverageRecord, ...]
    input_hash: str

    @property
    def complete_count(self) -> int:
        return sum(row.volume_average_5d is not None for row in self.records)


@dataclass(frozen=True, slots=True)
class VolumeAverageBuildSummary:
    status: str
    trade_date: date
    calculation_id: UUID | None
    version: int | None
    candidate_count: int
    complete_count: int
