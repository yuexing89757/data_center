"""Provider boundary contracts shared by pipeline code."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from market_data_center.domain.records import DailyBarRecord, SecurityRecord, TradingDayRecord

type ProviderRecord = SecurityRecord | TradingDayRecord | DailyBarRecord
type RawRow = Mapping[str, str]


class ProviderError(RuntimeError):
    """Raised when a provider request or response is unsuccessful."""


@dataclass(frozen=True, slots=True)
class ProviderBatch[RecordT: ProviderRecord]:
    records: Sequence[RecordT]
    raw_rows: Sequence[RawRow]
    request_params: Mapping[str, object]
    schema_version: str
