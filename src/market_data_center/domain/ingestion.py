"""Ingestion run, raw manifest and quality audit models."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import PurePosixPath
from re import fullmatch
from uuid import UUID


class ProviderCode(StrEnum):
    BAOSTOCK = "baostock"
    AKSHARE = "akshare"
    AKSHARE_THS = "akshare_ths"
    PYTDX = "pytdx"
    TUSHARE = "tushare"
    PYTDX_HQ = "pytdx_hq"
    EASTMONEY = "eastmoney"


class DatasetCode(StrEnum):
    SECURITY = "security"
    TRADING_CALENDAR = "trading_calendar"
    DAILY_BAR = "daily_bar"
    CAPITAL = "capital"
    CLASSIFICATION_CATALOG = "classification_catalog"
    CLASSIFICATION_MEMBERS = "classification_members"
    BOARD_INDEX = "board_index"
    BOARD_INDEX_DAILY_BAR = "board_index_daily_bar"
    BOARD_INDEX_CONSTITUENT_SNAPSHOT = "board_index_constituent_snapshot"
    STOCK_DAILY_INDICATOR = "stock_daily_indicator"
    DEDUCTED_PROFIT = "deducted_profit"
    FIVE_LEVEL_QUOTE = "five_level_quote"
    EOD_QUOTE_SNAPSHOT = "eod_quote_snapshot"
    CALL_AUCTION_SNAPSHOT = "call_auction_snapshot"
    CALL_AUCTION_MARKET_SNAPSHOT = "call_auction_market_snapshot"
    CALL_AUCTION_INDICATIVE_DETAIL = "call_auction_indicative_detail"
    DRAGON_TIGER = "dragon_tiger"
    TODAY_LIMIT_UP_SOURCE = "today_limit_up_source"
    CONVERTIBLE_BOND = "convertible_bond"
    CONVERTIBLE_BOND_DAILY_BAR = "convertible_bond_daily_bar"


class IngestionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PARTIAL = "partial"


class RawFileFormat(StrEnum):
    PARQUET = "parquet"
    JSONL = "jsonl"


class QualitySeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class QualityStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"


TERMINAL_INGESTION_STATUSES = frozenset(
    {IngestionStatus.SUCCEEDED, IngestionStatus.FAILED, IngestionStatus.PARTIAL}
)


@dataclass(frozen=True, slots=True)
class IngestionRun:
    ingestion_id: UUID
    provider_code: ProviderCode
    dataset_code: DatasetCode
    status: IngestionStatus
    requested_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    request_params: Mapping[str, object] = field(default_factory=dict)
    fetched_rows: int = 0
    accepted_rows: int = 0
    rejected_rows: int = 0
    error_summary: str | None = None
    replayed_from_raw_id: UUID | None = None

    def __post_init__(self) -> None:
        counts = (self.fetched_rows, self.accepted_rows, self.rejected_rows)
        if any(count < 0 for count in counts):
            raise ValueError("ingestion row counts must not be negative")
        if self.accepted_rows + self.rejected_rows > self.fetched_rows:
            raise ValueError("accepted_rows + rejected_rows must not exceed fetched_rows")
        if self.status in TERMINAL_INGESTION_STATUSES and self.finished_at is None:
            raise ValueError("terminal ingestion status requires finished_at")
        started_at = self.started_at
        finished_at = self.finished_at
        if finished_at is not None and started_at is None:
            raise ValueError("finished_at requires started_at")
        if started_at is not None and started_at < self.requested_at:
            raise ValueError("started_at must not precede requested_at")
        if finished_at is not None and started_at is not None and finished_at < started_at:
            raise ValueError("finished_at must not precede started_at")


@dataclass(frozen=True, slots=True)
class RawManifest:
    raw_id: UUID
    ingestion_id: UUID
    object_path: str
    file_format: RawFileFormat
    content_sha256: str
    byte_size: int
    row_count: int
    schema_version: str
    storage_backend: str = "local"

    def __post_init__(self) -> None:
        if self.storage_backend != "local":
            raise ValueError("phase-one storage_backend must be local")
        path = PurePosixPath(self.object_path)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("object_path must be a safe relative POSIX path")
        if not fullmatch(r"[0-9a-f]{64}", self.content_sha256):
            raise ValueError("content_sha256 must be 64 lowercase hexadecimal characters")
        if self.byte_size < 0 or self.row_count < 0:
            raise ValueError("raw byte_size and row_count must not be negative")
        if not self.schema_version.strip():
            raise ValueError("schema_version must not be blank")


@dataclass(frozen=True, slots=True)
class ReplaySource:
    source_ingestion_id: UUID
    provider_code: ProviderCode
    dataset_code: DatasetCode
    requested_at: datetime
    request_params: Mapping[str, object]
    manifest: RawManifest | None


@dataclass(frozen=True, slots=True)
class QualityResult:
    quality_result_id: UUID
    ingestion_id: UUID
    dataset_code: DatasetCode
    rule_code: str
    severity: QualitySeverity
    status: QualityStatus
    message: str
    natural_key: Mapping[str, object] | None = None
    details: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.rule_code.strip():
            raise ValueError("rule_code must not be blank")
        if not self.message.strip():
            raise ValueError("quality result message must not be blank")

    @property
    def blocks_core_write(self) -> bool:
        return self.severity is QualitySeverity.ERROR and self.status is QualityStatus.FAILED
