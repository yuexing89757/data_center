"""Stable JSON models for the external read-only API."""

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from market_data_center.domain import (
    Exchange,
    SecurityStatus,
    SecurityType,
    TradeStatus,
)


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HealthResponse(ApiModel):
    status: str
    service: str = "market-data-center-api"
    version: str = "0.2.0"


class ErrorDetail(ApiModel):
    code: str
    message: str


class ErrorResponse(ApiModel):
    error: ErrorDetail


class SecurityItem(ApiModel):
    symbol: str
    code: str
    exchange: Exchange
    current_name: str
    security_type: SecurityType
    status: SecurityStatus
    ipo_date: date | None
    delisting_date: date | None


class SecuritySearchResponse(ApiModel):
    count: int = Field(ge=0)
    items: list[SecurityItem]


class DailyBarItem(ApiModel):
    symbol: str
    trade_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    previous_close: Decimal | None
    volume: int | None
    amount: Decimal | None
    trade_status: TradeStatus
    is_st: bool | None


class DailyBarResponse(ApiModel):
    symbol: str
    start_date: date
    end_date: date
    count: int = Field(ge=0)
    items: list[DailyBarItem]


class ClassificationMembersResponse(ApiModel):
    snapshot_date: date
    member_count: int = Field(ge=0)
    returned_count: int = Field(ge=0)
    members: list[str]


class LimitUpPoolItem(ApiModel):
    symbol: str
    code: str
    name: str
    free_float_market_cap_cny: Decimal


class LimitUpPoolOmissionReasons(ApiModel):
    missing_name: int = Field(ge=0)
    missing_close: int = Field(ge=0)
    missing_free_float_shares: int = Field(ge=0)


class LimitUpPoolResponse(ApiModel):
    snapshot_id: UUID
    calculation_id: UUID
    trade_date: date
    effective_trade_date: date
    version: int = Field(ge=1)
    rule_version: str
    algorithm_version: str
    input_hash: str
    generated_at: datetime
    total_candidate_count: int = Field(ge=0)
    valid_count: int = Field(ge=0)
    returned_count: int = Field(ge=0)
    omitted_count: int = Field(ge=0)
    has_more: bool
    omission_reasons: LimitUpPoolOmissionReasons
    items: list[LimitUpPoolItem]


class DailyLimitUpListItem(ApiModel):
    symbol: str
    code: str
    name: str
    previous_close: Decimal
    close: Decimal
    limit_price: Decimal
    change_percent: Decimal
    free_float_shares: int = Field(gt=0)
    free_float_market_cap_cny: Decimal
    first_limit_up_at: datetime | None
    last_limit_up_at: datetime | None
    open_count: int | None = Field(default=None, ge=0)
    limit_up_duration_seconds: int | None = Field(default=None, ge=0)
    duration_semantics: str
    source_reported_sealed_funds_cny: Decimal | None
    closing_bid1_price: Decimal | None
    closing_bid1_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid2_price: Decimal | None
    closing_bid2_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid3_price: Decimal | None
    closing_bid3_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid4_price: Decimal | None
    closing_bid4_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid5_price: Decimal | None
    closing_bid5_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid1_sealing_amount_cny: Decimal | None
    daily_bar_ingestion_id: UUID
    indicator_ingestion_id: UUID
    name_ingestion_id: UUID
    pool_calculation_id: UUID
    source_observation_ingestion_id: UUID | None
    source_observation_raw_id: UUID | None
    order_book_ingestion_id: UUID | None
    volume: int | None = Field(default=None, ge=0)
    amount_cny: Decimal | None
    free_float_turnover_rate_pct: Decimal | None
    consecutive_limit_up_days: int | None = Field(default=None, ge=1)


class DailyLimitUpQualitySummary(ApiModel):
    total_findings: int = Field(ge=0)
    by_rule: dict[str, int]


class DailyLimitUpListResponse(ApiModel):
    snapshot_id: UUID
    calculation_id: UUID | None
    trade_date: date
    version: int = Field(ge=1)
    status: Literal["ready", "partial", "deferred", "failed"]
    rule_version: str
    algorithm_version: str
    input_hash: str
    source_ingestion_id: UUID | None
    generated_at: datetime
    candidate_count: int = Field(ge=0)
    member_count: int = Field(ge=0)
    rejected_count: int = Field(ge=0)
    offset: int = Field(ge=0)
    returned_count: int = Field(ge=0)
    has_more: bool
    quality: DailyLimitUpQualitySummary
    items: list[DailyLimitUpListItem]


SixDigitCode = Annotated[str, Field(pattern=r"^[0-9]{6}$")]


class CallAuctionMarketSnapshotQuery(ApiModel):
    trade_date: date
    codes: list[SixDigitCode] = Field(min_length=1, max_length=500)

    @field_validator("codes")
    @classmethod
    def deduplicate_codes(cls, codes: list[str]) -> list[str]:
        return list(dict.fromkeys(codes))


class CallAuctionMarketSnapshotItem(ApiModel):
    symbol: str
    code: SixDigitCode
    observed_at: datetime
    last_price: Decimal | None
    previous_close: Decimal | None
    high_price: Decimal | None
    low_price: Decimal | None
    cumulative_volume: int | None = Field(default=None, ge=0)
    cumulative_amount: Decimal | None


class CallAuctionMarketSnapshotResponse(ApiModel):
    trade_date: date
    ingestion_id: UUID
    ingestion_status: Literal["succeeded", "partial"]
    requested_count: int = Field(ge=1, le=500)
    returned_count: int = Field(ge=0)
    missing_codes: list[SixDigitCode]
    items: list[CallAuctionMarketSnapshotItem]
