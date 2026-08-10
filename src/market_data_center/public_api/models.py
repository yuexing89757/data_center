"""Stable JSON models for the external read-only API."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

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
    code: str
    name: str
    close: Decimal | None
    volume: int | None
    free_float_market_cap: Decimal | None
    free_float_turnover_rate_pct: Decimal | None
    seal_amount: Decimal | None
    seal_volume_ratio: Decimal | None
    consecutive_limit_up_days: int | None
    auction_volume: int | None
    auction_amount: Decimal | None
    auction_premium_pct: Decimal | None


class DailyLimitUpListResponse(ApiModel):
    trade_date: date
    count: int = Field(ge=0)
    items: list[DailyLimitUpListItem]
