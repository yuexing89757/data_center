"""Stable JSON models for the external read-only API."""

from datetime import date
from decimal import Decimal

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
