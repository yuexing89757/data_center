"""Bounded exact-date reads of immutable minute-volume peak publications."""

import re
from datetime import date, datetime
from typing import Annotated, Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request
from pydantic import Field, field_validator
from sqlalchemy import text

from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse
from market_data_center.public_api.queries import PostgreSQLPublicQueryService, PublicQueryInvalid

StockSymbol = Annotated[str, Field(pattern=r"^(SSE|SZSE|BSE):[0-9]{6}$")]


class StockMaxMinuteVolumeQuery(ApiModel):
    symbols: list[StockSymbol] = Field(
        min_length=1, max_length=500, description="标准股票代码集合, 最多500个, 按请求顺序去重"
    )
    trade_date: date = Field(description="必填准确交易日, 不回退历史日期")

    @field_validator("symbols")
    @classmethod
    def unique_symbols(cls, values: list[str]) -> list[str]:
        return list(dict.fromkeys(values))

    @field_validator("trade_date")
    @classmethod
    def nonfuture_date(cls, value: date) -> date:
        if value > datetime.now(ZoneInfo("Asia/Shanghai")).date():
            raise ValueError("trade_date must not be in the future")
        return value

    @field_validator("trade_date", mode="before")
    @classmethod
    def canonical_date_input(cls, value: object) -> object:
        if type(value) is date:
            return value
        if not isinstance(value, str) or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None:
            raise ValueError("trade_date must be an exact YYYY-MM-DD date")
        return value


class StockMaxMinuteVolumeItem(ApiModel):
    symbol: StockSymbol
    max_minute_volume_shares: int | None = Field(
        ge=0, description="当日完整且单位已认证的原生一分钟成交量最大值, 单位股; 缺失为null"
    )
    peak_at: ApiTimestamp | None = Field(
        description="最早峰值量柱的上海时间结束标签",
        json_schema_extra={"example": "2026-11-03 09:58:00"},
    )
    expected_count: int = Field(ge=0, description="该股票目标交易日应有的合法一分钟量柱数, 固定240")
    observed_count: int = Field(ge=0, description="该股票实际获取的目标日一分钟量柱数")
    quality_status: Literal["complete", "missing"] = Field(description="目标日完整性及数量认证状态")
    quality_reasons: list[str] = Field(description="缺失或认证失败的稳定原因码, 完整值为空数组")


class StockMaxMinuteVolumeResponse(ApiModel):
    trade_date: date
    status: Literal["not_ready", "partial", "ready"]
    version: int | None
    calculation_id: UUID | None
    algorithm_version: str | None
    calculated_at: ApiTimestamp | None = Field(
        description="计算批次的上海时间", json_schema_extra={"example": "2026-11-03 20:31:00"}
    )
    published_at: ApiTimestamp | None = Field(
        description="当前不可变版本的上海时间发布时刻",
        json_schema_extra={"example": "2026-11-03 20:31:00"},
    )
    items: list[StockMaxMinuteVolumeItem]


router = APIRouter()


@router.post(
    "/v1/stock-daily-indicators/minute-volume-peak/query",
    response_model=StockMaxMinuteVolumeResponse,
    tags=["市场数据"],
    summary="按准确交易日查询最大一分钟成交股数",
    description="只读单一已发布计算版本, 最多500只股票; 无发布时not_ready, 不触发采集或回退日期。",
    responses={status: {"model": ErrorResponse} for status in (401, 422, 503, 504)},
)
def query_stock_max_minute_volume(
    body: StockMaxMinuteVolumeQuery, request: Request
) -> StockMaxMinuteVolumeResponse:
    service = cast(PostgreSQLPublicQueryService, request.app.state.query_service)
    try:
        rows = service._execute(
            text("select api_v1.query_stock_max_minute_volume(:symbols,:trade_date) as payload"),
            {"symbols": body.symbols, "trade_date": body.trade_date},
        )
    except PublicQueryInvalid as error:
        raise HTTPException(422, "query parameters were rejected") from error
    return StockMaxMinuteVolumeResponse.model_validate(rows[0]["payload"])
