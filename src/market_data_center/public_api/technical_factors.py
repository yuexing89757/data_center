"""Bounded, exact-date reads of stored source technical factor snapshots."""

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from typing import Annotated, cast

from fastapi import APIRouter, HTTPException, Request
from pydantic import ConfigDict, Field, field_validator
from sqlalchemy import text

from market_data_center.public_api.models import ApiModel, ErrorResponse, QuantityAwareModel
from market_data_center.public_api.queries import PostgreSQLPublicQueryService, PublicQueryInvalid

StockCode = Annotated[str, Field(pattern=r"^[0-9]{6}$")]


class StockTechnicalFactorQuery(ApiModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"trade_date": "2026-10-15", "codes": ["601188", "000001", "600000"]}]
        }
    )
    codes: list[StockCode] = Field(
        min_length=1, max_length=100, description="六位股票代码集合, 最多100个, 重复代码去重"
    )
    trade_date: date = Field(description="必填精确交易日, 不回退历史日期")

    @field_validator("codes")
    @classmethod
    def deduplicate_codes(cls, codes: list[str]) -> list[str]:
        return list(dict.fromkeys(codes))


class StockTechnicalFactorItem(QuantityAwareModel):
    symbol: str = Field(description="标准证券代码")
    code: StockCode = Field(description="六位股票代码")
    name: str = Field(description="证券名称")
    trade_date: date = Field(description="来源快照的交易日")
    source_code: str = Field(description="快照来源代码")
    factor_catalog_version: str = Field(description="对外技术因子目录版本; v2隔离未认证数量叶子。")
    source_factor_catalog_version: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
        description="不可变来源快照的原始因子目录版本。",
    )
    unadjusted: Mapping[str, Decimal | None] = Field(
        description="不复权技术因子, 十进制字符串或空值"
    )
    forward_adjusted: Mapping[str, Decimal | None] = Field(
        description="前复权技术因子, 十进制字符串或空值"
    )
    backward_adjusted: Mapping[str, Decimal | None] = Field(
        description="后复权技术因子, 十进制字符串或空值"
    )
    common: Mapping[str, int | None] = Field(description="共同计数项, 整数或空值")


class StockTechnicalFactorResponse(ApiModel):
    trade_date: date = Field(description="请求的精确交易日")
    items: list[StockTechnicalFactorItem] = Field(description="当日已存储的技术因子快照")
    missing_codes: list[StockCode] = Field(description="当日无快照或未登记的请求代码")


router = APIRouter()


@router.post(
    "/v1/stock-technical-factors/query",
    response_model=StockTechnicalFactorResponse,
    tags=["市场数据"],
    summary="按股票代码和交易日查询专业版技术因子",
    description=(
        "查询最多100个六位股票代码在精确交易日的已存储技术因子快照, 不回退历史日期。"
        "三种复权口径分别返回十进制字符串或空值, 共同计数项返回整数或空值。"
        "仅调用有界只读接口, 不触发采集或计算。"
    ),
    responses={status: {"model": ErrorResponse} for status in (401, 422, 503, 504)},
)
def query_stock_technical_factors(
    body: StockTechnicalFactorQuery, request: Request
) -> StockTechnicalFactorResponse:
    service = cast(PostgreSQLPublicQueryService, request.app.state.query_service)
    try:
        rows = service._execute(
            text(
                "select api_v1.query_stock_technical_factors_by_codes"
                "(:codes, :trade_date) as payload"
            ),
            {"codes": body.codes, "trade_date": body.trade_date},
        )
    except PublicQueryInvalid as error:
        raise HTTPException(422, "query parameters were rejected") from error
    return StockTechnicalFactorResponse.model_validate(rows[0]["payload"])
