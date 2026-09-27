"""Authenticated, bounded Kaipanla money-effect list reads."""

# ruff: noqa: RUF001 - Chinese OpenAPI descriptions.

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Query, Request
from pydantic import ConfigDict, Field

from market_data_center.providers.kaipanla_money_effect import KaipanlaMoneyEffectProvider
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class KaipanlaMoneyEffectItem(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    trade_date: date = Field(description="来源日期，格式 YYYY-MM-DD。")
    board_success_rate_pct: Decimal = Field(
        ge=0, le=100, description="打板成功率，单位百分比；50.00 表示 50.00%。"
    )
    board_profit_rate_pct: Decimal = Field(description="打板盈利率，单位百分比；允许负值。")


class KaipanlaMoneyEffectResponse(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    source_code: Literal["kaipanla"] = Field(description="数据来源：开盘啦。")
    source_layout: Literal["kaipanla.market_emotion.money_effect.v1"] = Field(
        description="已验证的赚钱效应列表布局版本。"
    )
    persisted: Literal[False] = Field(description="固定 false；查询结果不写数据库或 Raw。")
    observed_at: ApiTimestamp = Field(description="本服务收到响应的上海时区时间。")
    offset: int = Field(ge=0, le=10000, description="本页记录偏移。")
    limit: int = Field(ge=1, le=100, description="每页上限，最多100条。")
    returned_count: int = Field(ge=0, le=100, description="本页实际返回数量。")
    total: None = Field(description="固定为空；上游没有提供可靠总数。")
    next_offset: int | None = Field(description="满页时可继续尝试的下一页偏移；下页可能为空。")
    items: list[KaipanlaMoneyEffectItem] = Field(description="按来源日期倒序排列的赚钱效应列表。")


def _provider(request: Request) -> KaipanlaMoneyEffectProvider:
    return cast(KaipanlaMoneyEffectProvider, request.app.state.kaipanla_money_effect_provider)


router = APIRouter(
    prefix="/api/v1/realtime/kaipanla/market-emotion",
    tags=["实时接口"],
    responses={
        401: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
    },
)


@router.get(
    "/money-effect",
    response_model=KaipanlaMoneyEffectResponse,
    summary="分页查询开盘啦赚钱效应",
    description=(
        "请求时直连开盘啦，每次只读取一页历史赚钱效应，不读取展开详情。"
        "返回日期、打板成功率和打板盈利率；保持来源倒序。"
        "复用 X-API-Key，不写数据库、不保存 Raw、不触发 Worker、不自动翻页或重试。"
    ),
)
def money_effect(
    provider: Annotated[KaipanlaMoneyEffectProvider, Depends(_provider)],
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> KaipanlaMoneyEffectResponse:
    return KaipanlaMoneyEffectResponse.model_validate(provider.fetch(offset=offset, limit=limit))
