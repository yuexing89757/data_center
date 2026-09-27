"""Authenticated market-emotion chart reads."""

# ruff: noqa: RUF001 - Chinese OpenAPI descriptions.

from datetime import date
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Query, Request
from pydantic import ConfigDict, Field

from market_data_center.providers.kaipanla_emotion_chart import KaipanlaEmotionChartProvider
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class KaipanlaEmotionChartItem(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    trade_date: date = Field(description="来源交易日期，YYYY-MM-DD。")
    emotion_index: int = Field(ge=0, le=100, description="开盘啦情绪指标，来源值，非本系统计算。")
    large_drawdown_count: int = Field(ge=0, description="来源标记为大幅回撤的股票家数。")
    limit_up_count: int = Field(ge=0, description="图表来源口径的涨停家数。")
    consecutive_limit_up_height: int = Field(ge=0, description="来源连板高度，单位板。")


class KaipanlaEmotionChartResponse(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    source_code: Literal["kaipanla"] = Field(description="数据来源：开盘啦。")
    source_layout: Literal["kaipanla.market_emotion.chart.v1"] = Field(
        description="已验证的历史图表布局版本。"
    )
    persisted: Literal[False] = Field(description="固定 false，响应不入库。")
    observed_at: ApiTimestamp = Field(description="收到响应的上海时区时间。")
    offset: int = Field(ge=0, le=10000, description="来源列表偏移。")
    limit: int = Field(ge=1, le=100, description="每页条数上限。")
    returned_count: int = Field(ge=0, le=100, description="实际返回条数。")
    total: None = Field(description="来源没有可靠总数，固定为空。")
    next_offset: int | None = Field(description="满页时可尝试的下一页偏移，下页可能为空。")
    items: list[KaipanlaEmotionChartItem] = Field(description="按日期倒序排列的历史图表数据。")


def _provider(request: Request) -> KaipanlaEmotionChartProvider:
    return cast(KaipanlaEmotionChartProvider, request.app.state.kaipanla_emotion_chart_provider)


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
    "/chart",
    response_model=KaipanlaEmotionChartResponse,
    summary="查询开盘啦市场情绪折线图",
    description=(
        "读取涨跌统计更多页面的历史图表，每次访问一页。"
        "返回情绪指标、大幅回撤家数、涨停家数和连板高度。"
        "复用 X-API-Key，不入库，不自动翻页。"
    ),
)
def emotion_chart(
    provider: Annotated[KaipanlaEmotionChartProvider, Depends(_provider)],
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
) -> KaipanlaEmotionChartResponse:
    return KaipanlaEmotionChartResponse.model_validate(provider.fetch(offset=offset, limit=limit))
