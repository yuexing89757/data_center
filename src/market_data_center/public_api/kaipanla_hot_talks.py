"""Authenticated reads of the newest published topic list."""

# ruff: noqa: RUF001 - Chinese OpenAPI descriptions.

from datetime import date
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Request
from pydantic import ConfigDict, Field

from market_data_center.providers.kaipanla_hot_talks import KaipanlaHotTalksProvider
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class KaipanlaHotTalkItem(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    topic_id: str = Field(description="来源条目 ID。")
    rank: int = Field(ge=1, le=100, description="来源日期组内的展示排名。")
    title: str = Field(description="话题标题，保留来源文字。")
    heat_value: int | None = Field(
        ge=0, description="来源页面热度值，精确整数；不推断为阅读人数；缺失为空。"
    )
    published_at: ApiTimestamp | None = Field(
        description="来源发布时间，可能与分组日期不同；缺失为空。",
        json_schema_extra={"example": "2026-10-08 18:42:50"},
    )
    has_vote: bool = Field(description="来源标记该条目是否包含投票，不提交投票。")


class KaipanlaHotTalksResponse(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    source_code: Literal["kaipanla"] = Field(description="固定来源：开盘啦。")
    source_layout: Literal["kaipanla.hot_talks.v1"] = Field(description="列表字段布局版本。")
    persisted: Literal[False] = Field(description="固定 false，响应不入库。")
    observed_at: ApiTimestamp = Field(
        description="收到响应的上海时区时间。",
        json_schema_extra={"example": "2026-10-10 19:05:00"},
    )
    topic_date: date | None = Field(description="来源最新日期组；不是交易日；成功空列表时为空。")
    returned_count: int = Field(ge=0, le=100, description="该日期全部条目数量。")
    items: list[KaipanlaHotTalkItem] = Field(description="最新来源日期的条目，按来源展示顺序排列。")


def _provider(request: Request) -> KaipanlaHotTalksProvider:
    return cast(KaipanlaHotTalksProvider, request.app.state.kaipanla_hot_talks_provider)


router = APIRouter(
    prefix="/api/v1/realtime/kaipanla",
    tags=["实时接口"],
    responses={status: {"model": ErrorResponse} for status in (401, 502)},
)


@router.get(
    "/hot-talks",
    response_model=KaipanlaHotTalksResponse,
    summary="查询开盘啦明天炒什么最新话题",
    description=(
        "请求时读取首页明天炒什么（盘中显示今天炒什么）查看全部列表，"
        "仅返回最新来源日期全部条目。单次上游请求，复用 X-API-Key，"
        "不入库、不自动翻页、不依赖模拟器。"
    ),
)
def hot_talks(
    provider: Annotated[KaipanlaHotTalksProvider, Depends(_provider)],
) -> KaipanlaHotTalksResponse:
    return KaipanlaHotTalksResponse.model_validate(provider.fetch())
