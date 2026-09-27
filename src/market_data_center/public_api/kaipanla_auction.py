"""Authenticated, typed auction-pool reads in the existing FastAPI application."""

# ruff: noqa: RUF001 - Chinese OpenAPI descriptions.

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Query, Request
from pydantic import ConfigDict, Field

from market_data_center.providers.kaipanla_auction import KaipanlaAuctionProvider
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class KaipanlaAuctionItem(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    symbol: str = Field(description="标准证券代码，如 SSE:600825。")
    code: str = Field(pattern=r"^[0-9]{6}$", description="六位股票代码，保留前导零。")
    name: str | None = Field(description="股票名称；来源缺失时为空。")
    board_name: str | None = Field(description="来源板块或题材文字；不等同于数据中心标准分类。")
    change_pct: Decimal | None = Field(description="来源普通行情涨跌幅，单位百分比；不是竞价涨幅。")
    actual_float_market_value_cny: Decimal | None = Field(
        description="来源实际流通市值，单位元；不是流通股数。"
    )
    limit_up_bid_amount_cny: Decimal | None = Field(
        description="涨停委买额，单位元；不是竞价成交额。"
    )
    auction_change_pct: Decimal | None = Field(
        description="竞价涨跌幅，单位百分比；10.0567 表示 10.0567%。"
    )
    auction_net_amount_cny: Decimal | None = Field(description="竞价净额，单位元；允许负值。")
    auction_turnover_rate_pct: Decimal | None = Field(
        description="竞价换手率，单位百分比；0.23 表示 0.23%。"
    )
    auction_amount_cny: Decimal | None = Field(description="竞价成交额，单位元。")


class KaipanlaAuctionResponse(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    source_code: Literal["kaipanla"] = Field(description="数据来源：开盘啦。")
    pool_scope: Literal["kaipanla_auction_pool"] = Field(
        description="开盘啦打板竞价池，不是全市场竞价或旧伯乐候选池。"
    )
    persisted: Literal[False] = Field(description="固定 false，数据不保存至数据库或 Raw。")
    query_mode: Literal["history", "current"] = Field(
        description="history 为指定日期的历史 HTTP 查询；current 为当前 HTTP 查询。"
    )
    requested_date: date | None = Field(description="调用方请求日期；省略日期的当前查询为空。")
    trade_date: date = Field(
        description="上游实际数据日期；历史查询必须与请求日期一致，否则返回 502。"
    )
    observed_at: ApiTimestamp = Field(
        description="响应取得时间，上海时区 YYYY-MM-DD HH:mm:ss；不代表行情时点。"
    )
    exclude_st: bool = Field(description="是否过滤 ST 股票。")
    offset: int = Field(ge=0, le=10000, description="本页记录偏移。")
    limit: int = Field(ge=1, le=30, description="每页上限，最多30条。")
    returned_count: int = Field(ge=0, le=30, description="本页实际返回数量。")
    total: None = Field(description="固定为空；上游未提供可靠总数，不以本页数量冒充。")
    next_offset: int | None = Field(
        description="满页时可继续尝试的偏移；下页可能为空，达到偏移上限则为空。"
    )
    sort_by: Literal["limit_up_bid_amount_cny"] = Field(description="固定按涨停委买额排序。")
    sort_order: Literal["desc"] = Field(description="固定降序，保持来源分页排序。")
    items: list[KaipanlaAuctionItem] = Field(
        description="规范化竞价股票记录；小数以字符串返回，金额单位元。"
    )


def _provider(request: Request) -> KaipanlaAuctionProvider:
    return cast(KaipanlaAuctionProvider, request.app.state.kaipanla_auction_provider)


router = APIRouter(
    prefix="/api/v1/realtime/kaipanla",
    tags=["实时接口"],
    responses={
        401: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
    },
)


@router.get(
    "/auction-pool",
    response_model=KaipanlaAuctionResponse,
    summary="分页查询开盘啦竞价池",
    description=(
        "复用 X-API-Key，每请求只读取一页，不入库、不保存 Raw、不触发 Worker。"
        "传 trade_date 使用已验证的历史接口；省略则请求当前 HTTP 接口。"
        "当前 HTTP 接口实测可能返回占位行，此时明确返回502；本接口尚未接入APP当日Socket链路。"
        "历史返回日期必须匹配；无自动日期回退。池范围为开盘啦打板竞价池，不是全市场。"
        "最多30条每页，沿 next_offset 分页，分页期间来源变化可能影响排序和跨页一致性。"
    ),
)
def auction_pool(
    provider: Annotated[KaipanlaAuctionProvider, Depends(_provider)],
    trade_date: Annotated[date | None, Query()] = None,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    limit: Annotated[int, Query(ge=1, le=30)] = 30,
    exclude_st: Annotated[bool, Query()] = True,
) -> KaipanlaAuctionResponse:
    return KaipanlaAuctionResponse.model_validate(
        provider.fetch(trade_date=trade_date, offset=offset, limit=limit, exclude_st=exclude_st)
    )
