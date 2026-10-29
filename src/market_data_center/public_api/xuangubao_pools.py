"""Authenticated Xuangubao latest/historical three-pool reads."""

# ruff: noqa: RUF001 - Chinese API documentation.

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Query, Request
from pydantic import ConfigDict, Field

from market_data_center.domain.xuangubao_pools import DateVerification, PoolType
from market_data_center.providers.xuangubao_pools import XuangubaoPoolsProvider
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class XuangubaoPoolItemResponse(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    symbol: str = Field(description="标准证券代码，如 SZSE:001234。")
    code: str = Field(pattern=r"^[0-9]{6}$", description="六位股票代码。")
    name: str | None = Field(description="来源股票名称，缺失为 null。")
    change_pct: Decimal | None = Field(
        description="本次池行情涨跌幅，百分比数值；-2.3表示-2.3%。缺失不补零。"
    )
    consecutive_limit_up_days: int | None = Field(
        ge=0, description="来源本期连续涨停板数，不是几天几板的累计板数；0保留，缺失或-1为null。"
    )
    previous_consecutive_limit_up_days: int | None = Field(
        ge=0, description="来源前一交易日连续涨停板数，未加1；缺失或-1为null。"
    )
    first_limit_up_at: ApiTimestamp | None = Field(description="本期首次涨停时间，零或缺失为null。")
    last_limit_up_at: ApiTimestamp | None = Field(description="本期最后涨停时间。")
    last_broken_limit_up_at: ApiTimestamp | None = Field(description="本期最后开板时间。")
    previous_last_limit_up_at: ApiTimestamp | None = Field(
        description="前一交易日最后涨停时间，不能证明本期行情日期。"
    )
    topics: list[str] = Field(description="来源关联题材名称；无题材时为空。")


class XuangubaoPoolResponse(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    source_code: Literal["xuangubao"] = Field(description="来源选股宝。")
    persisted: Literal[False] = Field(description="不入库，不保存Raw。")
    pool_type: PoolType = Field(description="涨停、炸板或昨日涨停；昨日池不等于已经去重的断板池。")
    requested_date: date | None = Field(description="请求日期；省略表示最新可用池，不保证今天。")
    source_event_date: date | None = Field(
        description="本期涨停/开板事件一致的日期；无可核验事件时null。不是所有记录行情日期的独立证明。"
    )
    date_verification: DateVerification = Field(
        description="event_dates_consistent仅表示已出现的本期事件日期一致；unverified表示无法核验。"
    )
    observed_at: ApiTimestamp = Field(description="取得响应的上海时间，不是行情日期。")
    returned_count: int = Field(
        ge=0, le=2000, description="本次来源列表条数，无本地分页或ST过滤，不保证全市场完整。"
    )
    items: list[XuangubaoPoolItemResponse]


router = APIRouter(prefix="/api/v1/realtime/xuangubao", tags=["实时接口"])


@router.get(
    "/stock-pools",
    response_model=XuangubaoPoolResponse,
    summary="查询选股宝涨停、炸板和昨日涨停池",
    description="一次有界来源查询，支持最新/当日/历史；连续板数和涨幅来自来源数值字段。指定日期与本期事件冲突返回502，无事件则明确未核验，不回退日期。",
    responses={status: {"model": ErrorResponse} for status in (401, 422, 502)},
)
def stock_pools(
    request: Request,
    pool_type: Annotated[
        PoolType,
        Query(description="limit_up涨停；broken_limit_up炸板；previous_limit_up昨日涨停。"),
    ],
    trade_date: Annotated[
        date | None, Query(description="可选YYYY-MM-DD，不得未来；今天或省略时读取最新池。")
    ] = None,
) -> XuangubaoPoolResponse:
    provider = cast(XuangubaoPoolsProvider, request.app.state.xuangubao_pools_provider)
    return XuangubaoPoolResponse.model_validate(
        provider.fetch(pool_type=pool_type, trade_date=trade_date)
    )
