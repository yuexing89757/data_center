"""API-key protected Xuangubao source observations; not canonical stock pools."""

# ruff: noqa: RUF001 - Chinese OpenAPI descriptions.

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Request
from pydantic import Field

from market_data_center.providers.xuangubao_limit_up import XuangubaoLimitUpProvider
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class XuangubaoPlate(ApiModel):
    source_id: str | None = Field(description="选股宝关联板块ID，不是数据中心分类ID。")
    name: str | None = Field(description="来源板块名称。")
    reason: str | None = Field(description="来源板块原因原文，不代表数据中心观点。")


class XuangubaoReason(ApiModel):
    symbol: str | None = Field(description="原因对象关联的标准证券代码，缺失为空。")
    text: str | None = Field(description="来源个股涨停原因原文，不作事实认证或交易判断。")
    related_plates: list[XuangubaoPlate] | None = Field(description="本池内附带的关联板块信息。")


class XuangubaoTimelineEvent(ApiModel):
    at: ApiTimestamp | None = Field(description="来源事件时间，源0或缺失为空。")
    source_status: int | None = Field(
        description="来源状态码，已观察1封板、2开板；其他码原样保留，不推断完整枚举。"
    )


class XuangubaoCounts(ApiModel):
    limit_up_days: int | None = Field(description="来源连续涨停天数，不是数据中心计算结果。")
    limit_down_days: int | None = Field(description="来源连续跌停天数。")
    yesterday_limit_up_days: int | None = Field(description="来源昨日连续涨停天数。")
    yesterday_limit_down_days: int | None = Field(description="来源昨日连续跌停天数。")
    break_limit_up_count: int | None = Field(description="来源开板次数。")
    break_limit_down_count: int | None = Field(description="来源打开跌停次数。")
    yesterday_break_limit_up_count: int | None = Field(description="来源昨日开板次数。")
    multi_board_days: int | None = Field(description="来源N天M板的N天数，不自行推导。")
    multi_board_count: int | None = Field(description="来源N天M板的M板数，不自行推导。")


class XuangubaoUnverifiedStatistics(ApiModel):
    buy_lock_ratio: Decimal | None = Field(description="来源买封单比值；分母、单位未核实。")
    sell_lock_ratio: Decimal | None = Field(description="来源卖封单比值；分母、单位未核实。")
    stock_type_code: Decimal | None = Field(description="来源股票类型码，枚举未核实。")
    new_stock_flag: Decimal | None = Field(description="来源新股标记数值，口径未核实。")
    momentum_value: Decimal | None = Field(description="来源mtm数值，口径及单位未核实。")
    volume_bias_ratio: Decimal | None = Field(description="来源量比值，计算口径未核实。")
    total_capital_value: Decimal | None = Field(
        description="来源total_capital数值，单位未核实；不能解释为股本或人民币市值。"
    )
    non_restricted_capital_value: Decimal | None = Field(
        description="来源non_restricted_capital数值，单位未核实；不能解释为股数或认证市值。"
    )
    nearly_new_accumulated_change: Decimal | None = Field(
        description="来源次新累计变化，口径未核实。"
    )
    nearly_new_break_days: Decimal | None = Field(
        description="来源次新开板天数值，统计口径未核实。"
    )
    new_stock_accumulated_change: Decimal | None = Field(
        description="来源新股累计变化，口径未核实。"
    )
    new_stock_break_flag: Decimal | None = Field(description="来源新股开板标记数值，口径未核实。")
    new_stock_limit_up_days: Decimal | None = Field(description="来源新股涨停天数值，口径未核实。")
    new_stock_pre_break_price: Decimal | None = Field(
        description="来源新股开板前价格值，口径未核实。"
    )


class XuangubaoLimitUpItem(ApiModel):
    symbol: str = Field(description="标准证券代码，如SZSE:000678。")
    source_symbol: str = Field(description="来源证券标识，如000678.SZ，保留前导零。")
    name: str = Field(description="来源股票名称，不过滤ST或重排。")
    last_price: Decimal | None = Field(description="来源当前价格，人民币元。")
    issue_price: Decimal | None = Field(description="来源发行价格，人民币元。")
    change_ratio: Decimal | None = Field(description="来源涨跌幅小数比例，0.1表示10%，不乘100。")
    turnover_ratio: Decimal | None = Field(
        description="来源换手小数比例，0.1表示10%，不估算成交额。"
    )
    listing_date: date | None = Field(description="来源上市时间在上海的日期，源0或缺失为空。")
    first_limit_up_at: ApiTimestamp | None = Field(description="来源首次涨停时间，源0或缺失为空。")
    last_limit_up_at: ApiTimestamp | None = Field(description="来源最后涨停时间，源0或缺失为空。")
    first_break_limit_up_at: ApiTimestamp | None = Field(
        description="来源首次开板时间，源0或缺失为空。"
    )
    last_break_limit_up_at: ApiTimestamp | None = Field(
        description="来源最后开板时间，源0或缺失为空。"
    )
    first_limit_down_at: ApiTimestamp | None = Field(
        description="来源首次跌停时间，源0或缺失为空。"
    )
    last_limit_down_at: ApiTimestamp | None = Field(description="来源最后跌停时间，源0或缺失为空。")
    first_break_limit_down_at: ApiTimestamp | None = Field(
        description="来源首次打开跌停时间，源0为空。"
    )
    last_break_limit_down_at: ApiTimestamp | None = Field(
        description="来源最后打开跌停时间，源0为空。"
    )
    yesterday_first_limit_up_at: ApiTimestamp | None = Field(description="来源昨日首次涨停时间。")
    yesterday_last_limit_up_at: ApiTimestamp | None = Field(description="来源昨日最后涨停时间。")
    timeline: list[XuangubaoTimelineEvent] | None = Field(
        description="来源封板/开板事件，按来源顺序；不是分钟行情，不推导交易指标。"
    )
    source_counts: XuangubaoCounts = Field(
        description="来源计数，不替代规范涨跌停事实或日终股票池。"
    )
    source_statistics: XuangubaoUnverifiedStatistics = Field(
        description="仅转述的未核实来源数值，禁止作为标准数量/金额/派生计算输入；零与缺失分开。"
    )
    source_reason: XuangubaoReason | None = Field(description="选股宝提供的原因与关联板块文字。")


class XuangubaoLimitUpResponse(ApiModel):
    source_code: Literal["xuangubao"] = Field(description="固定来源代码。")
    source_name: Literal["选股宝"] = Field(description="固定来源名称。")
    source_url: str = Field(description="固定上游最新涨停池URL。")
    source_layout: Literal["xuangubao.limit_up.v1"] = Field(description="来源适配布局版本。")
    persisted: Literal[False] = Field(description="固定false，不入库、不保存Raw、不触发Worker。")
    observed_at: ApiTimestamp = Field(description="实际收到响应的上海时间，与源数据日期分开。")
    source_date: date | None = Field(
        description="由每只股票当前首次/最后涨停事件核对的一致上海日期；可早于今天，缺证为空。"
    )
    date_status: Literal["verified_from_events", "unknown"] = Field(
        description="空池或任一条目缺少当前涨停事件时unknown；非交易日历认证。"
    )
    returned_count: int = Field(ge=0, le=5000, description="本次来源列表条数，不宣称全市场覆盖。")
    items: list[XuangubaoLimitUpItem] = Field(description="完整来源列表，保持来源顺序。")


def _provider(request: Request) -> XuangubaoLimitUpProvider:
    return cast(XuangubaoLimitUpProvider, request.app.state.xuangubao_limit_up_provider)


router = APIRouter(
    prefix="/api/v1/realtime/xuangubao",
    tags=["实时接口"],
    responses={code: {"model": ErrorResponse} for code in (401, 502)},
)


@router.get(
    "/limit-up",
    response_model=XuangubaoLimitUpResponse,
    summary="查询选股宝最新可用涨停池",
    description="单次匿名读取来源最新池；闭市/跨日可返回上一来源日期。API Key保护，"
    "不提供日期回退、历史查询或持久化，不等同数据中心日终涨停快照。",
)
def latest_limit_up(
    provider: Annotated[XuangubaoLimitUpProvider, Depends(_provider)],
) -> XuangubaoLimitUpResponse:
    return XuangubaoLimitUpResponse.model_validate(provider.fetch())
