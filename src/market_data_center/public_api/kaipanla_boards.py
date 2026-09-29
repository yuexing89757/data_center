"""Authenticated board ranking and historical member reads."""

# ruff: noqa: RUF001 - Chinese OpenAPI descriptions.

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import ConfigDict, Field

from market_data_center.domain.kaipanla_boards import ForecastStatus
from market_data_center.providers.kaipanla_boards import KaipanlaBoardsProvider
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class KaipanlaBoardItem(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    board_code: str = Field(
        pattern=r"^[0-9]{6}$", description="开盘啦板块代码，不等同于标准证券代码。"
    )
    name: str | None = Field(description="来源板块名称。")
    strength: Decimal | None = Field(description="来源板块强度指标；非数据中心计算或交易建议。")
    change_pct: Decimal | None = Field(description="板块涨跌幅，单位百分比。")
    change_speed_pct: Decimal | None = Field(description="来源涨速，单位百分比；统计窗口未披露。")
    amount_cny: Decimal | None = Field(description="成交额，单位元。")
    main_net_amount_cny: Decimal | None = Field(description="来源主力净额，单位元；允许负值。")
    volume_ratio: Decimal | None = Field(description="来源量比，倍数。")
    large_order_net_amount_cny: Decimal | None = Field(
        description="来源大单净额，单位元；允许负值。"
    )


class KaipanlaProfitForecast(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    year: int = Field(ge=1900, le=9999, description="来源标签确认的预测年份，不包含实际利润年份。")
    net_profit_cny: Decimal | None = Field(
        description="来源机构预测净利润，单位元；保留零及负值，须结合forecast_status判断占位。"
    )


class KaipanlaBoardMember(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    symbol: str = Field(pattern=r"^(SSE|SZSE|BSE):[0-9]{6}$", description="标准证券代码。")
    code: str = Field(pattern=r"^[0-9]{6}$", description="六位股票代码，保留前导零。")
    name: str | None = Field(description="来源股票名称。")
    concept: str | None = Field(description="来源概念文本，不等同于数据中心分类成员关系。")
    last_price: Decimal | None = Field(description="所选历史日的来源价格，单位元。")
    change_pct: Decimal | None = Field(description="来源涨跌幅，单位百分比。")
    amount_cny: Decimal | None = Field(description="成交额，单位元。")
    actual_float_market_value_cny: Decimal | None = Field(description="来源实际流通市值，单位元。")
    main_buy_amount_cny: Decimal | None = Field(description="主力买入金额，单位元。")
    main_sell_amount_cny: Decimal | None = Field(description="主力卖出金额，单位元；保留来源负号。")
    main_net_amount_cny: Decimal | None = Field(description="主力净额，单位元。")
    buy_share_pct: Decimal | None = Field(description="来源买占比，单位百分比。")
    sell_share_pct: Decimal | None = Field(description="来源卖占比，单位百分比。")
    volume_ratio: Decimal | None = Field(description="来源量比，倍数。")
    height_label: str | None = Field(description="来源高度文字原样保留，不推导连续涨停天数。")
    leader_label: str | None = Field(description="来源龙头标签，仅转述来源，不生成排名或选股判断。")
    close_limit_bid_amount_cny: Decimal | None = Field(description="来源收盘封单金额，单位元。")
    max_limit_bid_amount_cny: Decimal | None = Field(description="来源最大封单金额，单位元。")
    total_market_value_cny: Decimal | None = Field(description="总市值，单位元。")
    float_market_value_cny: Decimal | None = Field(description="流通市值，单位元。")
    leading_count: int | None = Field(ge=0, description="来源领涨次数，整数。")
    large_order_net_amount_cny: Decimal | None = Field(description="来源大单净额，单位元。")
    forecast_status: ForecastStatus = Field(
        description="available有预测值；not_provided三项全为零或缺失；years_unverified年份无法确认。"
    )
    profit_forecasts: list[KaipanlaProfitForecast] = Field(
        description="按来源标签顺序返回三年预测；年份无法核实时为空数组，零值不转为null。"
    )


class _BoardResponse(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    source_code: Literal["kaipanla"] = Field(description="数据来源：开盘啦。")
    persisted: Literal[False] = Field(description="固定false；不存数据库或Raw，不触发Worker。")
    query_mode: Literal["latest", "history"] = Field(description="最新快照或指定日期历史查询。")
    requested_date: date | None = Field(description="调用方请求日期；最新查询为空。")
    trade_date: date = Field(description="来源实际日期；历史必须匹配请求日，否则502。")
    observed_at: ApiTimestamp = Field(description="收到响应的上海时间；不是来源行情时点。")
    offset: int = Field(ge=0, le=10000, description="本页记录偏移。")
    limit: int = Field(ge=1, le=60, description="每页上限，最多60条。")
    returned_count: int = Field(ge=0, le=60, description="本页实际返回数量。")
    total: None = Field(description="固定null；来源Count可能仅为页长度，不能作为总数。")
    next_offset: int | None = Field(
        description="满页时可继续尝试的偏移；下页可能为空，达到上限则为空。"
    )


class KaipanlaBoardRankingResponse(_BoardResponse):
    items: list[KaipanlaBoardItem] = Field(description="本页板块排行，保留来源顺序。")


class KaipanlaBoardMembersResponse(_BoardResponse):
    board_code: str = Field(pattern=r"^[0-9]{6}$", description="请求的开盘啦板块代码。")
    forecast_years: list[int] = Field(description="已核实的三年预测年份；无法核实时为空。")
    items: list[KaipanlaBoardMember] = Field(description="本页历史成分股及来源机构预测。")


def _provider(request: Request) -> KaipanlaBoardsProvider:
    return cast(KaipanlaBoardsProvider, request.app.state.kaipanla_boards_provider)


Provider = Annotated[KaipanlaBoardsProvider, Depends(_provider)]
Offset = Annotated[int, Query(ge=0, le=10000)]
Limit = Annotated[int, Query(ge=1, le=60)]
TradeDate = Annotated[date, Query(description="指定日期，格式YYYY-MM-DD。")]
router = APIRouter(
    prefix="/api/v1/realtime/kaipanla/boards",
    tags=["实时接口"],
    responses={
        401: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
    },
)
DESCRIPTION = (
    "复用X-API-Key，每次只读取一页开盘啦HTTP响应，不重试、不自动全量翻页、不落库。"
    "小数以字符串返回，金额单位元。沿next_offset分页，来源变动可能影响跨页一致性。"
)


@router.get(
    "",
    response_model=KaipanlaBoardRankingResponse,
    summary="查询开盘啦最新板块排行",
    description=DESCRIPTION + "实际行情日以trade_date为准。",
)
def latest_boards(
    provider: Provider, offset: Offset = 0, limit: Limit = 60
) -> KaipanlaBoardRankingResponse:
    return KaipanlaBoardRankingResponse.model_validate(provider.ranking(offset=offset, limit=limit))


@router.get(
    "/history",
    response_model=KaipanlaBoardRankingResponse,
    summary="查询开盘啦历史板块排行",
    description=DESCRIPTION + "必填trade_date，拒绝未来日期和来源日期错位。",
)
def historical_boards(
    provider: Provider, trade_date: TradeDate, offset: Offset = 0, limit: Limit = 60
) -> KaipanlaBoardRankingResponse:
    return KaipanlaBoardRankingResponse.model_validate(
        provider.ranking(trade_date=trade_date, offset=offset, limit=limit)
    )


@router.get(
    "/{board_code}/members/history",
    response_model=KaipanlaBoardMembersResponse,
    summary="查询开盘啦历史成分股及机构预测",
    description=DESCRIPTION + "trade_date必须早于上海当天；本接口不包含当天TLS行情。"
    "预测年份由来源标签确定，无法核实时不猜测。",
)
def historical_members(
    provider: Provider,
    board_code: Annotated[str, Path(pattern=r"^[0-9]{6}$")],
    trade_date: TradeDate,
    offset: Offset = 0,
    limit: Limit = 60,
) -> KaipanlaBoardMembersResponse:
    return KaipanlaBoardMembersResponse.model_validate(
        provider.members(board_code, trade_date=trade_date, offset=offset, limit=limit)
    )
