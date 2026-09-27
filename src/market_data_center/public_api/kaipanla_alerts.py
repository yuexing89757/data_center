"""Kaipanla read-through routes: no query service, Raw store or Worker dependencies."""

# ruff: noqa: RUF001 - Chinese API documentation intentionally uses Chinese punctuation.

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Query, Request
from pydantic import ConfigDict, Field, field_validator

from market_data_center.providers.kaipanla_alerts import (
    AlertFilter,
    Endpoint,
    KaipanlaAlertsProvider,
)
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class KaipanlaAlertItem(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    symbol: str = Field(description="标准证券代码，例如 SZSE:001216。")
    code: str = Field(pattern=r"^[0-9]{6}$", description="六位股票代码，保留前导零。")
    name: str | None = Field(description="来源股票名称；现价接口未提供时为空。")
    source_values: list[str | int | Decimal | None] = Field(
        description=(
            "按 source_layout 标识的完整来源行；数组列语义尚未全部确认，不作为标准指标。"
            "小数以字符串返回。"
        )
    )
    last_price: Decimal | None = Field(description="现价刷新接口的最新价，单位元；其他接口为空。")
    change_pct: Decimal | None = Field(
        description="现价刷新接口的涨跌幅，单位百分比；其他接口为空。"
    )
    start_date: date | None = Field(description="重点监控开始日期；来源未提供时为空。")
    end_date: date | None = Field(description="重点监控结束日期；来源未提供时为空。")
    announcement_date: date | None = Field(description="问询函公告日期；来源未提供时为空。")
    document_url: str | None = Field(description="来源提供的公告链接；接口不下载该文档。")


class KaipanlaAlertGroup(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    key: Literal["items", "today", "tomorrow", "history", "monitor", "inquiries"] = Field(
        description="统一列表分组；today/tomorrow 对应来源快照当日与下一交易日，非服务器自然日。"
    )
    items: list[KaipanlaAlertItem] = Field(description="本分组返回的股票记录。")


class KaipanlaAlertResponse(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    source_code: Literal["kaipanla"] = Field(description="外部数据来源：开盘啦。")
    source_layout: str = Field(description="来源数组布局版本，用于区分严重异动、热门股等行结构。")
    persisted: Literal[False] = Field(description="固定为 false；此次查询不写数据库或 Raw 文件。")
    observed_at: ApiTimestamp = Field(description="本服务收到响应的上海时区时间。")
    source_timestamp: ApiTimestamp | None = Field(
        description="上游 Time 转换为上海时区时间；不代表行情发生时间。"
    )
    requested_date: date | None = Field(
        description="调用方指定日期；当前查询为空，监控历史中为日期游标。"
    )
    trade_date: date | None = Field(
        description="上游实际返回的数据日期；未提供时为空，休市可能返回最近交易日。"
    )
    date_matches_request: bool | None = Field(
        description="请求日期与来源日期是否一致；任一缺失时为空。"
    )
    groups: list[KaipanlaAlertGroup] = Field(
        description="统一名称的列表分组；不暴露上游 List_Tormorow 等键名。"
    )
    offset: int | None = Field(description="分页接口的本页偏移；非分页接口为空。")
    limit: int | None = Field(description="分页接口的每页上限，最多20条；非分页接口为空。")
    total: int | None = Field(description="来源明确提供的总数；缺失时为空，不以本页数量冒充。")
    next_offset: int | None = Field(
        description="下一页偏移；无总数时满页仅提示可继续查询，不保证下页非空。"
    )
    next_date: date | None = Field(description="监控历史下页日期游标；没有可继续向前的日期时为空。")
    missing_codes: list[str] = Field(description="现价查询未返回的请求代码；其他接口为空列表。")


class KaipanlaQuoteQuery(ApiModel):
    codes: list[Annotated[str, Field(pattern=r"^[0-9]{6}$")]] = Field(
        min_length=1, max_length=100, description="一至一百个六位股票代码；按首次出现顺序去重。"
    )

    @field_validator("codes")
    @classmethod
    def deduplicate(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(value))


def _provider(request: Request) -> KaipanlaAlertsProvider:
    return cast(KaipanlaAlertsProvider, request.app.state.kaipanla_alerts_provider)


ProviderDependency = Annotated[KaipanlaAlertsProvider, Depends(_provider)]
TradeDate = Annotated[date | None, Query(description="指定历史交易日期；省略则查询当前来源快照。")]
Offset = Annotated[int, Query(ge=0, le=10000)]
Limit = Annotated[int, Query(ge=1, le=20)]
router = APIRouter(
    prefix="/api/v1/realtime/kaipanla/alerts",
    tags=["实时接口"],
    responses={
        401: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
    },
)
_DESCRIPTION = (
    "请求时直连开盘啦，复用 X-API-Key 鉴权，不写数据库、不保存 Raw、不触发 Worker。"
    "数值字段使用 Decimal，时间采用上海时区。来源推算与数据中心监管规则计算结果独立。"
    "尚未确认的数组字段保留在 source_values，并以 source_layout 标记布局，不能当作标准指标。"
)


def _fetch(
    provider: KaipanlaAlertsProvider,
    endpoint: Endpoint,
    *,
    trade_date: date | None = None,
    offset: int = 0,
    limit: int = 20,
    filter: AlertFilter = "all",
    codes: tuple[str, ...] = (),
    triggered_only: bool = False,
) -> KaipanlaAlertResponse:
    return KaipanlaAlertResponse.model_validate(
        provider.fetch(
            endpoint,
            trade_date=trade_date,
            offset=offset,
            limit=limit,
            filter=filter,
            codes=codes,
            triggered_only=triggered_only,
        )
    )


@router.get(
    "/severe",
    response_model=KaipanlaAlertResponse,
    summary="查询严重异动当日及次日提醒",
    description=_DESCRIPTION,
)
def severe(provider: ProviderDependency, trade_date: TradeDate = None) -> KaipanlaAlertResponse:
    return _fetch(
        provider, "severe_history" if trade_date else "severe_current", trade_date=trade_date
    )


@router.get(
    "/severe/history",
    response_model=KaipanlaAlertResponse,
    summary="分页查询近期严重异动",
    description=_DESCRIPTION,
)
def severe_history(
    provider: ProviderDependency,
    trade_date: Annotated[date, Query()],
    offset: Offset = 0,
    limit: Limit = 20,
    filter: Annotated[AlertFilter, Query()] = "all",
) -> KaipanlaAlertResponse:
    return _fetch(
        provider,
        "severe_history_all",
        trade_date=trade_date,
        offset=offset,
        limit=limit,
        filter=filter,
    )


@router.get(
    "/hot",
    response_model=KaipanlaAlertResponse,
    summary="查询热门股偏离值",
    description=_DESCRIPTION,
)
def hot(provider: ProviderDependency, trade_date: TradeDate = None) -> KaipanlaAlertResponse:
    return _fetch(provider, "hot_history" if trade_date else "hot_current", trade_date=trade_date)


@router.get(
    "/monitor",
    response_model=KaipanlaAlertResponse,
    summary="查询当前重点监控名单",
    description=_DESCRIPTION,
)
def monitor(provider: ProviderDependency) -> KaipanlaAlertResponse:
    return _fetch(provider, "monitor_current")


@router.get(
    "/monitor/history",
    response_model=KaipanlaAlertResponse,
    summary="按日期游标查询历史重点监控",
    description=_DESCRIPTION,
)
def monitor_history(
    provider: ProviderDependency,
    before_date: Annotated[date | None, Query()] = None,
) -> KaipanlaAlertResponse:
    return _fetch(
        provider,
        "monitor_history_more" if before_date else "monitor_history_initial",
        trade_date=before_date,
    )


@router.get(
    "/inquiries",
    response_model=KaipanlaAlertResponse,
    summary="查询问询函首批名单",
    description=_DESCRIPTION,
)
def inquiries(provider: ProviderDependency) -> KaipanlaAlertResponse:
    return _fetch(provider, "inquiry_initial")


@router.get(
    "/inquiries/history",
    response_model=KaipanlaAlertResponse,
    summary="分页查询历史问询函",
    description=_DESCRIPTION,
)
def inquiry_history(
    provider: ProviderDependency, offset: Offset = 0, limit: Limit = 20
) -> KaipanlaAlertResponse:
    return _fetch(provider, "inquiry_history", offset=offset, limit=limit)


@router.post(
    "/quotes/query",
    response_model=KaipanlaAlertResponse,
    summary="批量刷新异动股票现价",
    description=_DESCRIPTION,
)
def quotes(provider: ProviderDependency, query: KaipanlaQuoteQuery) -> KaipanlaAlertResponse:
    return _fetch(provider, "quote_refresh", codes=tuple(query.codes))


@router.get(
    "/legacy",
    response_model=KaipanlaAlertResponse,
    summary="查询旧版偏离值列表",
    description=_DESCRIPTION + "旧版历史曾返回前一日数据，请检查 date_matches_request。",
)
def legacy(
    provider: ProviderDependency,
    trade_date: TradeDate = None,
    triggered_only: Annotated[bool, Query()] = False,
) -> KaipanlaAlertResponse:
    return _fetch(
        provider,
        "legacy_history" if trade_date else "legacy_index",
        trade_date=trade_date,
        triggered_only=triggered_only,
    )


@router.get(
    "/multiple",
    response_model=KaipanlaAlertResponse,
    summary="查询多次异动股票",
    description=_DESCRIPTION,
)
def multiple(provider: ProviderDependency) -> KaipanlaAlertResponse:
    return _fetch(provider, "legacy_many")
