"""Authenticated, bounded Kaipanla historical market-emotion stock lists."""

# ruff: noqa: RUF001 - Chinese OpenAPI descriptions.

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Query, Request
from pydantic import ConfigDict, Field

from market_data_center.providers.kaipanla_market_history import KaipanlaMarketHistoryProvider
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class _HistoryItem(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    symbol: str = Field(description="标准证券代码，如 SZSE:001368。")
    code: str = Field(pattern=r"^[0-9]{6}$", description="六位股票代码。")
    name: str | None = Field(description="来源股票名称；缺失时为空。")


class AuctionHistoryItem(_HistoryItem):
    board_name: str | None = Field(description="来源板块或题材文字。")
    limit_up_bid_amount_cny: Decimal | None = Field(description="涨停委买额，单位元。")
    auction_change_pct: Decimal | None = Field(description="竞价涨跌幅，单位百分比。")


class LimitUpHistoryItem(_HistoryItem):
    limit_up_at: ApiTimestamp | None = Field(
        description="来源涨停时间，上海时间。", json_schema_extra={"example": "2026-09-28 14:56:45"}
    )
    status: str | None = Field(description="来源首板/连板状态文字。")
    reason: str | None = Field(description="来源涨停原因文字。")


class LimitDownHistoryItem(_HistoryItem):
    limit_down_at: ApiTimestamp | None = Field(
        description="来源跌停时间，上海时间。", json_schema_extra={"example": "2026-09-28 14:51:33"}
    )
    sealed_amount_cny: Decimal | None = Field(description="来源封单金额，单位元。")
    board_name: str | None = Field(description="来源板块或题材文字。")


class BrokenLimitUpHistoryItem(_HistoryItem):
    change_pct: Decimal | None = Field(description="来源涨跌幅，单位百分比。")
    limit_up_at: ApiTimestamp | None = Field(
        description="来源涨停时间，上海时间。", json_schema_extra={"example": "2026-09-28 09:44:54"}
    )
    opened_at: ApiTimestamp | None = Field(
        description="来源开板时间，上海时间。", json_schema_extra={"example": "2026-09-28 09:45:12"}
    )


class _HistoryResponse(ApiModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)
    source_code: Literal["kaipanla"] = Field(description="数据来源：开盘啦。")
    persisted: Literal[False] = Field(description="固定 false；不保存数据库或 Raw。")
    requested_date: date = Field(description="请求的历史交易日期。")
    trade_date: date = Field(description="来源实际日期，必须与请求日期一致。")
    observed_at: ApiTimestamp = Field(
        description="取得响应的上海时间；不是行情事件时间。",
        json_schema_extra={"example": "2026-09-29 01:00:00"},
    )
    offset: int = Field(ge=0, le=10000, description="本页偏移。")
    limit: int = Field(ge=1, le=30, description="每页上限，最多30条。")
    returned_count: int = Field(ge=0, le=30, description="本页实际条数。")
    total: None = Field(description="固定为空；来源未提供可靠总数。")
    next_offset: int | None = Field(description="满页时可尝试的下页偏移。")


class KaipanlaAuctionHistoryResponse(_HistoryResponse):
    list_type: Literal["auction"] = Field(description="历史竞价股票列表。")
    items: list[AuctionHistoryItem] = Field(description="本页历史竞价股票记录。")


class KaipanlaLimitUpHistoryResponse(_HistoryResponse):
    list_type: Literal["limit_up"] = Field(description="历史涨停股票列表。")
    items: list[LimitUpHistoryItem] = Field(description="本页历史涨停股票记录。")


class KaipanlaLimitDownHistoryResponse(_HistoryResponse):
    list_type: Literal["limit_down"] = Field(description="历史跌停股票列表。")
    items: list[LimitDownHistoryItem] = Field(description="本页历史跌停股票记录。")


class KaipanlaBrokenLimitUpHistoryResponse(_HistoryResponse):
    list_type: Literal["broken_limit_up"] = Field(description="历史炸板股票列表。")
    items: list[BrokenLimitUpHistoryItem] = Field(description="本页历史炸板股票记录。")


def _provider(request: Request) -> KaipanlaMarketHistoryProvider:
    return cast(KaipanlaMarketHistoryProvider, request.app.state.kaipanla_market_history_provider)


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
    "/stocks/auction",
    response_model=KaipanlaAuctionHistoryResponse,
    summary="查询开盘啦历史竞价股票",
    description="按指定历史交易日读取一页竞价股票；仅返回已核实字段，不入库、不回退日期。",
)
def auction_history(
    provider: Annotated[KaipanlaMarketHistoryProvider, Depends(_provider)],
    trade_date: Annotated[date, Query(description="必填历史交易日，YYYY-MM-DD。")],
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    limit: Annotated[int, Query(ge=1, le=30)] = 30,
) -> KaipanlaAuctionHistoryResponse:
    return KaipanlaAuctionHistoryResponse.model_validate(
        provider.fetch(kind="auction", trade_date=trade_date, offset=offset, limit=limit)
    )


@router.get(
    "/stocks/limit-up",
    response_model=KaipanlaLimitUpHistoryResponse,
    summary="查询开盘啦历史涨停股票",
    description="按指定历史交易日读取一页涨停股票；仅返回已核实字段，不入库、不回退日期。",
)
def limit_up_history(
    provider: Annotated[KaipanlaMarketHistoryProvider, Depends(_provider)],
    trade_date: Annotated[date, Query(description="必填历史交易日，YYYY-MM-DD。")],
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    limit: Annotated[int, Query(ge=1, le=30)] = 30,
) -> KaipanlaLimitUpHistoryResponse:
    return KaipanlaLimitUpHistoryResponse.model_validate(
        provider.fetch(kind="limit_up", trade_date=trade_date, offset=offset, limit=limit)
    )


@router.get(
    "/stocks/limit-down",
    response_model=KaipanlaLimitDownHistoryResponse,
    summary="查询开盘啦历史跌停股票",
    description="按指定历史交易日读取一页跌停股票；仅返回已核实字段，不入库、不回退日期。",
)
def limit_down_history(
    provider: Annotated[KaipanlaMarketHistoryProvider, Depends(_provider)],
    trade_date: Annotated[date, Query(description="必填历史交易日，YYYY-MM-DD。")],
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    limit: Annotated[int, Query(ge=1, le=30)] = 30,
) -> KaipanlaLimitDownHistoryResponse:
    return KaipanlaLimitDownHistoryResponse.model_validate(
        provider.fetch(kind="limit_down", trade_date=trade_date, offset=offset, limit=limit)
    )


@router.get(
    "/stocks/broken-limit-up",
    response_model=KaipanlaBrokenLimitUpHistoryResponse,
    summary="查询开盘啦历史炸板股票",
    description="按指定历史交易日读取一页炸板股票；仅返回已核实字段，不入库、不回退日期。",
)
def broken_limit_up_history(
    provider: Annotated[KaipanlaMarketHistoryProvider, Depends(_provider)],
    trade_date: Annotated[date, Query(description="必填历史交易日，YYYY-MM-DD。")],
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    limit: Annotated[int, Query(ge=1, le=30)] = 30,
) -> KaipanlaBrokenLimitUpHistoryResponse:
    return KaipanlaBrokenLimitUpHistoryResponse.model_validate(
        provider.fetch(kind="broken_limit_up", trade_date=trade_date, offset=offset, limit=limit)
    )
