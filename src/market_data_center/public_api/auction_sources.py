"""Named authenticated source reads for the nine-in-one application interfaces."""

# ruff: noqa: RUF001 - Chinese interface documentation.

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import Field, JsonValue

from market_data_center.providers.auction_sources import AuctionSourcesProvider
from market_data_center.public_api.models import ApiModel, ApiTimestamp, ErrorResponse


class SourceQuoteItem(ApiModel):
    symbol: str = Field(description="带交易所前缀的标准证券代码。")
    code: str = Field(description="六位股票代码，保留前导零。")
    name: str | None = Field(description="来源股票名称，缺失为空。")
    last_price: Decimal | None = Field(description="当前价格，单位元。")
    change_pct: Decimal | None = Field(description="当前涨跌幅，单位百分比。")
    open: Decimal | None = Field(description="本交易日开盘价，单位元。")
    previous_close: Decimal | None = Field(description="前收盘价，单位元。")
    volume_shares: Decimal | None = Field(
        description="截至来源时点的累计成交量，单位股，来源手乘100。"
    )
    amount_cny: Decimal | None = Field(description="累计成交金额，单位人民币元。")
    turnover_rate_pct: Decimal | None = Field(description="换手率，单位百分比。")
    volume_ratio: Decimal | None = Field(description="来源量比，缺失为空。")
    total_market_value_cny: Decimal | None = Field(description="来源总市值，单位人民币元。")
    float_market_value_cny: Decimal | None = Field(description="来源流通市值，单位人民币元。")
    listing_date: date | None = Field(description="来源上市日期，不含时间。")
    industry: str | None = Field(description="来源行业名称，不创建分类事实。")
    concepts: str | None = Field(description="来源概念文字，不创建成分历史。")


class SourcePoolItem(ApiModel):
    symbol: str = Field(description="标准证券代码。")
    code: str = Field(description="来源六位股票代码。")
    name: str | None = Field(description="来源股票名称。")
    consecutive_limit_up_days: int | None = Field(
        description="仅涨停池提供的来源连板数，其他池为空。"
    )
    first_limit_time: str | None = Field(description="来源首次封板时间HH:mm:ss；缺失及跌停池为空。")
    break_count: int | None = Field(description="来源炸板次数，跌停池或缺失为空。")
    sealing_amount_cny: Decimal | None = Field(description="来源封单金额，单位人民币元。")
    industry: str | None = Field(description="来源行业名称。")


class SourceBarItem(ApiModel):
    trade_date: date = Field(description="来源K线日期；周K为来源标记日，不自行推算周末。")
    open: Decimal = Field(description="该周期开盘价，单位元。")
    close: Decimal = Field(description="该周期收盘或当前价格，末条可能未完成，单位元。")
    high: Decimal = Field(description="该周期最高价，单位元，不修正异常来源OHLC。")
    low: Decimal = Field(description="该周期最低价，单位元。")
    volume_shares: Decimal = Field(
        description="该周期来源成交量，单位股；东财/腾讯手乘100，新浪直接读股。"
    )
    amount_cny: Decimal | None = Field(
        description="来源成交额，单位人民币元；腾讯及新浪未提供时为空，不推算。"
    )


class SourceMinuteItem(ApiModel):
    time: str = Field(description="上海交易时区分时时间HH:mm。")
    price: Decimal = Field(description="来源分时价格，单位元。")
    cumulative_volume_shares: Decimal = Field(
        description="来源累计成交量，单位股；逐行手乘100，不再次累加。"
    )
    cumulative_amount_cny: Decimal | None = Field(
        description="来源累计成交额，单位人民币元；缺失为空，不使用0代替。"
    )


class SourceIndexItem(ApiModel):
    index_code: Literal["sh000001"] = Field(description="固定查询上证指数，不接受任意代码。")
    name: str | None = Field(description="来源指数名称。")
    current: Decimal | None = Field(description="当前指数点位。")
    previous_close: Decimal | None = Field(description="前收盘指数点位。")
    change_pct: Decimal | None = Field(description="来源涨跌幅，读取腾讯索引32，单位百分比。")


class SourceThemeItem(ApiModel):
    theme_code: str = Field(description="来源题材代码，保留前导零，例如00647。")
    theme_name: str | None = Field(description="来源题材名称。")
    change_pct: Decimal | None = Field(
        description="板块涨幅，单位百分比；今日机会f3、题材排行bf3。"
    )
    leader_change_pct: Decimal | None = Field(
        description="题材排行领涨股涨幅，单位百分比；今日机会为空。"
    )
    leader_name: str | None = Field(description="来源领涨股名称；今日机会未给此字段时为空。")
    limit_up_count: int | None = Field(description="来源fex3涨停数量，缺失为空。")
    stock_count: int | None = Field(description="来源明确stockNum/stockCount成分数量，缺失为空。")


class SourceThemeStockItem(ApiModel):
    symbol: str = Field(description="标准证券代码。")
    code: str = Field(description="来源六位股票代码。")
    name: str | None = Field(description="来源股票名称。")
    last_price: Decimal | None = Field(
        description="来源f2当前价格，单位元；不把未确认f57作为价格。"
    )
    change_pct: Decimal | None = Field(description="来源涨跌幅，单位百分比。")
    main_net_amount_cny: Decimal | None = Field(
        description="来源f62主力净流入，单位人民币元，允许负数。"
    )
    reason: str | None = Field(description="来源首条入选说明文字；不生成AI分析或交易结论。")


class SourceResponse[T: ApiModel](ApiModel):
    source_code: Literal["eastmoney", "tencent", "sina"] = Field(description="固定来源标识。")
    source_name: str = Field(description="来源网站名称。")
    source_website: str = Field(description="来源网站首页，区别于上游数据请求地址。")
    upstream_url: str = Field(description="本次固定上游地址，不含查询串。")
    upstream_method: Literal["GET", "POST"] = Field(description="实际发送的上游HTTP方法。")
    upstream_parameters: dict[str, JsonValue] = Field(
        description="实际查询参数或JSON请求体；固定字符串来自原软件，不代表永久授权。"
    )
    purpose: str = Field(description="此接口的来源数据用途，不含本地策略计算。")
    persisted: Literal[False] = Field(description="固定false；不写数据库、Raw或缓存。")
    observed_at: ApiTimestamp = Field(
        description="本服务收到来源响应的上海时间，不是来源成交时间。"
    )
    page: int = Field(description="本次请求页号，从1开始，非分页接口固定1。")
    page_size: int = Field(description="请求页长，非分页接口仅为默认值，以items数量为准。")
    trade_date: date | None = Field(description="来源池/分时日期；其他返回为空，不按观测日猜测。")
    symbol: str | None = Field(description="K线/分时请求的标准证券代码；其他接口为空。")
    adjustment: Literal["forward", "none", "unspecified"] | None = Field(
        description="实际价格复权口径；新浪unspecified，非K线为空。"
    )
    total: int | None = Field(description="来源显式总数；未提供时为空，不从满页推断。")
    items: list[T] = Field(description="经过来源边界转换的本页数据；成功空数组与来源失败区分。")


def _provider(request: Request) -> AuctionSourcesProvider:
    return cast(AuctionSourcesProvider, request.app.state.auction_sources_provider)


Provider = Annotated[AuctionSourcesProvider, Depends(_provider)]
Symbol = Annotated[
    str,
    Query(pattern=r"^(?:SSE:6[0-9]{5}|SZSE:[03][0-9]{5})$", examples=["SSE:600000", "SZSE:000001"]),
]
Page = Annotated[int, Query(ge=1, le=100, examples=[1])]
BarLimit = Annotated[int, Query(ge=1, le=640, examples=[160])]
PoolSize = Annotated[int, Query(ge=1, le=500, examples=[500])]
TradeDate = Annotated[date, Query(examples=["2026-10-12"])]
router = APIRouter(
    prefix="/api/v1/realtime",
    tags=["实时接口"],
    responses={status: {"model": ErrorResponse} for status in (401, 422, 502)},
)


def _description(source: str, upstream: str, purpose: str, example: str) -> str:
    return (
        f"来源网站：{source}。上游接口：{upstream}。用途：{purpose}。"
        f"调用示例：`GET {example}`，请求头`X-API-Key: <你的API Key>`。"
        "一次只读一个上游响应，不入库、不自动翻页、不跨源回退；失败502，参数错误422。"
        "响应内upstream_parameters回显实际查询参数/POST JSON体，金额及小数序列化为字符串。"
    )


@router.get(
    "/eastmoney/quotes",
    response_model=SourceResponse[SourceQuoteItem],
    summary="东方财富全市场即时行情（单页）",
    description=_description(
        "东方财富 https://www.eastmoney.com/",
        "https://push2dycalc.eastmoney.com/api/qt/clist/get",
        "综合、谛听、黑马、尾盘、一进二、猎手、顺势的行情读取；不是竞价快照",
        "/api/v1/realtime/eastmoney/quotes?market=mainboard&page=1&page_size=5000",
    ),
)
def quotes(
    provider: Provider,
    market: Annotated[
        Literal["all", "mainboard", "star", "growth"], Query(examples=["all"])
    ] = "all",
    page: Page = 1,
    page_size: Annotated[int, Query(ge=1, le=6000, examples=[5000])] = 5000,
) -> dict[str, JsonValue]:
    return provider.fetch("quotes", market=market, page=page, page_size=page_size)


@router.get(
    "/eastmoney/limit-up",
    response_model=SourceResponse[SourcePoolItem],
    summary="东方财富指定日涨停池",
    description=_description(
        "东方财富 https://www.eastmoney.com/",
        "https://push2ex.eastmoney.com/getTopicZTPool",
        "昨日涨停、首板筛选和来源连板数；精确匹配来源日期",
        "/api/v1/realtime/eastmoney/limit-up?trade_date=2026-10-12&page=1&page_size=500",
    ),
)
def limit_up(
    provider: Provider, trade_date: TradeDate, page: Page = 1, page_size: PoolSize = 500
) -> dict[str, JsonValue]:
    return provider.fetch("limit_up", trade_date=trade_date, page=page, page_size=page_size)


@router.get(
    "/eastmoney/limit-down",
    response_model=SourceResponse[SourcePoolItem],
    summary="东方财富指定日跌停池",
    description=_description(
        "东方财富 https://www.eastmoney.com/",
        "https://push2ex.eastmoney.com/getTopicDTPool",
        "共享面板使用的来源跌停数量及列表；不计算情绪等级",
        "/api/v1/realtime/eastmoney/limit-down?trade_date=2026-10-12&page_size=500",
    ),
)
def limit_down(
    provider: Provider, trade_date: TradeDate, page: Page = 1, page_size: PoolSize = 500
) -> dict[str, JsonValue]:
    return provider.fetch("limit_down", trade_date=trade_date, page=page, page_size=page_size)


@router.get(
    "/eastmoney/broken-limit",
    response_model=SourceResponse[SourcePoolItem],
    summary="东方财富指定日炸板池",
    description=_description(
        "东方财富 https://www.eastmoney.com/",
        "https://push2ex.eastmoney.com/getTopicZBPool",
        "共享面板使用的来源炸板数量及列表",
        "/api/v1/realtime/eastmoney/broken-limit?trade_date=2026-10-12&page_size=500",
    ),
)
def broken_limit(
    provider: Provider, trade_date: TradeDate, page: Page = 1, page_size: PoolSize = 500
) -> dict[str, JsonValue]:
    return provider.fetch("broken_limit", trade_date=trade_date, page=page, page_size=page_size)


@router.get(
    "/eastmoney/klines",
    response_model=SourceResponse[SourceBarItem],
    summary="东方财富前复权日K（即时读取）",
    description=_description(
        "东方财富 https://www.eastmoney.com/",
        "https://push2his.eastmoney.com/api/qt/stock/kline/get",
        "策略历史行情，klt=101、fqt=1、end=20500101；不写普通日K领域",
        "/api/v1/realtime/eastmoney/klines?symbol=SSE:600000&limit=160",
    ),
)
def eastmoney_klines(
    provider: Provider, symbol: Symbol, limit: BarLimit = 160
) -> dict[str, JsonValue]:
    return provider.fetch("eastmoney_kline", symbol=symbol, limit=limit)


@router.get(
    "/tencent/klines",
    response_model=SourceResponse[SourceBarItem],
    summary="腾讯日/周K（标明实际复权）",
    description=_description(
        "腾讯财经 https://finance.qq.com/",
        "https://{web.ifzq|ifzq}.gtimg.cn/appstock/app/fqkline/get",
        "综合主源、其他策略备用及猎手K线；请求qfq，回退day/week时adjustment=none",
        "/api/v1/realtime/tencent/klines?symbol=SSE:600000&period=day&limit=640&server=web",
    ),
)
def tencent_klines(
    provider: Provider,
    symbol: Symbol,
    limit: BarLimit = 160,
    period: Annotated[Literal["day", "week"], Query(examples=["day"])] = "day",
    server: Annotated[Literal["web", "ifzq"], Query(examples=["web"])] = "web",
) -> dict[str, JsonValue]:
    return provider.fetch("tencent_kline", symbol=symbol, limit=limit, period=period, server=server)


@router.get(
    "/sina/klines",
    response_model=SourceResponse[SourceBarItem],
    summary="新浪日K备用来源（即时读取）",
    description=_description(
        "新浪财经 https://finance.sina.com.cn/",
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData",
        "策略备用日K，scale=240、ma=no；复权未明确标注unspecified",
        "/api/v1/realtime/sina/klines?symbol=SZSE:000001&limit=120",
    ),
)
def sina_klines(provider: Provider, symbol: Symbol, limit: BarLimit = 160) -> dict[str, JsonValue]:
    return provider.fetch("sina_kline", symbol=symbol, limit=limit)


@router.get(
    "/tencent/minute",
    response_model=SourceResponse[SourceMinuteItem],
    summary="腾讯当日分时图数据（累计量额）",
    description=_description(
        "腾讯财经 https://finance.qq.com/",
        "https://web.ifzq.gtimg.cn/appstock/app/minute/query",
        "猎手分时图；价格、累计股数及累计金额，交易日期由来源给出；不重复累加或生成均价",
        "/api/v1/realtime/tencent/minute?symbol=SSE:600000",
    ),
)
def minute(provider: Provider, symbol: Symbol) -> dict[str, JsonValue]:
    return provider.fetch("minute", symbol=symbol)


@router.get(
    "/tencent/index",
    response_model=SourceResponse[SourceIndexItem],
    summary="腾讯上证指数当前行情",
    description=_description(
        "腾讯财经 https://finance.qq.com/",
        "https://qt.gtimg.cn/?q=sh000001",
        "猎手指数日志用的指数行情；读取索引32涨幅，纠正原软件取33最高价的问题",
        "/api/v1/realtime/tencent/index",
    ),
)
def index(provider: Provider) -> dict[str, JsonValue]:
    return provider.fetch("index")


@router.get(
    "/eastmoney/themes/today",
    response_model=SourceResponse[SourceThemeItem],
    summary="东方财富今日题材机会",
    description=_description(
        "东方财富 https://www.eastmoney.com/",
        "POST https://emcfgdata.eastmoney.com/api/themeInvest/getTodayChance",
        "热点概念今日题材；固定args.pageSize=100,pageNum=1,sort=-1",
        "/api/v1/realtime/eastmoney/themes/today",
    ),
)
def today_themes(provider: Provider) -> dict[str, JsonValue]:
    return provider.fetch("today", page_size=100)


@router.get(
    "/eastmoney/themes",
    response_model=SourceResponse[SourceThemeItem],
    summary="东方财富题材排行（单页）",
    description=_description(
        "东方财富 https://www.eastmoney.com/",
        "POST https://emcfgdata.eastmoney.com/api/themeInvest/getThemeList",
        "热点概念题材列表，sort=-1；bf3板块涨幅与f3领涨股涨幅分开",
        "/api/v1/realtime/eastmoney/themes?page=1&page_size=100",
    ),
)
def themes(
    provider: Provider,
    page: Page = 1,
    page_size: Annotated[int, Query(ge=1, le=100, examples=[100])] = 100,
) -> dict[str, JsonValue]:
    return provider.fetch("themes", page=page, page_size=page_size)


@router.get(
    "/eastmoney/themes/{theme_code}/stocks",
    response_model=SourceResponse[SourceThemeStockItem],
    summary="东方财富题材成分股（单页）",
    description=_description(
        "东方财富 https://www.eastmoney.com/",
        "POST https://emcfgdata.eastmoney.com/api/themeInvest/getStockList",
        "热点概念选中题材的股票，themeCode保留字符串、sort=-1、sortField=f3；不生成AI评分",
        "/api/v1/realtime/eastmoney/themes/00647/stocks?page=1&page_size=1000",
    ),
)
def theme_stocks(
    provider: Provider,
    theme_code: Annotated[str, Path(pattern=r"^[0-9A-Za-z]{1,32}$", examples=["00647"])],
    page: Page = 1,
    page_size: Annotated[int, Query(ge=1, le=1000, examples=[1000])] = 1000,
) -> dict[str, JsonValue]:
    return provider.fetch("stocks", theme_code=theme_code, page=page, page_size=page_size)
