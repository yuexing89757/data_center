"""FastAPI application factory for external read-only consumers."""

# ruff: noqa: RUF001 - Chinese API documentation intentionally uses Chinese punctuation.

from argparse import ArgumentParser
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date
from secrets import compare_digest
from typing import Annotated, cast

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Security
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from sqlalchemy import Engine, create_engine
from starlette.exceptions import HTTPException as StarletteHTTPException

from market_data_center.database_urls import sqlalchemy_url
from market_data_center.domain import ClassificationType
from market_data_center.providers.eastmoney_auction import EastmoneyAuctionIndicativeProvider
from market_data_center.public_api.auction_indicative_live import (
    AuctionIndicativeLiveBusy,
    AuctionIndicativeLiveInvalid,
    AuctionIndicativeLivePersistence,
    AuctionIndicativeLiveUnavailable,
    AuctionIndicativeLiveUpstream,
    LiveAuctionIndicativeService,
)
from market_data_center.public_api.auction_indicative_write import (
    AuctionIndicativeApiPersistence,
    AuctionIndicativePersistenceQueue,
)
from market_data_center.public_api.models import (
    AuctionIndicativeDetailResponse,
    AuctionOnePriceLimitResponse,
    BoardIndexBiasResponse,
    CallAuctionMarketSeriesSnapshotQuery,
    CallAuctionMarketSeriesSnapshotResponse,
    CallAuctionMarketSnapshotQuery,
    CallAuctionMarketSnapshotResponse,
    CallAuctionOnePricePatternResponse,
    ClassificationMembersResponse,
    ClosePriceNewHighs120dResponse,
    DailyBarResponse,
    DailyLimitUpListResponse,
    ErrorDetail,
    ErrorResponse,
    HealthResponse,
    LatestStockDailyIndicatorQuery,
    LatestStockDailyIndicatorResponse,
    LatestStockQuoteQuery,
    LatestStockQuoteResponse,
    LimitUpPoolResponse,
    SecuritySearchResponse,
    TopGainers20dResponse,
)
from market_data_center.public_api.openapi_zh import localize_openapi
from market_data_center.public_api.queries import (
    PostgreSQLPublicQueryService,
    PublicQueryAmbiguous,
    PublicQueryInvalid,
    PublicQueryNotFound,
    PublicQueryService,
    PublicQueryTimeout,
    PublicQueryUnavailable,
)
from market_data_center.public_api.tencent_quote_live import (
    DirectTencentQuoteLiveService,
    TencentQuoteLiveService,
    TencentQuoteLiveUpstream,
)
from market_data_center.raw_store import LocalRawStore
from market_data_center.settings import ApiSettings

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
STOCK_CODE_PATTERN = r"^[0-9]{6}$"


def create_app(
    *,
    settings: ApiSettings | None = None,
    query_service: PublicQueryService | None = None,
    auction_indicative_service: LiveAuctionIndicativeService | None = None,
    tencent_quote_live_service: TencentQuoteLiveService | None = None,
) -> FastAPI:
    configured = settings or ApiSettings()  # type: ignore[call-arg]
    owned_engine: Engine | None = None
    owned_write_engine: Engine | None = None
    owned_persistence_queue: AuctionIndicativePersistenceQueue | None = None
    if query_service is None:
        owned_engine = create_engine(
            sqlalchemy_url(configured.resolved_database_url()),
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={
                "options": "-c default_transaction_read_only=on -c statement_timeout=5000"
            },
        )
        query_service = PostgreSQLPublicQueryService(owned_engine)

    if auction_indicative_service is None:
        if owned_engine is None:
            raise RuntimeError("live services must be injected with a query-service stub")
        # This connection does not force a read-only transaction because it invokes one
        # narrowly granted SECURITY DEFINER persistence function.  The login role still has
        # no direct table-write privileges.
        owned_write_engine = create_engine(
            sqlalchemy_url(configured.resolved_database_url()),
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={
                "options": "-c default_transaction_read_only=off -c statement_timeout=5000"
            },
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if owned_persistence_queue is not None:
            owned_persistence_queue.shutdown()
        if owned_engine is not None:
            owned_engine.dispose()
        if owned_write_engine is not None:
            owned_write_engine.dispose()

    app = FastAPI(
        title="股票市场数据中心 API",
        summary="面向外部消费者的只读 A 股市场数据接口",
        description=(
            "提供有界、只读且可追溯的证券、日线、涨停池、集合竞价及客观衍生数据查询。"
            "除健康检查外，业务接口均需通过 X-API-Key 鉴权。"
        ),
        version="0.2.0",
        openapi_tags=[
            {"name": "系统状态", "description": "服务存活状态与数据库就绪状态检查。"},
            {"name": "市场数据", "description": "只读的 A 股市场事实与客观衍生结果查询。"},
        ],
        lifespan=lifespan,
    )
    app.state.api_settings = configured
    app.state.query_service = query_service
    if auction_indicative_service is None:
        assert owned_write_engine is not None
        raw_root = configured.fastapi_auction_raw_root
        owned_persistence_queue = AuctionIndicativePersistenceQueue(
            AuctionIndicativeApiPersistence(owned_write_engine, LocalRawStore(raw_root), raw_root)
        )
        auction_indicative_service = LiveAuctionIndicativeService(
            EastmoneyAuctionIndicativeProvider(
                timeout_seconds=configured.fastapi_auction_live_timeout_seconds,
                max_attempts=configured.fastapi_auction_live_max_attempts,
            ),
            owned_persistence_queue,
            cache_seconds=configured.fastapi_auction_live_cache_seconds,
            minimum_interval_seconds=configured.fastapi_auction_live_minimum_interval_seconds,
        )
    app.state.auction_indicative_service = auction_indicative_service
    app.state.tencent_quote_live_service = (
        tencent_quote_live_service or DirectTencentQuoteLiveService.from_settings(configured)
    )
    _install_exception_handlers(app)

    @app.get(
        "/healthz",
        response_model=HealthResponse,
        tags=["系统状态"],
        summary="检查 API 服务存活状态",
        description="仅检查 API 进程是否正常响应，不访问数据库。",
    )
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get(
        "/readyz",
        response_model=HealthResponse,
        responses={503: {"model": ErrorResponse}},
        tags=["系统状态"],
        summary="检查 API 服务就绪状态",
        description="检查只读数据库连接是否可用；不可用时返回 503。",
    )
    def readiness(service: QueryServiceDependency) -> HealthResponse:
        service.ready()
        return HealthResponse(status="ready")

    @app.get(
        "/api/v1/securities",
        response_model=SecuritySearchResponse,
        responses={401: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
        tags=["市场数据"],
        summary="按代码或名称搜索证券",
        description="在证券主数据中按代码或当前名称搜索，并按指定上限返回结果。",
    )
    def search_securities(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        query: Annotated[str, Query(min_length=1, max_length=100)],
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> SecuritySearchResponse:
        items = service.search_securities(query, limit)
        return SecuritySearchResponse(count=len(items), items=list(items))

    @app.get(
        "/api/v1/daily-bars/{symbol}",
        response_model=DailyBarResponse,
        responses={401: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
        tags=["市场数据"],
        summary="查询证券日线行情",
        description="查询一个标准证券代码在指定日期区间内的未复权日线事实。",
    )
    def daily_bars(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        symbol: Annotated[
            str,
            Path(
                pattern=STOCK_CODE_PATTERN,
                description="Six-digit stock code; the exchange is resolved from Security facts.",
            ),
        ],
        trade_date: Annotated[
            date,
            Query(description="Inclusive trading-date cutoff; no later bar is returned."),
        ],
        limit: Annotated[
            int,
            Query(
                ge=1,
                le=5000,
                description="Maximum number of most-recent stored trading-day bars.",
            ),
        ] = 20,
    ) -> DailyBarResponse:
        return service.daily_bars(symbol, trade_date, limit)

    @app.post(
        "/api/v1/stock-daily-indicators/latest/query",
        response_model=LatestStockDailyIndicatorResponse,
        responses={
            401: {"model": ErrorResponse},
            422: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        tags=["市场数据"],
        summary="批量查询股票最新每日指标",
        description=(
            "按一至五百个六位股票代码，逐股票返回当前保留数据中的最新每日指标。"
            "交易所由证券事实解析；各股票结果不要求来自同一交易日，未知或无指标代码会明确"
            "列入 missing_codes。接口不触发采集、补齐或历史回退。"
        ),
    )
    def latest_stock_daily_indicators(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        request: LatestStockDailyIndicatorQuery,
    ) -> LatestStockDailyIndicatorResponse:
        return service.latest_stock_daily_indicators(tuple(request.codes))

    @app.post(
        "/api/v1/realtime-quotes/latest/query",
        response_model=LatestStockQuoteResponse,
        responses={
            401: {"model": ErrorResponse},
            422: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        tags=["市场数据"],
        summary="批量查询股票最新五档行情快照",
        description=(
            "按一至五百个六位股票代码，在请求时有界访问腾讯批量行情并立即返回五档快照。"
            "接口不查询或写入行情数据库，不保存 Raw，不创建采集批次，也不回退历史快照。"
            "max_age_seconds 仅为兼容既有客户端保留，实时请求不使用该字段筛选结果。"
        ),
    )
    def latest_stock_quotes(
        _: ApiKeyDependency,
        service: TencentQuoteLiveServiceDependency,
        request: LatestStockQuoteQuery,
    ) -> LatestStockQuoteResponse:
        return service.fetch_current(tuple(request.codes), request.max_age_seconds)

    @app.get(
        "/api/v1/classifications/{namespace}/{classification_type}/{classification_code}/members",
        response_model=ClassificationMembersResponse,
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        tags=["市场数据"],
        summary="查询指定日期的分类成员",
        description="按分类体系、类型和代码查询指定有效日期的证券成员。",
    )
    def classification_members(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        namespace: Annotated[str, Path(min_length=1, max_length=50)],
        classification_type: ClassificationType,
        classification_code: Annotated[str, Path(min_length=1, max_length=100)],
        as_of_date: Annotated[date, Query()],
        limit: Annotated[int, Query(ge=1, le=5000)] = 5000,
    ) -> ClassificationMembersResponse:
        return service.classification_members(
            namespace,
            classification_type,
            classification_code,
            as_of_date,
            limit,
        )

    @app.get(
        "/api/v1/limit-up-pool",
        response_model=LimitUpPoolResponse,
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        tags=["市场数据"],
        summary="查询指定交易日的主板涨停池",
        description=(
            "返回指定交易日收盘价严格等于确定性涨停价的版本化主板股票池。流通市值按当日"
            "未复权收盘价乘以自由流通股本计算；无效记录按原因汇总后省略，有效记录先按"
            "标准证券代码排序再应用返回上限。不使用数值或日期回退。"
        ),
    )
    def limit_up_pool(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        trade_date: Annotated[date, Query()],
        version: Annotated[int | None, Query(ge=1)] = None,
        limit: Annotated[int, Query(ge=1, le=5000)] = 5000,
    ) -> LimitUpPoolResponse:
        return service.limit_up_pool(trade_date, version, limit)

    @app.get(
        "/api/v1/daily-limit-up-list",
        response_model=DailyLimitUpListResponse,
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        tags=["市场数据"],
        summary="查询指定交易日的不可变涨停列表快照",
        description=(
            "返回指定交易日最新或指定版本的涨停列表快照，包含不可变版本、状态、质量摘要、"
            "未复权价格事实、当日自由流通股本、来源封单事实、按收盘买一计算的封单额、"
            "可选五档买盘和来源追溯信息。不使用日期或数值回退；高精度数值序列化为字符串。"
        ),
    )
    def daily_limit_up_list(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        trade_date: Annotated[date, Query()],
        version: Annotated[int | None, Query(ge=1)] = None,
        offset: Annotated[int, Query(ge=0, le=50000)] = 0,
        limit: Annotated[int, Query(ge=1, le=500)] = 200,
    ) -> DailyLimitUpListResponse:
        return service.daily_limit_up_list(trade_date, version, offset, limit)

    @app.post(
        "/api/v1/call-auction-market-snapshots/query",
        response_model=CallAuctionMarketSnapshotResponse,
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        tags=["市场数据"],
        summary="批量查询开盘集合竞价市场快照",
        description=(
            "返回指定交易日最新成功采集批次的数据；仅在没有成功批次时使用最新部分成功批次。"
            "同一个六位代码可能同时匹配沪市和深市证券，未命中的代码会明确列出。不使用日期"
            "或其他批次回退。"
        ),
    )
    def call_auction_market_snapshots(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        request: CallAuctionMarketSnapshotQuery,
    ) -> CallAuctionMarketSnapshotResponse:
        return service.call_auction_market_snapshots(request.trade_date, tuple(request.codes))

    @app.post(
        "/api/v1/call-auction-market-series-snapshots/query",
        response_model=CallAuctionMarketSeriesSnapshotResponse,
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        tags=["市场数据"],
        summary="批量查询开盘集合竞价序列快照",
        description=(
            "返回指定交易日最新成功采集会话的全部轮次；仅在没有成功会话时使用最新部分成功"
            "会话。轮次按计划采集时间正序排列，并分别报告未命中的六位代码。不同会话或日期"
            "的数据不会合并或替代。可选 batch_code 按六位 HHMMSS 精确筛选所选会话内的"
            "单个采集批次；不传时返回全部轮次。"
        ),
    )
    def call_auction_market_series_snapshots(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        request: CallAuctionMarketSeriesSnapshotQuery,
    ) -> CallAuctionMarketSeriesSnapshotResponse:
        return service.call_auction_market_series_snapshots(
            request.trade_date, tuple(request.codes), request.batch_code
        )

    @app.get(
        "/api/v1/top-gainers-20d",
        response_model=TopGainers20dResponse,
        tags=["市场数据"],
        summary="查询最近二十个交易日涨幅榜",
        description="按严格的二十交易日窗口计算区间收益率，并返回涨幅最高的股票。",
    )
    def top_gainers_20d(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        end_date: Annotated[date | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=10)] = 10,
    ) -> TopGainers20dResponse:
        return service.top_gainers_20d(end_date, limit)

    @app.get(
        "/api/v1/close-price-new-highs-120d",
        response_model=ClosePriceNewHighs120dResponse,
        tags=["市场数据"],
        summary="查询沪深两市收盘价创一百二十日新高的股票",
        description="返回最新交易日收盘价严格高于此前一百一十九个交易日最高收盘价的股票。",
    )
    def close_price_new_highs_120d(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
    ) -> ClosePriceNewHighs120dResponse:
        return service.close_price_new_highs_120d()

    @app.get(
        "/api/v1/board-indexes/883423/bias",
        response_model=BoardIndexBiasResponse,
        tags=["市场数据"],
        summary="查询同花顺 883423 板块最新五日线乖离指标",
        description=(
            "只读取数据库中最新已持久化的 THS:883423 日线。返回当前五日简单移动平均乖离率、"
            "相对上一有效交易日的变化方向，以及近三十个交易日有效样本的最高和最低乖离率。"
            "当天尚未入库时返回数据库最近交易日并明确 trade_date；历史少于34条时返回404。"
            "本接口不接收参数，不访问行情提供方，也不写入数据。"
        ),
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    def board_index_bias_latest(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
    ) -> BoardIndexBiasResponse:
        return service.board_index_bias_latest()

    @app.get(
        "/api/v1/call-auction-one-price-limits",
        response_model=AuctionOnePriceLimitResponse,
        tags=["市场数据"],
        summary="实时计算 09:25:30 主板一字涨跌停列表",
        description=(
            "选取一批已存储的沪深市场 09:25:30 快照，在读取时按已接受的主板百分之十涨跌停"
            "规则计算结果。不依赖夜间涨跌停批次，不访问行情提供方，不使用更晚的日线，也不写入数据。"
        ),
    )
    def auction_one_price_limits(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        trade_date: Annotated[date | None, Query()] = None,
    ) -> AuctionOnePriceLimitResponse:
        return service.auction_one_price_limits(trade_date)

    @app.get(
        "/api/v1/call-auction-one-price-patterns",
        response_model=CallAuctionOnePricePatternResponse,
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        tags=["市场数据"],
        summary="查询集合竞价29轮同价形态股票",
        description=(
            "读取沪深上市股票在 09:15:20–09:24:40 的29轮集合竞价序列事实。"
            "仅返回29轮价格完全相同、相对昨收精确涨跌幅位于闭区间 [-4%, 4%] 的股票。"
            "显式交易日无完整窗口时返回404且不回退；省略日期时选择最近完整窗口。"
        ),
    )
    def auction_one_price_patterns(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        trade_date: Annotated[date | None, Query()] = None,
    ) -> CallAuctionOnePricePatternResponse:
        return service.auction_one_price_patterns(trade_date)

    @app.get(
        "/api/v1/call-auction-indicative-details",
        response_model=AuctionIndicativeDetailResponse,
        tags=["市场数据"],
        summary="查询当日集合竞价虚拟匹配明细",
        description=(
            "接收一个六位沪深股票代码，优先读取上海时区当日最新数据库快照。仅在数据库明确"
            "无数据时，有界访问东方财富，获取 09:15:00 至 09:25:59 的虚拟匹配或参考价格及"
            "展示匹配量。实时结果在保存不可变原始数据后立即返回，数据库登记异步执行。该数据"
            "不是交易所成交明细或逐笔委托，来源展示分类也不代表成交方向。结果按观察时间和"
            "来源序号正序排列，时间格式为上海时区 YYYY-MM-DD HH:mm:ss。"
        ),
    )
    def auction_indicative_details(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        live_service: AuctionIndicativeServiceDependency,
        code: Annotated[str, Query(pattern=r"^[0-9]{6}$")],
        offset: Annotated[int, Query(ge=0, le=5000)] = 0,
        limit: Annotated[int, Query(ge=1, le=500)] = 200,
    ) -> AuctionIndicativeDetailResponse:
        symbol = _auction_symbol_from_code(code)
        try:
            return service.auction_indicative_details(symbol, offset, limit)
        except PublicQueryNotFound:
            return live_service.fetch_current(symbol, offset, limit)

    app.openapi_schema = localize_openapi(app.openapi())
    return app


def _query_service(request: Request) -> PublicQueryService:
    return cast(PublicQueryService, request.app.state.query_service)


def _auction_indicative_service(request: Request) -> LiveAuctionIndicativeService:
    return cast(LiveAuctionIndicativeService, request.app.state.auction_indicative_service)


def _tencent_quote_live_service(request: Request) -> TencentQuoteLiveService:
    return cast(TencentQuoteLiveService, request.app.state.tencent_quote_live_service)


def _require_api_key(
    request: Request,
    provided: Annotated[str | None, Security(API_KEY_HEADER)],
) -> None:
    settings = cast(ApiSettings, request.app.state.api_settings)
    expected = settings.fastapi_api_key.get_secret_value()
    if provided is None or not compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="invalid or missing API key")


QueryServiceDependency = Annotated[PublicQueryService, Depends(_query_service)]
AuctionIndicativeServiceDependency = Annotated[
    LiveAuctionIndicativeService, Depends(_auction_indicative_service)
]
TencentQuoteLiveServiceDependency = Annotated[
    TencentQuoteLiveService, Depends(_tencent_quote_live_service)
]
ApiKeyDependency = Annotated[None, Depends(_require_api_key)]


def _auction_symbol_from_code(code: str) -> str:
    if code.startswith("6"):
        return f"SSE:{code}"
    if code.startswith(("0", "3")):
        return f"SZSE:{code}"
    raise HTTPException(status_code=422, detail="code is not a supported SSE/SZSE stock")


def _install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(TencentQuoteLiveUpstream)
    async def tencent_quote_upstream(_: Request, __: TencentQuoteLiveUpstream) -> JSONResponse:
        return _error_response(502, "upstream_error", "Tencent quote provider request failed")

    @app.exception_handler(AuctionIndicativeLiveInvalid)
    async def live_invalid(_: Request, __: AuctionIndicativeLiveInvalid) -> JSONResponse:
        return _error_response(422, "validation_error", "live auction request is invalid")

    @app.exception_handler(AuctionIndicativeLiveBusy)
    async def live_busy(_: Request, __: AuctionIndicativeLiveBusy) -> JSONResponse:
        return _error_response(429, "rate_limited", "live auction provider access is busy")

    @app.exception_handler(AuctionIndicativeLiveUpstream)
    async def live_upstream(_: Request, __: AuctionIndicativeLiveUpstream) -> JSONResponse:
        return _error_response(502, "upstream_error", "external auction provider request failed")

    @app.exception_handler(AuctionIndicativeLiveUnavailable)
    async def live_unavailable(_: Request, __: AuctionIndicativeLiveUnavailable) -> JSONResponse:
        return _error_response(503, "upstream_unavailable", "auction observations are unavailable")

    @app.exception_handler(AuctionIndicativeLivePersistence)
    async def live_persistence(_: Request, __: AuctionIndicativeLivePersistence) -> JSONResponse:
        return _error_response(
            503, "persistence_unavailable", "auction observations were not saved"
        )

    @app.exception_handler(PublicQueryInvalid)
    async def invalid_query(_: Request, __: PublicQueryInvalid) -> JSONResponse:
        return _error_response(400, "invalid_query", "query parameters were rejected")

    @app.exception_handler(PublicQueryAmbiguous)
    async def ambiguous_query(_: Request, __: PublicQueryAmbiguous) -> JSONResponse:
        return _error_response(422, "ambiguous_stock_code", "stock code is ambiguous")

    @app.exception_handler(PublicQueryNotFound)
    async def query_not_found(_: Request, __: PublicQueryNotFound) -> JSONResponse:
        return _error_response(404, "not_found", "requested data was not found")

    @app.exception_handler(PublicQueryTimeout)
    async def query_timeout(_: Request, __: PublicQueryTimeout) -> JSONResponse:
        return _error_response(504, "query_timeout", "data query timed out")

    @app.exception_handler(PublicQueryUnavailable)
    async def query_unavailable(_: Request, __: PublicQueryUnavailable) -> JSONResponse:
        return _error_response(503, "service_unavailable", "data service is unavailable")

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, __: RequestValidationError) -> JSONResponse:
        return _error_response(422, "validation_error", "request parameters are invalid")

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_: Request, error: StarletteHTTPException) -> JSONResponse:
        message = error.detail if isinstance(error.detail, str) else "request failed"
        code = "unauthorized" if error.status_code == 401 else "http_error"
        return _error_response(error.status_code, code, message)


def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
    body = ErrorResponse(error=ErrorDetail(code=code, message=message))
    return JSONResponse(status_code=status_code, content=body.model_dump(mode="json"))


def run() -> None:
    parser = ArgumentParser(description="run the Market Data Center read-only API")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    args = parser.parse_args()
    settings = ApiSettings()  # type: ignore[call-arg]
    uvicorn.run(
        create_app(settings=settings),
        host=args.host or settings.fastapi_host,
        port=args.port or settings.fastapi_port,
        log_level="info",
    )
