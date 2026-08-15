"""FastAPI application factory for external read-only consumers."""

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
    ClassificationMembersResponse,
    DailyBarResponse,
    DailyLimitUpListResponse,
    ErrorDetail,
    ErrorResponse,
    HealthResponse,
    LimitUpPoolResponse,
    SecuritySearchResponse,
    TopGainers20dResponse,
)
from market_data_center.public_api.queries import (
    PostgreSQLPublicQueryService,
    PublicQueryInvalid,
    PublicQueryNotFound,
    PublicQueryService,
    PublicQueryTimeout,
    PublicQueryUnavailable,
)
from market_data_center.raw_store import LocalRawStore
from market_data_center.settings import ApiSettings

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
STANDARD_SYMBOL_PATTERN = r"^(SSE|SZSE|BSE):[0-9]{6}$"


def create_app(
    *,
    settings: ApiSettings | None = None,
    query_service: PublicQueryService | None = None,
    auction_indicative_service: LiveAuctionIndicativeService | None = None,
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
            raise RuntimeError("live auction service must be injected with a query-service stub")
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
        title="Market Data Center API",
        summary="Read-only A-share market data API",
        version="0.2.0",
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

    _install_exception_handlers(app)

    @app.get("/healthz", response_model=HealthResponse, tags=["system"])
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get(
        "/readyz",
        response_model=HealthResponse,
        responses={503: {"model": ErrorResponse}},
        tags=["system"],
    )
    def readiness(service: QueryServiceDependency) -> HealthResponse:
        service.ready()
        return HealthResponse(status="ready")

    @app.get(
        "/api/v1/securities",
        response_model=SecuritySearchResponse,
        responses={401: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
        tags=["market-data"],
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
        tags=["market-data"],
    )
    def daily_bars(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        symbol: Annotated[str, Path(pattern=STANDARD_SYMBOL_PATTERN)],
        start_date: Annotated[date, Query()],
        end_date: Annotated[date, Query()],
        limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
    ) -> DailyBarResponse:
        if end_date < start_date:
            raise HTTPException(status_code=422, detail="end_date must not precede start_date")
        if (end_date - start_date).days > 3660:
            raise HTTPException(status_code=422, detail="date range must not exceed 3661 days")
        items = service.daily_bars(symbol, start_date, end_date, limit)
        return DailyBarResponse(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            count=len(items),
            items=list(items),
        )

    @app.get(
        "/api/v1/classifications/{namespace}/{classification_type}/{classification_code}/members",
        response_model=ClassificationMembersResponse,
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        tags=["market-data"],
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
        tags=["market-data"],
        summary="Get the exact-date mainboard limit-up pool",
        description=(
            "Returns the versioned pool whose stocks closed exactly at the deterministic "
            "upper price limit on trade_date. free_float_market_cap_cny is that date's "
            "unadjusted close multiplied by free-float shares. Invalid rows are omitted with "
            "grouped reason counts; valid rows are ordered by symbol before limit is applied. "
            "No value or date fallback is used."
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
        tags=["market-data"],
        summary="Get one immutable same-day limit-up snapshot",
        description=(
            "Returns the exact trade_date latest or requested today_limit_up snapshot. "
            "The response includes immutable revision, status, bounded quality metadata, "
            "canonical unadjusted price facts, close times same-date free-float shares, "
            "source-reported sealing facts, closing bid-1 computed sealing amount, optional "
            "five-level buy order-book context, and provider-neutral lineage. No date or "
            "value fallback is used; Decimal values are serialized as strings."
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
        tags=["market-data"],
        summary="Batch query one opening-auction market snapshot",
        description=(
            "Returns facts from the latest succeeded ingestion for the exact trade date, "
            "or the latest partial ingestion only when no succeeded ingestion exists. "
            "A six-digit code can return both SSE and SZSE symbols. Missing codes are "
            "reported explicitly; no date or batch fallback is used."
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
        tags=["market-data"],
        summary="Batch query one opening-auction market series session",
        description=(
            "Returns all recorded rounds from the latest succeeded session for the exact "
            "trade date, or the latest partial session only when no succeeded session exists. "
            "Rounds are ordered by scheduled time and report missing six-digit codes "
            "independently. Sessions and dates are never merged or substituted."
        ),
    )
    def call_auction_market_series_snapshots(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        request: CallAuctionMarketSeriesSnapshotQuery,
    ) -> CallAuctionMarketSeriesSnapshotResponse:
        return service.call_auction_market_series_snapshots(
            request.trade_date, tuple(request.codes)
        )

    @app.get(
        "/api/v1/top-gainers-20d",
        response_model=TopGainers20dResponse,
        tags=["market-data"],
        summary="Rank the top gainers over an exact 20-session window",
    )
    def top_gainers_20d(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        end_date: Annotated[date | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=10)] = 10,
    ) -> TopGainers20dResponse:
        return service.top_gainers_20d(end_date, limit)

    @app.get(
        "/api/v1/board-indexes/883423/bias",
        response_model=BoardIndexBiasResponse,
        tags=["market-data"],
        summary="Query the latest THS 883423 MA5 bias metrics",
        description=(
            "Uses only the latest stored THS:883423 daily bars. Returns the current "
            "five-session simple moving-average bias, its direction versus the previous "
            "available board session, and extrema from valid samples in the latest 30 "
            "stored sessions. The endpoint does not accept a date, fetch live data, or "
            "fall back to another board."
        ),
    )
    def board_index_bias_latest(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
    ) -> BoardIndexBiasResponse:
        return service.board_index_bias_latest()

    @app.get(
        "/api/v1/call-auction-one-price-limits",
        response_model=AuctionOnePriceLimitResponse,
        tags=["market-data"],
        summary="Query evidence-complete 09:26 one-price limit stocks",
    )
    def auction_one_price_limits(
        _: ApiKeyDependency,
        service: QueryServiceDependency,
        trade_date: Annotated[date | None, Query()] = None,
    ) -> AuctionOnePriceLimitResponse:
        return service.auction_one_price_limits(trade_date)

    @app.get(
        "/api/v1/call-auction-indicative-details",
        response_model=AuctionIndicativeDetailResponse,
        tags=["market-data"],
        summary="Query current-day call-auction virtual indicative matching details",
        description=(
            "Accepts one six-digit SSE/SZSE stock code and first reads the current Shanghai "
            "date's latest stored snapshot. Only an explicit database miss triggers a bounded "
            "Eastmoney fetch for 09:15:00-09:25:59 virtual indicative/reference price and "
            "displayed matching-volume observations. Live data is returned immediately after "
            "immutable Raw capture while database registration is queued asynchronously. "
            "These are not exchange trade ticks or order-by-order records. The source display "
            "classification is untrusted and is not a trade direction. data_origin and "
            "persistence_status distinguish stored data from a queued live result. Items are "
            "ordered by observed_at then source_sequence. Timestamp fields are rendered in "
            "Asia/Shanghai as YYYY-MM-DD HH:mm:ss."
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

    return app


def _query_service(request: Request) -> PublicQueryService:
    return cast(PublicQueryService, request.app.state.query_service)


def _auction_indicative_service(request: Request) -> LiveAuctionIndicativeService:
    return cast(LiveAuctionIndicativeService, request.app.state.auction_indicative_service)


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
ApiKeyDependency = Annotated[None, Depends(_require_api_key)]


def _auction_symbol_from_code(code: str) -> str:
    if code.startswith("6"):
        return f"SSE:{code}"
    if code.startswith(("0", "3")):
        return f"SZSE:{code}"
    raise HTTPException(status_code=422, detail="code is not a supported SSE/SZSE stock")


def _install_exception_handlers(app: FastAPI) -> None:
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
