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
from market_data_center.public_api.models import (
    ClassificationMembersResponse,
    DailyBarResponse,
    ErrorDetail,
    ErrorResponse,
    HealthResponse,
    SecuritySearchResponse,
)
from market_data_center.public_api.queries import (
    PostgreSQLPublicQueryService,
    PublicQueryInvalid,
    PublicQueryNotFound,
    PublicQueryService,
    PublicQueryTimeout,
    PublicQueryUnavailable,
)
from market_data_center.settings import ApiSettings

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
STANDARD_SYMBOL_PATTERN = r"^(SSE|SZSE|BSE):[0-9]{6}$"


def create_app(
    *,
    settings: ApiSettings | None = None,
    query_service: PublicQueryService | None = None,
) -> FastAPI:
    configured = settings or ApiSettings()  # type: ignore[call-arg]
    owned_engine: Engine | None = None
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

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if owned_engine is not None:
            owned_engine.dispose()

    app = FastAPI(
        title="Market Data Center API",
        summary="Read-only A-share market data API",
        version="0.2.0",
        lifespan=lifespan,
    )
    app.state.api_settings = configured
    app.state.query_service = query_service

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

    return app


def _query_service(request: Request) -> PublicQueryService:
    return cast(PublicQueryService, request.app.state.query_service)


def _require_api_key(
    request: Request,
    provided: Annotated[str | None, Security(API_KEY_HEADER)],
) -> None:
    settings = cast(ApiSettings, request.app.state.api_settings)
    expected = settings.fastapi_api_key.get_secret_value()
    if provided is None or not compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="invalid or missing API key")


QueryServiceDependency = Annotated[PublicQueryService, Depends(_query_service)]
ApiKeyDependency = Annotated[None, Depends(_require_api_key)]


def _install_exception_handlers(app: FastAPI) -> None:
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
