"""Read-only access to the bounded api_v1 PostgreSQL contract."""

import logging
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any, Never, Protocol

from sqlalchemy import Engine, RowMapping, text
from sqlalchemy.exc import DBAPIError

from market_data_center.domain import ClassificationType
from market_data_center.public_api.models import (
    AuctionIndicativeDetailResponse,
    AuctionOnePriceLimitResponse,
    BoardIndexBiasResponse,
    CallAuctionMarketSeriesSnapshotResponse,
    CallAuctionMarketSnapshotResponse,
    CallAuctionOnePricePatternResponse,
    ClassificationMembersResponse,
    ClosePriceNewHighs120dResponse,
    DailyBarResponse,
    DailyLimitUpListResponse,
    LimitUpPoolResponse,
    SecurityItem,
    TopGainers20dResponse,
)

LOGGER = logging.getLogger(__name__)

QUERY_SECURITIES = text("""
select *
from api_v1.query_securities(p_query => :query, p_limit => :limit)
""")

QUERY_DAILY_BARS = text("""
select api_v1.query_recent_daily_bars(
    p_code => :code,
    p_trade_date => :trade_date,
    p_limit => :limit
) as payload
""")

QUERY_CLASSIFICATION_MEMBERS = text("""
select *
from api_v1.query_classification_members_as_of(
    p_namespace => :namespace,
    p_classification_type => :classification_type,
    p_classification_code => :classification_code,
    p_as_of_date => :as_of_date,
    p_limit => :limit
)
""")

QUERY_LIMIT_UP_POOL = text("""
select api_v1.query_limit_up_pool(
    p_trade_date => :trade_date,
    p_version => :version,
    p_limit => :limit
) as payload
""")

QUERY_DAILY_LIMIT_UP_LIST = text("""
select api_v1.query_daily_limit_up_list(
    p_trade_date => :trade_date,
    p_version => :version,
    p_offset => :offset,
    p_limit => :limit
) as payload
""")

QUERY_CALL_AUCTION_MARKET_SNAPSHOTS = text("""
select api_v1.query_call_auction_market_snapshots(
    p_trade_date => :trade_date,
    p_codes => :codes
) as payload
""")

QUERY_CALL_AUCTION_MARKET_SERIES_SNAPSHOTS = text("""
select api_v1.query_call_auction_market_series_snapshots(
    p_trade_date => :trade_date,
    p_codes => :codes
) as payload
""")

QUERY_TOP_GAINERS_20D = text("""
select api_v1.query_top_gainers_20d(p_end_date => :end_date, p_limit => :limit) as payload
""")


QUERY_CLOSE_PRICE_NEW_HIGHS_120D = text("""
select api_v1.query_close_price_new_highs_120d() as payload
""")
QUERY_BOARD_INDEX_BIAS_LATEST = text("""
select api_v1.query_board_index_bias_latest() as payload
""")

QUERY_AUCTION_ONE_PRICE_LIMITS = text("""
select api_v1.query_auction_one_price_limits(p_trade_date => :trade_date) as payload
""")

QUERY_AUCTION_ONE_PRICE_PATTERNS = text("""
select api_v1.query_call_auction_one_price_patterns(
    p_trade_date => :trade_date
) as payload
""")

QUERY_AUCTION_INDICATIVE_DETAILS = text("""
select api_v1.query_call_auction_indicative_details(
    p_symbol => :symbol,
    p_trade_date => (now() at time zone 'Asia/Shanghai')::date,
    p_offset => :offset,
    p_limit => :limit
) as payload
""")


class PublicQueryError(RuntimeError):
    """Safe application-level query error."""


class PublicQueryInvalid(PublicQueryError):
    pass


class PublicQueryNotFound(PublicQueryError):
    pass


class BoardIndexBiasNotReady(PublicQueryNotFound):
    """The fixed board cache explicitly requested live fallback via SQLSTATE P0002."""


class PublicQueryTimeout(PublicQueryError):
    pass


class PublicQueryUnavailable(PublicQueryError):
    pass


class PublicQueryService(Protocol):
    def ready(self) -> None: ...

    def search_securities(self, query: str, limit: int) -> tuple[SecurityItem, ...]: ...

    def daily_bars(self, code: str, trade_date: date, limit: int) -> DailyBarResponse: ...

    def classification_members(
        self,
        namespace: str,
        classification_type: ClassificationType,
        classification_code: str,
        as_of_date: date,
        limit: int,
    ) -> ClassificationMembersResponse: ...

    def limit_up_pool(
        self, trade_date: date, version: int | None, limit: int
    ) -> LimitUpPoolResponse: ...

    def daily_limit_up_list(
        self, trade_date: date, version: int | None, offset: int, limit: int
    ) -> DailyLimitUpListResponse: ...

    def call_auction_market_snapshots(
        self, trade_date: date, codes: tuple[str, ...]
    ) -> CallAuctionMarketSnapshotResponse: ...

    def call_auction_market_series_snapshots(
        self, trade_date: date, codes: tuple[str, ...]
    ) -> CallAuctionMarketSeriesSnapshotResponse: ...

    def top_gainers_20d(self, end_date: date | None, limit: int) -> TopGainers20dResponse: ...

    def close_price_new_highs_120d(self) -> ClosePriceNewHighs120dResponse: ...

    def board_index_bias_latest(self) -> BoardIndexBiasResponse: ...

    def auction_one_price_limits(self, trade_date: date | None) -> AuctionOnePriceLimitResponse: ...

    def auction_one_price_patterns(
        self, trade_date: date | None
    ) -> CallAuctionOnePricePatternResponse: ...

    def auction_indicative_details(
        self, symbol: str, offset: int, limit: int
    ) -> AuctionIndicativeDetailResponse: ...


class PostgreSQLPublicQueryService:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def ready(self) -> None:
        self._execute(text("select 1"), {})

    def search_securities(self, query: str, limit: int) -> tuple[SecurityItem, ...]:
        rows = self._execute(QUERY_SECURITIES, {"query": query, "limit": limit})
        return tuple(SecurityItem.model_validate(dict(row)) for row in rows)

    def daily_bars(self, code: str, trade_date: date, limit: int) -> DailyBarResponse:
        rows = self._execute(
            QUERY_DAILY_BARS,
            {"code": code, "trade_date": trade_date, "limit": limit},
        )
        return DailyBarResponse.model_validate(rows[0]["payload"])

    def classification_members(
        self,
        namespace: str,
        classification_type: ClassificationType,
        classification_code: str,
        as_of_date: date,
        limit: int,
    ) -> ClassificationMembersResponse:
        rows = self._execute(
            QUERY_CLASSIFICATION_MEMBERS,
            {
                "namespace": namespace,
                "classification_type": classification_type.value,
                "classification_code": classification_code,
                "as_of_date": as_of_date,
                "limit": limit,
            },
        )
        if not rows:
            raise PublicQueryNotFound("classification snapshot was not found")
        return ClassificationMembersResponse.model_validate(dict(rows[0]))

    def limit_up_pool(
        self, trade_date: date, version: int | None, limit: int
    ) -> LimitUpPoolResponse:
        rows = self._execute(
            QUERY_LIMIT_UP_POOL,
            {"trade_date": trade_date, "version": version, "limit": limit},
        )
        if not rows:
            raise PublicQueryNotFound("limit-up pool was not found")
        return LimitUpPoolResponse.model_validate(rows[0]["payload"])

    def daily_limit_up_list(
        self, trade_date: date, version: int | None, offset: int, limit: int
    ) -> DailyLimitUpListResponse:
        rows = self._execute(
            QUERY_DAILY_LIMIT_UP_LIST,
            {
                "trade_date": trade_date,
                "version": version,
                "offset": offset,
                "limit": limit,
            },
        )
        if not rows:
            raise PublicQueryNotFound("daily limit-up list was not found")
        return DailyLimitUpListResponse.model_validate(rows[0]["payload"])

    def call_auction_market_snapshots(
        self, trade_date: date, codes: tuple[str, ...]
    ) -> CallAuctionMarketSnapshotResponse:
        rows = self._execute(
            QUERY_CALL_AUCTION_MARKET_SNAPSHOTS,
            {"trade_date": trade_date, "codes": list(codes)},
        )
        if not rows:
            raise PublicQueryNotFound("call-auction market snapshot was not found")
        return CallAuctionMarketSnapshotResponse.model_validate(rows[0]["payload"])

    def call_auction_market_series_snapshots(
        self, trade_date: date, codes: tuple[str, ...]
    ) -> CallAuctionMarketSeriesSnapshotResponse:
        rows = self._execute(
            QUERY_CALL_AUCTION_MARKET_SERIES_SNAPSHOTS,
            {"trade_date": trade_date, "codes": list(codes)},
        )
        if not rows:
            raise PublicQueryNotFound("call-auction market series snapshot was not found")
        return CallAuctionMarketSeriesSnapshotResponse.model_validate(rows[0]["payload"])

    def top_gainers_20d(self, end_date: date | None, limit: int) -> TopGainers20dResponse:
        rows = self._execute(QUERY_TOP_GAINERS_20D, {"end_date": end_date, "limit": limit})
        return TopGainers20dResponse.model_validate(rows[0]["payload"])

    def close_price_new_highs_120d(self) -> ClosePriceNewHighs120dResponse:
        rows = self._execute(QUERY_CLOSE_PRICE_NEW_HIGHS_120D, {}, statement_timeout_ms=10_000)
        return ClosePriceNewHighs120dResponse.model_validate(rows[0]["payload"])

    def board_index_bias_latest(self) -> BoardIndexBiasResponse:
        try:
            rows = self._execute(QUERY_BOARD_INDEX_BIAS_LATEST, {})
        except PublicQueryNotFound as error:
            raise BoardIndexBiasNotReady("board-index history requires live fallback") from error
        return BoardIndexBiasResponse.model_validate(rows[0]["payload"])

    def auction_one_price_limits(self, trade_date: date | None) -> AuctionOnePriceLimitResponse:
        rows = self._execute(QUERY_AUCTION_ONE_PRICE_LIMITS, {"trade_date": trade_date})
        return AuctionOnePriceLimitResponse.model_validate(rows[0]["payload"])

    def auction_one_price_patterns(
        self, trade_date: date | None
    ) -> CallAuctionOnePricePatternResponse:
        rows = self._execute(QUERY_AUCTION_ONE_PRICE_PATTERNS, {"trade_date": trade_date})
        if not rows or rows[0]["payload"] is None:
            raise PublicQueryNotFound("call-auction one-price pattern session was not found")
        return CallAuctionOnePricePatternResponse.model_validate(rows[0]["payload"])

    def auction_indicative_details(
        self, symbol: str, offset: int, limit: int
    ) -> AuctionIndicativeDetailResponse:
        rows = self._execute(
            QUERY_AUCTION_INDICATIVE_DETAILS,
            {"symbol": symbol, "offset": offset, "limit": limit},
        )
        return AuctionIndicativeDetailResponse.model_validate(rows[0]["payload"])

    def _execute(
        self,
        statement: Any,
        parameters: Mapping[str, object],
        *,
        statement_timeout_ms: int | None = None,
    ) -> Sequence[RowMapping]:
        try:
            with self._engine.connect() as connection:
                if statement_timeout_ms is not None:
                    connection.execute(
                        text("select set_config('statement_timeout', :statement_timeout, true)"),
                        {"statement_timeout": f"{statement_timeout_ms}ms"},
                    )
                return connection.execute(statement, parameters).mappings().all()
        except DBAPIError as error:
            _raise_safe_query_error(error)


def _raise_safe_query_error(error: DBAPIError) -> Never:
    sqlstate = getattr(error.orig, "sqlstate", None)
    driver = getattr(getattr(error, "connection", None), "dialect", None)
    driver_name = getattr(driver, "driver", None)
    LOGGER.warning(
        "public query failed: sqlstate=%s driver=%s detail=%r",
        sqlstate,
        driver_name,
        str(error.orig),
    )
    if sqlstate == "22023":
        raise PublicQueryInvalid("query parameters were rejected") from error
    if sqlstate == "P0002":
        raise PublicQueryNotFound("requested data was not found") from error
    if sqlstate == "57014":
        raise PublicQueryTimeout("query timed out") from error
    raise PublicQueryUnavailable("data service is unavailable") from error
