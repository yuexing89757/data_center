"""Read-only access to the bounded api_v1 PostgreSQL contract."""

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any, Never, Protocol

from sqlalchemy import Engine, RowMapping, text
from sqlalchemy.exc import DBAPIError

from market_data_center.domain import ClassificationType
from market_data_center.public_api.models import (
    ClassificationMembersResponse,
    DailyBarItem,
    SecurityItem,
)

QUERY_SECURITIES = text("""
select *
from api_v1.query_securities(p_query => :query, p_limit => :limit)
""")

QUERY_DAILY_BARS = text("""
select *
from api_v1.query_daily_bars(
    p_symbol => :symbol,
    p_start_date => :start_date,
    p_end_date => :end_date,
    p_limit => :limit
)
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


class PublicQueryError(RuntimeError):
    """Safe application-level query error."""


class PublicQueryInvalid(PublicQueryError):
    pass


class PublicQueryNotFound(PublicQueryError):
    pass


class PublicQueryTimeout(PublicQueryError):
    pass


class PublicQueryUnavailable(PublicQueryError):
    pass


class PublicQueryService(Protocol):
    def ready(self) -> None: ...

    def search_securities(self, query: str, limit: int) -> tuple[SecurityItem, ...]: ...

    def daily_bars(
        self, symbol: str, start_date: date, end_date: date, limit: int
    ) -> tuple[DailyBarItem, ...]: ...

    def classification_members(
        self,
        namespace: str,
        classification_type: ClassificationType,
        classification_code: str,
        as_of_date: date,
        limit: int,
    ) -> ClassificationMembersResponse: ...


class PostgreSQLPublicQueryService:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def ready(self) -> None:
        self._execute(text("select 1"), {})

    def search_securities(self, query: str, limit: int) -> tuple[SecurityItem, ...]:
        rows = self._execute(QUERY_SECURITIES, {"query": query, "limit": limit})
        return tuple(SecurityItem.model_validate(dict(row)) for row in rows)

    def daily_bars(
        self, symbol: str, start_date: date, end_date: date, limit: int
    ) -> tuple[DailyBarItem, ...]:
        rows = self._execute(
            QUERY_DAILY_BARS,
            {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            },
        )
        return tuple(DailyBarItem.model_validate(dict(row)) for row in rows)

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

    def _execute(self, statement: Any, parameters: Mapping[str, object]) -> Sequence[RowMapping]:
        try:
            with self._engine.connect() as connection:
                return connection.execute(statement, parameters).mappings().all()
        except DBAPIError as error:
            _raise_safe_query_error(error)


def _raise_safe_query_error(error: DBAPIError) -> Never:
    sqlstate = getattr(error.orig, "sqlstate", None)
    if sqlstate == "22023":
        raise PublicQueryInvalid("query parameters were rejected") from error
    if sqlstate == "P0002":
        raise PublicQueryNotFound("requested data was not found") from error
    if sqlstate == "57014":
        raise PublicQueryTimeout("query timed out") from error
    raise PublicQueryUnavailable("data service is unavailable") from error
