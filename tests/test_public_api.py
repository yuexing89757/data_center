from datetime import date
from decimal import Decimal

from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.domain import (
    ClassificationType,
    Exchange,
    SecurityStatus,
    SecurityType,
    TradeStatus,
)
from market_data_center.public_api import create_app
from market_data_center.public_api.models import (
    ClassificationMembersResponse,
    DailyBarItem,
    SecurityItem,
)
from market_data_center.public_api.queries import (
    PublicQueryNotFound,
    PublicQueryUnavailable,
)
from market_data_center.settings import ApiSettings

API_KEY = "test-api-key-000000000000"


class FakeQueryService:
    def __init__(self) -> None:
        self.ready_error: Exception | None = None
        self.classification_error: Exception | None = None
        self.security_calls: list[tuple[str, int]] = []
        self.daily_bar_calls: list[tuple[str, date, date, int]] = []

    def ready(self) -> None:
        if self.ready_error is not None:
            raise self.ready_error

    def search_securities(self, query: str, limit: int) -> tuple[SecurityItem, ...]:
        self.security_calls.append((query, limit))
        return (
            SecurityItem(
                symbol="SSE:600000",
                code="600000",
                exchange=Exchange.SSE,
                current_name="浦发银行",
                security_type=SecurityType.STOCK,
                status=SecurityStatus.LISTED,
                ipo_date=date(1999, 11, 10),
                delisting_date=None,
            ),
        )

    def daily_bars(
        self, symbol: str, start_date: date, end_date: date, limit: int
    ) -> tuple[DailyBarItem, ...]:
        self.daily_bar_calls.append((symbol, start_date, end_date, limit))
        return (
            DailyBarItem(
                symbol=symbol,
                trade_date=start_date,
                open=Decimal("10.10"),
                high=Decimal("10.30"),
                low=Decimal("10.00"),
                close=Decimal("10.20"),
                previous_close=Decimal("10.05"),
                volume=123_400,
                amount=Decimal("1258680.00"),
                trade_status=TradeStatus.TRADING,
                is_st=False,
            ),
        )

    def classification_members(
        self,
        namespace: str,
        classification_type: ClassificationType,
        classification_code: str,
        as_of_date: date,
        limit: int,
    ) -> ClassificationMembersResponse:
        if self.classification_error is not None:
            raise self.classification_error
        assert (namespace, classification_type, classification_code, as_of_date, limit) == (
            "tdx",
            ClassificationType.INDUSTRY,
            "T1001",
            date(2026, 7, 29),
            5000,
        )
        return ClassificationMembersResponse(
            snapshot_date=date(2026, 7, 29),
            member_count=2,
            returned_count=2,
            members=["SSE:600000", "SZSE:000001"],
        )


def _client(service: FakeQueryService) -> TestClient:
    settings = ApiSettings(
        database_url=SecretStr("unused"),
        fastapi_api_key=SecretStr(API_KEY),
    )
    return TestClient(create_app(settings=settings, query_service=service))


def _headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


def test_health_and_readiness_are_public() -> None:
    service = FakeQueryService()
    client = _client(service)

    assert client.get("/healthz").json()["status"] == "ok"
    assert client.get("/readyz").json()["status"] == "ready"


def test_readiness_hides_database_error_details() -> None:
    service = FakeQueryService()
    service.ready_error = PublicQueryUnavailable("secret database details")

    response = _client(service).get("/readyz")

    assert response.status_code == 503
    assert response.json() == {
        "error": {
            "code": "service_unavailable",
            "message": "data service is unavailable",
        }
    }
    assert "secret" not in response.text


def test_market_routes_require_the_api_key() -> None:
    client = _client(FakeQueryService())

    missing = client.get("/api/v1/securities", params={"query": "600000"})
    wrong = client.get(
        "/api/v1/securities",
        params={"query": "600000"},
        headers={"X-API-Key": "wrong-key"},
    )

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert missing.json()["error"]["code"] == "unauthorized"


def test_security_search_returns_a_bounded_envelope() -> None:
    service = FakeQueryService()

    response = _client(service).get(
        "/api/v1/securities",
        params={"query": "浦发", "limit": 10},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["items"][0]["symbol"] == "SSE:600000"
    assert service.security_calls == [("浦发", 10)]


def test_daily_bars_keep_decimal_values_as_strings() -> None:
    service = FakeQueryService()

    response = _client(service).get(
        "/api/v1/daily-bars/SSE:600000",
        params={"start_date": "2026-07-29", "end_date": "2026-07-29"},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["close"] == "10.20"
    assert response.json()["items"][0]["amount"] == "1258680.00"
    assert service.daily_bar_calls == [("SSE:600000", date(2026, 7, 29), date(2026, 7, 29), 1000)]


def test_daily_bar_validation_does_not_call_the_service() -> None:
    service = FakeQueryService()

    invalid_symbol = _client(service).get(
        "/api/v1/daily-bars/600000",
        params={"start_date": "2026-07-29", "end_date": "2026-07-29"},
        headers=_headers(),
    )
    reversed_dates = _client(service).get(
        "/api/v1/daily-bars/SSE:600000",
        params={"start_date": "2026-07-30", "end_date": "2026-07-29"},
        headers=_headers(),
    )

    assert invalid_symbol.status_code == 422
    assert reversed_dates.status_code == 422
    assert service.daily_bar_calls == []


def test_classification_members_and_not_found_response() -> None:
    service = FakeQueryService()
    client = _client(service)
    path = "/api/v1/classifications/tdx/industry/T1001/members"
    params = {"as_of_date": "2026-07-29"}

    response = client.get(path, params=params, headers=_headers())
    service.classification_error = PublicQueryNotFound("internal details")
    missing = client.get(path, params=params, headers=_headers())

    assert response.status_code == 200
    assert response.json()["members"] == ["SSE:600000", "SZSE:000001"]
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "not_found"
    assert "internal" not in missing.text


def test_openapi_only_contains_the_active_non_derived_routes() -> None:
    schema = _client(FakeQueryService()).get("/openapi.json").json()

    assert "/api/v1/securities" in schema["paths"]
    assert "/api/v1/daily-bars/{symbol}" in schema["paths"]
    assert not any("adjusted" in path or "metric" in path for path in schema["paths"])
