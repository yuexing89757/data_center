from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError

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
    DailyLimitUpListItem,
    DailyLimitUpListResponse,
    LimitUpPoolItem,
    LimitUpPoolOmissionReasons,
    LimitUpPoolResponse,
    SecurityItem,
)
from market_data_center.public_api.queries import (
    PublicQueryNotFound,
    PublicQueryUnavailable,
)
from market_data_center.settings import ApiSettings

API_KEY = "test-api-key-00000000000000000000"


class FakeQueryService:
    def __init__(self) -> None:
        self.ready_error: Exception | None = None
        self.classification_error: Exception | None = None
        self.security_calls: list[tuple[str, int]] = []
        self.daily_bar_calls: list[tuple[str, date, date, int]] = []
        self.limit_up_calls: list[tuple[date, int | None, int]] = []

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

    def limit_up_pool(
        self, trade_date: date, version: int | None, limit: int
    ) -> LimitUpPoolResponse:
        self.limit_up_calls.append((trade_date, version, limit))
        return LimitUpPoolResponse(
            snapshot_id="11111111-1111-1111-1111-111111111111",
            calculation_id="22222222-2222-2222-2222-222222222222",
            trade_date=trade_date,
            effective_trade_date=date(2026, 8, 3),
            version=version or 2,
            rule_version="CN_MAINBOARD_2026_07_06",
            algorithm_version="1.0.0",
            input_hash="0" * 64,
            generated_at=datetime(2026, 8, 1, tzinfo=UTC),
            total_candidate_count=3,
            valid_count=2,
            returned_count=1,
            omitted_count=1,
            has_more=True,
            omission_reasons=LimitUpPoolOmissionReasons(
                missing_name=0,
                missing_close=1,
                missing_free_float_shares=1,
            ),
            items=[
                LimitUpPoolItem(
                    symbol="SSE:600000",
                    code="600000",
                    name="浦发银行",
                    free_float_market_cap_cny=Decimal("61200000.000000"),
                )
            ],
        )

    def daily_limit_up_list(self, trade_date: date, limit: int) -> DailyLimitUpListResponse:
        return DailyLimitUpListResponse(
            trade_date=trade_date,
            count=1,
            items=[
                DailyLimitUpListItem(
                    code="600000",
                    name="浦发银行",
                    close=Decimal("9.29"),
                    volume=62542540,
                    free_float_market_cap=Decimal("5000000000"),
                    free_float_turnover_pct=Decimal("2.5"),
                    seal_amount=Decimal("1200000"),
                    seal_volume_ratio=Decimal("0.05"),
                    consecutive_limit_up_days=2,
                    auction_volume=5000,
                    auction_amount=Decimal("46000"),
                    auction_premium_pct=Decimal("1.2"),
                )
            ],
        )


def _client(service: FakeQueryService) -> TestClient:
    settings = ApiSettings(
        fastapi_database_url=SecretStr("unused"),
        fastapi_api_key=SecretStr(API_KEY),
    )
    return TestClient(create_app(settings=settings, query_service=service))


def _headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


def test_api_settings_never_fall_back_to_worker_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://worker-credential-must-not-be-used")
    monkeypatch.delenv("FASTAPI_DATABASE_URL", raising=False)
    monkeypatch.setenv("FASTAPI_API_KEY", API_KEY)

    with pytest.raises(ValidationError):
        ApiSettings(_env_file=None)  # type: ignore[call-arg]


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


def test_limit_up_pool_returns_exact_decimal_market_cap_and_revision() -> None:
    service = FakeQueryService()

    response = _client(service).get(
        "/api/v1/limit-up-pool",
        params={"trade_date": "2026-07-31", "version": 2, "limit": 100},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["items"] == [
        {
            "symbol": "SSE:600000",
            "code": "600000",
            "name": "浦发银行",
            "free_float_market_cap_cny": "61200000.000000",
        }
    ]
    assert service.limit_up_calls == [(date(2026, 7, 31), 2, 100)]
    assert response.json()["total_candidate_count"] == 3
    assert response.json()["valid_count"] == 2
    assert response.json()["omitted_count"] == 1
    assert response.json()["has_more"] is True
    assert response.json()["omission_reasons"] == {
        "missing_name": 0,
        "missing_close": 1,
        "missing_free_float_shares": 1,
    }


def test_limit_up_pool_is_api_key_protected_and_bounded() -> None:
    service = FakeQueryService()
    client = _client(service)

    assert (
        client.get("/api/v1/limit-up-pool", params={"trade_date": "2026-07-31"}).status_code == 401
    )
    assert (
        client.get(
            "/api/v1/limit-up-pool",
            params={"trade_date": "2026-07-31", "limit": 5001},
            headers=_headers(),
        ).status_code
        == 422
    )
    assert service.limit_up_calls == []


def test_openapi_only_contains_the_active_non_derived_routes() -> None:
    schema = _client(FakeQueryService()).get("/openapi.json").json()

    assert "/api/v1/securities" in schema["paths"]
    assert "/api/v1/daily-bars/{symbol}" in schema["paths"]
    assert "/api/v1/limit-up-pool" in schema["paths"]
    assert "/api/v1/daily-limit-up-list" in schema["paths"]
    assert not any("adjusted" in path or "metric" in path for path in schema["paths"])


def test_daily_limit_up_list_returns_items() -> None:
    service = FakeQueryService()

    response = _client(service).get(
        "/api/v1/daily-limit-up-list",
        params={"trade_date": "2026-08-10", "limit": 200},
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["trade_date"] == "2026-08-10"
    assert body["count"] == 1
    item = body["items"][0]
    assert item["code"] == "600000"
    assert item["close"] == "9.29"
    assert item["consecutive_limit_up_days"] == 2
    assert item["seal_amount"] == "1200000"
