from datetime import date
from decimal import Decimal

from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.public_api import create_app
from market_data_center.public_api import models as api_models
from market_data_center.public_api.queries import PostgreSQLPublicQueryService
from market_data_center.settings import ApiSettings

API_KEY = "test-api-key-00000000000000000000"


def _settings() -> ApiSettings:
    return ApiSettings(
        fastapi_api_key=SecretStr(API_KEY),
        fastapi_database_url=SecretStr("postgresql+psycopg://reader:secret@db.example/test"),
        _env_file=None,
    )


def _headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


class FakeQueryService:
    def __init__(self) -> None:
        self.calls: list[tuple[date, Decimal, Decimal | None, Decimal, Decimal | None]] = []

    def ready(self) -> None:
        return None

    def auction_grab_lines(
        self,
        trade_date: date,
        min_grab_line_pct: Decimal,
        max_grab_line_pct: Decimal | None,
        min_change_pct: Decimal,
        max_change_pct: Decimal | None,
    ) -> dict[str, object]:
        self.calls.append(
            (
                trade_date,
                min_grab_line_pct,
                max_grab_line_pct,
                min_change_pct,
                max_change_pct,
            )
        )
        return {
            "trade_date": trade_date,
            "session_id": "00000000-0000-0000-0000-000000000057",
            "session_status": "succeeded",
            "min_grab_line_pct": min_grab_line_pct,
            "max_grab_line_pct": max_grab_line_pct,
            "min_change_pct": min_change_pct,
            "max_change_pct": max_change_pct,
            "first_batch_code": "092453",
            "final_batch_code": "092520",
            "count": 1,
            "items": [
                {
                    "code": "600000",
                    "name": "浦发银行",
                    "grab_line_pct": Decimal("2.0000000000"),
                    "change_pct_092520": Decimal("5.0000000000"),
                    "trade_date": trade_date,
                }
            ],
        }


def _client(service: FakeQueryService) -> TestClient:
    return TestClient(
        create_app(
            settings=_settings(),
            query_service=service,  # type: ignore[arg-type]
            auction_indicative_service=object(),  # type: ignore[arg-type]
            tencent_quote_live_service=object(),  # type: ignore[arg-type]
        )
    )


def test_call_auction_grab_line_models_preserve_exact_percentages() -> None:
    assert hasattr(api_models, "CallAuctionGrabLineItem")
    assert hasattr(api_models, "CallAuctionGrabLineResponse")
    item_type = api_models.CallAuctionGrabLineItem
    response_type = api_models.CallAuctionGrabLineResponse
    item = item_type(
        code="600000",
        name="浦发银行",
        grab_line_pct=Decimal("2.0000000000"),
        change_pct_092520=Decimal("5.0000000000"),
        trade_date=date(2026, 9, 7),
    )
    response = response_type(
        trade_date=date(2026, 9, 7),
        session_id="00000000-0000-0000-0000-000000000057",
        session_status="succeeded",
        min_grab_line_pct=Decimal("1.25"),
        max_grab_line_pct=Decimal("3.25"),
        min_change_pct=Decimal("3.50"),
        max_change_pct=Decimal("6.50"),
        first_batch_code="092453",
        final_batch_code="092520",
        count=1,
        items=[item],
    )

    assert response.items[0].grab_line_pct == Decimal("2.0000000000")
    assert response.items[0].change_pct_092520 == Decimal("5.0000000000")


def test_call_auction_grab_line_query_uses_required_date_and_threshold() -> None:
    calls: list[tuple[str, object]] = []
    payload = {
        "trade_date": "2026-09-07",
        "session_id": "00000000-0000-0000-0000-000000000057",
        "session_status": "succeeded",
        "min_grab_line_pct": "1.25",
        "max_grab_line_pct": "3.25",
        "min_change_pct": "3.50",
        "max_change_pct": "6.50",
        "first_batch_code": "092453",
        "final_batch_code": "092520",
        "count": 0,
        "items": [],
    }

    class StubResult:
        def mappings(self) -> "StubResult":
            return self

        def all(self) -> list[dict[str, object]]:
            return [{"payload": payload}]

    class StubConnection:
        def __enter__(self) -> "StubConnection":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, statement: object, parameters: object) -> StubResult:
            calls.append((str(statement), parameters))
            return StubResult()

    class StubEngine:
        def connect(self) -> StubConnection:
            return StubConnection()

    service = PostgreSQLPublicQueryService(StubEngine())  # type: ignore[arg-type]
    response = service.auction_grab_lines(
        date(2026, 9, 7),
        Decimal("1.25"),
        Decimal("3.25"),
        Decimal("3.50"),
        Decimal("6.50"),
    )

    assert response.count == 0
    assert "query_call_auction_grab_lines" in calls[-1][0]
    assert calls[-1][1] == {
        "trade_date": date(2026, 9, 7),
        "min_grab_line_pct": Decimal("1.25"),
        "max_grab_line_pct": Decimal("3.25"),
        "min_change_pct": Decimal("3.50"),
        "max_change_pct": Decimal("6.50"),
    }


def test_call_auction_grab_lines_requires_trade_date_and_defaults_open_ranges() -> None:
    service = FakeQueryService()

    missing_date = _client(service).get(
        "/api/v1/call-auction-grab-lines",
        headers=_headers(),
    )
    response = _client(service).get(
        "/api/v1/call-auction-grab-lines",
        params={"trade_date": "2026-09-07"},
        headers=_headers(),
    )

    assert missing_date.status_code == 422
    assert response.status_code == 200
    assert service.calls == [(date(2026, 9, 7), Decimal("0"), None, Decimal("0"), None)]
    assert response.json()["min_grab_line_pct"] == "0"
    assert response.json()["max_grab_line_pct"] is None
    assert response.json()["min_change_pct"] == "0"
    assert response.json()["max_change_pct"] is None
    assert response.json()["items"] == [
        {
            "code": "600000",
            "name": "浦发银行",
            "grab_line_pct": "2.0000000000",
            "change_pct_092520": "5.0000000000",
            "trade_date": "2026-09-07",
        }
    ]


def test_call_auction_grab_lines_passes_decimal_n_and_documents_formula() -> None:
    service = FakeQueryService()
    response = _client(service).get(
        "/api/v1/call-auction-grab-lines",
        params={
            "trade_date": "2026-09-07",
            "min_grab_line_pct": "1.25",
            "max_grab_line_pct": "3.25",
            "min_change_pct": "3.50",
            "max_change_pct": "6.50",
        },
        headers=_headers(),
    )
    schema = _client(service).get("/openapi.json").json()
    operation = schema["paths"]["/api/v1/call-auction-grab-lines"]["get"]

    assert response.status_code == 200
    assert service.calls == [
        (
            date(2026, 9, 7),
            Decimal("1.25"),
            Decimal("3.25"),
            Decimal("3.50"),
            Decimal("6.50"),
        )
    ]
    assert "09:25:20" in operation["description"]
    assert "09:24:53" in operation["description"]
    assert "严格大于" in operation["description"]
    assert operation["parameters"][0]["description"]
    assert operation["parameters"][1]["description"]
    assert operation["parameters"][2]["description"]
    assert operation["parameters"][3]["description"]
    assert operation["parameters"][4]["description"]


def test_call_auction_grab_lines_rejects_closed_or_reversed_ranges() -> None:
    service = FakeQueryService()
    client = _client(service)

    equal_grab_line = client.get(
        "/api/v1/call-auction-grab-lines",
        params={
            "trade_date": "2026-09-07",
            "min_grab_line_pct": "2",
            "max_grab_line_pct": "2",
        },
        headers=_headers(),
    )
    reversed_change = client.get(
        "/api/v1/call-auction-grab-lines",
        params={
            "trade_date": "2026-09-07",
            "min_change_pct": "5",
            "max_change_pct": "4",
        },
        headers=_headers(),
    )

    assert equal_grab_line.status_code == 422
    assert reversed_change.status_code == 422
    assert service.calls == []
