from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from market_data_center.public_api.stock_max_minute_volume import (
    StockMaxMinuteVolumeQuery,
    StockMaxMinuteVolumeResponse,
    router,
)


def test_minute_query_requires_exact_nonfuture_date_and_bounded_standard_symbols():
    with pytest.raises(ValidationError):
        StockMaxMinuteVolumeQuery(symbols=["SSE:600000"])
    for symbols in ([], ["600000"], ["THS:883423"], ["SSE:600000"] * 501):
        with pytest.raises(ValidationError):
            StockMaxMinuteVolumeQuery(symbols=symbols, trade_date=date(2026, 11, 3))
    with pytest.raises(ValidationError):
        StockMaxMinuteVolumeQuery(
            symbols=["SSE:600000"], trade_date=date.today() + timedelta(days=2)
        )
    assert StockMaxMinuteVolumeQuery(
        symbols=["SSE:600000"] * 2, trade_date=date(2026, 11, 3)
    ).symbols == ["SSE:600000"]


@pytest.mark.parametrize(
    "value", [0, 1.0, True, "20261103", "2026-11-03T00:00:00", "2026-11-03 00:00:00", "2026-02-30"]
)
def test_minute_query_rejects_implicit_epoch_and_datetime_coercion(value):
    with pytest.raises(ValidationError):
        StockMaxMinuteVolumeQuery(symbols=["SSE:600327"], trade_date=value)


def test_minute_route_only_calls_exact_rpc_and_keeps_not_ready_date():
    app = FastAPI()
    app.include_router(router, prefix="/api")
    service = MagicMock()
    service._execute.return_value = [
        {
            "payload": {
                "trade_date": "2026-11-03",
                "status": "not_ready",
                "version": None,
                "calculation_id": None,
                "algorithm_version": None,
                "calculated_at": None,
                "published_at": None,
                "items": [
                    {
                        "symbol": "SSE:600000",
                        "max_minute_volume_shares": None,
                        "peak_at": None,
                        "expected_count": 240,
                        "observed_count": 0,
                        "quality_status": "missing",
                        "quality_reasons": ["not_ready"],
                    }
                ],
            }
        }
    ]
    app.state.query_service = service
    response = TestClient(app).post(
        "/api/v1/stock-daily-indicators/minute-volume-peak/query",
        json={"symbols": ["SSE:600000"], "trade_date": "2026-11-03"},
    )
    assert response.status_code == 200
    assert response.json()["trade_date"] == "2026-11-03"
    sql, parameters = service._execute.call_args.args
    assert "api_v1.query_stock_max_minute_volume" in str(sql)
    assert parameters == {"symbols": ["SSE:600000"], "trade_date": date(2026, 11, 3)}


def test_minute_response_uses_shanghai_wall_clock_and_preserves_real_zero():
    response = StockMaxMinuteVolumeResponse.model_validate(
        {
            "trade_date": "2026-11-03",
            "status": "partial",
            "version": 1,
            "calculation_id": "00000000-0000-0000-0000-000000000001",
            "algorithm_version": "stock_max_minute_volume.v1",
            "calculated_at": "2026-11-03T12:31:00.123Z",
            "published_at": "2026-11-03T12:31:01.123Z",
            "items": [
                {
                    "symbol": "SSE:600000",
                    "max_minute_volume_shares": 0,
                    "peak_at": "2026-11-03T01:31:00Z",
                    "expected_count": 240,
                    "observed_count": 240,
                    "quality_status": "complete",
                    "quality_reasons": [],
                }
            ],
        }
    ).model_dump(mode="json")
    assert response["calculated_at"] == "2026-11-03 20:31:00"
    assert response["published_at"] == "2026-11-03 20:31:01"
    assert response["items"][0]["peak_at"] == "2026-11-03 09:31:00"
    assert response["items"][0]["max_minute_volume_shares"] == 0
