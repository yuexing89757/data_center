"""Public Regulation reads: precision, isolation and the approved error contract."""

from copy import deepcopy
from datetime import date
from decimal import Decimal
from json import loads
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError
from sqlalchemy.exc import DBAPIError

from market_data_center.public_api import create_app, models
from market_data_center.public_api.queries import PostgreSQLPublicQueryService
from market_data_center.settings import ApiSettings

KEY = "regulation-test-api-key-000000000000"
HEADERS = {"X-API-Key": KEY}
ENDPOINTS = [
    ("triggers", "query_regulation_triggers", "RegulationTriggerResponse"),
    (
        "recent-events/next-triggers",
        "query_regulation_recent_event_next_triggers",
        "RegulationRecentNextTriggerResponse",
    ),
]


def regulation_payload(recent: bool = False) -> dict[str, Any]:
    stock = {
        "code": "000001",
        "symbol": "SZSE:000001",
        "name": "测试证券",
        "exchange": "SZSE",
        "segment": "SZSE_MAIN",
    }
    payload = {
        "trade_date": "2026-09-18",
        "calculation_id": "00000000-0000-0000-0000-000000000123",
        "calculation_status": "PARTIAL",
        "completed_at": "2026-09-18T14:05:00.123+00:00",
        "event_watermark": "2026-09-18T13:00:00+00:00",
        "algorithm_version": "regulation-core.v1",
        "rule_set_version": "cn-a-share-regulation-2026-07-06.v1",
        "coverage": {
            "expected_count": 3,
            "complete_count": 1,
            "incomplete_count": 1,
            "not_applicable_count": 1,
        },
        "returned_count": 1,
        "has_more": False,
        "next_cursor": None,
        "items": [stock],
    }
    if not recent:
        stock.update(
            {
                "calculated_state": "ABNORMAL_TRIGGERED",
                "announced_state": "NONE",
                "triggered_rules": [
                    {
                        "rule_code": "SZSE_MAIN_ABNORMAL_3D_DEV_UP",
                        "level": "ABNORMAL",
                        "direction": "UP",
                        "kind": "CUMULATIVE_DEVIATION",
                        "window_start_date": "2026-09-16",
                        "window_end_date": "2026-09-18",
                        "observed_window_days": 3,
                        "current_value": "20.12345678",
                        "threshold": "20.00000000",
                        "secondary_current_value": None,
                        "secondary_threshold": None,
                        "event_count": None,
                        "required_count": None,
                        "selected_reset_date": None,
                    }
                ],
            }
        )
    else:
        payload.update(
            {
                "next_trade_date": "2026-09-21",
                "lookback_trading_days": 30,
                "lookback_start_date": "2026-08-10",
            }
        )
        stock.update(
            {
                "official_event_count_30d": 1,
                "latest_source_event_id": "official-1",
                "latest_event_date": "2026-09-17",
                "latest_event_published_at": "2026-09-17T10:30:00+00:00",
                "latest_event_level": "ABNORMAL",
                "latest_event_direction": None,
                "latest_event_source_title": "交易异常波动公告",
                "latest_event_source_url": "https://www.szse.cn/example.html",
                "next_triggers": [
                    {
                        "rule_code": "SZSE_MAIN_ABNORMAL_3D_DEV_UP",
                        "level": "ABNORMAL",
                        "direction": "UP",
                        "benchmark_symbol": "SZSE:399107",
                        "scenario_code": "INDEX_FLAT",
                        "scenario_index_pct": "0.00000000",
                        "next_day_reference_price": "10.00000000",
                        "raw_trigger_price": "10.64800000",
                        "trigger_price": "10.65000000",
                        "trigger_change_pct": "6.50000000",
                        "lower_limit_price": "9.00000000",
                        "upper_limit_price": "11.00000000",
                        "reachability": "REACHABLE_NEXT_SESSION",
                        "window_start_date": "2026-09-17",
                        "window_end_date": "2026-09-21",
                        "requires_official_event_confirmation": False,
                    }
                ],
            }
        )
    return payload


def client_for(payload: dict[str, Any]) -> tuple[TestClient, MagicMock]:
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.mappings.return_value.all.return_value = [{"payload": payload}]
    app = create_app(
        settings=ApiSettings(
            _env_file=None,
            fastapi_api_key=SecretStr(KEY),
            fastapi_database_url=SecretStr("unused"),
        ),
        query_service=PostgreSQLPublicQueryService(engine),
        auction_indicative_service=object(),  # type: ignore[arg-type]
        tencent_quote_live_service=object(),  # type: ignore[arg-type]
    )
    return TestClient(app), connection


@pytest.mark.parametrize("path,rpc,model_name", ENDPOINTS)
def test_regulation_routes_read_only_rpc_and_preserve_precision(path, rpc, model_name):
    payload = regulation_payload(path != "triggers")
    client, connection = client_for(payload)
    response = client.get(
        f"/api/v1/regulation/{path}",
        params={"trade_date": "2026-09-18", "limit": 200, "cursor": "test-cursor"},
        headers=HEADERS,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["calculation_status"] == "PARTIAL"
    assert body["coverage"]["incomplete_count"] == 1
    assert body["completed_at"] == "2026-09-18 22:05:00"
    assert body["event_watermark"] == "2026-09-18 21:00:00"
    assert body["items"][0]["code"] == "000001"
    calls = connection.execute.call_args_list
    assert len(calls) == 2  # transaction-local timeout, then one bounded read
    assert calls[0].args[1] == {"statement_timeout": "5000ms"}
    assert f"api_v1.{rpc}(" in str(calls[1].args[0])
    assert calls[1].args[1] == {
        "trade_date": date(2026, 9, 18),
        "cursor": "test-cursor",
        "limit": 200,
    }
    result = getattr(models, model_name).model_validate(payload)
    if path == "triggers":
        assert body["items"][0]["triggered_rules"][0]["current_value"] == "20.12345678"
        assert result.items[0].triggered_rules[0].threshold == Decimal("20")
    else:
        assert body["items"][0]["latest_event_published_at"] == "2026-09-17 18:30:00"
        assert result.items[0].next_triggers[0].trigger_change_pct == Decimal("6.50")
        assert body["items"][0]["latest_event_direction"] is None


@pytest.mark.parametrize("path,_,__", ENDPOINTS)
def test_regulation_auth_validation_and_defaults(path, _, __):
    client, connection = client_for(regulation_payload(path != "triggers"))
    url = f"/api/v1/regulation/{path}"
    assert client.get(url, params={"trade_date": "2026-09-18"}).status_code == 401
    for params in (
        {},
        {"trade_date": "bad"},
        {"trade_date": "2026-07-05"},
        {"trade_date": "2026-09-18", "limit": 0},
        {"trade_date": "2026-09-18", "limit": 501},
        {"trade_date": "2026-09-18", "cursor": ""},
        {"trade_date": "2026-09-18", "cursor": "x" * 2049},
    ):
        assert client.get(url, params=params, headers=HEADERS).status_code == 422
    assert connection.execute.call_count == 0
    assert client.get(url, params={"trade_date": "2026-09-18"}, headers=HEADERS).status_code == 200
    assert connection.execute.call_args.args[1]["limit"] == 100
    assert connection.execute.call_args.args[1]["cursor"] is None


@pytest.mark.parametrize("path,_,__", ENDPOINTS)
@pytest.mark.parametrize(
    "sqlstate,status", [("22023", 422), ("P0002", 404), ("57014", 503), ("08006", 503)]
)
def test_regulation_safe_database_errors(path, _, __, sqlstate, status):
    client, connection = client_for(regulation_payload(path != "triggers"))
    connection.execute.side_effect = DBAPIError(
        "secret SQL",
        {},
        SimpleNamespace(sqlstate=sqlstate, detail="secret"),
        False,
    )
    response = client.get(
        f"/api/v1/regulation/{path}",
        params={"trade_date": "2026-09-18"},
        headers=HEADERS,
    )
    assert response.status_code == status
    assert "secret" not in response.text
    assert connection.execute.call_count > 0
    assert response.json()["error"]["code"] != "http_error" or status == 422


@pytest.mark.parametrize(
    "scenario,reachability",
    [
        ("CURRENT", "REACHABLE_NEXT_SESSION"),
        ("NONE", "CURRENTLY_TRIGGERED"),
        ("INDEX_FLAT", "NOT_PRICE_CALCULABLE"),
    ],
)
def test_regulation_models_reject_impossible_scenario_reachability(scenario, reachability):
    model = getattr(models, "RegulationRecentNextTriggerResponse", None)
    assert model is not None
    payload = deepcopy(regulation_payload(True))
    item = payload["items"][0]["next_triggers"][0]
    item.update(scenario_code=scenario, reachability=reachability)
    with pytest.raises(ValidationError):
        model.model_validate(payload)


@pytest.mark.parametrize(
    "scenario,state", [("CURRENT", "CURRENTLY_TRIGGERED"), ("NONE", "NOT_PRICE_CALCULABLE")]
)
def test_regulation_nonprice_states_preserve_nulls(scenario, state):
    model = getattr(models, "RegulationRecentNextTriggerResponse", None)
    assert model is not None
    payload = regulation_payload(True)
    item = payload["items"][0]["next_triggers"][0]
    item.update(
        scenario_code=scenario,
        reachability=state,
        scenario_index_pct=None,
        trigger_price=None,
        trigger_change_pct=None,
        raw_trigger_price=None,
    )
    result = model.model_validate(payload)
    assert result.items[0].next_triggers[0].trigger_price is None


def test_regulation_checked_contracts_are_bounded_and_datetime_formats_differ():
    root = Path(__file__).parents[1] / "contracts"
    fastapi = loads((root / "fastapi-openapi-v1.json").read_text(encoding="utf-8"))
    postgrest = loads((root / "postgrest-openapi-v1.json").read_text(encoding="utf-8"))
    agent = loads((root / "agent-tools-v1.json").read_text(encoding="utf-8"))
    tools = {item["endpoint"]: item for item in agent["tools"]}
    for path, rpc, name in ENDPOINTS:
        assert f"/api/v1/regulation/{path}" in fastapi["paths"]
        operation = postgrest["paths"][f"/rpc/{rpc}"]["post"]
        request = operation["requestBody"]["content"]["application/json"]["schema"]
        assert request["required"] == ["p_trade_date"]
        assert request["properties"]["p_limit"]["maximum"] == 500
        assert request["properties"]["p_cursor"]["maxLength"] == 2048
        assert tools[rpc]["input_schema"] == request
        assert tools[rpc]["read_only"] is True
        timestamp = fastapi["components"]["schemas"][name]["properties"]["completed_at"]
        assert timestamp["example"] == "2026-09-18 09:35:00"
        assert (
            postgrest["components"]["schemas"][name]["properties"]["completed_at"]["format"]
            == "date-time"
        )
    numeric = postgrest["components"]["schemas"]["RegulationTriggeredRuleItem"]["properties"][
        "current_value"
    ]
    assert numeric["anyOf"][0]["type"] == "string"
