"""Technical factor reads preserve exact dates, precision and bounded access."""

from datetime import date
from json import loads
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import DBAPIError
from test_regulation_public_api import HEADERS, client_for

URL = "/v1/stock-technical-factors/query"
QUERY = {"trade_date": "2026-10-09", "codes": ["000001", "999999", "000001"]}


def payload():
    return {
        "trade_date": "2026-10-09",
        "items": [
            {
                "symbol": "SZSE:000001",
                "code": "000001",
                "name": "测试证券",
                "trade_date": "2026-10-09",
                "source_code": "tushare",
                "factor_catalog_version": "stock-technical-factor.v1",
                "unadjusted": {"macd": "-0.12345678901234567890123456789", "atr": None},
                "forward_adjusted": {"macd": "0"},
                "backward_adjusted": {"macd": "999999999999999999.123456789"},
                "common": {"down_days": 0, "up_days": None},
            }
        ],
        "missing_codes": ["999999"],
    }


def test_query_is_authenticated_bounded_read_only_and_decimal_safe():
    raw = payload()
    client, connection = client_for(raw)
    response = client.post(URL, headers=HEADERS, json=QUERY)
    assert response.status_code == 200, response.text
    assert response.json() == raw
    statement, parameters = connection.execute.call_args.args
    assert str(statement).strip() == (
        "select api_v1.query_stock_technical_factors_by_codes(:codes, :trade_date) as payload"
    )
    assert parameters == {"codes": ["000001", "999999"], "trade_date": date(2026, 10, 9)}


@pytest.mark.parametrize(
    "codes", [[], ["1"], [123456], ["\uff16\uff10\uff10\uff10\uff10\uff10"], ["000001"] * 101]
)
def test_invalid_codes_and_missing_auth_do_not_query_database(codes):
    client, connection = client_for(payload())
    assert client.post(URL, json=QUERY).status_code == 401
    assert client.post(URL, headers=HEADERS, json={**QUERY, "codes": codes}).status_code == 422
    assert not connection.execute.called


@pytest.mark.parametrize(
    "query",
    [{"codes": ["000001"]}, {**QUERY, "trade_date": "2026-02-30"}, {**QUERY, "limit": 1}],
)
def test_date_required_and_unknown_parameters_rejected(query):
    client, connection = client_for(payload())
    assert client.post(URL, headers=HEADERS, json=query).status_code == 422
    assert not connection.execute.called


def test_missing_exact_date_returns_explicit_gaps():
    raw = {"trade_date": "2026-10-09", "items": [], "missing_codes": ["000001", "999999"]}
    client, _ = client_for(raw)
    assert client.post(URL, headers=HEADERS, json=QUERY).json() == raw


@pytest.mark.parametrize(
    "sqlstate,status", [("22023", 422), ("P0003", 422), ("57014", 504), ("08006", 503)]
)
def test_database_failures_are_safe(sqlstate, status):
    client, connection = client_for(payload())
    connection.execute.side_effect = DBAPIError(
        "private SQL", {}, SimpleNamespace(sqlstate=sqlstate, detail="private"), False
    )
    response = client.post(URL, headers=HEADERS, json=QUERY)
    assert response.status_code == status
    assert "private" not in response.text


def test_openapi_describes_exact_bounded_request_and_decimal_string_maps():
    client, _ = client_for(payload())
    schema = client.get("/openapi.json").json()
    operation = schema["paths"][URL]["post"]
    assert operation["security"] == [{"APIKeyHeader": []}]
    models = schema["components"]["schemas"]
    request = models["StockTechnicalFactorQuery"]
    assert set(request["required"]) == {"codes", "trade_date"}
    assert request["properties"]["codes"]["maxItems"] == 100
    item = models["StockTechnicalFactorItem"]
    for name in ("unadjusted", "forward_adjusted", "backward_adjusted"):
        values = item["properties"][name]["additionalProperties"]["anyOf"]
        assert {value["type"] for value in values} == {"string", "null"}


def test_public_rpc_contracts_are_bounded_strict_and_decimal_safe():
    root = Path(__file__).parents[1] / "contracts"
    postgrest = loads((root / "postgrest-openapi-v1.json").read_text(encoding="utf-8"))
    agent = loads((root / "agent-tools-v1.json").read_text(encoding="utf-8"))
    for suffix, parameter in (("", "p_symbols"), ("_by_codes", "p_codes")):
        endpoint = "query_stock_technical_factors" + suffix
        tool = next(tool for tool in agent["tools"] if tool["endpoint"] == endpoint)
        schema = tool["input_schema"]
        assert tool["read_only"] is True
        assert schema["additionalProperties"] is False
        assert set(schema["required"]) == {parameter, "p_trade_date"}
        assert schema["properties"][parameter]["minItems"] == 1
        assert schema["properties"][parameter]["maxItems"] == 100
        assert schema["properties"]["p_trade_date"]["format"] == "date"
        operation = postgrest["paths"]["/rpc/" + endpoint]["post"]
        body_name = operation["requestBody"]["$ref"].rsplit("/", 1)[1]
        request = postgrest["components"]["requestBodies"][body_name]
        assert request["content"]["application/json"]["schema"] == schema
        response_ref = operation["responses"]["200"]["content"]["application/json"]["schema"]
        response_name = response_ref["$ref"].rsplit("/", 1)[1]
        response = postgrest["components"]["schemas"][response_name]
        assert response["properties"]["items"]["maxItems"] == 100
        assert ("missing_codes" if suffix else "missing_symbols") in response["required"]
    item = postgrest["components"]["schemas"]["StockTechnicalFactorItem"]
    for name in ("unadjusted", "forward_adjusted", "backward_adjusted"):
        assert item["properties"][name]["additionalProperties"]["type"] == ["string", "null"]
    assert item["properties"]["common"]["additionalProperties"]["type"] == ["integer", "null"]
    assert "ingestion_id" not in item["properties"]
