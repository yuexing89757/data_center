from json import dumps, loads
from pathlib import Path
from typing import cast

from pydantic import SecretStr

from market_data_center.public_api import create_app
from market_data_center.public_api.queries import PublicQueryService
from market_data_center.settings import ApiSettings

CONTRACT_ROOT = Path(__file__).parents[1] / "contracts"
EXPECTED_ENDPOINTS = {
    "query_securities",
    "query_daily_bars",
    "query_adjusted_daily_bars",
    "query_market_snapshot",
    "query_classification_members_as_of",
    "query_deducted_profits_as_of",
    "query_stock_pool_snapshot",
    "query_auction_quotes",
    "query_call_auction_market_snapshots",
    "query_call_auction_market_series_snapshots",
    "query_board_index_bias_latest",
    "query_close_price_new_highs_120d",
}


def _load(name: str) -> dict[str, object]:
    value = loads((CONTRACT_ROOT / name).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_openapi_and_agent_tools_expose_the_same_rpc_set() -> None:
    openapi = _load("postgrest-openapi-v1.json")
    agent = _load("agent-tools-v1.json")
    paths = openapi["paths"]
    tools = agent["tools"]
    assert isinstance(paths, dict)
    assert isinstance(tools, list)

    openapi_endpoints = {path.removeprefix("/rpc/") for path in paths}
    tool_endpoints = {tool["endpoint"] for tool in tools if isinstance(tool, dict)}

    assert openapi["openapi"] == "3.1.0"
    assert openapi_endpoints == EXPECTED_ENDPOINTS
    assert tool_endpoints == EXPECTED_ENDPOINTS


def test_agent_tools_are_bounded_strict_and_read_only() -> None:
    agent = _load("agent-tools-v1.json")
    transport = agent["transport"]
    tools = agent["tools"]
    assert isinstance(transport, dict) and transport["read_only"] is True
    assert isinstance(tools, list)

    for tool in tools:
        assert isinstance(tool, dict)
        assert tool["read_only"] is True
        schema = tool["input_schema"]
        assert isinstance(schema, dict)
        assert schema["additionalProperties"] is False
        properties = schema["properties"]
        assert isinstance(properties, dict)
        if "p_limit" in properties:
            limit = properties["p_limit"]
            assert isinstance(limit, dict)
            assert int(limit["maximum"]) <= 5000


def test_public_contracts_do_not_name_internal_schemas_or_contain_secrets() -> None:
    contracts = dumps(
        [
            _load("postgrest-openapi-v1.json"),
            _load("agent-tools-v1.json"),
            _load("fastapi-openapi-v1.json"),
        ],
        ensure_ascii=False,
    ).lower()

    assert all(
        internal not in contracts
        for internal in (
            "core.",
            "derived.",
            "metrics.",
            "classification.",
            "ingestion.",
            "audit.",
        )
    )
    assert all(secret not in contracts for secret in ("password", "secret key", "jwt secret"))


def test_fastapi_openapi_contract_matches_the_application() -> None:
    settings = ApiSettings(
        fastapi_database_url=SecretStr("unused"),
        fastapi_api_key=SecretStr("contract-api-key-0000000000000000"),
    )

    assert (
        _load("fastapi-openapi-v1.json")
        == create_app(
            settings=settings,
            query_service=cast(PublicQueryService, object()),
            auction_indicative_service=cast(object, object()),  # type: ignore[arg-type]
            board_index_bias_live_service=cast(object, object()),  # type: ignore[arg-type]
        ).openapi()
    )


def test_auction_series_item_contract_exposes_batch_and_five_levels() -> None:
    schema = _load("fastapi-openapi-v1.json")["components"]["schemas"][  # type: ignore[index]
        "CallAuctionMarketSeriesSnapshotItem"
    ]
    properties = schema["properties"]

    assert properties["batch_code"]["pattern"] == "^[0-9]{6}$"
    assert properties["bid2_volume"]["anyOf"][0]["minimum"] == 0
    assert "ask5_price" in properties


def test_close_price_new_highs_contract_is_no_input_strict_and_bounded() -> None:
    postgrest = _load("postgrest-openapi-v1.json")
    agent = _load("agent-tools-v1.json")
    fastapi = _load("fastapi-openapi-v1.json")

    postgrest_operation = postgrest["paths"]["/rpc/query_close_price_new_highs_120d"]["post"]
    assert "requestBody" not in postgrest_operation

    agent_tool = next(
        tool for tool in agent["tools"] if tool["endpoint"] == "query_close_price_new_highs_120d"
    )
    assert agent_tool["read_only"] is True
    assert agent_tool["input_schema"] == {
        "type": "object",
        "additionalProperties": False,
        "properties": {},
    }

    operation = fastapi["paths"]["/api/v1/close-price-new-highs-120d"]["get"]
    assert operation.get("parameters", []) == []
    response_schema = fastapi["components"]["schemas"]["ClosePriceNewHighs120dResponse"]
    assert response_schema["properties"]["window_trading_session_count"]["const"] == 120
    assert response_schema["properties"]["comparison_session_count"]["const"] == 119
    assert response_schema["properties"]["returned_count"]["maximum"] == 10000
    item_schema = fastapi["components"]["schemas"]["ClosePriceNewHigh120dItem"]
    assert item_schema["properties"]["code"]["pattern"] == "^[0-9]{6}$"


def test_board_index_bias_contract_is_fixed_bounded_and_no_input() -> None:
    postgrest = _load("postgrest-openapi-v1.json")
    agent = _load("agent-tools-v1.json")
    fastapi = _load("fastapi-openapi-v1.json")

    postgrest_operation = postgrest["paths"]["/rpc/query_board_index_bias_latest"]["post"]
    assert "requestBody" not in postgrest_operation
    assert postgrest_operation["responses"]["404"]["description"]

    agent_tool = next(
        tool for tool in agent["tools"] if tool["endpoint"] == "query_board_index_bias_latest"
    )
    assert agent_tool["read_only"] is True
    assert agent_tool["input_schema"] == {
        "type": "object",
        "additionalProperties": False,
        "properties": {},
    }

    operation = fastapi["paths"]["/api/v1/board-indexes/883423/bias"]["get"]
    assert operation.get("parameters", []) == []
    response_schema = fastapi["components"]["schemas"]["BoardIndexBiasResponse"]
    assert response_schema["properties"]["algorithm_version"]["const"] == ("board_index_bias_v1")
    assert response_schema["properties"]["window_trading_days"]["const"] == 30
    assert response_schema["properties"]["close"]["type"] == "string"
    assert response_schema["properties"]["data_origin"]["enum"] == ["database", "ths_live"]
    assert response_schema["properties"]["persistence_status"]["enum"] == [
        "persisted",
        "queued",
    ]
    assert response_schema["properties"]["fetched_at"]["format"] == "date-time"
    assert {"429", "502", "503"}.issubset(operation["responses"])
