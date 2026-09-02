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
    "query_shareholder_counts_as_of",
    "query_shareholder_count_history_as_of",
    "query_shareholder_count_history_latest",
    "query_dragon_tiger_events_by_date",
    "query_dragon_tiger_events_by_symbol",
    "query_dragon_tiger_trades_by_seat",
    "query_dragon_tiger_event_metrics",
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


def test_shareholder_count_contracts_are_strict_bounded_and_decimal_safe() -> None:
    postgrest = _load("postgrest-openapi-v1.json")
    agent = _load("agent-tools-v1.json")
    fastapi = _load("fastapi-openapi-v1.json")
    endpoints = {
        "query_shareholder_counts_as_of",
        "query_shareholder_count_history_as_of",
        "query_shareholder_count_history_latest",
    }

    request_bodies = postgrest["components"]["requestBodies"]  # type: ignore[index]
    cross_section = request_bodies["ShareholderCountsAsOf"]["content"][  # type: ignore[index]
        "application/json"
    ]["schema"]
    assert cross_section["properties"]["p_symbols"]["maxItems"] == 500

    for body_name in (
        "ShareholderCountsAsOf",
        "ShareholderCountHistoryAsOf",
        "ShareholderCountHistoryLatest",
    ):
        schema = request_bodies[body_name]["content"]["application/json"]["schema"]  # type: ignore[index]
        assert schema["properties"]["p_limit"]["maximum"] == 2000
        for name, value in schema["properties"].items():
            if name == "p_symbol":
                assert value["$ref"] == "#/components/schemas/StandardSymbol"
            if "date" in name:
                assert value["$ref"] == "#/components/schemas/Date"

    item = postgrest["components"]["schemas"]["ShareholderCountItem"]  # type: ignore[index]
    properties = item["properties"]
    for name in (
        "previous_statistics_date",
        "previous_shareholder_count",
        "change_count",
        "change_ratio",
    ):
        assert "null" in properties[name]["type"]
    assert "minimum" not in properties["change_count"]
    assert properties["change_ratio"]["type"] == ["number", "null"]

    strict_descriptions = " ".join(
        postgrest["paths"][f"/rpc/{endpoint}"]["post"]["summary"]  # type: ignore[index]
        for endpoint in endpoints
        if endpoint.endswith("as_of")
    ).lower()
    latest_description = postgrest["paths"][  # type: ignore[index]
        "/rpc/query_shareholder_count_history_latest"
    ]["post"]["summary"].lower()
    assert "strict" in strict_descriptions
    assert "current-known" in latest_description

    tools = {
        tool["endpoint"]: tool
        for tool in agent["tools"]  # type: ignore[union-attr]
        if tool["endpoint"] in endpoints
    }
    assert set(tools) == endpoints
    for endpoint, tool in tools.items():
        properties = tool["input_schema"]["properties"]
        assert properties["p_limit"]["maximum"] == 2000
        if "p_symbol" in properties:
            assert properties["p_symbol"]["pattern"] == "^(SSE|SZSE|BSE):[0-9]{6}$"
        for name, value in properties.items():
            if "date" in name:
                assert value["format"] == "date"
        expected_word = "current-known" if endpoint.endswith("latest") else "strict"
        assert expected_word in tool["description"].lower()

    fastapi_contract = dumps(fastapi)
    assert all(endpoint not in fastapi_contract for endpoint in endpoints)


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
        ).openapi()
    )


def test_fastapi_docs_use_chinese_annotations_for_owned_contracts() -> None:
    settings = ApiSettings(
        fastapi_database_url=SecretStr("unused"),
        fastapi_api_key=SecretStr("contract-api-key-0000000000000000"),
    )
    schema = create_app(
        settings=settings,
        query_service=cast(PublicQueryService, object()),
        auction_indicative_service=cast(object, object()),  # type: ignore[arg-type]
    ).openapi()

    def contains_chinese(value: object) -> bool:
        return isinstance(value, str) and any("\u4e00" <= char <= "\u9fff" for char in value)

    info = schema["info"]
    assert contains_chinese(info["title"])
    assert contains_chinese(info["summary"])
    assert contains_chinese(info["description"])

    tags = schema["tags"]
    assert tags
    assert all(contains_chinese(tag["name"]) for tag in tags)
    assert all(contains_chinese(tag["description"]) for tag in tags)

    for path_item in schema["paths"].values():
        for method, operation in path_item.items():
            if method == "parameters":
                continue
            assert contains_chinese(operation["summary"])
            assert contains_chinese(operation["description"])
            assert all(contains_chinese(tag) for tag in operation["tags"])
            assert all(
                contains_chinese(parameter.get("description"))
                for parameter in operation.get("parameters", [])
            )

    owned_schemas = {
        name: component
        for name, component in schema["components"]["schemas"].items()
        if name not in {"HTTPValidationError", "ValidationError"}
    }
    for component in owned_schemas.values():
        assert all(
            contains_chinese(property_schema.get("description"))
            for property_schema in component.get("properties", {}).values()
        )

    snapshot_properties = owned_schemas["CallAuctionMarketSnapshotItem"]["properties"]
    assert "买二" in snapshot_properties["bid2_volume"]["description"]
    assert "封单额" in snapshot_properties["seal_amount"]["description"]


def test_auction_series_item_contract_exposes_batch_and_five_levels() -> None:
    fastapi = _load("fastapi-openapi-v1.json")
    schema = fastapi["components"]["schemas"][  # type: ignore[index]
        "CallAuctionMarketSeriesSnapshotItem"
    ]
    properties = schema["properties"]

    assert properties["batch_code"]["pattern"] == "^[0-9]{6}$"
    assert properties["bid2_volume"]["anyOf"][0]["minimum"] == 0
    assert "ask5_price" in properties
    query_schema = fastapi["components"]["schemas"][  # type: ignore[index]
        "CallAuctionMarketSeriesSnapshotQuery"
    ]
    assert query_schema["properties"]["batch_code"]["anyOf"][0]["pattern"] == "^[0-9]{6}$"
    assert "batch_code" not in query_schema["required"]

    postgrest = _load("postgrest-openapi-v1.json")
    request_schema = postgrest["components"]["requestBodies"][  # type: ignore[index]
        "CallAuctionMarketSeriesSnapshots"
    ]["content"]["application/json"]["schema"]
    assert request_schema["properties"]["p_batch_code"]["pattern"] == "^[0-9]{6}$"
    assert "p_batch_code" not in request_schema["required"]

    agent = _load("agent-tools-v1.json")
    tool = next(
        item
        for item in agent["tools"]  # type: ignore[union-attr]
        if item["endpoint"] == "query_call_auction_market_series_snapshots"
    )
    assert tool["input_schema"]["properties"]["p_batch_code"]["pattern"] == "^[0-9]{6}$"


def test_market_snapshot_item_contract_exposes_five_levels_and_seal_amount() -> None:
    schema = _load("fastapi-openapi-v1.json")["components"]["schemas"][  # type: ignore[index]
        "CallAuctionMarketSnapshotItem"
    ]
    properties = schema["properties"]

    assert properties["bid2_volume"]["anyOf"][0]["minimum"] == 0
    assert "ask5_price" in properties
    assert "seal_amount" in properties


def test_latest_stock_daily_indicator_contract_is_bounded_and_decimal_safe() -> None:
    fastapi = _load("fastapi-openapi-v1.json")
    operation = fastapi["paths"]["/api/v1/stock-daily-indicators/latest/query"]["post"]
    query_schema = fastapi["components"]["schemas"]["LatestStockDailyIndicatorQuery"]
    response_schema = fastapi["components"]["schemas"]["LatestStockDailyIndicatorResponse"]
    item_schema = fastapi["components"]["schemas"]["LatestStockDailyIndicatorItem"]

    assert query_schema["properties"]["codes"]["minItems"] == 1
    assert query_schema["properties"]["codes"]["maxItems"] == 500
    assert query_schema["properties"]["codes"]["items"]["pattern"] == "^[0-9]{6}$"
    assert response_schema["properties"]["requested_count"]["maximum"] == 500
    assert response_schema["properties"]["found_count"]["maximum"] == 500
    assert item_schema["properties"]["close"]["anyOf"][0]["type"] == "string"
    assert item_schema["properties"]["total_market_value"]["anyOf"][0]["type"] == "string"
    assert {"401", "422", "503"}.issubset(operation["responses"])


def test_dragon_tiger_replaces_trading_billboard_contracts() -> None:
    postgrest = _load("postgrest-openapi-v1.json")
    agent = _load("agent-tools-v1.json")
    fastapi = _load("fastapi-openapi-v1.json")

    for endpoint in (
        "query_dragon_tiger_events_by_date",
        "query_dragon_tiger_events_by_symbol",
        "query_dragon_tiger_trades_by_seat",
        "query_dragon_tiger_event_metrics",
    ):
        assert f"/rpc/{endpoint}" in postgrest["paths"]
        assert any(tool["endpoint"] == endpoint for tool in agent["tools"])

    paths = fastapi["paths"]
    assert "/api/v1/dragon-tiger/events/by-date" in paths
    assert "/api/v1/dragon-tiger/events/by-symbol/{code}" in paths
    seat_operation = paths["/api/v1/dragon-tiger/seats/{seat_id}/trades"]["get"]
    parameters = {item["name"]: item["schema"] for item in seat_operation["parameters"]}
    assert parameters["limit"]["maximum"] == 500
    assert parameters["offset"]["maximum"] == 10000
    assert parameters["seat_id"]["format"] == "uuid"

    item = fastapi["components"]["schemas"]["DragonTigerEventItem"]
    assert item["properties"]["close_price"]["anyOf"][0]["type"] == "string"
    assert "seat_trades" in item["properties"]
    metrics = fastapi["components"]["schemas"]["DragonTigerCapitalMetricsItem"]
    assert metrics["properties"]["top5_buy_concentration"]["anyOf"][0]["type"] == "string"

    serialized = dumps([postgrest, agent, fastapi], ensure_ascii=False).lower()
    assert "query_trading_billboard_by_date" not in serialized
    assert "/api/v1/trading-billboard" not in serialized
    assert "tradingbillboarditem" not in serialized
    assert "billboard.entry" not in serialized
    assert "billboard.seat" not in serialized
    assert "payload_json" not in serialized


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
    assert response_schema["properties"]["data_origin"]["const"] == "database"
    assert response_schema["properties"]["persistence_status"]["const"] == "persisted"
    assert response_schema["properties"]["fetched_at"]["format"] == "date-time"
    assert {"404", "503"}.issubset(operation["responses"])
    assert "429" not in operation["responses"]
    assert "502" not in operation["responses"]
