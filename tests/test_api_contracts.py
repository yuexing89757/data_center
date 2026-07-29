from json import dumps, loads
from pathlib import Path

CONTRACT_ROOT = Path(__file__).parents[1] / "contracts"
EXPECTED_ENDPOINTS = {
    "query_securities",
    "query_daily_bars",
    "query_adjusted_daily_bars",
    "query_market_snapshot",
    "query_classification_members_as_of",
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
