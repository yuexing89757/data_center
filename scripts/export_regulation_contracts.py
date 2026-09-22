"""Synchronize the two bounded Regulation RPC contracts from the owned API models."""

from copy import deepcopy
from json import dumps, loads
from pathlib import Path

from market_data_center.public_api.models import (
    RegulationRecentNextTriggerResponse,
    RegulationTriggerResponse,
)

CONTRACT_ROOT = Path(__file__).parents[1] / "contracts"
OPERATIONS = (
    (
        "query_regulation_triggers",
        RegulationTriggerResponse,
        "Read exact-date calculated closing triggers from one published version; no date fallback.",
    ),
    (
        "query_regulation_recent_event_next_triggers",
        RegulationRecentNextTriggerResponse,
        "Read official events in exactly 30 trading sessions and next-session conditions "
        "under three "
        "index scenarios from one version. Conditions are not price predictions.",
    ),
)
REQUEST = {
    "type": "object",
    "additionalProperties": False,
    "required": ["p_trade_date"],
    "properties": {
        "p_trade_date": {
            "type": "string",
            "format": "date",
            "description": "Exact trading date, not before 2026-07-06.",
        },
        "p_cursor": {
            "type": ["string", "null"],
            "minLength": 1,
            "maxLength": 2048,
            "default": None,
            "description": "Opaque cursor bound to endpoint, date, "
            "page size and calculation version. Do not decode or modify.",
        },
        "p_limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
    },
}


def main() -> None:
    postgrest_path = CONTRACT_ROOT / "postgrest-openapi-v1.json"
    agent_path = CONTRACT_ROOT / "agent-tools-v1.json"
    postgrest = loads(postgrest_path.read_text(encoding="utf-8"))
    agent = loads(agent_path.read_text(encoding="utf-8"))
    endpoints = {rpc for rpc, _, _ in OPERATIONS}
    agent["tools"] = [tool for tool in agent["tools"] if tool["endpoint"] not in endpoints]
    for rpc, model, description in OPERATIONS:
        schema = model.model_json_schema(
            mode="serialization", ref_template="#/components/schemas/{model}"
        )
        definitions = schema.pop("$defs", {})
        definitions[model.__name__] = schema
        # PostgREST retains timestamptz semantics. Only FastAPI uses Shanghai wall-clock strings.
        for definition in definitions.values():
            for name, field in definition.get("properties", {}).items():
                if name in {"completed_at", "event_watermark", "latest_event_published_at"}:
                    field.pop("pattern", None)
                    field.pop("example", None)
                    field["type"] = "string"
                    field["format"] = "date-time"
        postgrest["components"]["schemas"].update(definitions)
        postgrest["paths"][f"/rpc/{rpc}"] = {
            "post": {
                "operationId": rpc,
                "summary": description,
                "security": [{"bearerAuth": []}],
                "description": "Requires the dedicated API role. Anonymous roles cannot execute. "
                "Decimals are strings, missing values are null. SQLSTATE 22023 means "
                "invalid parameters; P0002 means no compatible exact-date publication.",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {"schema": deepcopy(REQUEST)},
                    },
                },
                "responses": {
                    "200": {
                        "description": (
                            "One coherent published calculation with coverage "
                            "and keyset pagination."
                        ),
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": f"#/components/schemas/{model.__name__}",
                                }
                            }
                        },
                    },
                    "400": {"description": "Invalid date, bounds or cursor (22023)."},
                    "404": {"description": "No compatible published calculation (P0002)."},
                },
            }
        }
        agent["tools"].append(
            {
                "name": f"market_{rpc.removeprefix('query_')}",
                "description": description
                + " Requires the dedicated API role; anonymous access is denied.",
                "endpoint": rpc,
                "read_only": True,
                "input_schema": deepcopy(REQUEST),
            }
        )
    for path, content in ((postgrest_path, postgrest), (agent_path, agent)):
        path.write_text(dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
