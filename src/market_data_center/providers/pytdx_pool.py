"""Strict local contract for the shared PYTDX endpoint pool."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType

from market_data_center.providers.contracts import ProviderError

POOL_SCHEMA_VERSION = "pytdx.endpoint_pool.v1"


class PytdxCapability(StrEnum):
    QUOTE = "quote"
    DAILY_BAR_SSE = "daily_bar_sse"
    DAILY_BAR_SZSE = "daily_bar_szse"
    DAILY_BAR_BSE = "daily_bar_bse"


@dataclass(frozen=True, slots=True)
class PytdxPoolNode:
    host: str
    port: int
    latency_ms: int
    capabilities: Mapping[PytdxCapability, bool]


@dataclass(frozen=True, slots=True)
class PytdxEndpointPool:
    refreshed_at: datetime
    nodes: tuple[PytdxPoolNode, ...]


def load_endpoint_pool(path: Path) -> PytdxEndpointPool:
    """Read and strictly validate a versioned endpoint-pool document."""
    try:
        document: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProviderError("pytdx endpoint pool is unreadable") from error
    try:
        return _parse_endpoint_pool(document)
    except (TypeError, ValueError, KeyError) as error:
        raise ProviderError("invalid pytdx endpoint pool") from error


def endpoints_for(
    pool: PytdxEndpointPool, capability: PytdxCapability
) -> tuple[tuple[str, int], ...]:
    """Return stable endpoints that explicitly advertise one capability."""
    return tuple(
        (node.host, node.port)
        for node in pool.nodes
        if node.capabilities[capability]
    )


def _parse_endpoint_pool(document: object) -> PytdxEndpointPool:
    root = _string_object(document)
    _require_fields(root, {"schema_version", "refreshed_at", "nodes"})
    if root["schema_version"] != POOL_SCHEMA_VERSION:
        raise ValueError("unknown schema version")
    refreshed_at = _aware_datetime(root["refreshed_at"])
    raw_nodes = root["nodes"]
    if not isinstance(raw_nodes, list):
        raise TypeError("nodes must be a list")
    nodes = tuple(_parse_node(raw_node) for raw_node in raw_nodes)
    endpoints = {(node.host, node.port) for node in nodes}
    if len(endpoints) != len(nodes):
        raise ValueError("duplicate endpoint")
    return PytdxEndpointPool(
        refreshed_at,
        tuple(sorted(nodes, key=lambda node: (node.latency_ms, node.host, node.port))),
    )


def _parse_node(raw_node: object) -> PytdxPoolNode:
    node = _string_object(raw_node)
    _require_fields(node, {"host", "port", "latency_ms", "capabilities"})
    host = node["host"]
    port = node["port"]
    latency_ms = node["latency_ms"]
    if not isinstance(host, str) or not host or host != host.strip():
        raise ValueError("invalid host")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65_535:
        raise ValueError("invalid port")
    if isinstance(latency_ms, bool) or not isinstance(latency_ms, int) or latency_ms < 0:
        raise ValueError("invalid latency")
    raw_capabilities = _string_object(node["capabilities"])
    expected_capabilities = {capability.value for capability in PytdxCapability}
    _require_fields(raw_capabilities, expected_capabilities)
    capabilities: dict[PytdxCapability, bool] = {}
    for capability in PytdxCapability:
        enabled = raw_capabilities[capability.value]
        if not isinstance(enabled, bool):
            raise TypeError("capability must be boolean")
        capabilities[capability] = enabled
    return PytdxPoolNode(
        host,
        port,
        latency_ms,
        MappingProxyType(capabilities),
    )


def _string_object(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise TypeError("expected an object with string keys")
    return value


def _require_fields(value: Mapping[str, object], expected: set[str]) -> None:
    if set(value) != expected:
        raise ValueError("object fields do not match the schema")


def _aware_datetime(value: object) -> datetime:
    if not isinstance(value, str):
        raise TypeError("refreshed_at must be a string")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("refreshed_at must include a timezone")
    return parsed
