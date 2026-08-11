"""Strict local contract for the shared PYTDX endpoint pool."""

import json
import os
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import perf_counter
from types import MappingProxyType
from typing import Protocol

from market_data_center.providers.contracts import ProviderError

POOL_SCHEMA_VERSION = "pytdx.endpoint_pool.v1"
PROBE_MAX_WORKERS = 16
PROBE_TIMEOUT_SECONDS = 4.0
PROBE_OVERALL_TIMEOUT_SECONDS = 180.0


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


@dataclass(frozen=True, slots=True)
class PytdxProbeResult:
    host: str
    port: int
    latency_ms: int
    capabilities: Mapping[PytdxCapability, bool]


@dataclass(frozen=True, slots=True)
class PytdxPoolRefreshResult:
    candidate_count: int
    usable_node_count: int
    rejected_node_count: int
    published: bool
    used_last_good: bool
    pool: PytdxEndpointPool


class PytdxEndpointProbe(Protocol):
    def probe(self, host: str, port: int) -> PytdxProbeResult | None:
        """Return measured capabilities, or None when the node is unusable."""


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


def refresh_endpoint_pool(
    path: Path,
    *,
    candidates: Sequence[tuple[str, int]] | None = None,
    probe: PytdxEndpointProbe | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PytdxPoolRefreshResult:
    """Probe candidates and atomically publish a valid pool or use last-good."""
    old_pool = _load_last_good(path)
    candidate_endpoints = _normalize_candidates(
        default_pytdx_candidates() if candidates is None else candidates
    )
    active_probe = probe or _NetworkPytdxEndpointProbe()
    nodes = _probe_candidates(candidate_endpoints, active_probe)
    refreshed_at = clock()
    if refreshed_at.tzinfo is None or refreshed_at.utcoffset() is None:
        raise ProviderError("pytdx endpoint pool refresh clock must be timezone-aware")
    new_pool = PytdxEndpointPool(
        refreshed_at,
        tuple(sorted(nodes, key=lambda node: (node.latency_ms, node.host, node.port))),
    )
    published = False
    if _is_publishable(new_pool):
        try:
            _atomic_write_pool(path, new_pool)
            new_pool = load_endpoint_pool(path)
            published = True
        except (OSError, ProviderError):
            published = False
    if published:
        return PytdxPoolRefreshResult(
            len(candidate_endpoints),
            len(nodes),
            len(candidate_endpoints) - len(nodes),
            True,
            False,
            new_pool,
        )
    if old_pool is None:
        raise ProviderError("no usable pytdx endpoint pool")
    return PytdxPoolRefreshResult(
        len(candidate_endpoints),
        len(nodes),
        len(candidate_endpoints) - len(nodes),
        False,
        True,
        old_pool,
    )


def default_pytdx_candidates() -> tuple[tuple[str, int], ...]:
    """Return the deduplicated network candidates bundled with pytdx."""
    from pytdx.config.hosts import hq_hosts  # type: ignore[import-untyped]

    candidates: list[tuple[str, int]] = []
    for entry in hq_hosts:
        if not isinstance(entry, (list, tuple)) or len(entry) < 3:
            continue
        host = entry[1]
        port = entry[2]
        if isinstance(host, str) and isinstance(port, int):
            candidates.append((host, port))
    return _normalize_candidates(candidates)


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


def _normalize_candidates(
    candidates: Sequence[tuple[str, int]],
) -> tuple[tuple[str, int], ...]:
    normalized: list[tuple[str, int]] = []
    for host, port in candidates:
        if not host or host != host.strip() or not 1 <= port <= 65_535:
            continue
        endpoint = (host, port)
        if endpoint not in normalized:
            normalized.append(endpoint)
    return tuple(normalized)


def _load_last_good(path: Path) -> PytdxEndpointPool | None:
    try:
        pool = load_endpoint_pool(path)
    except ProviderError:
        return None
    return pool if _is_publishable(pool) else None


def _probe_candidates(
    candidates: tuple[tuple[str, int], ...], probe: PytdxEndpointProbe
) -> tuple[PytdxPoolNode, ...]:
    if not candidates:
        return ()
    executor = ThreadPoolExecutor(max_workers=min(PROBE_MAX_WORKERS, len(candidates)))
    futures = {
        executor.submit(_safe_probe, probe, host, port): (host, port)
        for host, port in candidates
    }
    try:
        completed, pending = wait(futures, timeout=PROBE_OVERALL_TIMEOUT_SECONDS)
        for future in pending:
            future.cancel()
        nodes = tuple(
            node
            for future in completed
            if (node := future.result()) is not None
        )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return nodes


def _safe_probe(
    probe: PytdxEndpointProbe, host: str, port: int
) -> PytdxPoolNode | None:
    try:
        result = probe.probe(host, port)
        if result is None or (result.host, result.port) != (host, port):
            return None
        if set(result.capabilities) != set(PytdxCapability):
            return None
        if any(not isinstance(enabled, bool) for enabled in result.capabilities.values()):
            return None
        if not any(result.capabilities.values()):
            return None
        if result.latency_ms < 0:
            return None
        return PytdxPoolNode(
            host,
            port,
            result.latency_ms,
            MappingProxyType(dict(result.capabilities)),
        )
    except Exception:
        return None


def _is_publishable(pool: PytdxEndpointPool) -> bool:
    required = (
        PytdxCapability.QUOTE,
        PytdxCapability.DAILY_BAR_SSE,
        PytdxCapability.DAILY_BAR_SZSE,
    )
    return all(endpoints_for(pool, capability) for capability in required)


def _atomic_write_pool(path: Path, pool: PytdxEndpointPool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(_pool_document(pool), temporary, ensure_ascii=False, indent=2)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _pool_document(pool: PytdxEndpointPool) -> dict[str, object]:
    return {
        "schema_version": POOL_SCHEMA_VERSION,
        "refreshed_at": pool.refreshed_at.isoformat(),
        "nodes": [
            {
                "host": node.host,
                "port": node.port,
                "latency_ms": node.latency_ms,
                "capabilities": {
                    capability.value: node.capabilities[capability]
                    for capability in PytdxCapability
                },
            }
            for node in pool.nodes
        ],
    }


class _NetworkPytdxEndpointProbe:
    def probe(self, host: str, port: int) -> PytdxProbeResult | None:
        from pytdx.hq import TdxHq_API  # type: ignore[import-untyped]

        api = TdxHq_API(heartbeat=False, auto_retry=False, raise_exception=True)
        started = perf_counter()
        try:
            if not api.connect(host, port, time_out=PROBE_TIMEOUT_SECONDS):
                return None
            latency_ms = max(0, round((perf_counter() - started) * 1000))
            capabilities = MappingProxyType(
                {
                    PytdxCapability.QUOTE: _probe_call(
                        lambda: api.get_security_quotes([(1, "600000")])
                    ),
                    PytdxCapability.DAILY_BAR_SSE: _probe_call(
                        lambda: api.get_security_bars(9, 1, "600000", 0, 1)
                    ),
                    PytdxCapability.DAILY_BAR_SZSE: _probe_call(
                        lambda: api.get_security_bars(9, 0, "000001", 0, 1)
                    ),
                    PytdxCapability.DAILY_BAR_BSE: _probe_call(
                        lambda: api.get_security_bars(9, 0, "920000", 0, 1)
                    ),
                }
            )
            return PytdxProbeResult(host, port, latency_ms, capabilities)
        finally:
            with suppress(Exception):
                api.disconnect()


def _probe_call(operation: Callable[[], object]) -> bool:
    try:
        response = operation()
    except Exception:
        return False
    return isinstance(response, list) and bool(response)
