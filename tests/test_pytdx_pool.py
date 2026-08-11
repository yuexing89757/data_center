import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pytest

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx_pool import (
    PytdxCapability,
    endpoints_for,
    load_endpoint_pool,
)


def _valid_document() -> dict[str, object]:
    return {
        "schema_version": "pytdx.endpoint_pool.v1",
        "refreshed_at": "2026-08-11T10:00:00+08:00",
        "nodes": [
            {
                "host": "b.example",
                "port": 7709,
                "latency_ms": 20,
                "capabilities": {
                    "quote": True,
                    "daily_bar_sse": True,
                    "daily_bar_szse": False,
                    "daily_bar_bse": False,
                },
            },
            {
                "host": "a.example",
                "port": 7709,
                "latency_ms": 10,
                "capabilities": {
                    "quote": False,
                    "daily_bar_sse": True,
                    "daily_bar_szse": True,
                    "daily_bar_bse": False,
                },
            },
        ],
    }


def _write_document(path: Path, document: object) -> None:
    path.write_text(json.dumps(document), encoding="utf-8")


def test_loads_v1_pool_and_filters_stably_by_capability(tmp_path: Path) -> None:
    path = tmp_path / "pool.json"
    _write_document(path, _valid_document())

    pool = load_endpoint_pool(path)

    assert pool.refreshed_at == datetime.fromisoformat("2026-08-11T10:00:00+08:00")
    assert tuple(node.host for node in pool.nodes) == ("a.example", "b.example")
    assert endpoints_for(pool, PytdxCapability.DAILY_BAR_SSE) == (
        ("a.example", 7709),
        ("b.example", 7709),
    )
    assert endpoints_for(pool, PytdxCapability.QUOTE) == (("b.example", 7709),)
    assert endpoints_for(pool, PytdxCapability.DAILY_BAR_BSE) == ()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda document: document.update(schema_version="unknown"),
        lambda document: document["nodes"][0].update(port=0),
        lambda document: document["nodes"][0].update(latency_ms=-1),
        lambda document: document["nodes"][0]["capabilities"].pop("quote"),
        lambda document: document["nodes"][0]["capabilities"].update(quote=1),
        lambda document: document.update(refreshed_at="2026-08-11T10:00:00"),
        lambda document: document.update(unexpected=True),
    ],
    ids=(
        "unknown-version",
        "invalid-port",
        "negative-latency",
        "missing-capability",
        "non-boolean-capability",
        "naive-refreshed-at",
        "unexpected-root-field",
    ),
)
def test_rejects_invalid_pool_documents(
    tmp_path: Path, mutate: Callable[[dict[str, object]], object]
) -> None:
    document = _valid_document()
    mutate(document)
    path = tmp_path / "pool.json"
    _write_document(path, document)

    with pytest.raises(ProviderError, match="invalid pytdx endpoint pool"):
        load_endpoint_pool(path)


def test_rejects_duplicate_endpoints(tmp_path: Path) -> None:
    document = _valid_document()
    nodes = document["nodes"]
    assert isinstance(nodes, list)
    first = nodes[0]
    assert isinstance(first, dict)
    nodes.append(first.copy())
    path = tmp_path / "pool.json"
    _write_document(path, document)

    with pytest.raises(ProviderError, match="invalid pytdx endpoint pool"):
        load_endpoint_pool(path)


def test_rejects_corrupt_or_missing_pool(tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not-json", encoding="utf-8")

    with pytest.raises(ProviderError, match="pytdx endpoint pool is unreadable"):
        load_endpoint_pool(corrupt)
    with pytest.raises(ProviderError, match="pytdx endpoint pool is unreadable"):
        load_endpoint_pool(tmp_path / "missing.json")
