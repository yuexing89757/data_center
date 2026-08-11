import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType

import pytest

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx_pool import (
    PytdxCapability,
    PytdxPoolRefreshResult,
    PytdxProbeResult,
    endpoints_for,
    load_endpoint_pool,
    refresh_endpoint_pool,
)

AWARE_NOW = datetime(2026, 8, 11, 2, tzinfo=UTC)


class FakeProbe:
    def __init__(
        self,
        results: dict[tuple[str, int], PytdxProbeResult | BaseException | None],
    ) -> None:
        self._results = results

    def probe(self, host: str, port: int) -> PytdxProbeResult | None:
        result = self._results.get((host, port))
        if isinstance(result, BaseException):
            raise result
        return result


def _capabilities(
    *, quote: bool = False, sse: bool = False, szse: bool = False, bse: bool = False
) -> MappingProxyType[PytdxCapability, bool]:
    return MappingProxyType(
        {
            PytdxCapability.QUOTE: quote,
            PytdxCapability.DAILY_BAR_SSE: sse,
            PytdxCapability.DAILY_BAR_SZSE: szse,
            PytdxCapability.DAILY_BAR_BSE: bse,
        }
    )


def _probe_result(
    host: str,
    latency_ms: int,
    *,
    quote: bool = False,
    sse: bool = False,
    szse: bool = False,
    bse: bool = False,
) -> PytdxProbeResult:
    return PytdxProbeResult(
        host,
        7709,
        latency_ms,
        _capabilities(quote=quote, sse=sse, szse=szse, bse=bse),
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


def test_refresh_publishes_a_complete_pool_atomically(tmp_path: Path) -> None:
    target = tmp_path / "pytdx_pool.json"
    probe = FakeProbe(
        {
            ("slow", 7709): _probe_result("slow", 20, quote=True),
            ("fast", 7709): _probe_result(
                "fast", 10, quote=True, sse=True, szse=True
            ),
        }
    )

    result = refresh_endpoint_pool(
        target,
        candidates=(("slow", 7709), ("fast", 7709)),
        probe=probe,
        clock=lambda: AWARE_NOW,
    )

    assert result == PytdxPoolRefreshResult(
        candidate_count=2,
        usable_node_count=2,
        rejected_node_count=0,
        published=True,
        used_last_good=False,
        pool=load_endpoint_pool(target),
    )
    assert tuple(node.host for node in result.pool.nodes) == ("fast", "slow")
    assert list(tmp_path.glob("*.tmp")) == []


def test_refresh_preserves_last_good_when_new_pool_fails_gate(tmp_path: Path) -> None:
    target = tmp_path / "pytdx_pool.json"
    _write_document(target, _valid_document())
    before = target.read_bytes()
    probe = FakeProbe(
        {
            ("incomplete", 7709): _probe_result(
                "incomplete", 5, quote=True, sse=True
            )
        }
    )

    result = refresh_endpoint_pool(
        target,
        candidates=(("incomplete", 7709),),
        probe=probe,
        clock=lambda: AWARE_NOW,
    )

    assert result.published is False
    assert result.used_last_good is True
    assert result.usable_node_count == 1
    assert target.read_bytes() == before
    assert tuple(node.host for node in result.pool.nodes) == ("a.example", "b.example")


def test_refresh_fails_when_no_new_or_last_good_pool_exists(tmp_path: Path) -> None:
    target = tmp_path / "missing.json"
    probe = FakeProbe({("dead", 7709): RuntimeError("token=secret")})

    with pytest.raises(ProviderError, match="no usable pytdx endpoint pool"):
        refresh_endpoint_pool(
            target,
            candidates=(("dead", 7709),),
            probe=probe,
            clock=lambda: AWARE_NOW,
        )

    assert not target.exists()


def test_refresh_does_not_require_bse_capability(tmp_path: Path) -> None:
    target = tmp_path / "pytdx_pool.json"
    probe = FakeProbe(
        {
            ("core", 7709): _probe_result(
                "core", 5, quote=True, sse=True, szse=True, bse=False
            )
        }
    )

    result = refresh_endpoint_pool(
        target,
        candidates=(("core", 7709),),
        probe=probe,
        clock=lambda: AWARE_NOW,
    )

    assert result.published is True
    assert endpoints_for(result.pool, PytdxCapability.DAILY_BAR_BSE) == ()
