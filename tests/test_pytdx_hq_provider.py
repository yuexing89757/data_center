import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import TracebackType

import pytest

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx_hq import PytdxHqProvider
from market_data_center.settings import PytdxHqSettings, PytdxPoolSettings


def _node(
    host: str,
    port: int = 7709,
    *,
    quote: bool = False,
    sse: bool = False,
    szse: bool = False,
) -> dict[str, object]:
    return {
        "host": host,
        "port": port,
        "latency_ms": 10,
        "capabilities": {
            "quote": quote,
            "daily_bar_sse": sse,
            "daily_bar_szse": szse,
            "daily_bar_bse": False,
        },
    }


def _write_pool(tmp_path: Path, nodes: list[dict[str, object]]) -> Path:
    pool = tmp_path / "pytdx_pool.json"
    pool.write_text(
        json.dumps(
            {
                "schema_version": "pytdx.endpoint_pool.v1",
                "refreshed_at": "2026-08-11T10:00:00+08:00",
                "nodes": nodes,
            }
        ),
        encoding="utf-8",
    )
    return pool


def _symbols(count: int) -> tuple[str, ...]:
    return tuple(f"SSE:{600000 + index:06d}" for index in range(count))


class RecordedClient:
    def __enter__(self) -> "RecordedClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def fetch(self, requests: Sequence[tuple[int, str]]) -> Sequence[Mapping[str, object]]:
        return [self._row(*requests[0])]

    def _row(self, market: int, code: str) -> Mapping[str, object]:
        row: dict[str, object] = {
            "market": market,
            "code": code,
            "price": Decimal("10.00"),
            "last_close": Decimal("9.50"),
            "open": Decimal("10.00"),
            "high": Decimal("10.00"),
            "low": Decimal("10.00"),
            "server_time_raw": "91500",
            "volume_lots": 123,
            "current_volume_lots": 1,
            "amount": Decimal("123000"),
            "sell_volume_lots": 2,
            "buy_volume_lots": 3,
        }
        for level in range(1, 6):
            row[f"bid{level}"] = Decimal("10.00") - Decimal(level - 1) / 100
            row[f"ask{level}"] = Decimal("10.01") + Decimal(level - 1) / 100
            row[f"bid_vol{level}"] = level
            row[f"ask_vol{level}"] = level + 5
        return row


class BatchRecordedClient(RecordedClient):
    def __init__(self, batches: list[tuple[tuple[int, str], ...]]) -> None:
        self.batches = batches
        self.hosts: tuple[tuple[str, int], ...] = ()

    def record_hosts(self, hosts: Sequence[tuple[str, int]]) -> "BatchRecordedClient":
        self.hosts = tuple(hosts)
        return self

    def fetch(self, requests: Sequence[tuple[int, str]]) -> Sequence[Mapping[str, object]]:
        self.batches.append(tuple(requests))
        return [self._row(market, code) for market, code in requests]


def test_pytdx_hq_contract_keeps_decimal_and_converts_lots_to_shares(tmp_path: Path) -> None:
    observed = datetime(2026, 8, 3, 1, 15, tzinfo=UTC)
    pool = _write_pool(tmp_path, [_node("quote.example", quote=True)])
    provider = PytdxHqProvider(
        PytdxHqSettings(_env_file=None),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=lambda _hosts, _timeout: RecordedClient(),
        clock=lambda: observed,
    )

    with provider:
        result = provider.fetch_five_level_quotes(("SSE:600000",))

    quote = result.records[0]
    assert quote.last_price == Decimal("10.00")
    assert quote.cumulative_volume == 12_300
    assert quote.bid_levels[0].volume == 100
    assert quote.source_timestamp is None
    assert result.failed_symbols == ()


def test_hq_provider_uses_only_quote_nodes_and_fixes_one_session(tmp_path: Path) -> None:
    pool = _write_pool(
        tmp_path,
        [
            _node("daily-only", sse=True, szse=True),
            _node("quote", quote=True),
        ],
    )
    calls: list[tuple[tuple[tuple[str, int], ...], float]] = []

    def factory(hosts: Sequence[tuple[str, int]], timeout: float) -> RecordedClient:
        calls.append((tuple(hosts), timeout))
        return RecordedClient()

    provider = PytdxHqProvider(
        PytdxHqSettings(_env_file=None),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=factory,
    )

    with provider:
        provider.fetch_five_level_quotes(("SSE:600000",))
        provider.fetch_five_level_quotes(("SZSE:000001",))

    assert calls == [((("quote", 7709),), 2.0)]


def test_hq_provider_rejects_pool_without_quote_nodes(tmp_path: Path) -> None:
    pool = _write_pool(tmp_path, [_node("daily-only", sse=True, szse=True)])
    provider = PytdxHqProvider(
        PytdxHqSettings(_env_file=None),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=lambda _hosts, _timeout: RecordedClient(),
    )

    with pytest.raises(ProviderError, match="no quote-capable node"), provider:
        pass


def test_hq_provider_does_not_fallback_when_pool_is_corrupt(tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not json", encoding="utf-8")
    provider = PytdxHqProvider(
        PytdxHqSettings(_env_file=None),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=corrupt, _env_file=None),
        client_factory=lambda _hosts, _timeout: RecordedClient(),
    )

    with pytest.raises(ProviderError, match="pool is unreadable"), provider:
        pass


def test_market_provider_uses_one_explicit_endpoint_and_batches_by_eighty(tmp_path: Path) -> None:
    batches: list[tuple[tuple[int, str], ...]] = []
    client = BatchRecordedClient(batches)
    provider = PytdxHqProvider(
        PytdxHqSettings(_env_file=None),
        endpoints=(("second.quote", 7709),),
        client_factory=lambda hosts, _timeout: client.record_hosts(hosts),
    )
    symbols = _symbols(161)
    with provider:
        result = provider.fetch_five_level_quotes(symbols)
    assert client.hosts == (("second.quote", 7709),)
    assert [len(batch) for batch in batches] == [80, 80, 1]
    assert result.requested_symbols == symbols


def test_provider_stops_starting_batches_at_deadline() -> None:
    batches: list[tuple[tuple[int, str], ...]] = []
    client = BatchRecordedClient(batches)
    clock = iter(
        (
            datetime(2026, 8, 12, 1, 29, 29, tzinfo=UTC),
            datetime(2026, 8, 12, 1, 29, 29, 500000, tzinfo=UTC),
            datetime(2026, 8, 12, 1, 29, 31, tzinfo=UTC),
        )
    ).__next__
    provider = PytdxHqProvider(
        PytdxHqSettings(_env_file=None),
        endpoints=(("only.quote", 7709),),
        client_factory=lambda hosts, _timeout: client.record_hosts(hosts),
        clock=clock,
    )
    with provider:
        result = provider.fetch_five_level_quotes(
            _symbols(161),
            deadline=datetime(2026, 8, 12, 1, 29, 30, tzinfo=UTC),
        )
    assert len(result.records) == 80
    assert result.failed_symbols == _symbols(161)[80:]


class _FakeApi:
    """A stand-in for TdxHq_API whose connect succeeds only on allowlisted hosts."""

    def __init__(self, *, allow: set[str], **_kwargs):
        self._allow = allow
        self.client = None

    def connect(self, ip, port, time_out=5.0, **_):
        if ip in self._allow:
            self.client = object()
            return True
        return False

    def disconnect(self):
        self.client = None


def test_network_client_failover_to_second_host(monkeypatch):
    from market_data_center.providers import pytdx_hq
    from market_data_center.providers.pytdx_hq import _NetworkQuoteClient

    monkeypatch.setattr(pytdx_hq, "TdxHq_API", lambda **kw: _FakeApi(allow={"good.host"}))
    client = _NetworkQuoteClient([("bad.host", 7709), ("good.host", 7700)], timeout_seconds=2.0)

    with client:
        assert client.connected_host == ("good.host", 7700)


def test_network_client_raises_when_all_hosts_fail(monkeypatch):
    from market_data_center.providers import pytdx_hq
    from market_data_center.providers.contracts import ProviderError
    from market_data_center.providers.pytdx_hq import _NetworkQuoteClient

    monkeypatch.setattr(pytdx_hq, "TdxHq_API", lambda **kw: _FakeApi(allow=set()))
    client = _NetworkQuoteClient([("a.host", 7709), ("b.host", 7709)], timeout_seconds=1.0)

    with pytest.raises(ProviderError, match="connection failed"), client:
        pass
