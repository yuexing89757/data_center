from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType

import pytest

from market_data_center.providers.pytdx_hq import PytdxHqProvider
from market_data_center.settings import PytdxHqSettings


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
        market, code = requests[0]
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
        return [row]


def test_pytdx_hq_contract_keeps_decimal_and_converts_lots_to_shares() -> None:
    observed = datetime(2026, 8, 3, 1, 15, tzinfo=UTC)
    provider = PytdxHqProvider(
        PytdxHqSettings(pytdx_hq_host="recorded.invalid"),
        client_factory=RecordedClient,
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


def test_resolve_hosts_prefers_pool(tmp_path):
    import json

    from market_data_center.providers.pytdx_hq import _resolve_hosts
    from market_data_center.settings import PytdxHqSettings

    pool = tmp_path / "pool.json"
    pool.write_text(
        json.dumps(
            [
                {"name": "fast", "ip": "1.1.1.1", "port": 7709, "latency_ms": 10},
                {"name": "slow", "ip": "2.2.2.2", "port": 7700, "latency_ms": 200},
            ]
        ),
        encoding="utf-8",
    )
    settings = PytdxHqSettings(pytdx_hq_host="fallback.invalid", pytdx_hq_pool_path=pool)

    assert _resolve_hosts(settings) == [("1.1.1.1", 7709), ("2.2.2.2", 7700)]


def test_resolve_hosts_falls_back_to_env_host(tmp_path):
    from market_data_center.providers.pytdx_hq import _resolve_hosts
    from market_data_center.settings import PytdxHqSettings

    # Pool path points to a non-existent file.
    settings = PytdxHqSettings(
        pytdx_hq_host="env.host.example",
        pytdx_hq_port=7707,
        pytdx_hq_pool_path=tmp_path / "missing.json",
    )

    assert _resolve_hosts(settings) == [("env.host.example", 7707)]


def test_resolve_hosts_raises_without_any_source(tmp_path):
    from market_data_center.providers.contracts import ProviderError
    from market_data_center.providers.pytdx_hq import _resolve_hosts
    from market_data_center.settings import PytdxHqSettings

    settings = PytdxHqSettings(pytdx_hq_host=None, pytdx_hq_pool_path=tmp_path / "missing.json")

    with pytest.raises(ProviderError, match="no endpoints"):
        _resolve_hosts(settings)


def test_resolve_hosts_ignores_corrupt_pool(tmp_path):
    from market_data_center.providers.pytdx_hq import _resolve_hosts
    from market_data_center.settings import PytdxHqSettings

    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not json", encoding="utf-8")
    settings = PytdxHqSettings(pytdx_hq_host="fallback.invalid", pytdx_hq_pool_path=corrupt)

    assert _resolve_hosts(settings) == [("fallback.invalid", 7709)]


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
