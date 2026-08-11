import json
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path
from struct import pack

import pytest

from market_data_center.domain import DailyBarRecord, DatasetCode, TradeStatus
from market_data_center.providers import ProviderError, ProviderRequestUnavailable, PytdxProvider
from market_data_center.providers.pytdx import _read_local_day_file, normalize_pytdx_raw
from market_data_center.settings import PytdxDailyBarSettings, PytdxPoolSettings


def _settings(**overrides: object) -> PytdxDailyBarSettings:
    return PytdxDailyBarSettings(
        pytdx_vipdoc_path="",
        _env_file=None,
        **overrides,
    )


def _node(
    host: str,
    port: int = 7709,
    *,
    quote: bool = False,
    sse: bool = False,
    szse: bool = False,
    bse: bool = False,
    latency_ms: int = 10,
) -> dict[str, object]:
    return {
        "host": host,
        "port": port,
        "latency_ms": latency_ms,
        "capabilities": {
            "quote": quote,
            "daily_bar_sse": sse,
            "daily_bar_szse": szse,
            "daily_bar_bse": bse,
        },
    }


def _write_pool(tmp_path: Path, nodes: list[dict[str, object]]) -> Path:
    path = tmp_path / "pytdx_pool.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "pytdx.endpoint_pool.v1",
                "refreshed_at": "2026-08-11T10:00:00+08:00",
                "nodes": nodes,
            }
        ),
        encoding="utf-8",
    )
    return path


def _bar(day: str, *, close: str = "10.20", volume: int = 123_400) -> dict[str, object]:
    return {
        "datetime": day,
        "open": "10.00",
        "high": "10.50",
        "low": "9.90",
        "close": close,
        "amount": "1258680.00",
        "vol": volume,
    }


class FakeClient:
    def __init__(
        self,
        pages: Mapping[int, object] | None = None,
        *,
        connect_result: bool | Exception = True,
    ) -> None:
        self.pages = dict(pages or {})
        self.connect_result = connect_result
        self.connect_calls: list[tuple[str, int, float]] = []
        self.bar_calls: list[tuple[int, int, str, int, int]] = []
        self.disconnected = False

    def connect(self, host: str, port: int, *, time_out: float) -> bool:
        self.connect_calls.append((host, port, time_out))
        if isinstance(self.connect_result, Exception):
            raise self.connect_result
        return self.connect_result

    def disconnect(self) -> None:
        self.disconnected = True

    def get_security_bars(
        self, category: int, market: int, code: str, start: int, count: int
    ) -> object:
        self.bar_calls.append((category, market, code, start, count))
        result = self.pages.get(start, [])
        if isinstance(result, Exception):
            raise result
        return result


def _provider(tmp_path: Path, client: FakeClient, **settings: object) -> PytdxProvider:
    pool = _write_pool(
        tmp_path,
        [_node("first.example", quote=True, sse=True, szse=True, bse=True)],
    )
    return PytdxProvider(
        _settings(**settings),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=lambda: client,
    )


def test_remote_session_uses_bounded_endpoint_failover_and_disconnects(tmp_path: Path) -> None:
    failed = FakeClient(connect_result=TimeoutError())
    connected = FakeClient({0: [_bar("2026-07-28")]})
    clients = iter((failed, connected))
    pool = _write_pool(
        tmp_path,
        [
            _node("first.example", sse=True, latency_ms=10),
            _node("second.example", 7710, sse=True, latency_ms=20),
        ],
    )
    provider = PytdxProvider(
        _settings(),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=lambda: next(clients),
    )

    with provider as entered:
        assert entered is provider
        entered.fetch_daily_bars("sh.600000", date(2026, 7, 28), date(2026, 7, 28))

    assert failed.connect_calls == [("first.example", 7709, 3.0)]
    assert connected.connect_calls == [("second.example", 7710, 3.0)]
    assert failed.disconnected is True
    assert connected.disconnected is True


def test_connection_attempt_limit_is_enforced(tmp_path: Path) -> None:
    failed = FakeClient(connect_result=False)
    pool = _write_pool(
        tmp_path,
        [
            _node("first.example", sse=True),
            _node("second.example", 7710, sse=True),
        ],
    )
    provider = PytdxProvider(
        _settings(pytdx_daily_bar_max_attempts=1),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=lambda: failed,
    )

    with provider, pytest.raises(ProviderError, match="remote connection failed for sh"):
        provider.fetch_daily_bars("sh.600000", date(2026, 7, 28), date(2026, 7, 28))
    assert len(failed.connect_calls) == 1


def test_standard_symbol_mapping_covers_all_supported_exchanges(tmp_path: Path) -> None:
    provider = _provider(tmp_path, FakeClient())

    assert provider.source_symbol("SSE:600000") == "sh.600000"
    assert provider.source_symbol("SZSE:000001") == "sz.000001"
    assert provider.source_symbol("BSE:920000") == "bj.920000"


def test_remote_daily_bars_select_nodes_by_market_capability(tmp_path: Path) -> None:
    pool = _write_pool(
        tmp_path,
        [
            _node("quote-only", quote=True, latency_ms=1),
            _node("sse", sse=True, latency_ms=2),
            _node("szse", szse=True, latency_ms=3),
        ],
    )
    sse_client = FakeClient({0: [_bar("2026-07-28")]})
    szse_client = FakeClient({0: [_bar("2026-07-28")]})
    clients = iter((sse_client, szse_client))
    provider = PytdxProvider(
        _settings(),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=lambda: next(clients),
    )

    with provider:
        provider.fetch_daily_bars("sh.600000", date(2026, 7, 28), date(2026, 7, 28))
        provider.fetch_daily_bars("sz.000001", date(2026, 7, 28), date(2026, 7, 28))

    assert sse_client.connect_calls == [("sse", 7709, 3.0)]
    assert szse_client.connect_calls == [("szse", 7709, 3.0)]


def test_remote_daily_bars_reuse_one_fixed_session_per_market(tmp_path: Path) -> None:
    client = FakeClient({0: [_bar("2026-07-28")]})
    provider = _provider(tmp_path, client)

    with provider:
        provider.fetch_daily_bars("sh.600000", date(2026, 7, 28), date(2026, 7, 28))
        provider.fetch_daily_bars("sh.601398", date(2026, 7, 28), date(2026, 7, 28))

    assert client.connect_calls == [("first.example", 7709, 3.0)]


def test_remote_daily_bars_crop_sort_normalize_and_capture_endpoint(tmp_path: Path) -> None:
    client = FakeClient(
        {
            0: [
                _bar("2026-07-28", close="10.40"),
                _bar("2026-07-27", close="10.20"),
                _bar("2026-07-23", close="9.80"),
            ]
        }
    )
    provider = _provider(tmp_path, client)

    with provider:
        batch = provider.fetch_daily_bars("sh.600000", date(2026, 7, 24), date(2026, 7, 28))

    assert [record.trade_date for record in batch.records] == [
        date(2026, 7, 27),
        date(2026, 7, 28),
    ]
    record = batch.records[0]
    assert record.symbol == "SSE:600000"
    assert record.close == Decimal("10.20")
    assert record.previous_close == Decimal("9.80")
    assert record.volume == 123_400
    assert record.amount == Decimal("1258680.00")
    assert record.trade_status is TradeStatus.UNKNOWN
    assert batch.schema_version == "pytdx.remote_daily_bar.v1"
    assert batch.request_params["endpoint"] == "first.example:7709"
    assert "endpoint" not in batch.raw_rows[0]

    replayed = normalize_pytdx_raw(
        DatasetCode.DAILY_BAR,
        batch.schema_version,
        batch.raw_rows,
        batch.request_params,
    )
    assert tuple(batch.records) == replayed
    assert isinstance(replayed[0], DailyBarRecord)


def test_remote_daily_bars_paginate_with_hard_bounds(tmp_path: Path) -> None:
    client = FakeClient(
        {
            0: [_bar("2026-07-28"), _bar("2026-07-27")],
            2: [_bar("2026-07-24"), _bar("2026-07-23")],
        }
    )
    provider = _provider(
        tmp_path,
        client,
        pytdx_daily_bar_page_size=2,
        pytdx_daily_bar_max_pages=2,
    )

    with provider:
        batch = provider.fetch_daily_bars("sh.600000", date(2026, 7, 24), date(2026, 7, 28))

    assert [call[3] for call in client.bar_calls] == [0, 2]
    assert len(batch.records) == 3


def test_bse_uses_market_zero_and_empty_result_is_visible_gap(tmp_path: Path) -> None:
    client = FakeClient({0: []})
    provider = _provider(tmp_path, client)

    with provider, pytest.raises(ProviderRequestUnavailable, match="no Daily Bars"):
        provider.fetch_daily_bars("bj.920000", date(2026, 7, 28), date(2026, 7, 28))

    assert client.bar_calls[0][1:3] == (0, "920000")


def test_request_failure_does_not_fail_over_mid_session(tmp_path: Path) -> None:
    client = FakeClient({0: TimeoutError()})
    second = FakeClient({0: [_bar("2026-07-28")]})
    clients = iter((client, second))
    pool = _write_pool(
        tmp_path,
        [_node("first.example", szse=True), _node("second.example", 7710, szse=True)],
    )
    provider = PytdxProvider(
        _settings(),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=lambda: next(clients),
    )

    with provider, pytest.raises(ProviderError, match="request failed"):
        provider.fetch_daily_bars("sz.000001", date(2026, 7, 28), date(2026, 7, 28))

    assert client.connect_calls == [("first.example", 7709, 3.0)]
    assert second.connect_calls == []


def test_missing_market_capability_is_a_visible_gap(tmp_path: Path) -> None:
    pool = _write_pool(tmp_path, [_node("quote-only", quote=True)])
    provider = PytdxProvider(
        _settings(),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=lambda: FakeClient(),
    )

    with provider, pytest.raises(ProviderRequestUnavailable, match="no daily_bar_bse"):
        provider.fetch_daily_bars("bj.920000", date(2026, 7, 28), date(2026, 7, 28))


def test_local_only_mode_without_endpoints(tmp_path: Path) -> None:
    """When vipdoc_path is set and no endpoints configured, local-only mode works."""
    vipdoc = tmp_path / "vipdoc"
    _write_day_file(vipdoc, "sh", "600000", [(20260807, 920, 930, 915, 925, 400000.0, 40000)])
    settings = PytdxDailyBarSettings(
        pytdx_vipdoc_path=str(vipdoc),
        _env_file=None,
    )

    provider = PytdxProvider(
        settings,
        pool_settings=PytdxPoolSettings(
            pytdx_pool_path=tmp_path / "missing.json", _env_file=None
        ),
        client_factory=FakeClient,
    )
    with provider:
        batch = provider.fetch_daily_bars("sh.600000", date(2026, 8, 7), date(2026, 8, 7))

    assert len(batch.records) == 1
    assert batch.records[0].close == Decimal("9.25")
    assert batch.schema_version == "pytdx.local_daily_bar.v2"


def test_local_only_mode_missing_file_is_visible_gap(tmp_path: Path) -> None:
    """In local-only mode, a missing .day file raises ProviderRequestUnavailable."""
    settings = PytdxDailyBarSettings(
        pytdx_vipdoc_path=str(tmp_path / "vipdoc"),
        _env_file=None,
    )

    provider = PytdxProvider(
        settings,
        pool_settings=PytdxPoolSettings(
            pytdx_pool_path=tmp_path / "missing.json", _env_file=None
        ),
        client_factory=FakeClient,
    )
    with provider, pytest.raises(ProviderRequestUnavailable, match=r"local \.day file not found"):
        provider.fetch_daily_bars("sh.600000", date(2026, 8, 7), date(2026, 8, 7))


def test_legacy_local_raw_remains_replayable() -> None:
    rows = (
        {
            "date": "20260727",
            "open": "1000",
            "high": "1050",
            "low": "990",
            "close": "1020",
            "amount": "1258680",
            "volume": "123400",
            "previous_close": "980",
        },
    )

    replayed = normalize_pytdx_raw(
        DatasetCode.DAILY_BAR,
        "pytdx.local_daily_bar.v2",
        rows,
        {"source_symbol": "sh.600000"},
    )

    assert replayed[0].close == Decimal("10.20")
    assert replayed[0].previous_close == Decimal("9.80")


def _write_day_file(vipdoc: Path, exchange: str, code: str, bars: list[tuple]) -> Path:
    """Write a minimal .day file; each bar is (yyyymmdd, o, h, l, c, amount, vol)."""
    day_dir = vipdoc / exchange / "lday"
    day_dir.mkdir(parents=True, exist_ok=True)
    day_file = day_dir / f"{exchange}{code}.day"
    with open(day_file, "wb") as f:
        for d, o, h, lo, c, amount, vol in bars:
            f.write(pack("<IIIIIfII", d, o, h, lo, c, amount, vol, 0))
    return day_file


def test_read_local_day_file_parses_and_filters(tmp_path: Path) -> None:
    vipdoc = tmp_path / "vipdoc"
    _write_day_file(
        vipdoc,
        "sh",
        "600000",
        [
            (20260804, 900, 910, 895, 905, 100000.0, 10000),
            (20260805, 905, 920, 900, 915, 200000.0, 20000),
            (20260806, 915, 925, 910, 920, 300000.0, 30000),
            (20260807, 920, 930, 915, 925, 400000.0, 40000),
        ],
    )
    rows = _read_local_day_file(str(vipdoc), "sh", "600000", date(2026, 8, 5), date(2026, 8, 6))
    assert rows is not None
    assert len(rows) == 2
    assert rows[0]["date"] == "20260805"
    assert rows[0]["close"] == "915"
    assert rows[1]["date"] == "20260806"


def test_read_local_day_file_returns_none_when_absent(tmp_path: Path) -> None:
    rows = _read_local_day_file(str(tmp_path), "sh", "999999", date(2026, 8, 5), date(2026, 8, 7))
    assert rows is None


def test_local_daily_bar_takes_priority_over_remote(tmp_path: Path) -> None:
    """When vipdoc_path is set and the .day file exists, fetch skips the network client."""
    vipdoc = tmp_path / "vipdoc"
    _write_day_file(vipdoc, "sh", "600000", [(20260807, 920, 930, 915, 925, 400000.0, 40000)])
    settings = PytdxDailyBarSettings(
        pytdx_vipdoc_path=str(vipdoc),
        _env_file=None,
    )

    class _FailingClient:
        def connect(self, *args: object) -> bool:
            raise AssertionError("network should not be reached")

        def disconnect(self) -> None:
            pass

    provider = PytdxProvider(
        settings,
        pool_settings=PytdxPoolSettings(
            pytdx_pool_path=tmp_path / "missing.json", _env_file=None
        ),
        client_factory=_FailingClient,
    )
    provider.__enter__()
    try:
        batch = provider.fetch_daily_bars("sh.600000", date(2026, 8, 7), date(2026, 8, 7))
    finally:
        provider.__exit__(None, None, None)
    assert len(batch.records) == 1
    assert batch.records[0].close == Decimal("9.25")
    assert batch.schema_version == "pytdx.local_daily_bar.v2"
