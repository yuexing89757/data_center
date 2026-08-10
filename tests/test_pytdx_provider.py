from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path
from struct import pack

import pytest

from market_data_center.domain import DailyBarRecord, DatasetCode, TradeStatus
from market_data_center.providers import ProviderError, ProviderRequestUnavailable, PytdxProvider
from market_data_center.providers.pytdx import _read_local_day_file, normalize_pytdx_raw
from market_data_center.settings import PytdxDailyBarSettings


def _settings(**overrides: object) -> PytdxDailyBarSettings:
    return PytdxDailyBarSettings(
        pytdx_daily_bar_endpoints="first.example:7709,second.example:7710",
        pytdx_daily_bar_pool_path=Path("nonexistent-pool.json"),
        pytdx_vipdoc_path="",
        **overrides,
    )


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


def _provider(client: FakeClient, **settings: object) -> PytdxProvider:
    return PytdxProvider(_settings(**settings), client_factory=lambda: client)


def test_context_uses_bounded_endpoint_failover_and_disconnects() -> None:
    failed = FakeClient(connect_result=TimeoutError())
    connected = FakeClient()
    clients = iter((failed, connected))
    provider = PytdxProvider(_settings(), client_factory=lambda: next(clients))

    with provider as entered:
        assert entered is provider

    assert failed.connect_calls == [("first.example", 7709, 3.0)]
    assert connected.connect_calls == [("second.example", 7710, 3.0)]
    assert failed.disconnected is True
    assert connected.disconnected is True


def test_connection_attempt_limit_is_enforced() -> None:
    failed = FakeClient(connect_result=False)
    provider = PytdxProvider(
        _settings(pytdx_daily_bar_max_attempts=1), client_factory=lambda: failed
    )

    with pytest.raises(ProviderError, match=r"tried first\.example:7709"):
        provider.__enter__()
    assert len(failed.connect_calls) == 1


def test_standard_symbol_mapping_covers_all_supported_exchanges() -> None:
    provider = _provider(FakeClient())

    assert provider.source_symbol("SSE:600000") == "sh.600000"
    assert provider.source_symbol("SZSE:000001") == "sz.000001"
    assert provider.source_symbol("BSE:920000") == "bj.920000"


def test_remote_daily_bars_crop_sort_normalize_and_capture_endpoint() -> None:
    client = FakeClient(
        {
            0: [
                _bar("2026-07-28", close="10.40"),
                _bar("2026-07-27", close="10.20"),
                _bar("2026-07-23", close="9.80"),
            ]
        }
    )
    provider = _provider(client)

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


def test_remote_daily_bars_paginate_with_hard_bounds() -> None:
    client = FakeClient(
        {
            0: [_bar("2026-07-28"), _bar("2026-07-27")],
            2: [_bar("2026-07-24"), _bar("2026-07-23")],
        }
    )
    provider = _provider(client, pytdx_daily_bar_page_size=2, pytdx_daily_bar_max_pages=2)

    with provider:
        batch = provider.fetch_daily_bars("sh.600000", date(2026, 7, 24), date(2026, 7, 28))

    assert [call[3] for call in client.bar_calls] == [0, 2]
    assert len(batch.records) == 3


def test_bse_uses_market_zero_and_empty_result_is_visible_gap() -> None:
    client = FakeClient({0: []})
    provider = _provider(client)

    with provider, pytest.raises(ProviderRequestUnavailable, match="no Daily Bars"):
        provider.fetch_daily_bars("bj.920000", date(2026, 7, 28), date(2026, 7, 28))

    assert client.bar_calls[0][1:3] == (0, "920000")


def test_request_failure_does_not_fail_over_mid_session() -> None:
    client = FakeClient({0: TimeoutError()})
    provider = _provider(client)

    with provider, pytest.raises(ProviderError, match="request failed"):
        provider.fetch_daily_bars("sz.000001", date(2026, 7, 28), date(2026, 7, 28))

    assert client.connect_calls == [("first.example", 7709, 3.0)]


@pytest.mark.parametrize(
    "endpoints",
    ["missing-port", "host:not-a-port", "host:0", "host:7709,host:7709"],
)
def test_endpoint_configuration_is_rejected(endpoints: str) -> None:
    settings = PytdxDailyBarSettings(
        pytdx_daily_bar_endpoints=endpoints,
        pytdx_daily_bar_pool_path=Path("nonexistent-pool.json"),
    )

    with pytest.raises(ProviderError):
        PytdxProvider(settings, client_factory=FakeClient)


def test_no_endpoints_without_pool_is_rejected() -> None:
    """Empty endpoints with no pool file raises ProviderError."""
    settings = PytdxDailyBarSettings(
        pytdx_daily_bar_endpoints="",
        pytdx_daily_bar_pool_path=Path("nonexistent-pool.json"),
    )

    with pytest.raises(ProviderError, match="no Daily Bar endpoints"):
        PytdxProvider(settings, client_factory=FakeClient)


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
        pytdx_daily_bar_endpoints="fake.example:7709",
        pytdx_daily_bar_pool_path=Path("nonexistent-pool.json"),
    )

    class _FailingClient:
        def connect(self, *args: object) -> bool:
            raise AssertionError("network should not be reached")

        def disconnect(self) -> None:
            pass

    provider = PytdxProvider(settings, client_factory=_FailingClient)
    provider.__enter__()
    try:
        batch = provider.fetch_daily_bars("sh.600000", date(2026, 8, 7), date(2026, 8, 7))
    finally:
        provider.__exit__(None, None, None)
    assert len(batch.records) == 1
    assert batch.records[0].close == Decimal("9.25")
    assert batch.schema_version == "pytdx.local_daily_bar.v2"
