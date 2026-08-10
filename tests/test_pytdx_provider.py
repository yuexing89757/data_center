from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from market_data_center.domain import DailyBarRecord, DatasetCode, TradeStatus
from market_data_center.providers import ProviderError, ProviderRequestUnavailable, PytdxProvider
from market_data_center.providers.pytdx import normalize_pytdx_raw
from market_data_center.settings import PytdxDailyBarSettings


def _settings(**overrides: object) -> PytdxDailyBarSettings:
    return PytdxDailyBarSettings(
        pytdx_daily_bar_endpoints="first.example:7709,second.example:7710",
        pytdx_daily_bar_pool_path=Path("nonexistent-pool.json"),
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
