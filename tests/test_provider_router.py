from datetime import date
from types import TracebackType
from typing import Self

import pytest

from market_data_center.domain import DailyBarRecord, SecurityRecord, TradingDayRecord
from market_data_center.domain.ingestion import DatasetCode
from market_data_center.providers import (
    DEFAULT_PROVIDER_ROUTES,
    ManagedMarketDataProvider,
    ProviderBatch,
    ProviderError,
    ProviderRouter,
    ProviderRoutingError,
)


class FakeProvider:
    def __init__(self, source_code: str) -> None:
        self.source_code = source_code
        self.entered = 0
        self.exited = 0

    def __enter__(self) -> Self:
        self.entered += 1
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.exited += 1

    def source_symbol(self, symbol: str) -> str:
        return symbol

    def fetch_securities(self) -> ProviderBatch[SecurityRecord]:
        raise NotImplementedError

    def fetch_trading_calendar(
        self, start_date: date, end_date: date
    ) -> ProviderBatch[TradingDayRecord]:
        raise NotImplementedError

    def fetch_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> ProviderBatch[DailyBarRecord]:
        raise NotImplementedError


class FakeFactory:
    def __init__(self) -> None:
        self.created: list[FakeProvider] = []

    def __call__(self, provider_code: str) -> FakeProvider:
        provider = FakeProvider(provider_code)
        self.created.append(provider)
        return provider


def test_default_routes_are_capability_specific_and_deterministic() -> None:
    assert DEFAULT_PROVIDER_ROUTES == {
        DatasetCode.SECURITY: ("baostock", "akshare"),
        DatasetCode.TRADING_CALENDAR: ("baostock", "akshare"),
        DatasetCode.DAILY_BAR: ("pytdx", "baostock", "akshare"),
    }


def test_router_falls_back_after_provider_error_and_reports_actual_source() -> None:
    factory = FakeFactory()

    with ProviderRouter(provider_factory=factory) as router:
        result = router.route(
            DatasetCode.SECURITY,
            lambda provider: _fail_baostock(provider.source_code),
        )

    assert result.provider_code == "akshare"
    assert result.value == "akshare"
    assert [attempt.provider_code for attempt in result.failed_attempts] == ["baostock"]
    assert all(provider.exited == 1 for provider in factory.created)


def test_router_does_not_hide_non_provider_errors() -> None:
    factory = FakeFactory()

    with (
        ProviderRouter(provider_factory=factory) as router,
        pytest.raises(RuntimeError, match="database unavailable"),
    ):
        router.route(
            DatasetCode.SECURITY,
            lambda provider: _raise_runtime_error(provider.source_code),
        )

    assert [provider.source_code for provider in factory.created] == ["baostock"]


def test_router_opens_circuit_after_consecutive_failures() -> None:
    factory = FakeFactory()

    with ProviderRouter(provider_factory=factory, failure_threshold=2) as router:
        first = router.route(DatasetCode.DAILY_BAR, _fail_pytdx)
        second = router.route(DatasetCode.DAILY_BAR, _fail_pytdx)
        third = router.route(DatasetCode.DAILY_BAR, _fail_pytdx)

    assert first.provider_code == second.provider_code == third.provider_code == "baostock"
    assert third.failed_attempts[0].error_type == "CircuitOpen"
    assert [provider.source_code for provider in factory.created].count("pytdx") == 2


def test_router_raises_summary_when_every_candidate_fails() -> None:
    with (
        ProviderRouter(provider_factory=FakeFactory()) as router,
        pytest.raises(ProviderRoutingError) as captured,
    ):
        router.route(
            DatasetCode.TRADING_CALENDAR,
            lambda provider: _raise_provider_error(provider.source_code),
        )

    assert [attempt.provider_code for attempt in captured.value.attempts] == [
        "baostock",
        "akshare",
    ]


def _fail_baostock(provider_code: str) -> str:
    if provider_code == "baostock":
        raise ProviderError("baostock unavailable")
    return provider_code


def _fail_pytdx(provider: ManagedMarketDataProvider) -> str:
    if provider.source_code == "pytdx":
        raise ProviderError("local file unavailable")
    return provider.source_code


def _raise_runtime_error(provider_code: str) -> str:
    raise RuntimeError("database unavailable")


def _raise_provider_error(provider_code: str) -> str:
    raise ProviderError(f"{provider_code} unavailable")
