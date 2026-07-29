"""Deterministic provider selection and failover orchestration."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Self

from market_data_center.domain.ingestion import DatasetCode
from market_data_center.providers.contracts import (
    ManagedMarketDataProvider,
    ProviderError,
    ProviderRequestUnavailable,
)
from market_data_center.providers.registry import create_provider

type ProviderFactory = Callable[[str], ManagedMarketDataProvider]

DEFAULT_PROVIDER_ROUTES: Mapping[DatasetCode, tuple[str, ...]] = {
    DatasetCode.SECURITY: ("baostock", "akshare"),
    DatasetCode.TRADING_CALENDAR: ("baostock", "akshare"),
    DatasetCode.DAILY_BAR: ("pytdx", "baostock", "akshare"),
    DatasetCode.CAPITAL: ("akshare",),
}


@dataclass(frozen=True, slots=True)
class RoutingAttempt:
    provider_code: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class RoutedResult[ResultT]:
    provider_code: str
    value: ResultT
    failed_attempts: tuple[RoutingAttempt, ...]


class ProviderRoutingError(ProviderError):
    def __init__(self, dataset_code: DatasetCode, attempts: Sequence[RoutingAttempt]) -> None:
        self.dataset_code = dataset_code
        self.attempts = tuple(attempts)
        attempted = ", ".join(attempt.provider_code for attempt in attempts) or "none"
        super().__init__(
            f"all providers failed for {dataset_code.value}; attempted providers: {attempted}"
        )


class ProviderRouter:
    """Route operations without disguising the router as a data provider."""

    def __init__(
        self,
        *,
        routes: Mapping[DatasetCode, Sequence[str]] = DEFAULT_PROVIDER_ROUTES,
        provider_factory: ProviderFactory = create_provider,
        failure_threshold: int = 3,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be positive")
        self._routes = {dataset: tuple(candidates) for dataset, candidates in routes.items()}
        for dataset, candidates in self._routes.items():
            if not candidates:
                raise ValueError(f"provider route must not be empty: {dataset.value}")
            if len(candidates) != len(set(candidates)):
                raise ValueError(f"provider route contains duplicates: {dataset.value}")
        self._provider_factory = provider_factory
        self._failure_threshold = failure_threshold
        self._providers: dict[str, ManagedMarketDataProvider] = {}
        self._consecutive_failures: dict[str, int] = {}
        self._open_circuits: set[str] = set()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        close_errors: list[Exception] = []
        for provider_code in tuple(reversed(self._providers)):
            error = self._discard_provider(provider_code, exc_type, exc_value, traceback)
            if error is not None:
                close_errors.append(error)
        if exc_type is None and close_errors:
            raise ProviderError("one or more provider sessions failed to close") from close_errors[
                0
            ]

    def candidates(self, dataset_code: DatasetCode) -> tuple[str, ...]:
        return self._routes.get(dataset_code, ())

    def route[ResultT](
        self,
        dataset_code: DatasetCode,
        operation: Callable[[ManagedMarketDataProvider], ResultT],
    ) -> RoutedResult[ResultT]:
        attempts: list[RoutingAttempt] = []
        for provider_code in self.candidates(dataset_code):
            if provider_code in self._open_circuits:
                attempts.append(
                    RoutingAttempt(
                        provider_code, "CircuitOpen", "consecutive failure limit reached"
                    )
                )
                continue
            try:
                provider = self._get_provider(provider_code)
                value = operation(provider)
            except ProviderRequestUnavailable as error:
                attempts.append(RoutingAttempt(provider_code, type(error).__name__, str(error)))
                continue
            except ProviderError as error:
                attempts.append(RoutingAttempt(provider_code, type(error).__name__, str(error)))
                self._record_failure(provider_code)
                self._discard_provider(provider_code, type(error), error, error.__traceback__)
                continue
            self._consecutive_failures[provider_code] = 0
            return RoutedResult(provider_code, value, tuple(attempts))
        raise ProviderRoutingError(dataset_code, attempts)

    def _get_provider(self, provider_code: str) -> ManagedMarketDataProvider:
        existing = self._providers.get(provider_code)
        if existing is not None:
            return existing
        try:
            provider = self._provider_factory(provider_code)
            entered = provider.__enter__()
        except ProviderError:
            raise
        except Exception as error:
            raise ProviderError(f"provider session failed to open: {provider_code}") from error
        self._providers[provider_code] = entered
        return entered

    def _record_failure(self, provider_code: str) -> None:
        failures = self._consecutive_failures.get(provider_code, 0) + 1
        self._consecutive_failures[provider_code] = failures
        if failures >= self._failure_threshold:
            self._open_circuits.add(provider_code)

    def _discard_provider(
        self,
        provider_code: str,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Exception | None:
        provider = self._providers.pop(provider_code, None)
        if provider is None:
            return None
        try:
            provider.__exit__(exc_type, exc_value, traceback)
        except Exception as error:
            return error
        return None
