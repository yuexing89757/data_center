"""Provider adapter registry and construction boundary."""

from collections.abc import Callable

from market_data_center.providers.akshare import AKShareProvider
from market_data_center.providers.baostock import BaoStockProvider
from market_data_center.providers.contracts import ManagedMarketDataProvider, ProviderError

type ProviderFactory = Callable[[], ManagedMarketDataProvider]

_PROVIDER_FACTORIES: dict[str, ProviderFactory] = {
    "akshare": AKShareProvider.default,
    "baostock": BaoStockProvider.default,
}


def available_provider_codes() -> tuple[str, ...]:
    """Return stable provider codes accepted by configuration and CLI."""
    return tuple(sorted(_PROVIDER_FACTORIES))


def create_provider(provider_code: str) -> ManagedMarketDataProvider:
    """Construct a registered adapter without leaking concrete types to callers."""
    try:
        factory = _PROVIDER_FACTORIES[provider_code]
    except KeyError as error:
        supported = ", ".join(available_provider_codes())
        raise ProviderError(
            f"unsupported provider: {provider_code}; supported providers: {supported}"
        ) from error
    provider = factory()
    if provider.source_code != provider_code:
        raise ProviderError(
            f"provider registry mismatch: requested {provider_code}, got {provider.source_code}"
        )
    return provider
