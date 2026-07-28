"""External market-data provider adapters."""

from market_data_center.providers.akshare import AKShareProvider
from market_data_center.providers.baostock import BaoStockProvider
from market_data_center.providers.contracts import (
    ManagedMarketDataProvider,
    MarketDataProvider,
    ProviderBatch,
    ProviderError,
)
from market_data_center.providers.pytdx import PytdxProvider
from market_data_center.providers.registry import available_provider_codes, create_provider
from market_data_center.providers.router import (
    DEFAULT_PROVIDER_ROUTES,
    ProviderRouter,
    ProviderRoutingError,
    RoutedResult,
    RoutingAttempt,
)

__all__ = [
    "DEFAULT_PROVIDER_ROUTES",
    "AKShareProvider",
    "BaoStockProvider",
    "ManagedMarketDataProvider",
    "MarketDataProvider",
    "ProviderBatch",
    "ProviderError",
    "ProviderRouter",
    "ProviderRoutingError",
    "PytdxProvider",
    "RoutedResult",
    "RoutingAttempt",
    "available_provider_codes",
    "create_provider",
]
