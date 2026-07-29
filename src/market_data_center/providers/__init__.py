"""External market-data provider adapters."""

from market_data_center.providers.akshare import AKShareProvider
from market_data_center.providers.akshare_ths import AKShareTHSProvider
from market_data_center.providers.baostock import BaoStockProvider
from market_data_center.providers.contracts import (
    BoardIndexProvider,
    ManagedBoardIndexProvider,
    ManagedMarketDataProvider,
    MarketDataProvider,
    ProviderBatch,
    ProviderError,
    ProviderRequestUnavailable,
)
from market_data_center.providers.pytdx import PytdxProvider
from market_data_center.providers.registry import (
    available_board_index_provider_codes,
    available_provider_codes,
    create_board_index_provider,
    create_provider,
)
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
    "AKShareTHSProvider",
    "BaoStockProvider",
    "BoardIndexProvider",
    "ManagedBoardIndexProvider",
    "ManagedMarketDataProvider",
    "MarketDataProvider",
    "ProviderBatch",
    "ProviderError",
    "ProviderRequestUnavailable",
    "ProviderRouter",
    "ProviderRoutingError",
    "PytdxProvider",
    "RoutedResult",
    "RoutingAttempt",
    "available_board_index_provider_codes",
    "available_provider_codes",
    "create_board_index_provider",
    "create_provider",
]
