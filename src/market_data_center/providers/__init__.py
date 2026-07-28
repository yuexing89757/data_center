"""External market-data provider adapters."""

from market_data_center.providers.baostock import BaoStockProvider
from market_data_center.providers.contracts import ProviderBatch, ProviderError

__all__ = ["BaoStockProvider", "ProviderBatch", "ProviderError"]
