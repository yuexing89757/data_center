"""Read-only deployment check for the shared PYTDX endpoint pool."""

import json
import sys

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx_pool import (
    PytdxCapability,
    endpoints_for,
    load_endpoint_pool,
)
from market_data_center.settings import PytdxPoolSettings


def main() -> int:
    try:
        pool = load_endpoint_pool(PytdxPoolSettings().pytdx_pool_path)
    except ProviderError:
        print("pytdx endpoint pool is unavailable", file=sys.stderr)
        return 1
    counts = {
        capability.value: len(endpoints_for(pool, capability)) for capability in PytdxCapability
    }
    required = (
        PytdxCapability.QUOTE,
        PytdxCapability.DAILY_BAR_SSE,
        PytdxCapability.DAILY_BAR_SZSE,
    )
    if any(counts[capability.value] == 0 for capability in required):
        print("pytdx endpoint pool is missing required capabilities", file=sys.stderr)
        return 1
    print(json.dumps(counts, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
