import pytest

from market_data_center.providers import (
    AKShareProvider,
    BaoStockProvider,
    ProviderError,
    available_provider_codes,
    create_provider,
)


def test_registry_exposes_stable_provider_codes() -> None:
    assert available_provider_codes() == ("akshare", "baostock")


def test_registry_builds_each_adapter() -> None:
    assert isinstance(create_provider("akshare"), AKShareProvider)
    assert isinstance(create_provider("baostock"), BaoStockProvider)


def test_registry_rejects_unknown_provider() -> None:
    with pytest.raises(ProviderError, match="unsupported provider"):
        create_provider("unknown")
