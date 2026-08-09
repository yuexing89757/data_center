from pathlib import Path

import pytest
from pytest import MonkeyPatch

from market_data_center.providers import (
    AKShareProvider,
    AKShareTHSProvider,
    BaoStockProvider,
    ProviderError,
    PytdxProvider,
    TushareProvider,
    available_board_index_provider_codes,
    available_provider_codes,
    create_board_index_provider,
    create_provider,
)


def test_registry_exposes_stable_provider_codes() -> None:
    assert available_provider_codes() == ("akshare", "baostock", "pytdx", "tushare")
    assert available_board_index_provider_codes() == ("akshare_ths",)


def test_registry_builds_each_adapter(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    assert isinstance(create_provider("akshare"), AKShareProvider)
    assert isinstance(create_provider("baostock"), BaoStockProvider)
    monkeypatch.setenv("PYTDX_DAILY_BAR_ENDPOINTS", "tdx.example:7709")
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    assert isinstance(create_provider("pytdx"), PytdxProvider)
    assert isinstance(create_provider("tushare"), TushareProvider)
    assert isinstance(create_board_index_provider("akshare_ths"), AKShareTHSProvider)


def test_registry_rejects_unknown_provider() -> None:
    with pytest.raises(ProviderError, match="unsupported provider"):
        create_provider("unknown")
    with pytest.raises(ProviderError, match="unsupported board-index provider"):
        create_board_index_provider("unknown")
