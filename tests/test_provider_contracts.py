import pytest

from market_data_center.domain.dragon_tiger import DragonTigerNormalizationResult
from market_data_center.providers.contracts import DragonTigerProviderBatch, ProviderError


def test_dragon_tiger_batch_normalizes_only_once() -> None:
    calls = 0

    def normalize() -> DragonTigerNormalizationResult:
        nonlocal calls
        calls += 1
        return DragonTigerNormalizationResult(events=())

    batch = DragonTigerProviderBatch(
        raw_rows=(),
        request_params={"trade_date": "2026-08-20"},
        schema_version="eastmoney.dragon_tiger.v3",
        normalization_factory=normalize,
    )

    assert batch.normalization == DragonTigerNormalizationResult(events=())
    assert batch.normalization == DragonTigerNormalizationResult(events=())
    assert calls == 1


def test_dragon_tiger_batch_wraps_unexpected_normalization_errors() -> None:
    def normalize() -> DragonTigerNormalizationResult:
        raise ValueError("unsafe source detail")

    batch = DragonTigerProviderBatch(
        raw_rows=(),
        request_params={"trade_date": "2026-08-20"},
        schema_version="eastmoney.dragon_tiger.v3",
        normalization_factory=normalize,
    )

    with pytest.raises(ProviderError, match="provider response normalization failed"):
        _ = batch.normalization
