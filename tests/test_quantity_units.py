from decimal import Decimal, localcontext

import pytest

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.quantity_units import (
    normalize_source_stock_quantity,
    normalize_stock_quantity,
)


@pytest.mark.parametrize(
    ("value", "unit", "expected"),
    [
        (None, "share", None),
        ("", "lot", None),
        (0, "lot", 0),
        ("37", "share", 37),
        ("0.37", "lot", 37),
        ("0.5", "lot", 50),
        (Decimal("0.0037"), "ten_thousand_share", 37),
        ("9223372036854775807", "share", 9223372036854775807),
    ],
)
def test_exact_stock_quantities(value: object, unit: str, expected: int | None) -> None:
    assert normalize_stock_quantity(value, source_unit=unit, context="fixture") == expected


@pytest.mark.parametrize(
    "value", [True, False, 0.37, "NaN", "Infinity", "-1", "0.001", "92233720368547758.08"]
)
def test_stock_quantities_reject_invalid_or_inexact_values(value: object) -> None:
    with pytest.raises(ProviderError):
        normalize_stock_quantity(value, source_unit="lot", context="fixture")


def test_decimal_context_cannot_round_large_share_quantity() -> None:
    with localcontext() as context:
        context.prec = 3
        assert (
            normalize_stock_quantity("92233720368547758.07", source_unit="lot", context="fixture")
            == 9223372036854775807
        )


@pytest.mark.parametrize("unit", ["", "lots", "bond", "index", "cny", "thousand_cny"])
def test_unknown_unit_rejected_even_for_null(unit: str) -> None:
    with pytest.raises(ProviderError):
        normalize_stock_quantity(None, source_unit=unit, context="fixture")


def test_source_contract_exact_identity_and_unknown_rejection() -> None:
    identity = dict(
        provider="akshare",
        endpoint="stock_zh_a_hist",
        schema_version="akshare.daily_bar.v1",
        instrument_type="stock",
        field="成交量",
    )
    assert normalize_source_stock_quantity("0.37", **identity) == 37
    for key in identity:
        with pytest.raises(ProviderError, match="quantity unit contract"):
            normalize_source_stock_quantity("1", **(identity | {key: "unknown"}))
