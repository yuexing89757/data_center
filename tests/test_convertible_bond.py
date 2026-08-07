"""Convertible bond domain record and validation tests."""

from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain.convertible_bond import (
    ConvertibleBondBasicRecord,
    ConvertibleBondCallEventRecord,
    ConvertibleBondConvertPriceRevisionRecord,
    ConvertibleBondDailyBarRecord,
    convertible_bond_natural_key,
    validate_convertible_bond,
)

BOND = ConvertibleBondBasicRecord(
    symbol="SSE:113527",
    bond_code="113527",
    bond_short_name="中信转债",
    bond_full_name="中信转债",
    underlying_symbol="SSE:600030",
    exchange="SSE",
    par_value=Decimal("100"),
    lifecycle_status="listed",
    source_code="tushare",
)


def test_basic_record_rejects_non_positive_par_value() -> None:
    with pytest.raises(ValueError, match="par_value must be positive"):
        replace(BOND, par_value=Decimal("0"))


def test_basic_record_rejects_negative_issue_size() -> None:
    with pytest.raises(ValueError, match="issue_size must be non-negative"):
        replace(BOND, issue_size=Decimal("-1"))


def test_basic_record_rejects_inverted_convert_period() -> None:
    with pytest.raises(ValueError, match="convert_end_date must not precede"):
        replace(
            BOND,
            convert_start_date=date(2026, 1, 10),
            convert_end_date=date(2026, 1, 9),
        )


def test_daily_bar_record_rejects_low_above_high() -> None:
    with pytest.raises(ValueError, match="low must not exceed high"):
        ConvertibleBondDailyBarRecord(
            symbol="SSE:113527",
            trade_date=date(2026, 8, 7),
            market="CN_A_SHARE",
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("101"),
            close=Decimal("100"),
            trade_status="trading",
            source_code="tushare",
        )


def test_revision_record_requires_positive_after_price() -> None:
    with pytest.raises(ValueError, match="convert_price_after must be positive"):
        ConvertibleBondConvertPriceRevisionRecord(
            symbol="SSE:113527",
            effective_date=date(2026, 8, 7),
            convert_price_after=Decimal("0"),
            revision_reason="downward_revision",
            source_code="tushare",
        )


def test_natural_key_distinguishes_record_types() -> None:
    assert convertible_bond_natural_key(BOND) == ("bond", "SSE:113527")
    bar = ConvertibleBondDailyBarRecord(
        symbol="SSE:113527",
        trade_date=date(2026, 8, 7),
        market="CN_A_SHARE",
        trade_status="trading",
        source_code="tushare",
    )
    assert convertible_bond_natural_key(bar) == ("daily_bar", "SSE:113527", "2026-08-07")


def test_validate_rejects_unknown_symbol() -> None:
    result = validate_convertible_bond([BOND], known_symbols=set())
    assert len(result.accepted) == 0
    assert len(result.rejected_records) == 1
    assert result.findings[0].rule_code == "convertible_bond.unknown_symbol"


def test_validate_accepts_known_symbol() -> None:
    result = validate_convertible_bond([BOND], known_symbols={"SSE:113527"})
    assert len(result.accepted) == 1
    assert len(result.findings) == 0


def test_validate_rejects_conflicting_duplicate() -> None:
    twin = replace(BOND, bond_short_name="不同简称")
    result = validate_convertible_bond([BOND, twin], known_symbols={"SSE:113527"})
    assert len(result.accepted) == 0
    assert len(result.rejected_records) == 2
    assert result.findings[0].rule_code == "convertible_bond.conflicting_duplicate"


def test_call_event_record_allows_null_call_price() -> None:
    event = ConvertibleBondCallEventRecord(
        symbol="SSE:113527",
        event_type="forced_redemption",
        announcement_date=date(2026, 8, 7),
        status="announced",
        source_code="tushare",
    )
    assert event.call_price is None
