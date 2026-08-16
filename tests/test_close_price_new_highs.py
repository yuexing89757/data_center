from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.close_price_new_highs_calculator import (
    calculate_close_price_new_highs_120d,
)
from market_data_center.domain.close_price_new_highs import (
    ClosePriceNewHighCandidate,
    ClosePriceNewHighInput,
)
from market_data_center.domain.records import TradeStatus

TRADE_DATE = date(2026, 8, 14)
FIRST_DAY = date(2026, 2, 23)


def _candidate(
    symbol: str = "SSE:600000",
    *,
    close: str = "12.50",
    previous_high: str = "12.00",
) -> ClosePriceNewHighCandidate:
    return ClosePriceNewHighCandidate(
        symbol=symbol,
        code=symbol.split(":", maxsplit=1)[1],
        display_name="浦发银行",
        valid_bar_count=120,
        close=Decimal(close),
        current_status=TradeStatus.UNKNOWN,
        previous_119d_high=Decimal(previous_high),
        has_non_trading_bar=False,
        has_nonpositive_price=False,
    )


def _source(*candidates: ClosePriceNewHighCandidate) -> ClosePriceNewHighInput:
    return ClosePriceNewHighInput(TRADE_DATE, FIRST_DAY, 120, tuple(candidates))


def test_calculator_returns_only_strict_breakouts_with_decimal_percentage() -> None:
    breakout = _candidate()
    equal = _candidate("SZSE:000001", close="12.00")

    result = calculate_close_price_new_highs_120d(_source(equal, breakout))

    assert result.candidate_count == 2
    assert result.eligible_history_count == 2
    assert result.omitted_count == 0
    assert result.member_count == 1
    assert result.members[0].symbol == "SSE:600000"
    assert result.members[0].breakout_pct == Decimal("4.1666666667")


def test_calculator_classifies_overlapping_omissions_without_fabricating_values() -> None:
    incomplete = replace(
        _candidate(),
        valid_bar_count=119,
        current_status=TradeStatus.SUSPENDED,
        has_non_trading_bar=True,
    )
    nonpositive = replace(
        _candidate("SSE:600001"),
        valid_bar_count=119,
        close=Decimal("0"),
        has_nonpositive_price=True,
    )
    missing_name = replace(_candidate("SZSE:000002"), display_name=None)

    result = calculate_close_price_new_highs_120d(_source(incomplete, nonpositive, missing_name))

    assert result.members == ()
    assert result.omitted_count == 3
    assert result.incomplete_history_count == 2
    assert result.non_trading_bar_count == 1
    assert result.nonpositive_price_count == 1
    assert result.missing_name_count == 1


def test_members_are_sorted_by_breakout_descending_then_symbol() -> None:
    low = _candidate("SSE:600002", close="11", previous_high="10")
    high_z = _candidate("SZSE:000003", close="12", previous_high="10")
    high_a = _candidate("SSE:600001", close="12", previous_high="10")

    result = calculate_close_price_new_highs_120d(_source(low, high_z, high_a))

    assert [item.symbol for item in result.members] == [
        "SSE:600001",
        "SZSE:000003",
        "SSE:600002",
    ]


def test_hashes_are_deterministic_independent_of_candidate_order() -> None:
    first = _candidate()
    second = _candidate("SZSE:000001", close="13")

    left = calculate_close_price_new_highs_120d(_source(first, second))
    right = calculate_close_price_new_highs_120d(_source(second, first))

    assert left.input_hash == right.input_hash
    assert left.content_hash == right.content_hash


def test_input_requires_exactly_120_sessions_and_unique_bounded_candidates() -> None:
    candidate = _candidate()

    with pytest.raises(ValueError, match="exactly 120"):
        ClosePriceNewHighInput(TRADE_DATE, FIRST_DAY, 119, (candidate,))
    with pytest.raises(ValueError, match="unique"):
        ClosePriceNewHighInput(TRADE_DATE, FIRST_DAY, 120, (candidate, candidate))
    with pytest.raises(ValueError, match="10,000"):
        ClosePriceNewHighInput(TRADE_DATE, FIRST_DAY, 120, (candidate,) * 10_001)
