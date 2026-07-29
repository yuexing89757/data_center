from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.calculators import (
    CalculationInputError,
    calculate_adjusted_daily_bars,
    calculate_derived_facts,
    calculation_input_hash,
)
from market_data_center.domain import (
    AdjustmentType,
    ClassificationMembershipSnapshot,
    ClassificationType,
    CorporateActionStatus,
    DailyBarRecord,
    DerivedCalculationInput,
    DistributionRecord,
    Exchange,
    Market,
    SecurityRecord,
    SecurityStatus,
    SecurityType,
    ShareCapitalRecord,
    TradeStatus,
)

SYMBOL = "SSE:600000"
DAY_1 = date(2026, 7, 27)
DAY_2 = date(2026, 7, 28)
DAY_3 = date(2026, 7, 29)


def _bar(
    trade_date: date,
    *,
    close: str,
    previous_close: str,
    volume: int = 100,
    amount: str = "1000",
) -> DailyBarRecord:
    price = Decimal(close)
    return DailyBarRecord(
        symbol=SYMBOL,
        trade_date=trade_date,
        market=Market.CN_A_SHARE,
        open=price,
        high=price,
        low=price,
        close=price,
        previous_close=Decimal(previous_close),
        volume=volume,
        amount=Decimal(amount),
        trade_status=TradeStatus.TRADING,
        is_st=False,
        source_code="baostock",
    )


def _distribution() -> DistributionRecord:
    return DistributionRecord(
        symbol=SYMBOL,
        report_period=date(2025, 12, 31),
        announcement_date=date(2026, 6, 1),
        record_date=DAY_1,
        ex_date=DAY_2,
        cash_dividend_per_share=None,
        bonus_share_ratio=Decimal("1"),
        transfer_share_ratio=None,
        status=CorporateActionStatus.IMPLEMENTED,
        source_code="akshare",
    )


def _inputs() -> DerivedCalculationInput:
    return DerivedCalculationInput(
        daily_bars=(
            _bar(DAY_1, close="10", previous_close="9"),
            _bar(DAY_2, close="5.2", previous_close="10", volume=200, amount="1040"),
            _bar(DAY_3, close="6", previous_close="5.2"),
        ),
        distributions=(_distribution(),),
        rights_issues=(),
        share_capital=(
            ShareCapitalRecord(
                symbol=SYMBOL,
                effective_date=DAY_1,
                total_shares=1_000_000,
                restricted_shares=None,
                circulating_shares=900_000,
                listed_a_shares=900_000,
                change_reason="before bonus",
                source_code="akshare",
            ),
            ShareCapitalRecord(
                symbol=SYMBOL,
                effective_date=DAY_2,
                total_shares=2_000_000,
                restricted_shares=None,
                circulating_shares=1_800_000,
                listed_a_shares=1_800_000,
                change_reason="after bonus",
                source_code="akshare",
            ),
        ),
        memberships=(
            ClassificationMembershipSnapshot(
                namespace="eastmoney",
                classification_type=ClassificationType.INDUSTRY,
                classification_code="BK0475",
                snapshot_date=DAY_1,
                members=(SYMBOL,),
            ),
        ),
    )


def test_golden_dataset_covers_adjustment_return_market_cap_and_classification() -> None:
    output = calculate_derived_facts(_inputs(), start_date=DAY_1, end_date=DAY_3)
    forward = {
        record.trade_date: record
        for record in output.adjusted_daily_bars
        if record.adjustment_type is AdjustmentType.FORWARD
    }
    backward = {
        record.trade_date: record
        for record in output.adjusted_daily_bars
        if record.adjustment_type is AdjustmentType.BACKWARD
    }
    daily = {record.trade_date: record for record in output.daily_metrics}
    caps = {record.trade_date: record for record in output.market_capitalizations}
    classification = {record.trade_date: record for record in output.classification_metrics}

    assert forward[DAY_1].adjustment_factor == Decimal("0.5")
    assert forward[DAY_1].close == Decimal("5.0")
    assert forward[DAY_2].close == Decimal("5.2")
    assert backward[DAY_1].close == Decimal("10")
    assert backward[DAY_2].adjustment_factor == Decimal("2")
    assert backward[DAY_2].close == Decimal("10.4")
    assert daily[DAY_2].total_return_1d == Decimal("0.04")
    assert caps[DAY_2].total_market_cap == Decimal("10400000.0")
    assert caps[DAY_2].circulating_market_cap == Decimal("9360000.0")
    assert classification[DAY_2].priced_member_count == 1
    assert classification[DAY_2].advancing_count == 1
    assert classification[DAY_2].equal_weight_return == Decimal("0.04")
    assert classification[DAY_2].membership_snapshot_date == DAY_1


@pytest.mark.parametrize("bonus_ratio", ["0.1", "0.5", "1", "2"])
def test_adjustment_preserves_theoretical_continuity_for_bonus_ratios(
    bonus_ratio: str,
) -> None:
    ratio = Decimal(bonus_ratio)
    theoretical_ex_price = Decimal("12") / (Decimal(1) + ratio)
    bars = (
        _bar(DAY_1, close="12", previous_close="11"),
        _bar(DAY_2, close=str(theoretical_ex_price), previous_close="12"),
    )
    distribution = replace(_distribution(), bonus_share_ratio=ratio)

    output = calculate_adjusted_daily_bars(
        bars,
        distributions=(distribution,),
        rights_issues=(),
        start_date=DAY_1,
        end_date=DAY_2,
    )
    forward = [item for item in output if item.adjustment_type is AdjustmentType.FORWARD]

    assert forward[0].close == forward[1].close


def test_corporate_action_without_matching_daily_bar_is_rejected() -> None:
    with pytest.raises(CalculationInputError, match="no Daily Bar"):
        calculate_adjusted_daily_bars(
            (
                _bar(DAY_1, close="10", previous_close="9"),
                _bar(DAY_3, close="6", previous_close="5.2"),
            ),
            distributions=(_distribution(),),
            rights_issues=(),
            start_date=DAY_1,
            end_date=DAY_3,
        )


def test_new_algorithm_defers_action_to_next_available_daily_bar() -> None:
    output = calculate_adjusted_daily_bars(
        (
            _bar(DAY_1, close="10", previous_close="9"),
            _bar(DAY_3, close="6", previous_close="10"),
        ),
        distributions=(_distribution(),),
        rights_issues=(),
        start_date=DAY_1,
        end_date=DAY_3,
        defer_missing_events=True,
    )

    backward = [item for item in output if item.adjustment_type is AdjustmentType.BACKWARD]
    assert backward[-1].adjustment_factor != Decimal(1)


def test_new_algorithm_anchors_an_event_on_the_first_loaded_bar() -> None:
    first_bar = replace(_bar(DAY_2, close="5", previous_close="10"), previous_close=None)

    output = calculate_adjusted_daily_bars(
        (first_bar, _bar(DAY_3, close="6", previous_close="5")),
        distributions=(_distribution(),),
        rights_issues=(),
        start_date=DAY_2,
        end_date=DAY_3,
        defer_missing_events=True,
    )

    assert {item.adjustment_factor for item in output} == {Decimal(1)}


def test_corporate_action_before_loaded_price_history_is_ignored() -> None:
    old_action = replace(
        _distribution(),
        report_period=date(2020, 12, 31),
        ex_date=date(2021, 7, 28),
    )

    output = calculate_adjusted_daily_bars(
        (_bar(DAY_1, close="10", previous_close="9"),),
        distributions=(old_action,),
        rights_issues=(),
        start_date=DAY_1,
        end_date=DAY_1,
    )

    assert {record.adjustment_factor for record in output} == {Decimal(1)}


def test_moving_average_uses_history_before_requested_output_range() -> None:
    dates = [date(2026, 7, day) for day in range(21, 27)]
    bars = tuple(
        _bar(
            trade_date,
            close=str(index),
            previous_close=str(max(index - 1, 1)),
        )
        for index, trade_date in enumerate(dates, start=1)
    )
    inputs = replace(_inputs(), daily_bars=bars, distributions=(), memberships=())

    output = calculate_derived_facts(
        inputs,
        start_date=dates[-1],
        end_date=dates[-1],
    )

    assert len(output.daily_metrics) == 1
    assert output.daily_metrics[0].moving_average_5 == Decimal("4")


def test_input_hash_is_order_independent_and_changes_with_business_data() -> None:
    inputs = _inputs()
    reordered = replace(inputs, daily_bars=tuple(reversed(inputs.daily_bars)))
    changed = replace(
        inputs,
        daily_bars=(
            replace(
                inputs.daily_bars[0],
                open=Decimal("10.01"),
                high=Decimal("10.01"),
                low=Decimal("10.01"),
                close=Decimal("10.01"),
            ),
            *inputs.daily_bars[1:],
        ),
    )

    assert calculation_input_hash(inputs) == calculation_input_hash(reordered)
    assert calculation_input_hash(inputs) != calculation_input_hash(changed)


def test_security_type_remains_unrelated_to_classification() -> None:
    security = SecurityRecord(
        symbol=SYMBOL,
        code="600000",
        exchange=Exchange.SSE,
        name="浦发银行",
        security_type=SecurityType.STOCK,
        status=SecurityStatus.LISTED,
        ipo_date=None,
        delisting_date=None,
        source_code="baostock",
    )

    assert security.security_type is SecurityType.STOCK
