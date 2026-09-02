from dataclasses import fields, replace
from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest

from market_data_center.dragon_tiger_analytics import (
    DragonTigerCapitalMetrics,
    TradingSeatProfile,
)
from market_data_center.dragon_tiger_features import (
    UNADJUSTED_CLOSE_TO_CLOSE,
    DragonTigerFeature,
    DragonTigerLabel,
    FollowingSessionClose,
    build_dragon_tiger_feature,
    build_dragon_tiger_labels,
)


def _metrics() -> DragonTigerCapitalMetrics:
    return DragonTigerCapitalMetrics(
        event_source_record_id="event-1",
        net_amount=Decimal("80"),
        net_buy_strength=Decimal("0.08"),
        buy_seat_count=2,
        sell_seat_count=1,
        pure_buy_seat_count=1,
        pure_sell_seat_count=0,
        buy_sell_overlap_count=1,
        top1_buy_concentration=Decimal("0.6"),
        top3_buy_concentration=Decimal("1"),
        top5_buy_concentration=Decimal("1"),
        top1_sell_concentration=Decimal("1"),
        top3_sell_concentration=Decimal("1"),
        top5_sell_concentration=Decimal("1"),
        institution_buy_amount=Decimal("60"),
        institution_sell_amount=Decimal("20"),
        institution_net_amount=Decimal("40"),
        northbound_buy_amount=None,
        northbound_sell_amount=None,
        northbound_net_amount=None,
    )


def _profile(as_of_date: date) -> TradingSeatProfile:
    return TradingSeatProfile(
        seat_id=UUID("00000000-0000-0000-0000-000000000001"),
        as_of_date=as_of_date,
        algorithm_version="seat-profile-v1",
        metric_definition="return_value > 0",
        return_definition="unadjusted_close_to_close",
        participation_definition=("next_session_participation / eligible_participation_sessions"),
        total_lhb_count=3,
        total_buy_amount=Decimal("300"),
        total_sell_amount=Decimal("100"),
        t1_sample_count=2,
        t1_win_rate=Decimal("0.5"),
        t1_avg_return=Decimal("0.02"),
        t3_sample_count=0,
        t3_win_rate=None,
        t3_avg_return=None,
        t5_sample_count=0,
        t5_win_rate=None,
        t5_avg_return=None,
        consecutive_participation_sample_count=2,
        consecutive_participation_rate=Decimal("0.5"),
    )


def test_feature_type_contains_no_forward_return_or_label_fields() -> None:
    names = {item.name for item in fields(DragonTigerFeature)}

    assert not any("label" in name for name in names)
    assert "target_return" not in names
    assert "forward_return" not in names


@pytest.mark.parametrize("profile_date", [date(2026, 8, 20), date(2026, 8, 21)])
def test_feature_requires_profiles_from_strictly_before_the_feature_date(
    profile_date: date,
) -> None:
    with pytest.raises(ValueError, match="strictly before"):
        build_dragon_tiger_feature(
            event_source_record_id="event-1",
            symbol="SSE:600000",
            feature_date=date(2026, 8, 20),
            period_type="DAY",
            change_pct=Decimal("7"),
            turnover_rate=Decimal("8"),
            metrics=_metrics(),
            seat_profiles=(_profile(profile_date),),
            algorithm_version="dragon-tiger-feature-v1",
        )


def test_label_is_a_separate_type_with_an_availability_date() -> None:
    label = DragonTigerLabel(
        event_source_record_id="event-1",
        event_date=date(2026, 8, 20),
        horizon_sessions=1,
        return_value=Decimal("0.1"),
        label_available_date=date(2026, 8, 21),
        return_definition="unadjusted_close_to_close",
    )

    assert label.label_available_date > date(2026, 8, 20)


def test_label_builder_emits_only_observations_available_as_of_date() -> None:
    trading_dates = (
        date(2026, 8, 21),
        date(2026, 8, 24),
        date(2026, 8, 25),
        date(2026, 8, 26),
        date(2026, 8, 27),
    )
    closes = tuple(
        FollowingSessionClose(trade_date=trade_date, close=close)
        for trade_date, close in zip(
            trading_dates,
            (Decimal("11"), Decimal("12"), Decimal("13"), Decimal("14"), Decimal("15")),
            strict=True,
        )
    )

    labels = build_dragon_tiger_labels(
        event_source_record_id="event-1",
        event_date=date(2026, 8, 20),
        event_close=Decimal("10"),
        following_trading_dates=trading_dates,
        following_session_closes=closes,
        as_of_date=date(2026, 8, 25),
        return_definition=UNADJUSTED_CLOSE_TO_CLOSE,
    )

    assert [label.horizon_sessions for label in labels] == [1, 3]
    assert [label.return_value for label in labels] == [Decimal("0.1"), Decimal("0.3")]


def test_label_builder_preserves_a_missing_bar_at_its_exact_horizon() -> None:
    trading_dates = (date(2026, 8, 21), date(2026, 8, 24), date(2026, 8, 25))
    labels = build_dragon_tiger_labels(
        event_source_record_id="event-1",
        event_date=date(2026, 8, 20),
        event_close=Decimal("10"),
        following_trading_dates=trading_dates,
        following_session_closes=(
            FollowingSessionClose(trade_date=trading_dates[0], close=None),
            FollowingSessionClose(trade_date=trading_dates[1], close=Decimal("12")),
            FollowingSessionClose(trade_date=trading_dates[2], close=Decimal("13")),
        ),
        as_of_date=date(2026, 8, 25),
        return_definition=UNADJUSTED_CLOSE_TO_CLOSE,
    )

    assert [label.horizon_sessions for label in labels] == [3]


def test_feature_keeps_objective_capital_and_profile_components() -> None:
    feature = build_dragon_tiger_feature(
        event_source_record_id="event-1",
        symbol="SSE:600000",
        feature_date=date(2026, 8, 20),
        period_type="DAY",
        change_pct=Decimal("7"),
        turnover_rate=Decimal("8"),
        metrics=_metrics(),
        seat_profiles=(_profile(date(2026, 8, 19)),),
        algorithm_version="dragon-tiger-feature-v1",
    )

    assert feature.pure_buy_seat_count == 1
    assert feature.institution_net_amount == Decimal("40")
    assert feature.profile_total_lhb_count == 3
    assert feature.profile_t1_sample_count == 2
    assert feature.profile_t3_sample_count == 0
    assert feature.profile_t5_sample_count == 0
    assert feature.profile_consecutive_participation_rate == Decimal("0.5")


def test_feature_rejects_mixed_profile_algorithm_versions() -> None:
    profiles = (
        _profile(date(2026, 8, 19)),
        replace(
            _profile(date(2026, 8, 19)),
            seat_id=UUID("00000000-0000-0000-0000-000000000002"),
            algorithm_version="seat-profile-v2",
        ),
    )

    with pytest.raises(ValueError, match="algorithm versions"):
        build_dragon_tiger_feature(
            event_source_record_id="event-1",
            symbol="SSE:600000",
            feature_date=date(2026, 8, 20),
            period_type="DAY",
            change_pct=Decimal("7"),
            turnover_rate=Decimal("8"),
            metrics=_metrics(),
            seat_profiles=profiles,
            algorithm_version="dragon-tiger-feature-v1",
        )
