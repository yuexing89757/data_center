"""Time-safe DragonTiger model features and separately typed labels."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from market_data_center.dragon_tiger_analytics import (
    DragonTigerCapitalMetrics,
    TradingSeatProfile,
)

UNADJUSTED_CLOSE_TO_CLOSE = "unadjusted_close_to_close"


@dataclass(frozen=True, slots=True)
class DragonTigerFeature:
    event_source_record_id: str
    symbol: str
    feature_date: date
    period_type: str
    algorithm_version: str
    change_pct: Decimal | None
    turnover_rate: Decimal | None
    net_amount: Decimal | None
    net_buy_strength: Decimal | None
    buy_seat_count: int
    sell_seat_count: int
    pure_buy_seat_count: int
    pure_sell_seat_count: int
    buy_sell_overlap_count: int
    top1_buy_concentration: Decimal | None
    top3_buy_concentration: Decimal | None
    top5_buy_concentration: Decimal | None
    top1_sell_concentration: Decimal | None
    top3_sell_concentration: Decimal | None
    top5_sell_concentration: Decimal | None
    institution_buy_amount: Decimal | None
    institution_sell_amount: Decimal | None
    institution_net_amount: Decimal | None
    northbound_buy_amount: Decimal | None
    northbound_sell_amount: Decimal | None
    northbound_net_amount: Decimal | None
    profile_count: int
    profile_algorithm_version: str | None
    profile_metric_definition: str | None
    profile_return_definition: str | None
    profile_participation_definition: str | None
    profile_total_lhb_count: int
    profile_total_buy_amount: Decimal | None
    profile_total_sell_amount: Decimal | None
    profile_t1_sample_count: int
    profile_t1_win_rate: Decimal | None
    profile_t1_avg_return: Decimal | None
    profile_t3_sample_count: int
    profile_t3_win_rate: Decimal | None
    profile_t3_avg_return: Decimal | None
    profile_t5_sample_count: int
    profile_t5_win_rate: Decimal | None
    profile_t5_avg_return: Decimal | None
    profile_consecutive_participation_sample_count: int
    profile_consecutive_participation_rate: Decimal | None


@dataclass(frozen=True, slots=True)
class DragonTigerLabel:
    event_source_record_id: str
    event_date: date
    horizon_sessions: int
    return_value: Decimal
    label_available_date: date
    return_definition: str

    def __post_init__(self) -> None:
        if self.horizon_sessions not in {1, 3, 5}:
            raise ValueError("label horizon must be 1, 3 or 5 sessions")
        if self.label_available_date <= self.event_date:
            raise ValueError("label must become available after its event")
        if not self.return_definition.strip():
            raise ValueError("return_definition must not be blank")


@dataclass(frozen=True, slots=True)
class FollowingSessionClose:
    trade_date: date
    close: Decimal | None

    def __post_init__(self) -> None:
        if self.close is not None and self.close <= 0:
            raise ValueError("following session close must be positive")


def build_dragon_tiger_labels(
    *,
    event_source_record_id: str,
    event_date: date,
    event_close: Decimal,
    following_trading_dates: tuple[date, ...],
    following_session_closes: tuple[FollowingSessionClose, ...],
    as_of_date: date,
    return_definition: str,
) -> tuple[DragonTigerLabel, ...]:
    if event_close <= 0:
        raise ValueError("event close must be positive")
    if return_definition != UNADJUSTED_CLOSE_TO_CLOSE:
        raise ValueError("unsupported DragonTiger label return_definition")
    if tuple(sorted(set(following_trading_dates))) != following_trading_dates:
        raise ValueError("following trading dates must be unique and ascending")
    if any(item <= event_date for item in following_trading_dates):
        raise ValueError("following trading dates must be after the event")
    session_dates = tuple(item.trade_date for item in following_session_closes)
    if session_dates != following_trading_dates:
        raise ValueError("session closes must align exactly with the trading calendar")
    labels: list[DragonTigerLabel] = []
    for horizon in (1, 3, 5):
        if len(following_session_closes) < horizon:
            continue
        observation = following_session_closes[horizon - 1]
        if observation.trade_date > as_of_date or observation.close is None:
            continue
        labels.append(
            DragonTigerLabel(
                event_source_record_id=event_source_record_id,
                event_date=event_date,
                horizon_sessions=horizon,
                return_value=observation.close / event_close - Decimal(1),
                label_available_date=observation.trade_date,
                return_definition=return_definition,
            )
        )
    return tuple(labels)


def build_dragon_tiger_feature(
    *,
    event_source_record_id: str,
    symbol: str,
    feature_date: date,
    period_type: str,
    change_pct: Decimal | None,
    turnover_rate: Decimal | None,
    metrics: DragonTigerCapitalMetrics,
    seat_profiles: tuple[TradingSeatProfile, ...],
    algorithm_version: str,
) -> DragonTigerFeature:
    if metrics.event_source_record_id != event_source_record_id:
        raise ValueError("feature metrics do not match the event")
    if any(profile.as_of_date >= feature_date for profile in seat_profiles):
        raise ValueError("feature profiles must be from strictly before the feature date")
    if period_type not in {"DAY", "THREE_DAY"}:
        raise ValueError("period_type must be DAY or THREE_DAY")
    if not algorithm_version.strip():
        raise ValueError("algorithm_version must not be blank")
    metric_definitions = {profile.metric_definition for profile in seat_profiles}
    return_definitions = {profile.return_definition for profile in seat_profiles}
    profile_versions = {profile.algorithm_version for profile in seat_profiles}
    participation_definitions = {profile.participation_definition for profile in seat_profiles}
    if len(metric_definitions) > 1 or len(return_definitions) > 1:
        raise ValueError("feature requires consistent profile metric definitions")
    if len(profile_versions) > 1:
        raise ValueError("feature requires consistent profile algorithm versions")
    if len(participation_definitions) > 1:
        raise ValueError("feature requires consistent profile participation definitions")
    t1_samples = sum(profile.t1_sample_count for profile in seat_profiles)
    t3_samples = sum(profile.t3_sample_count for profile in seat_profiles)
    t5_samples = sum(profile.t5_sample_count for profile in seat_profiles)
    return DragonTigerFeature(
        event_source_record_id=event_source_record_id,
        symbol=symbol,
        feature_date=feature_date,
        period_type=period_type,
        algorithm_version=algorithm_version,
        change_pct=change_pct,
        turnover_rate=turnover_rate,
        net_amount=metrics.net_amount,
        net_buy_strength=metrics.net_buy_strength,
        buy_seat_count=metrics.buy_seat_count,
        sell_seat_count=metrics.sell_seat_count,
        pure_buy_seat_count=metrics.pure_buy_seat_count,
        pure_sell_seat_count=metrics.pure_sell_seat_count,
        buy_sell_overlap_count=metrics.buy_sell_overlap_count,
        top1_buy_concentration=metrics.top1_buy_concentration,
        top3_buy_concentration=metrics.top3_buy_concentration,
        top5_buy_concentration=metrics.top5_buy_concentration,
        top1_sell_concentration=metrics.top1_sell_concentration,
        top3_sell_concentration=metrics.top3_sell_concentration,
        top5_sell_concentration=metrics.top5_sell_concentration,
        institution_buy_amount=metrics.institution_buy_amount,
        institution_sell_amount=metrics.institution_sell_amount,
        institution_net_amount=metrics.institution_net_amount,
        northbound_buy_amount=metrics.northbound_buy_amount,
        northbound_sell_amount=metrics.northbound_sell_amount,
        northbound_net_amount=metrics.northbound_net_amount,
        profile_count=len(seat_profiles),
        profile_algorithm_version=next(iter(profile_versions), None),
        profile_metric_definition=next(iter(metric_definitions), None),
        profile_return_definition=next(iter(return_definitions), None),
        profile_participation_definition=next(iter(participation_definitions), None),
        profile_total_lhb_count=sum(profile.total_lhb_count for profile in seat_profiles),
        profile_total_buy_amount=_complete_profile_total(seat_profiles, "buy"),
        profile_total_sell_amount=_complete_profile_total(seat_profiles, "sell"),
        profile_t1_sample_count=t1_samples,
        profile_t1_win_rate=_weighted_profile_value(
            tuple((profile.t1_win_rate, profile.t1_sample_count) for profile in seat_profiles)
        ),
        profile_t1_avg_return=_weighted_profile_value(
            tuple((profile.t1_avg_return, profile.t1_sample_count) for profile in seat_profiles)
        ),
        profile_t3_sample_count=t3_samples,
        profile_t3_win_rate=_weighted_profile_value(
            tuple((profile.t3_win_rate, profile.t3_sample_count) for profile in seat_profiles)
        ),
        profile_t3_avg_return=_weighted_profile_value(
            tuple((profile.t3_avg_return, profile.t3_sample_count) for profile in seat_profiles)
        ),
        profile_t5_sample_count=t5_samples,
        profile_t5_win_rate=_weighted_profile_value(
            tuple((profile.t5_win_rate, profile.t5_sample_count) for profile in seat_profiles)
        ),
        profile_t5_avg_return=_weighted_profile_value(
            tuple((profile.t5_avg_return, profile.t5_sample_count) for profile in seat_profiles)
        ),
        profile_consecutive_participation_sample_count=sum(
            profile.consecutive_participation_sample_count for profile in seat_profiles
        ),
        profile_consecutive_participation_rate=_weighted_profile_value(
            tuple(
                (
                    profile.consecutive_participation_rate,
                    profile.consecutive_participation_sample_count,
                )
                for profile in seat_profiles
            )
        ),
    )


def _weighted_profile_value(values: tuple[tuple[Decimal | None, int], ...]) -> Decimal | None:
    numerator = Decimal(0)
    counted = 0
    for value, sample_count in values:
        if value is None or sample_count == 0:
            continue
        numerator += value * sample_count
        counted += sample_count
    return numerator / counted if counted else None


def _complete_profile_total(profiles: tuple[TradingSeatProfile, ...], side: str) -> Decimal | None:
    amounts = tuple(
        profile.total_buy_amount if side == "buy" else profile.total_sell_amount
        for profile in profiles
    )
    if any(amount is None for amount in amounts):
        return None
    return sum((amount for amount in amounts if amount is not None), Decimal(0))
