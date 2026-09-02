"""Pure, objective analytics over DragonTiger facts."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from uuid import UUID

from market_data_center.domain.dragon_tiger import DragonTigerEventRecord, SeatTradeRecord

POSITIVE_RETURN_WIN_DEFINITION = "return_value > 0"
NEXT_SESSION_PARTICIPATION_DEFINITION = (
    "next_session_participation / eligible_participation_sessions"
)


@dataclass(frozen=True, slots=True)
class DragonTigerCapitalMetrics:
    event_source_record_id: str
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


@dataclass(frozen=True, slots=True)
class SeatParticipation:
    event_source_record_id: str
    seat_id: UUID
    event_date: date
    buy_amount: Decimal | None
    sell_amount: Decimal | None

    def __post_init__(self) -> None:
        if not self.event_source_record_id.strip():
            raise ValueError("seat participation event identity must not be blank")


@dataclass(frozen=True, slots=True)
class SeatOutcome:
    event_source_record_id: str
    seat_id: UUID
    event_date: date
    horizon_sessions: int
    return_value: Decimal
    label_available_date: date
    return_definition: str

    def __post_init__(self) -> None:
        if not self.event_source_record_id.strip():
            raise ValueError("seat outcome event identity must not be blank")
        if self.horizon_sessions not in {1, 3, 5}:
            raise ValueError("seat outcome horizon must be 1, 3 or 5 sessions")
        if self.label_available_date <= self.event_date:
            raise ValueError("seat outcome must become available after its event")
        if not self.return_definition.strip():
            raise ValueError("seat outcome return_definition must not be blank")


@dataclass(frozen=True, slots=True)
class TradingSeatProfile:
    seat_id: UUID
    as_of_date: date
    algorithm_version: str
    metric_definition: str
    return_definition: str
    participation_definition: str
    total_lhb_count: int
    total_buy_amount: Decimal | None
    total_sell_amount: Decimal | None
    t1_sample_count: int
    t1_win_rate: Decimal | None
    t1_avg_return: Decimal | None
    t3_sample_count: int
    t3_win_rate: Decimal | None
    t3_avg_return: Decimal | None
    t5_sample_count: int
    t5_win_rate: Decimal | None
    t5_avg_return: Decimal | None
    consecutive_participation_sample_count: int
    consecutive_participation_rate: Decimal | None


def calculate_dragon_tiger_capital_metrics(
    event: DragonTigerEventRecord,
) -> DragonTigerCapitalMetrics:
    net_amount = _difference(event.lhb_buy_amount, event.lhb_sell_amount)
    return DragonTigerCapitalMetrics(
        event_source_record_id=event.source_record_id,
        net_amount=net_amount,
        net_buy_strength=_ratio(net_amount, event.turnover_amount),
        buy_seat_count=sum(trade.buy_rank is not None for trade in event.seat_trades),
        sell_seat_count=sum(trade.sell_rank is not None for trade in event.seat_trades),
        pure_buy_seat_count=sum(trade.is_pure_buy for trade in event.seat_trades),
        pure_sell_seat_count=sum(trade.is_pure_sell for trade in event.seat_trades),
        buy_sell_overlap_count=sum(
            trade.buy_rank is not None and trade.sell_rank is not None
            for trade in event.seat_trades
        ),
        top1_buy_concentration=_concentration(event, "buy", 1),
        top3_buy_concentration=_concentration(event, "buy", 3),
        top5_buy_concentration=_concentration(event, "buy", 5),
        top1_sell_concentration=_concentration(event, "sell", 1),
        top3_sell_concentration=_concentration(event, "sell", 3),
        top5_sell_concentration=_concentration(event, "sell", 5),
        institution_buy_amount=_flagged_total(event.seat_trades, "buy", "institution"),
        institution_sell_amount=_flagged_total(event.seat_trades, "sell", "institution"),
        institution_net_amount=_flagged_net(event.seat_trades, "institution"),
        northbound_buy_amount=_flagged_total(event.seat_trades, "buy", "northbound"),
        northbound_sell_amount=_flagged_total(event.seat_trades, "sell", "northbound"),
        northbound_net_amount=_flagged_net(event.seat_trades, "northbound"),
    )


def build_trading_seat_profile(
    *,
    seat_id: UUID,
    participations: tuple[SeatParticipation, ...],
    outcomes: tuple[SeatOutcome, ...],
    as_of_date: date,
    algorithm_version: str,
    metric_definition: str,
    return_definition: str,
    participation_definition: str,
    trading_dates: tuple[date, ...],
) -> TradingSeatProfile:
    if not algorithm_version.strip():
        raise ValueError("algorithm_version must not be blank")
    if metric_definition != POSITIVE_RETURN_WIN_DEFINITION:
        raise ValueError("unsupported seat profile metric_definition")
    if not return_definition.strip():
        raise ValueError("return_definition must not be blank")
    if participation_definition != NEXT_SESSION_PARTICIPATION_DEFINITION:
        raise ValueError("unsupported seat profile participation_definition")
    if tuple(sorted(set(trading_dates))) != trading_dates:
        raise ValueError("trading_dates must be unique and ascending")
    known_trading_dates = tuple(item for item in trading_dates if item <= as_of_date)
    known_participations = tuple(
        item for item in participations if item.seat_id == seat_id and item.event_date <= as_of_date
    )
    participation_by_event = {item.event_source_record_id: item for item in known_participations}
    if len(participation_by_event) != len(known_participations):
        raise ValueError("seat profile contains duplicate participation event identities")
    candidate_outcomes = tuple(
        item for item in outcomes if item.seat_id == seat_id and item.event_date <= as_of_date
    )
    outcome_keys = {
        (item.event_source_record_id, item.horizon_sessions) for item in candidate_outcomes
    }
    if len(outcome_keys) != len(candidate_outcomes):
        raise ValueError("seat profile contains duplicate event horizon outcomes")
    for outcome in candidate_outcomes:
        participation = participation_by_event.get(outcome.event_source_record_id)
        if participation is None or participation.event_date != outcome.event_date:
            raise ValueError("seat profile outcome has no matching participation event")
    known_outcomes = tuple(
        item for item in candidate_outcomes if item.label_available_date <= as_of_date
    )
    if any(item.return_definition != return_definition for item in known_outcomes):
        raise ValueError("seat outcome return_definition does not match the profile")
    participation_dates = {item.event_date for item in known_participations}
    if not participation_dates.issubset(known_trading_dates):
        raise ValueError("seat participation date is absent from the supplied trading calendar")
    horizon_metrics = {horizon: _outcome_metrics(known_outcomes, horizon) for horizon in (1, 3, 5)}
    consecutive_samples, consecutive_rate = _consecutive_participation_metrics(
        participation_dates, known_trading_dates
    )
    return TradingSeatProfile(
        seat_id=seat_id,
        as_of_date=as_of_date,
        algorithm_version=algorithm_version,
        metric_definition=metric_definition,
        return_definition=return_definition,
        participation_definition=participation_definition,
        total_lhb_count=len(known_participations),
        total_buy_amount=_complete_participation_total(known_participations, "buy"),
        total_sell_amount=_complete_participation_total(known_participations, "sell"),
        t1_sample_count=horizon_metrics[1][0],
        t1_win_rate=horizon_metrics[1][1],
        t1_avg_return=horizon_metrics[1][2],
        t3_sample_count=horizon_metrics[3][0],
        t3_win_rate=horizon_metrics[3][1],
        t3_avg_return=horizon_metrics[3][2],
        t5_sample_count=horizon_metrics[5][0],
        t5_win_rate=horizon_metrics[5][1],
        t5_avg_return=horizon_metrics[5][2],
        consecutive_participation_sample_count=consecutive_samples,
        consecutive_participation_rate=consecutive_rate,
    )


def _concentration(event: DragonTigerEventRecord, side: str, top_n: int) -> Decimal | None:
    denominator = event.lhb_buy_amount if side == "buy" else event.lhb_sell_amount
    if denominator is None or denominator == 0:
        return None
    ranked: list[tuple[int, Decimal]] = []
    for trade in event.seat_trades:
        rank = trade.buy_rank if side == "buy" else trade.sell_rank
        amount = trade.buy_amount if side == "buy" else trade.sell_amount
        if rank is not None and rank <= top_n:
            if amount is None:
                return None
            ranked.append((rank, amount))
    return sum((amount for _, amount in ranked), Decimal(0)) / denominator


def _difference(left: Decimal | None, right: Decimal | None) -> Decimal | None:
    if left is None or right is None:
        return None
    return left - right


def _ratio(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _flagged_total(trades: tuple[SeatTradeRecord, ...], side: str, flag: str) -> Decimal | None:
    matching = tuple(
        trade
        for trade in trades
        if (trade.is_institution if flag == "institution" else trade.is_northbound)
        and (trade.buy_rank if side == "buy" else trade.sell_rank) is not None
    )
    amounts = tuple(
        amount
        for trade in matching
        if (amount := trade.buy_amount if side == "buy" else trade.sell_amount) is not None
    )
    if not matching or len(amounts) != len(matching):
        return None
    return sum(amounts, Decimal(0))


def _flagged_net(trades: tuple[SeatTradeRecord, ...], flag: str) -> Decimal | None:
    buy_total = _flagged_total(trades, "buy", flag)
    sell_total = _flagged_total(trades, "sell", flag)
    if buy_total is None or sell_total is None:
        return None
    return buy_total - sell_total


def _outcome_metrics(
    outcomes: tuple[SeatOutcome, ...], horizon: int
) -> tuple[int, Decimal | None, Decimal | None]:
    values = tuple(item.return_value for item in outcomes if item.horizon_sessions == horizon)
    if not values:
        return 0, None, None
    wins = sum(value > 0 for value in values)
    count = len(values)
    return count, Decimal(wins) / Decimal(count), sum(values, Decimal(0)) / Decimal(count)


def _complete_participation_total(
    participations: tuple[SeatParticipation, ...], side: str
) -> Decimal | None:
    amounts = tuple(
        item.buy_amount if side == "buy" else item.sell_amount for item in participations
    )
    if any(amount is None for amount in amounts):
        return None
    return sum((amount for amount in amounts if amount is not None), Decimal(0))


def _consecutive_participation_metrics(
    participation_dates: set[date], trading_dates: tuple[date, ...]
) -> tuple[int, Decimal | None]:
    next_sessions = {
        trading_dates[index]: trading_dates[index + 1] for index in range(len(trading_dates) - 1)
    }
    eligible = tuple(day for day in participation_dates if day in next_sessions)
    if not eligible:
        return 0, None
    consecutive = sum(next_sessions[day] in participation_dates for day in eligible)
    return len(eligible), Decimal(consecutive) / Decimal(len(eligible))
