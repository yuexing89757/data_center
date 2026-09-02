from dataclasses import replace
from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest

from market_data_center.domain.dragon_tiger import (
    DragonTigerEventRecord,
    DragonTigerPeriodType,
    DragonTigerReason,
    DragonTigerReasonType,
    SeatTradeRecord,
)
from market_data_center.dragon_tiger_analytics import (
    NEXT_SESSION_PARTICIPATION_DEFINITION,
    SeatOutcome,
    SeatParticipation,
    build_trading_seat_profile,
    calculate_dragon_tiger_capital_metrics,
)

SEAT_ID = UUID("00000000-0000-0000-0000-000000000001")


def _trade(
    source_id: str,
    buy: str | None,
    sell: str | None,
    buy_rank: int | None,
    sell_rank: int | None,
    *,
    seat_id: UUID | None = SEAT_ID,
    institution: bool = False,
) -> SeatTradeRecord:
    return SeatTradeRecord(
        source_record_id=source_id,
        source_event_id="event-1",
        symbol="SSE:600000",
        trade_date=date(2026, 8, 20),
        seat_id=seat_id,
        seat_source_key=source_id,
        seat_name_raw=source_id,
        buy_amount=None if buy is None else Decimal(buy),
        sell_amount=None if sell is None else Decimal(sell),
        buy_rank=buy_rank,
        sell_rank=sell_rank,
        is_institution=institution,
        is_northbound=False,
        source_code="eastmoney",
    )


def _event() -> DragonTigerEventRecord:
    reason = DragonTigerReason(
        reason_code="PRICE_DEVIATION_DAY",
        reason_name="价格偏离",
        reason_type=DragonTigerReasonType.PRICE_DEVIATION,
        period_type=DragonTigerPeriodType.DAY,
        source_code="eastmoney",
        source_reason_code="01",
        source_reason_name="日价格涨幅偏离值达到7%",
    )
    return DragonTigerEventRecord(
        source_record_id="event-1",
        symbol="SSE:600000",
        trade_date=date(2026, 8, 20),
        period_type=DragonTigerPeriodType.DAY,
        period_start_date=date(2026, 8, 20),
        period_end_date=date(2026, 8, 20),
        reason=reason,
        reason_name_raw=reason.source_reason_name,
        close_price=Decimal("10"),
        change_pct=Decimal("7"),
        turnover_amount=Decimal("1000"),
        turnover_rate=Decimal("8"),
        amplitude=None,
        lhb_buy_amount=Decimal("100"),
        lhb_sell_amount=Decimal("100"),
        seat_trades=(
            _trade("seat-a", "50", "60", 1, 1, institution=True),
            _trade("seat-b", "30", "30", 2, 2),
            _trade("seat-c", "20", None, 3, None, seat_id=None),
        ),
        source_code="eastmoney",
    )


def test_capital_metrics_use_disclosed_totals_and_preserve_unknown_amounts() -> None:
    metrics = calculate_dragon_tiger_capital_metrics(_event())

    assert metrics.net_amount == Decimal("0")
    assert metrics.net_buy_strength == Decimal("0")
    assert metrics.top1_buy_concentration == Decimal("0.5")
    assert metrics.top3_buy_concentration == Decimal("1")
    assert metrics.top1_sell_concentration == Decimal("0.6")
    assert metrics.top3_sell_concentration == Decimal("0.9")
    assert metrics.buy_seat_count == 3
    assert metrics.sell_seat_count == 2
    assert metrics.buy_sell_overlap_count == 2
    assert metrics.pure_buy_seat_count == 0
    assert metrics.institution_net_amount == Decimal("-10")


def test_zero_denominator_yields_missing_concentration() -> None:
    event = replace(_event(), lhb_buy_amount=Decimal("0"))

    assert calculate_dragon_tiger_capital_metrics(event).top1_buy_concentration is None


def test_partial_disclosure_does_not_become_a_partial_concentration_or_flagged_total() -> None:
    event = _event()
    incomplete = replace(
        event,
        seat_trades=(replace(event.seat_trades[0], buy_amount=None), *event.seat_trades[1:]),
    )

    metrics = calculate_dragon_tiger_capital_metrics(incomplete)

    assert metrics.top1_buy_concentration is None
    assert metrics.institution_buy_amount is None


def test_anonymous_institution_rows_are_aggregated_only_on_their_ranked_side() -> None:
    event = replace(
        _event(),
        seat_trades=(
            _trade("inst-buy", "80", "5", 1, None, seat_id=None, institution=True),
            _trade("inst-sell", "7", "30", None, 1, seat_id=None, institution=True),
        ),
    )

    metrics = calculate_dragon_tiger_capital_metrics(event)

    assert metrics.institution_buy_amount == Decimal("80")
    assert metrics.institution_sell_amount == Decimal("30")
    assert metrics.institution_net_amount == Decimal("50")


def test_profile_excludes_labels_not_available_at_the_as_of_date() -> None:
    participation = SeatParticipation(
        event_source_record_id="event-18",
        seat_id=SEAT_ID,
        event_date=date(2026, 8, 18),
        buy_amount=Decimal("100"),
        sell_amount=Decimal("20"),
    )
    labels = (
        SeatOutcome(
            event_source_record_id="event-18",
            seat_id=SEAT_ID,
            event_date=date(2026, 8, 18),
            horizon_sessions=1,
            return_value=Decimal("0.1"),
            label_available_date=date(2026, 8, 19),
            return_definition="unadjusted_close_to_close",
        ),
        SeatOutcome(
            event_source_record_id="event-18",
            seat_id=SEAT_ID,
            event_date=date(2026, 8, 18),
            horizon_sessions=3,
            return_value=Decimal("-0.2"),
            label_available_date=date(2026, 8, 21),
            return_definition="unadjusted_close_to_close",
        ),
    )

    profile = build_trading_seat_profile(
        seat_id=SEAT_ID,
        participations=(participation,),
        outcomes=labels,
        as_of_date=date(2026, 8, 20),
        algorithm_version="seat-profile-v1",
        metric_definition="return_value > 0",
        return_definition="unadjusted_close_to_close",
        participation_definition=NEXT_SESSION_PARTICIPATION_DEFINITION,
        trading_dates=(date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20)),
    )

    assert profile.total_lhb_count == 1
    assert profile.total_buy_amount == Decimal("100")
    assert profile.t1_sample_count == 1
    assert profile.t1_win_rate == Decimal("1")
    assert profile.t1_avg_return == Decimal("0.1")
    assert profile.metric_definition == "return_value > 0"
    assert profile.return_definition == "unadjusted_close_to_close"
    assert profile.consecutive_participation_sample_count == 1
    assert profile.consecutive_participation_rate == Decimal("0")


def test_profile_preserves_missing_participation_amounts() -> None:
    profile = build_trading_seat_profile(
        seat_id=SEAT_ID,
        participations=(
            SeatParticipation(
                event_source_record_id="event-18",
                seat_id=SEAT_ID,
                event_date=date(2026, 8, 18),
                buy_amount=None,
                sell_amount=Decimal("20"),
            ),
        ),
        outcomes=(),
        as_of_date=date(2026, 8, 20),
        algorithm_version="seat-profile-v1",
        metric_definition="return_value > 0",
        return_definition="unadjusted_close_to_close",
        participation_definition=NEXT_SESSION_PARTICIPATION_DEFINITION,
        trading_dates=(date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20)),
    )

    assert profile.total_buy_amount is None
    assert profile.total_sell_amount == Decimal("20")


def test_profile_calculates_versioned_consecutive_session_participation() -> None:
    profile = build_trading_seat_profile(
        seat_id=SEAT_ID,
        participations=tuple(
            SeatParticipation(
                event_source_record_id=f"event-{event_date.isoformat()}",
                seat_id=SEAT_ID,
                event_date=event_date,
                buy_amount=Decimal("10"),
                sell_amount=Decimal("0"),
            )
            for event_date in (date(2026, 8, 18), date(2026, 8, 19))
        ),
        outcomes=(),
        as_of_date=date(2026, 8, 20),
        algorithm_version="seat-profile-v1",
        metric_definition="return_value > 0",
        return_definition="unadjusted_close_to_close",
        participation_definition=NEXT_SESSION_PARTICIPATION_DEFINITION,
        trading_dates=(date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20)),
    )

    assert profile.consecutive_participation_sample_count == 2
    assert profile.consecutive_participation_rate == Decimal("0.5")


def test_profile_rejects_orphan_or_duplicate_event_outcomes() -> None:
    participation = SeatParticipation(
        event_source_record_id="event-18",
        seat_id=SEAT_ID,
        event_date=date(2026, 8, 18),
        buy_amount=Decimal("10"),
        sell_amount=Decimal("0"),
    )
    outcome = SeatOutcome(
        event_source_record_id="orphan",
        seat_id=SEAT_ID,
        event_date=date(2026, 8, 18),
        horizon_sessions=1,
        return_value=Decimal("0.1"),
        label_available_date=date(2026, 8, 19),
        return_definition="unadjusted_close_to_close",
    )
    kwargs = {
        "seat_id": SEAT_ID,
        "participations": (participation,),
        "as_of_date": date(2026, 8, 20),
        "algorithm_version": "seat-profile-v1",
        "metric_definition": "return_value > 0",
        "return_definition": "unadjusted_close_to_close",
        "participation_definition": NEXT_SESSION_PARTICIPATION_DEFINITION,
        "trading_dates": (date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20)),
    }

    with pytest.raises(ValueError, match="matching participation"):
        build_trading_seat_profile(outcomes=(outcome,), **kwargs)
    with pytest.raises(ValueError, match="duplicate event horizon"):
        build_trading_seat_profile(
            outcomes=(
                replace(outcome, event_source_record_id="event-18"),
                replace(outcome, event_source_record_id="event-18"),
            ),
            **kwargs,
        )
