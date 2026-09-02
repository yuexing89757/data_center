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
    TradingSeat,
    TradingSeatAlias,
    TradingSeatType,
    dragon_tiger_content_hash,
    validate_dragon_tiger_events,
)


def _reason(
    period_type: DragonTigerPeriodType = DragonTigerPeriodType.DAY,
) -> DragonTigerReason:
    return DragonTigerReason(
        reason_code=f"PRICE_DEVIATION_{period_type.value}",
        reason_name="价格偏离",
        reason_type=DragonTigerReasonType.PRICE_DEVIATION,
        period_type=period_type,
        source_code="eastmoney",
        source_reason_code="01",
        source_reason_name="日价格涨幅偏离值达到7%",
    )


def _trade(**overrides: object) -> SeatTradeRecord:
    values: dict[str, object] = {
        "source_record_id": "event-1:seat-1",
        "source_event_id": "event-1",
        "symbol": "SSE:600000",
        "trade_date": date(2026, 8, 20),
        "seat_id": UUID("00000000-0000-0000-0000-000000000001"),
        "seat_source_key": "seat-1",
        "seat_name_raw": "某证券营业部",
        "buy_amount": Decimal("100"),
        "sell_amount": Decimal("20"),
        "buy_rank": 1,
        "sell_rank": 3,
        "is_institution": False,
        "is_northbound": False,
        "source_code": "eastmoney",
    }
    values.update(overrides)
    return SeatTradeRecord(**values)  # type: ignore[arg-type]


def _event(**overrides: object) -> DragonTigerEventRecord:
    values: dict[str, object] = {
        "source_record_id": "event-1",
        "symbol": "SSE:600000",
        "trade_date": date(2026, 8, 20),
        "period_type": DragonTigerPeriodType.DAY,
        "period_start_date": date(2026, 8, 20),
        "period_end_date": date(2026, 8, 20),
        "reason": _reason(),
        "reason_name_raw": "日价格涨幅偏离值达到7%",
        "close_price": Decimal("12.34"),
        "change_pct": Decimal("7.10"),
        "turnover_amount": Decimal("1000"),
        "turnover_rate": Decimal("8.2"),
        "amplitude": None,
        "lhb_buy_amount": Decimal("100"),
        "lhb_sell_amount": Decimal("20"),
        "seat_trades": (_trade(),),
        "source_code": "eastmoney",
    }
    values.update(overrides)
    return DragonTigerEventRecord(**values)  # type: ignore[arg-type]


def test_day_event_uses_one_trading_date_for_the_period() -> None:
    event = _event()

    assert event.period_start_date == event.trade_date
    assert event.period_end_date == event.trade_date


def test_three_day_event_requires_an_explicit_calendar_period() -> None:
    event = _event(
        period_type=DragonTigerPeriodType.THREE_DAY,
        period_start_date=date(2026, 8, 18),
        reason=_reason(DragonTigerPeriodType.THREE_DAY),
    )

    assert event.period_start_date == date(2026, 8, 18)
    assert event.period_end_date == event.trade_date


def test_three_day_event_rejects_a_same_day_period() -> None:
    with pytest.raises(ValueError, match="three-day event"):
        _event(
            period_type=DragonTigerPeriodType.THREE_DAY,
            reason=_reason(DragonTigerPeriodType.THREE_DAY),
        )


def test_missing_opposing_amount_is_not_zero_or_pure_buy() -> None:
    trade = _trade(sell_amount=None, sell_rank=None)

    assert trade.net_amount is None
    assert trade.is_pure_buy is False
    assert trade.is_buy_and_sell is False


def test_disclosed_zero_sell_is_pure_buy_and_has_computable_net() -> None:
    trade = _trade(sell_amount=Decimal("0"), sell_rank=None)

    assert trade.net_amount == Decimal("100")
    assert trade.is_pure_buy is True


def test_seat_trade_rejects_two_explicit_zero_amounts() -> None:
    with pytest.raises(ValueError, match="cannot both be zero"):
        _trade(buy_amount=Decimal("0"), sell_amount=Decimal("0"))


def test_reliable_seat_can_carry_both_ranks_in_one_trade() -> None:
    trade = _trade()

    assert trade.buy_rank == 1
    assert trade.sell_rank == 3
    assert trade.is_buy_and_sell is True


def test_anonymous_institution_does_not_claim_a_stable_seat_identity() -> None:
    trade = _trade(
        source_record_id="event-1:buy:2",
        seat_id=None,
        seat_source_key=None,
        seat_name_raw="机构专用",
        sell_amount=None,
        sell_rank=None,
        buy_rank=2,
        is_institution=True,
    )

    assert trade.seat_id is None
    assert trade.is_institution is True


def test_trading_seat_alias_preserves_the_source_name() -> None:
    seat_id = UUID("00000000-0000-0000-0000-000000000001")
    seat = TradingSeat(
        seat_id=seat_id,
        canonical_name="某证券营业部",
        broker_name="某证券",
        branch_name="营业部",
        seat_type=TradingSeatType.BROKER,
        province=None,
        city=None,
        first_seen_date=date(2026, 8, 20),
        last_seen_date=date(2026, 8, 20),
        is_active=True,
    )
    alias = TradingSeatAlias(
        seat_id=seat_id,
        source_code="eastmoney",
        source_seat_key="seat-1",
        alias_name="某证券股份有限公司营业部",
    )

    assert alias.seat_id == seat.seat_id
    assert alias.alias_name != seat.canonical_name


def test_validation_accepts_a_known_bse_security() -> None:
    trade = _trade(symbol="BSE:920000")
    event = _event(symbol="BSE:920000", seat_trades=(trade,))

    result = validate_dragon_tiger_events(
        (event,),
        known_symbols={"BSE:920000"},
        known_trading_dates={date(2026, 8, 20)},
    )

    assert result.accepted == (event,)
    assert result.findings == ()


def test_validation_rejects_a_seat_parent_mismatch() -> None:
    event = _event(seat_trades=(_trade(symbol="SZSE:000001"),))

    result = validate_dragon_tiger_events(
        (event,),
        known_symbols={"SSE:600000", "SZSE:000001"},
        known_trading_dates={date(2026, 8, 20)},
    )

    assert result.accepted == ()
    assert result.findings[0].rule_code == "dragon_tiger.seat_parent_mismatch"


def test_content_hash_is_deterministic_and_excludes_calculated_net() -> None:
    first = _event()
    second = _event()

    assert dragon_tiger_content_hash(first) == dragon_tiger_content_hash(second)
    assert len(dragon_tiger_content_hash(first)) == 64
