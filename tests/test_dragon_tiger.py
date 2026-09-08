from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest

from market_data_center.domain.dragon_tiger import (
    DragonTigerAmountPeriod,
    DragonTigerAmountPeriodBasis,
    DragonTigerEventDraft,
    DragonTigerEventRecord,
    DragonTigerReason,
    DragonTigerReasonType,
    DragonTigerTriggerWindow,
    DragonTigerWindowBasis,
    SeatTradeRecord,
    TradingSeat,
    TradingSeatAlias,
    TradingSeatSourceIdentity,
    TradingSeatType,
    dragon_tiger_content_hash,
    validate_dragon_tiger_events,
)

TRADE_DATE = date(2026, 8, 20)


def _reason() -> DragonTigerReason:
    return DragonTigerReason(
        reason_code="PRICE_DEVIATION_MARKET_1",
        reason_name="价格偏离",
        reason_type=DragonTigerReasonType.PRICE_DEVIATION,
        source_code="eastmoney",
        source_reason_code="01",
        source_reason_name="日价格涨幅偏离值达到7%",
    )


def _trigger(**overrides: object) -> DragonTigerTriggerWindow:
    values: dict[str, object] = {
        "basis": DragonTigerWindowBasis.MARKET_SESSIONS,
        "session_count": 1,
        "occurrence_count": None,
        "start_date": TRADE_DATE,
        "end_date": TRADE_DATE,
    }
    values.update(overrides)
    return DragonTigerTriggerWindow(**values)  # type: ignore[arg-type]


def _amount_period(**overrides: object) -> DragonTigerAmountPeriod:
    values: dict[str, object] = {
        "basis": DragonTigerAmountPeriodBasis.MARKET_SESSIONS,
        "session_count": 1,
        "start_date": TRADE_DATE,
        "end_date": TRADE_DATE,
    }
    values.update(overrides)
    return DragonTigerAmountPeriod(**values)  # type: ignore[arg-type]


def _trade(**overrides: object) -> SeatTradeRecord:
    values: dict[str, object] = {
        "source_record_id": "event-1:seat-1",
        "source_event_id": "event-1",
        "symbol": "SSE:600000",
        "trade_date": TRADE_DATE,
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
        "trade_date": TRADE_DATE,
        "trigger_window": _trigger(),
        "amount_period": _amount_period(),
        "reason": _reason(),
        "reason_name_raw": "日价格涨幅偏离值达到7%",
        "close_price": Decimal("12.34"),
        "change_pct": Decimal("7.10"),
        "turnover_amount": Decimal("1000"),
        "turnover_rate": Decimal("8.2"),
        "amplitude": None,
        "lhb_buy_amount": Decimal("100"),
        "lhb_sell_amount": Decimal("20"),
        "buy_disclosure_present": True,
        "sell_disclosure_present": True,
        "seat_trades": (_trade(),),
        "source_code": "eastmoney",
    }
    values.update(overrides)
    return DragonTigerEventRecord(**values)  # type: ignore[arg-type]


def test_event_keeps_trigger_window_separate_from_unspecified_amount_period() -> None:
    trigger = _trigger(
        session_count=10,
        occurrence_count=4,
        start_date=None,
    )
    amount = DragonTigerAmountPeriod(
        basis=DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED,
        session_count=None,
        start_date=None,
        end_date=None,
    )
    draft = DragonTigerEventDraft(
        source_record_id="event-1",
        symbol="SSE:600000",
        trade_date=TRADE_DATE,
        trigger_window=trigger,
        amount_period=amount,
        reason=_reason(),
        reason_name_raw="连续10个交易日内4次出现同正向异常波动的证券",
        close_price=Decimal("12.34"),
        change_pct=Decimal("7.10"),
        turnover_amount=Decimal("1000"),
        turnover_rate=Decimal("8.2"),
        amplitude=None,
        lhb_buy_amount=Decimal("100"),
        lhb_sell_amount=Decimal("20"),
        buy_disclosure_present=True,
        sell_disclosure_present=True,
        seat_trades=(_trade(),),
        source_code="eastmoney",
    )

    record = draft.resolve_windows(date(2026, 8, 7), None)

    assert record.trigger_window.start_date == date(2026, 8, 7)
    assert record.trigger_window.occurrence_count == 4
    assert record.amount_period.basis is DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED
    assert record.amount_period.start_date is None


@pytest.mark.parametrize(
    "basis",
    [DragonTigerWindowBasis.MARKET_SESSIONS, DragonTigerWindowBasis.SECURITY_TRADED_SESSIONS],
)
def test_trigger_window_accepts_supported_bases(basis: DragonTigerWindowBasis) -> None:
    assert _trigger(basis=basis).basis is basis


def test_trigger_window_rejects_nonpositive_session_count() -> None:
    with pytest.raises(ValueError, match="session_count must be positive"):
        _trigger(session_count=0)


def test_unspecified_amount_period_rejects_invented_dates() -> None:
    with pytest.raises(ValueError, match="unspecified amount period"):
        DragonTigerAmountPeriod(
            basis=DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED,
            session_count=None,
            start_date=TRADE_DATE,
            end_date=TRADE_DATE,
        )


def test_resolved_event_requires_a_complete_verified_amount_period() -> None:
    with pytest.raises(ValueError, match="verified amount period requires resolved dates"):
        _event(amount_period=_amount_period(start_date=None))


def test_event_requires_at_least_one_disclosure_side() -> None:
    with pytest.raises(ValueError, match="at least one disclosure side"):
        _event(buy_disclosure_present=False, sell_disclosure_present=False)


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


def test_alias_is_a_temporal_name_observation_for_one_source_identity() -> None:
    seat_id = UUID("00000000-0000-0000-0000-000000000001")
    identity_id = UUID("00000000-0000-0000-0000-000000000002")
    seat = TradingSeat(
        seat_id=seat_id,
        canonical_name="某证券营业部",
        broker_name="某证券",
        branch_name="营业部",
        seat_type=TradingSeatType.BROKER,
        province=None,
        city=None,
        first_seen_date=TRADE_DATE,
        last_seen_date=TRADE_DATE,
        is_active=True,
    )
    identity = TradingSeatSourceIdentity(
        identity_id=identity_id,
        seat_id=seat_id,
        source_code="eastmoney",
        source_seat_key="seat-1",
        first_seen_date=TRADE_DATE,
        last_seen_date=TRADE_DATE,
    )
    alias = TradingSeatAlias(
        alias_id=UUID("00000000-0000-0000-0000-000000000003"),
        identity_id=identity_id,
        alias_name="某证券股份有限公司营业部",
        first_seen_date=TRADE_DATE,
        last_seen_date=TRADE_DATE,
    )

    assert identity.seat_id == seat.seat_id
    assert alias.identity_id == identity.identity_id
    assert alias.alias_name != seat.canonical_name


def test_validation_accepts_a_known_bse_security() -> None:
    trade = _trade(symbol="BSE:920000")
    event = _event(symbol="BSE:920000", seat_trades=(trade,))

    result = validate_dragon_tiger_events(
        (event,),
        known_symbols={"BSE:920000"},
        known_trading_dates={TRADE_DATE},
    )

    assert result.accepted == (event,)
    assert result.findings == ()


def test_validation_rejects_a_seat_parent_mismatch() -> None:
    event = _event(seat_trades=(_trade(symbol="SZSE:000001"),))

    result = validate_dragon_tiger_events(
        (event,),
        known_symbols={"SSE:600000", "SZSE:000001"},
        known_trading_dates={TRADE_DATE},
    )

    assert result.accepted == ()
    assert result.findings[0].rule_code == "dragon_tiger.seat_parent_mismatch"


def test_content_hash_is_deterministic_and_excludes_calculated_net() -> None:
    first = _event()
    second = _event()

    assert dragon_tiger_content_hash(first) == dragon_tiger_content_hash(second)
    assert len(dragon_tiger_content_hash(first)) == 64
