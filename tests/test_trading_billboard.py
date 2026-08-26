from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain import (
    TradingBillboardRecord,
    TradingBillboardSeatRecord,
    TradingBillboardSide,
    trading_billboard_content_hash,
    trading_billboard_natural_key,
    validate_trading_billboards,
)

TRADE_DATE = date(2026, 8, 17)


def _seat(
    side: TradingBillboardSide,
    rank: int,
    **changes: object,
) -> TradingBillboardSeatRecord:
    seat = TradingBillboardSeatRecord(
        source_event_id="100396303",
        symbol="SZSE:000711",
        trade_date=TRADE_DATE,
        side=side,
        rank=rank,
        seat_code=None,
        seat_name=f"机构专用{rank}",
        buy_amount=Decimal("120.00") if side is TradingBillboardSide.BUY else Decimal("20.00"),
        sell_amount=Decimal("20.00") if side is TradingBillboardSide.BUY else Decimal("120.00"),
        net_amount=Decimal("100.00") if side is TradingBillboardSide.BUY else Decimal("-100.00"),
        buy_to_market_pct=Decimal("1.20"),
        sell_to_market_pct=Decimal("0.20"),
    )
    return replace(seat, **changes)


def _record(**changes: object) -> TradingBillboardRecord:
    record = TradingBillboardRecord(
        symbol="SZSE:000711",
        trade_date=TRADE_DATE,
        source_event_id="100396303",
        reason_code="106001",
        reason_text="日涨幅偏离值达到7%的前5只证券",
        close_price=Decimal("12.34"),
        change_rate_pct=Decimal("9.98"),
        turnover_rate_pct=Decimal("18.25"),
        market_amount=Decimal("10000.00"),
        buy_amount=Decimal("600.00"),
        sell_amount=Decimal("400.00"),
        net_amount=Decimal("200.00"),
        deal_amount=Decimal("1000.00"),
        deal_to_market_pct=Decimal("10.00"),
        net_to_market_pct=Decimal("2.00"),
        free_float_market_value=Decimal("50000.00"),
        buy_seats=(
            _seat(TradingBillboardSide.BUY, 1),
            _seat(TradingBillboardSide.BUY, 2),
        ),
        sell_seats=(
            _seat(TradingBillboardSide.SELL, 1),
            _seat(TradingBillboardSide.SELL, 2),
        ),
    )
    return replace(record, **changes)


def _validate(*records: TradingBillboardRecord):
    return validate_trading_billboards(
        records,
        known_symbols={"SZSE:000711"},
        known_trading_dates={TRADE_DATE},
    )


def test_record_preserves_parent_identity_and_has_stable_hash() -> None:
    record = _record()

    assert trading_billboard_natural_key(record) == ("eastmoney", "100396303")
    assert record.buy_seats[0].symbol == "SZSE:000711"
    assert record.buy_seats[0].trade_date == TRADE_DATE
    assert trading_billboard_content_hash(record) == trading_billboard_content_hash(record)
    assert len(trading_billboard_content_hash(record)) == 64


def test_content_hash_canonicalizes_equivalent_decimal_scales() -> None:
    record = _record()

    assert trading_billboard_content_hash(record) == trading_billboard_content_hash(
        replace(record, close_price=Decimal("12.340"))
    )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"symbol": "NYSE:000711"}, "standard A-share symbol"),
        ({"buy_amount": Decimal("-0.01")}, "must not be negative"),
        ({"deal_to_market_pct": Decimal("-0.01")}, "must not be negative"),
    ],
)
def test_record_rejects_invalid_local_values(changes: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _record(**changes)


def test_validation_accepts_repeated_institution_only_seats() -> None:
    record = _record()

    result = _validate(record)

    assert result.accepted == (record,)
    assert result.findings == ()
    assert [seat.seat_code for seat in record.buy_seats] == [None, None]


def test_validation_skips_bse_and_unknown_security_without_blocking_known_records() -> None:
    known_shenzhen = _record()
    known_bse = replace(
        known_shenzhen,
        symbol="BSE:920176",
        source_event_id="100401578",
        buy_seats=tuple(
            replace(seat, symbol="BSE:920176", source_event_id="100401578")
            for seat in known_shenzhen.buy_seats
        ),
        sell_seats=tuple(
            replace(seat, symbol="BSE:920176", source_event_id="100401578")
            for seat in known_shenzhen.sell_seats
        ),
    )
    unknown_shanghai = replace(
        known_bse,
        symbol="SSE:600999",
        source_event_id="unknown-event",
        buy_seats=tuple(
            replace(seat, symbol="SSE:600999", source_event_id="unknown-event")
            for seat in known_bse.buy_seats
        ),
        sell_seats=tuple(
            replace(seat, symbol="SSE:600999", source_event_id="unknown-event")
            for seat in known_bse.sell_seats
        ),
    )

    result = validate_trading_billboards(
        (known_shenzhen, known_bse, unknown_shanghai),
        known_symbols={"SZSE:000711", "BSE:920176"},
        known_trading_dates={TRADE_DATE},
    )

    assert result.accepted == (known_shenzhen,)
    assert result.findings == ()
    assert result.rejected_rows == 2


@pytest.mark.parametrize(
    ("changes", "rule_suffix"),
    [
        ({"trade_date": date(2026, 8, 16)}, "unknown_trading_date"),
        ({"buy_amount": Decimal("601.00")}, "invalid_deal_amount"),
        ({"net_amount": Decimal("201.00")}, "invalid_net_amount"),
        (
            {"buy_seats": (_seat(TradingBillboardSide.SELL, 1),)},
            "seat_side_mismatch",
        ),
        (
            {
                "buy_seats": (
                    _seat(TradingBillboardSide.BUY, 1),
                    _seat(TradingBillboardSide.BUY, 1, seat_name="机构专用B"),
                )
            },
            "seat_rank_sequence",
        ),
        (
            {
                "buy_seats": (
                    _seat(TradingBillboardSide.BUY, 1),
                    _seat(TradingBillboardSide.BUY, 3),
                )
            },
            "seat_rank_sequence",
        ),
        (
            {"buy_seats": (_seat(TradingBillboardSide.BUY, 1, symbol="SSE:600000"),)},
            "seat_parent_mismatch",
        ),
        (
            {"sell_seats": (_seat(TradingBillboardSide.SELL, 1, trade_date=date(2026, 8, 18)),)},
            "seat_parent_mismatch",
        ),
    ],
)
def test_validation_rejects_hard_invariants(changes: dict[str, object], rule_suffix: str) -> None:
    result = _validate(_record(**changes))

    assert result.accepted == ()
    assert result.rejected_rows == 1
    assert result.findings[0].rule_code.endswith(rule_suffix)


def test_validation_rejects_conflicting_source_key() -> None:
    record = _record()
    conflict = replace(record, reason_code="106002", reason_text="另一个原因")

    result = _validate(record, conflict)

    assert result.accepted == ()
    assert result.rejected_rows == 2
    assert result.findings[0].rule_code.endswith("conflicting_source_key")


def test_validation_rejects_identical_duplicate_source_key() -> None:
    record = _record()

    result = _validate(record, record)

    assert result.accepted == ()
    assert result.rejected_rows == 2
    assert result.findings[0].rule_code.endswith("duplicate_source_key")


@pytest.mark.parametrize(
    "record",
    [
        _record(
            buy_amount=Decimal("60.002"),
            sell_amount=Decimal("40.002"),
            net_amount=Decimal("20.000"),
            deal_amount=Decimal("100.005"),
        ),
        _record(
            buy_amount=Decimal("40.000"),
            sell_amount=Decimal("60.004"),
            net_amount=Decimal("-20.005"),
            deal_amount=Decimal("100.004"),
        ),
    ],
)
def test_validation_matches_postgres_half_away_from_zero_rounding(
    record: TradingBillboardRecord,
) -> None:
    result = _validate(record)

    assert result.accepted == ()
    assert result.findings[0].rule_code.endswith(("invalid_deal_amount", "invalid_net_amount"))


def test_validation_rejects_conflicting_semantic_key() -> None:
    record = _record()
    conflict = replace(record, source_event_id="100396304")
    conflict = replace(
        conflict,
        buy_seats=tuple(
            replace(seat, source_event_id=conflict.source_event_id) for seat in conflict.buy_seats
        ),
        sell_seats=tuple(
            replace(seat, source_event_id=conflict.source_event_id) for seat in conflict.sell_seats
        ),
    )

    result = _validate(record, conflict)

    assert result.accepted == ()
    assert result.rejected_rows == 2
    assert result.findings[0].rule_code.endswith("conflicting_semantic_key")
