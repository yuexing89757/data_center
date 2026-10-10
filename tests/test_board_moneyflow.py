from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain.board_moneyflow import (
    BoardMoneyflowRecord,
    BoardMoneyflowType,
    partition_board_moneyflows,
    validate_board_moneyflows,
)


def _record() -> BoardMoneyflowRecord:
    return BoardMoneyflowRecord(
        board_code="DC:BK0145",
        trade_date=date(2026, 9, 30),
        board_type=BoardMoneyflowType.REGION,
        name="上海板块",
        pct_change=Decimal("0.23"),
        close=Decimal("22111.86"),
        net_amount=Decimal("-726583552.00"),
        net_amount_rate=Decimal("-0.56"),
        extra_large_amount=Decimal("-415738368.00"),
        extra_large_rate=Decimal("-0.32"),
        large_amount=Decimal("-310845184.00"),
        large_rate=Decimal("-0.24"),
        medium_amount=Decimal("195981312.00"),
        medium_rate=Decimal("0.15"),
        small_amount=Decimal("507232000.00"),
        small_rate=Decimal("0.39"),
        top_small_buy_stock="剑桥科技",
        rank=27,
        source_code="tushare",
    )


def test_valid_record_is_preserved() -> None:
    record = _record()

    assert validate_board_moneyflows((record,)) == (record,)


def test_invalid_board_code_or_type_is_rejected() -> None:
    with pytest.raises(ValueError, match="code"):
        validate_board_moneyflows((replace(_record(), board_code="BK0145.DC"),))
    with pytest.raises(ValueError, match="type"):
        validate_board_moneyflows((replace(_record(), board_type="concept"),))  # type: ignore[arg-type]


def test_blank_name_nonpositive_rank_and_wrong_source_are_rejected() -> None:
    with pytest.raises(ValueError, match="name"):
        validate_board_moneyflows((replace(_record(), name="  "),))
    with pytest.raises(ValueError, match="rank"):
        validate_board_moneyflows((replace(_record(), rank=0),))
    with pytest.raises(ValueError, match="source"):
        validate_board_moneyflows((replace(_record(), source_code="akshare"),))


def test_partition_quarantines_duplicate_board_days_and_invalid_siblings() -> None:
    first = _record()
    duplicate = replace(_record(), rank=5)
    invalid = replace(
        _record(), board_code="DC:XX9999", board_type=BoardMoneyflowType.CONCEPT, rank=None
    )

    result = partition_board_moneyflows((first, duplicate, invalid))

    assert result.accepted == ()
    assert [reason for _, reason in result.rejected] == [
        "duplicate board money-flow key",
        "duplicate board money-flow key",
        "invalid board money-flow code: DC:XX9999",
    ]


def test_partition_keeps_valid_records_with_optional_fields_missing() -> None:
    minimal = replace(
        _record(),
        board_code="DC:BK0475",
        board_type=BoardMoneyflowType.CONCEPT,
        close=None,
        net_amount_rate=None,
        extra_large_amount=None,
        extra_large_rate=None,
        large_amount=None,
        large_rate=None,
        medium_amount=None,
        medium_rate=None,
        small_amount=None,
        small_rate=None,
        top_small_buy_stock=None,
        rank=None,
    )

    result = partition_board_moneyflows((minimal,))

    assert result.accepted == (minimal,)
    assert result.rejected == ()
