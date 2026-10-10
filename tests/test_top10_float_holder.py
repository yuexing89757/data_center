from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain.top10_float_holder import (
    Top10FloatHolderRecord,
    partition_top10_float_holders,
    validate_top10_float_holders,
)

KNOWN = {"SSE:600000"}


def _record() -> Top10FloatHolderRecord:
    return Top10FloatHolderRecord(
        symbol="SSE:600000",
        end_date=date(2026, 6, 30),
        announcement_date=date(2026, 8, 28),
        holder_rank=1,
        holder_name="上海国际集团有限公司",
        hold_amount=7_086_834_641,
        hold_ratio=Decimal("21.2781"),
        hold_float_ratio=Decimal("21.2781"),
        hold_change=None,
        holder_type="一般企业",
        source_code="tushare",
    )


def test_valid_record_is_preserved() -> None:
    record = _record()

    assert validate_top10_float_holders((record,), known_symbols=KNOWN) == (record,)


def test_unknown_symbol_is_rejected() -> None:
    record = replace(_record(), symbol="SSE:999999")

    with pytest.raises(ValueError, match="unknown"):
        validate_top10_float_holders((record,), known_symbols=KNOWN)


@pytest.mark.parametrize("rank", [0, 11])
def test_rank_outside_one_to_ten_is_rejected(rank: int) -> None:
    record = replace(_record(), holder_rank=rank)

    with pytest.raises(ValueError, match="rank"):
        validate_top10_float_holders((record,), known_symbols=KNOWN)


@pytest.mark.parametrize("hold_amount", [0, -1])
def test_nonpositive_amount_is_rejected(hold_amount: int) -> None:
    record = replace(_record(), hold_amount=hold_amount)

    with pytest.raises(ValueError, match="positive"):
        validate_top10_float_holders((record,), known_symbols=KNOWN)


def test_announcement_date_before_end_date_is_rejected() -> None:
    record = replace(_record(), announcement_date=date(2026, 6, 29))

    with pytest.raises(ValueError, match="announcement"):
        validate_top10_float_holders((record,), known_symbols=KNOWN)


def test_negative_ratio_and_unsupported_source_are_rejected() -> None:
    with pytest.raises(ValueError, match="ratio"):
        validate_top10_float_holders(
            (replace(_record(), hold_ratio=Decimal("-0.1")),), known_symbols=KNOWN
        )
    with pytest.raises(ValueError, match="source"):
        validate_top10_float_holders(
            (replace(_record(), source_code="akshare"),), known_symbols=KNOWN
        )


def test_partition_quarantines_conflicting_pairs_and_keeps_clean_groups() -> None:
    # Conflicting rows are quarantined as a pair: the persistence layer's
    # holder-name unique key has no ON CONFLICT target, so partition must
    # never let one of two contradictory rows through.
    clean = replace(_record(), holder_name="独家股东")
    name_clash_a = replace(_record(), holder_rank=2)
    name_clash_b = replace(_record(), holder_rank=3)
    rank_clash_a = replace(_record(), holder_name="股东甲", holder_rank=4)
    rank_clash_b = replace(_record(), holder_name="股东乙", holder_rank=4)

    result = partition_top10_float_holders(
        (clean, name_clash_a, name_clash_b, rank_clash_a, rank_clash_b),
        known_symbols=KNOWN,
    )

    assert result.accepted == (clean,)
    assert [(record.holder_rank, reason) for record, reason in result.rejected] == [
        (2, "duplicate top10 float holder name"),
        (3, "duplicate top10 float holder name"),
        (4, "duplicate top10 float holder rank"),
        (4, "duplicate top10 float holder rank"),
    ]


def test_partition_quarantines_invalid_fact_without_dropping_valid_siblings() -> None:
    first = _record()
    invalid = replace(_record(), holder_rank=2, holder_name="股东乙", hold_amount=0)

    result = partition_top10_float_holders((first, invalid), known_symbols=KNOWN)

    assert result.accepted == (first,)
    assert len(result.rejected) == 1
    assert "positive" in result.rejected[0][1]
