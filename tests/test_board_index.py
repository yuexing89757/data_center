from datetime import date
from decimal import Decimal

from market_data_center.domain import (
    BoardIndexConstituentSnapshotRecord,
    BoardIndexDailyBarRecord,
    Market,
    validate_board_index_constituent_snapshot,
    validate_board_index_daily_bars,
)

BOARD_ID = "THS:883423"
TRADE_DATE = date(2026, 7, 29)


def _bar(*, close: str = "10.5") -> BoardIndexDailyBarRecord:
    return BoardIndexDailyBarRecord(
        board_id=BOARD_ID,
        trade_date=TRADE_DATE,
        market=Market.CN_A_SHARE,
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal(close),
        volume=100,
        amount=Decimal("1050"),
        source_code="akshare_ths",
    )


def test_board_index_daily_bar_validation_deduplicates_identical_natural_keys() -> None:
    bar = _bar()

    result = validate_board_index_daily_bars(
        [bar, bar],
        known_board_ids={BOARD_ID},
        known_trading_dates={TRADE_DATE},
    )

    assert result.accepted == (bar,)
    assert result.findings == ()
    assert result.rejected_rows == 0


def test_board_index_daily_bar_validation_blocks_conflicting_revision_in_batch() -> None:
    result = validate_board_index_daily_bars(
        [_bar(), _bar(close="10.6")],
        known_board_ids={BOARD_ID},
        known_trading_dates={TRADE_DATE},
    )

    assert result.accepted == ()
    assert result.findings[0].rule_code == "board_index_daily_bar.conflicting_duplicate"
    assert result.rejected_rows == 2


def test_board_index_constituents_block_duplicates_unknown_security_and_date() -> None:
    record = BoardIndexConstituentSnapshotRecord(
        board_id=BOARD_ID,
        trade_date=TRADE_DATE,
        members=("SSE:600000", "SSE:600000", "SZSE:000001"),
        source_code="akshare_ths",
    )

    findings = validate_board_index_constituent_snapshot(
        record,
        known_board_ids={BOARD_ID},
        known_symbols={"SSE:600000"},
        known_trading_dates=set(),
    )

    assert {finding.rule_code for finding in findings} == {
        "board_index_constituent.non_trading_date",
        "board_index_constituent.duplicate_member",
        "board_index_constituent.unknown_security",
    }
