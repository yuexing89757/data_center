from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from market_data_center.board_index_bias import calculate_board_index_bias
from market_data_center.domain.board_index import BoardIndexDailyBarRecord
from market_data_center.domain.records import Market


def _bar(day: int, close: str) -> BoardIndexDailyBarRecord:
    value = Decimal(close)
    return BoardIndexDailyBarRecord(
        board_id="THS:883423",
        trade_date=date(2026, 1, 1) + timedelta(days=day - 1),
        market=Market.CN_A_SHARE,
        open=value,
        high=value,
        low=value,
        close=value,
        volume=day,
        amount=value * day,
        source_code="akshare_ths",
    )


def test_calculates_latest_bias_direction_and_latest_30_extrema() -> None:
    fetched_at = datetime(2026, 8, 15, 2, 1, 2, tzinfo=UTC)
    records = [_bar(day, str(day)) for day in range(35, 0, -1)]

    result = calculate_board_index_bias(
        records,
        fetched_at=fetched_at,
        data_origin="ths_live",
        persistence_status="queued",
    )

    assert result.trade_date == date(2026, 2, 4)
    assert result.close == Decimal("35")
    assert result.moving_average_5 == Decimal("33.000000")
    assert result.bias_5_pct == Decimal("6.060606")
    assert result.previous_trade_date == date(2026, 2, 3)
    assert result.previous_bias_5_pct == Decimal("6.250000")
    assert result.bias_direction == "down"
    assert result.bias_sample_count == 30
    assert result.highest_bias_5_pct == Decimal("50.000000")
    assert result.highest_bias_trade_date == date(2026, 1, 6)
    assert result.lowest_bias_5_pct == Decimal("6.060606")
    assert result.lowest_bias_trade_date == date(2026, 2, 4)
    assert result.data_origin == "ths_live"
    assert result.persistence_status == "queued"
    assert result.fetched_at == fetched_at


def test_returns_null_calculations_with_fewer_than_five_rows() -> None:
    result = calculate_board_index_bias(
        [_bar(day, str(day)) for day in range(1, 5)],
        fetched_at=datetime(2026, 8, 15, tzinfo=UTC),
        data_origin="database",
        persistence_status="persisted",
    )

    assert result.moving_average_5 is None
    assert result.bias_5_pct is None
    assert result.previous_bias_5_pct is None
    assert result.bias_direction is None
    assert result.bias_sample_count == 0
    assert result.highest_bias_5_pct is None
    assert result.lowest_bias_5_pct is None


def test_rejects_duplicate_dates() -> None:
    duplicate = _bar(1, "1")

    with pytest.raises(ValueError, match="duplicate trade_date"):
        calculate_board_index_bias(
            [duplicate, duplicate],
            fetched_at=datetime(2026, 8, 15, tzinfo=UTC),
            data_origin="ths_live",
            persistence_status="queued",
        )


def test_rejects_empty_or_wrong_board_history() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        calculate_board_index_bias(
            [],
            fetched_at=datetime(2026, 8, 15, tzinfo=UTC),
            data_origin="ths_live",
            persistence_status="queued",
        )

    wrong = _bar(1, "1")
    object.__setattr__(wrong, "board_id", "THS:000000")
    with pytest.raises(ValueError, match="THS:883423"):
        calculate_board_index_bias(
            [wrong],
            fetched_at=datetime(2026, 8, 15, tzinfo=UTC),
            data_origin="ths_live",
            persistence_status="queued",
        )
