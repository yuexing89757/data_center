from datetime import date

import pytest

from market_data_center.board_index_daily_schedule import (
    BoardIndexDailyBarCollectionSummary,
    collect_board_index_daily_bar_gap,
)
from market_data_center.providers.contracts import ProviderError


def test_collection_skips_when_expected_date_is_already_stored() -> None:
    calls: list[tuple[date, date]] = []

    result = collect_board_index_daily_bar_gap(
        expected_date=date(2026, 8, 20),
        latest_stored_date=date(2026, 8, 20),
        ingest=lambda start, end: calls.append((start, end)),
    )

    assert result == BoardIndexDailyBarCollectionSummary(
        expected_date=date(2026, 8, 20),
        start_date=None,
        attempts=0,
        skipped=True,
    )
    assert calls == []


def test_collection_retries_provider_errors_and_backfills_from_next_date() -> None:
    calls: list[tuple[date, date]] = []

    def ingest(start_date: date, end_date: date) -> None:
        calls.append((start_date, end_date))
        if len(calls) < 3:
            raise ProviderError("temporary upstream failure")

    result = collect_board_index_daily_bar_gap(
        expected_date=date(2026, 8, 20),
        latest_stored_date=date(2026, 8, 17),
        ingest=ingest,
    )

    assert calls == [(date(2026, 8, 18), date(2026, 8, 20))] * 3
    assert result.attempts == 3
    assert result.skipped is False


def test_collection_uses_bounded_bootstrap_window_when_database_is_empty() -> None:
    calls: list[tuple[date, date]] = []

    collect_board_index_daily_bar_gap(
        expected_date=date(2026, 8, 20),
        latest_stored_date=None,
        ingest=lambda start, end: calls.append((start, end)),
    )

    assert calls == [(date(2026, 5, 22), date(2026, 8, 20))]


def test_collection_does_not_retry_non_provider_failures() -> None:
    calls = 0

    def ingest(_start_date: date, _end_date: date) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("database rejected the batch")

    with pytest.raises(RuntimeError, match="database rejected"):
        collect_board_index_daily_bar_gap(
            expected_date=date(2026, 8, 20),
            latest_stored_date=date(2026, 8, 19),
            ingest=ingest,
        )

    assert calls == 1


def test_collection_raises_after_three_provider_failures() -> None:
    calls = 0

    def ingest(_start_date: date, _end_date: date) -> None:
        nonlocal calls
        calls += 1
        raise ProviderError("still unavailable")

    with pytest.raises(ProviderError, match="still unavailable"):
        collect_board_index_daily_bar_gap(
            expected_date=date(2026, 8, 20),
            latest_stored_date=date(2026, 8, 19),
            ingest=ingest,
        )

    assert calls == 3
