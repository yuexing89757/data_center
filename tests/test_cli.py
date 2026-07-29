from argparse import Namespace
from datetime import date
from pathlib import Path
from typing import cast

import pytest

import market_data_center.cli as cli
from market_data_center.cli import AUTO_PROVIDER_CODE, _parser, _run_daily_workflow
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.raw_store import LocalRawStore


class FakeDailyRunPersistence:
    def __init__(self, latest_trading_date: date | None) -> None:
        self._latest_trading_date = latest_trading_date

    def latest_trading_date(self, start_date: date, end_date: date) -> date | None:
        return self._latest_trading_date


def test_cli_uses_automatic_routing_by_default() -> None:
    args = _parser().parse_args(["security"])

    assert args.provider == AUTO_PROVIDER_CODE


def test_cli_still_accepts_an_explicit_provider() -> None:
    args = _parser().parse_args(["--provider", "pytdx", "security"])

    assert args.provider == "pytdx"


def test_daily_run_has_repair_window_defaults() -> None:
    args = _parser().parse_args(["daily-run"])

    assert args.as_of_date is None
    assert args.bar_lookback_days == 7
    assert args.calendar_lookback_days == 14
    assert args.shard_count == 1
    assert args.shard_index == 0


def test_daily_run_orders_prerequisites_before_incremental_bars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Namespace] = []

    def record_run(
        args: Namespace,
        persistence: PostgreSQLPersistence,
        raw_store: LocalRawStore,
    ) -> None:
        calls.append(args)

    monkeypatch.setattr(cli, "_run_automatic", record_run)
    args = _parser().parse_args(
        [
            "daily-run",
            "--as-of-date",
            "2026-07-29",
            "--bar-lookback-days",
            "3",
            "--calendar-lookback-days",
            "10",
        ]
    )

    _run_daily_workflow(
        args,
        cast(PostgreSQLPersistence, FakeDailyRunPersistence(date(2026, 7, 29))),
        cast(LocalRawStore, Path("unused")),
    )

    assert [call.dataset for call in calls] == [
        "security",
        "trading-calendar",
        "daily-bars-bulk",
    ]
    assert calls[1].start_date == "2026-07-20"
    assert calls[1].end_date == "2026-07-29"
    assert calls[2].start_date == "2026-07-27"
    assert calls[2].end_date == "2026-07-29"


def test_daily_run_rejects_calendar_window_shorter_than_bar_window() -> None:
    args = _parser().parse_args(
        [
            "daily-run",
            "--as-of-date",
            "2026-07-29",
            "--bar-lookback-days",
            "7",
            "--calendar-lookback-days",
            "3",
        ]
    )

    with pytest.raises(SystemExit, match="calendar-lookback-days"):
        _run_daily_workflow(
            args,
            cast(PostgreSQLPersistence, FakeDailyRunPersistence(date(2026, 7, 29))),
            cast(LocalRawStore, Path("unused")),
        )


def test_daily_run_uses_latest_trading_day_on_weekend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Namespace] = []

    def record_run(
        args: Namespace,
        persistence: PostgreSQLPersistence,
        raw_store: LocalRawStore,
    ) -> None:
        calls.append(args)

    monkeypatch.setattr(cli, "_run_automatic", record_run)
    args = _parser().parse_args(
        ["daily-run", "--as-of-date", "2026-07-26", "--bar-lookback-days", "7"]
    )

    _run_daily_workflow(
        args,
        cast(PostgreSQLPersistence, FakeDailyRunPersistence(date(2026, 7, 24))),
        cast(LocalRawStore, Path("unused")),
    )

    assert calls[-1].dataset == "daily-bars-bulk"
    assert calls[-1].end_date == "2026-07-24"
