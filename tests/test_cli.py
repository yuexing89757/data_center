from argparse import Namespace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import market_data_center.cli as cli
from market_data_center.cli import (
    AUTO_PROVIDER_CODE,
    _one_month_before,
    _parser,
    run_daily_workflow,
    run_stock_daily_indicator_workflow,
)
from market_data_center.domain import IngestionRun, IngestionStatus
from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.raw_store import LocalRawStore


class FakeDailyRunPersistence:
    def __init__(self, latest_trading_date: date | None) -> None:
        self._latest_trading_date = latest_trading_date
        self.deleted_before: list[date] = []

    def latest_trading_date(self, start_date: date, end_date: date) -> date | None:
        return self._latest_trading_date

    def delete_stock_daily_indicators_before(self, cutoff_date: date) -> int:
        self.deleted_before.append(cutoff_date)
        return 12


def test_cli_uses_automatic_routing_by_default() -> None:
    args = _parser().parse_args(["security"])

    assert args.provider == AUTO_PROVIDER_CODE


def test_cli_still_accepts_an_explicit_provider() -> None:
    args = _parser().parse_args(["--provider", "pytdx", "security"])

    assert args.provider == "pytdx"


def test_stock_daily_indicator_bulk_parses_one_trade_date() -> None:
    args = _parser().parse_args(
        [
            "--provider",
            "tushare",
            "stock-daily-indicators-bulk",
            "--trade-date",
            "2026-07-31",
        ]
    )

    assert args.provider == "tushare"
    assert args.trade_date == "2026-07-31"


def test_stock_pool_commands_require_exact_dates_and_known_pool_codes() -> None:
    build = _parser().parse_args(["stock-pools-build", "--basis-trade-date", "2026-07-31"])
    check = _parser().parse_args(
        [
            "stock-pool-check",
            "--pool-code",
            "CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN",
            "--effective-trade-date",
            "2026-08-03",
            "--version",
            "2",
        ]
    )

    assert build.basis_trade_date == "2026-07-31"
    assert check.effective_trade_date == "2026-08-03"
    assert check.version == 2


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (date(2026, 8, 31), date(2026, 7, 31)),
        (date(2026, 3, 31), date(2026, 2, 28)),
        (date(2026, 1, 30), date(2025, 12, 30)),
    ],
)
def test_one_month_retention_cutoff_uses_calendar_month(value: date, expected: date) -> None:
    assert _one_month_before(value) == expected


def test_stock_daily_indicator_daily_collects_then_enforces_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Namespace] = []
    persistence = FakeDailyRunPersistence(date(2026, 7, 31))

    def record_run(
        args: Namespace,
        persistence: PostgreSQLPersistence,
        raw_store: LocalRawStore,
    ) -> IngestionRun:
        calls.append(args)
        return cast(
            IngestionRun,
            SimpleNamespace(status=IngestionStatus.SUCCEEDED, accepted_rows=1),
        )

    monkeypatch.setattr(cli, "_execute_operation", record_run)
    args = _parser().parse_args(
        [
            "--provider",
            "tushare",
            "stock-daily-indicators-daily",
            "--as-of-date",
            "2026-07-31",
        ]
    )

    result = run_stock_daily_indicator_workflow(
        args,
        cast(PostgreSQLPersistence, persistence),
        cast(LocalRawStore, Path("unused")),
    )

    assert result is not None
    assert result.as_of_date == date(2026, 7, 31)
    assert result.cutoff_date == date(2026, 6, 30)
    assert [call.dataset for call in calls] == [
        "trading-calendar",
        "stock-daily-indicators-bulk",
    ]
    assert persistence.deleted_before == [date(2026, 6, 30)]


def test_stock_daily_indicator_daily_skips_closed_market_without_deleting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Namespace] = []
    persistence = FakeDailyRunPersistence(None)

    def record_run(
        args: Namespace,
        persistence: PostgreSQLPersistence,
        raw_store: LocalRawStore,
    ) -> IngestionRun:
        calls.append(args)
        return cast(
            IngestionRun,
            SimpleNamespace(status=IngestionStatus.SUCCEEDED, accepted_rows=1),
        )

    monkeypatch.setattr(cli, "_execute_operation", record_run)
    args = _parser().parse_args(
        [
            "--provider",
            "tushare",
            "stock-daily-indicators-daily",
            "--as-of-date",
            "2026-08-01",
        ]
    )

    result = run_stock_daily_indicator_workflow(
        args,
        cast(PostgreSQLPersistence, persistence),
        cast(LocalRawStore, Path("unused")),
    )

    assert result is None
    assert [call.dataset for call in calls] == ["trading-calendar"]
    assert persistence.deleted_before == []


def test_stock_daily_indicator_daily_does_not_delete_after_failed_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persistence = FakeDailyRunPersistence(date(2026, 7, 31))

    def record_run(
        args: Namespace,
        persistence: PostgreSQLPersistence,
        raw_store: LocalRawStore,
    ) -> IngestionRun:
        status = (
            IngestionStatus.FAILED
            if args.dataset == "stock-daily-indicators-bulk"
            else IngestionStatus.SUCCEEDED
        )
        return cast(IngestionRun, SimpleNamespace(status=status, accepted_rows=0))

    monkeypatch.setattr(cli, "_execute_operation", record_run)
    args = _parser().parse_args(
        [
            "--provider",
            "tushare",
            "stock-daily-indicators-daily",
            "--as-of-date",
            "2026-07-31",
        ]
    )

    with pytest.raises(RuntimeError, match="not safe for retention"):
        run_stock_daily_indicator_workflow(
            args,
            cast(PostgreSQLPersistence, persistence),
            cast(LocalRawStore, Path("unused")),
        )

    assert persistence.deleted_before == []


def test_daily_run_has_current_day_defaults() -> None:
    args = _parser().parse_args(["daily-run"])

    assert args.as_of_date is None
    assert args.bar_lookback_days == 1
    assert args.calendar_lookback_days == 14
    assert args.shard_count == 1
    assert args.shard_index == 0


def test_reliability_commands_parse_without_a_provider() -> None:
    replay = _parser().parse_args(
        ["raw-replay", "--ingestion-id", "74b11082-4ec0-4ae4-826f-a80a96cb9985"]
    )
    stale = _parser().parse_args(["recover-stale-runs", "--dry-run"])
    comparison = _parser().parse_args(
        [
            "compare-daily-bars",
            "--symbol",
            "SSE:600000",
            "--start-date",
            "2026-07-01",
            "--end-date",
            "2026-07-28",
        ]
    )

    assert replay.provider == AUTO_PROVIDER_CODE
    assert not replay.dry_run
    assert stale.older_than_minutes == 60
    assert comparison.symbol == "SSE:600000"


def test_derived_recompute_parses_versioned_full_or_incremental_mode() -> None:
    args = _parser().parse_args(
        [
            "derived-recompute",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-07-29",
            "--mode",
            "full",
            "--algorithm-version",
            "1.1.0",
        ]
    )

    assert args.mode == "full"
    assert args.algorithm_version == "1.1.0"


def test_board_index_commands_use_explicit_identity_and_current_snapshot() -> None:
    directory = _parser().parse_args(["board-index"])
    bars = _parser().parse_args(
        [
            "--provider",
            "akshare_ths",
            "board-index-daily-bar",
            "--start-date",
            "2026-07-01",
            "--end-date",
            "2026-07-29",
        ]
    )
    members = _parser().parse_args(["board-index-constituents"])

    assert directory.provider == AUTO_PROVIDER_CODE
    assert bars.board_id == "THS:883423"
    assert members.board_id == "THS:883423"
    assert members.snapshot_date is None


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

    run_daily_workflow(
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
    assert calls[2].start_date == "2026-07-29"
    assert calls[2].end_date == "2026-07-29"
    assert calls[2].allow_unavailable


def test_worker_command_exposes_embedded_scheduler_health_check() -> None:
    args = _parser().parse_args(["worker", "--check"])

    assert args.dataset == "worker"
    assert args.check


def test_daily_run_rejects_a_nonpositive_calendar_window() -> None:
    args = _parser().parse_args(
        [
            "daily-run",
            "--as-of-date",
            "2026-07-29",
            "--calendar-lookback-days",
            "0",
        ]
    )

    with pytest.raises(SystemExit, match="calendar-lookback-days must be positive"):
        run_daily_workflow(
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

    run_daily_workflow(
        args,
        cast(PostgreSQLPersistence, FakeDailyRunPersistence(date(2026, 7, 24))),
        cast(LocalRawStore, Path("unused")),
    )

    assert calls[-1].dataset == "daily-bars-bulk"
    assert calls[-1].start_date == "2026-07-24"
    assert calls[-1].end_date == "2026-07-24"
