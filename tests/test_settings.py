from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from market_data_center.settings import (
    PytdxPoolSettings,
    SchedulerSettings,
    TushareSettings,
    WorkerSettings,
)


def test_optional_scheduled_tasks_default_enabled() -> None:
    settings = SchedulerSettings(_env_file=None)

    assert settings.eod_quote_snapshot_enabled is True
    assert settings.call_auction_snapshot_enabled is True
    assert settings.call_auction_market_series_enabled is True
    assert settings.close_price_new_highs_120d_enabled is True
    assert settings.shareholder_count_daily_enabled is False
    assert settings.trading_billboard_enabled is False


def test_optional_scheduled_tasks_can_be_disabled_by_environment(monkeypatch) -> None:
    monkeypatch.setenv("EOD_QUOTE_SNAPSHOT_ENABLED", "false")
    monkeypatch.setenv("CALL_AUCTION_SNAPSHOT_ENABLED", "false")
    monkeypatch.setenv("CALL_AUCTION_MARKET_SERIES_ENABLED", "false")
    monkeypatch.setenv("CLOSE_PRICE_NEW_HIGHS_120D_ENABLED", "false")

    settings = SchedulerSettings(_env_file=None)

    assert settings.eod_quote_snapshot_enabled is False
    assert settings.call_auction_snapshot_enabled is False
    assert settings.call_auction_market_series_enabled is False
    assert settings.close_price_new_highs_120d_enabled is False


def test_trading_billboard_schedule_requires_explicit_opt_in(monkeypatch) -> None:
    monkeypatch.setenv("TRADING_BILLBOARD_ENABLED", "true")

    assert SchedulerSettings(_env_file=None).trading_billboard_enabled is True


def test_task_timing_is_not_part_of_environment_settings() -> None:
    scheduler = SchedulerSettings(_env_file=None)
    pool = PytdxPoolSettings(_env_file=None)

    removed_fields = (
        "scheduler_timezone",
        "daily_run_hour",
        "daily_run_minute",
        "stock_daily_indicator_hour",
        "stock_daily_indicator_minute",
        "stock_pool_hour",
        "stock_pool_minute",
        "deducted_profit_hour",
        "deducted_profit_minute",
        "scheduler_misfire_grace_seconds",
        "auction_collection_hour",
        "auction_collection_minute",
        "auction_collection_cadence_seconds",
        "eod_quote_hour",
        "eod_quote_minute",
        "call_auction_hour",
        "call_auction_minute",
        "call_auction_market_series_hour",
        "call_auction_market_series_minute",
        "call_auction_market_series_cadence_seconds",
        "call_auction_market_series_batch_size",
        "close_price_new_highs_120d_hour",
        "close_price_new_highs_120d_minute",
        "trading_billboard_hour",
        "trading_billboard_minute",
    )
    assert all(not hasattr(scheduler, field) for field in removed_fields)
    assert not hasattr(pool, "pytdx_pool_refresh_hours")


def test_pytdx_pool_settings_have_safe_defaults() -> None:
    settings = PytdxPoolSettings(_env_file=None)

    assert settings.pytdx_pool_path == Path("data/pytdx_pool.json")


def test_worker_daily_bar_write_batch_size_is_bounded() -> None:
    settings = WorkerSettings(database_url=SecretStr("unused"), _env_file=None)

    assert settings.daily_bar_write_batch_size == 100


def test_tushare_shareholder_count_rate_limit_has_safe_default() -> None:
    settings = TushareSettings(tushare_token=SecretStr("unused"), _env_file=None)

    assert settings.tushare_shareholder_count_max_calls_per_minute == 180


@pytest.mark.parametrize("value", [0, 201])
def test_tushare_shareholder_count_rate_limit_is_bounded(value: int) -> None:
    with pytest.raises(ValidationError):
        TushareSettings(
            tushare_token=SecretStr("unused"),
            tushare_shareholder_count_max_calls_per_minute=value,
            _env_file=None,
        )
