from pathlib import Path

from pydantic import SecretStr

from market_data_center.settings import PytdxPoolSettings, SchedulerSettings, WorkerSettings


def test_optional_scheduled_tasks_default_enabled() -> None:
    settings = SchedulerSettings(_env_file=None)

    assert settings.auction_collection_enabled is True
    assert settings.eod_quote_snapshot_enabled is True
    assert settings.call_auction_snapshot_enabled is True


def test_optional_scheduled_tasks_can_be_disabled_by_environment(monkeypatch) -> None:
    monkeypatch.setenv("AUCTION_COLLECTION_ENABLED", "false")
    monkeypatch.setenv("EOD_QUOTE_SNAPSHOT_ENABLED", "false")
    monkeypatch.setenv("CALL_AUCTION_SNAPSHOT_ENABLED", "false")

    settings = SchedulerSettings(_env_file=None)

    assert settings.auction_collection_enabled is False
    assert settings.eod_quote_snapshot_enabled is False
    assert settings.call_auction_snapshot_enabled is False


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
    )
    assert all(not hasattr(scheduler, field) for field in removed_fields)
    assert not hasattr(pool, "pytdx_pool_refresh_hours")


def test_pytdx_pool_settings_have_safe_defaults() -> None:
    settings = PytdxPoolSettings(_env_file=None)

    assert settings.pytdx_pool_path == Path("data/pytdx_pool.json")


def test_worker_daily_bar_write_batch_size_is_bounded() -> None:
    settings = WorkerSettings(database_url=SecretStr("unused"), _env_file=None)

    assert settings.daily_bar_write_batch_size == 100
