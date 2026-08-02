"""Worker environment configuration."""

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: SecretStr
    raw_data_root: Path = Path("data/raw")


class TushareSettings(BaseSettings):
    """Credentials for the optional Tushare provider."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    tushare_token: SecretStr


class SchedulerSettings(BaseSettings):
    """Embedded scheduling configuration for the collection Worker."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    scheduler_store_path: Path = Path("data/scheduler/jobs.sqlite")
    scheduler_timezone: str = "Asia/Shanghai"
    daily_run_hour: int = 18
    daily_run_minute: int = 30
    stock_daily_indicator_hour: int = 19
    stock_daily_indicator_minute: int = 0
    stock_pool_hour: int = 19
    stock_pool_minute: int = 30
    deducted_profit_hour: int = 20
    deducted_profit_minute: int = 0
    scheduler_misfire_grace_seconds: int = 21_600
    worker_admin_port: int = Field(default=8765, ge=1, le=65_535)
    auction_collection_enabled: bool = False
    auction_collection_hour: int = Field(default=9, ge=0, le=23)
    auction_collection_minute: int = Field(default=15, ge=0, le=59)
    auction_collection_cadence_seconds: int = Field(default=5, ge=1, le=60)


class PytdxHqSettings(BaseSettings):
    """Explicit network endpoint and bounded realtime quote settings."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pytdx_hq_host: SecretStr
    pytdx_hq_port: int = Field(default=7709, ge=1, le=65_535)
    pytdx_hq_timeout_seconds: float = Field(default=2.0, gt=0, le=4.0)
    pytdx_hq_batch_size: int = Field(default=80, ge=1, le=80)
    pytdx_hq_max_retries: int = Field(default=1, ge=0, le=1)
