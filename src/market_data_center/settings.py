"""Worker environment configuration."""

from pathlib import Path
from typing import Self

from pydantic import Field, SecretStr, model_validator
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
    """Runtime paths and optional-task switches for the collection Worker."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    scheduler_store_path: Path = Path("data/scheduler/jobs.sqlite")
    worker_admin_port: int = Field(default=8765, ge=1, le=65_535)
    auction_collection_enabled: bool = True
    eod_quote_snapshot_enabled: bool = True
    call_auction_snapshot_enabled: bool = True


class PytdxPoolSettings(BaseSettings):
    """Shared endpoint-pool runtime location."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pytdx_pool_path: Path = Path("data/pytdx_pool.json")


class PytdxHqSettings(BaseSettings):
    """Bounded realtime quote request settings."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pytdx_hq_timeout_seconds: float = Field(default=2.0, gt=0, le=4.0)
    pytdx_hq_batch_size: int = Field(default=80, ge=1, le=80)
    pytdx_hq_max_retries: int = Field(default=1, ge=0, le=1)


class PytdxDailyBarSettings(BaseSettings):
    """Bounded local and remote unadjusted Daily Bar settings."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pytdx_vipdoc_path: str = ""
    pytdx_daily_bar_timeout_seconds: float = Field(default=3.0, gt=0, le=10)
    pytdx_daily_bar_max_attempts: int = Field(default=2, ge=1, le=5)
    pytdx_daily_bar_page_size: int = Field(default=800, ge=1, le=800)
    pytdx_daily_bar_max_pages: int = Field(default=16, ge=1, le=64)


class ApiSettings(BaseSettings):
    """External read-only API configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    fastapi_database_url: SecretStr
    fastapi_api_key: SecretStr = Field(min_length=32)
    fastapi_host: str = "127.0.0.1"
    fastapi_port: int = Field(default=8000, ge=1, le=65535)

    @model_validator(mode="after")
    def require_database_url(self) -> Self:
        if not self.fastapi_database_url.get_secret_value().strip():
            raise ValueError("FASTAPI_DATABASE_URL is required")
        return self

    def resolved_database_url(self) -> str:
        return self.fastapi_database_url.get_secret_value()
