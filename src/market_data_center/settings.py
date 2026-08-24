"""Worker environment configuration."""

from pathlib import Path
from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: SecretStr
    raw_data_root: Path = Path("data/raw")
    daily_bar_write_batch_size: int = Field(default=100, ge=1, le=500)


class TushareSettings(BaseSettings):
    """Credentials for the optional Tushare provider."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    tushare_token: SecretStr
    tushare_shareholder_count_max_calls_per_minute: int = Field(default=180, ge=1, le=200)


class SchedulerSettings(BaseSettings):
    """Runtime paths and optional-task switches for the collection Worker."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    scheduler_store_path: Path = Path("data/scheduler/jobs.sqlite")
    worker_admin_port: int = Field(default=8765, ge=1, le=65_535)
    eod_quote_snapshot_enabled: bool = True
    call_auction_snapshot_enabled: bool = True
    call_auction_market_series_enabled: bool = True
    # Remains opt-in until the new migration and provider preflight are explicitly deployed.
    today_limit_up_snapshot_enabled: bool = False
    close_price_new_highs_120d_enabled: bool = True
    board_index_daily_bar_enabled: bool = True


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


class TencentQuoteSettings(BaseSettings):
    """Bounded Tencent batch quote request settings."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    tencent_quote_timeout_seconds: float = Field(default=3.0, gt=0, le=10)
    tencent_quote_batch_size: int = Field(default=50, ge=1, le=50)


class PytdxDailyBarSettings(BaseSettings):
    """Bounded local and remote unadjusted Daily Bar settings."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pytdx_vipdoc_path: str = ""
    pytdx_daily_bar_timeout_seconds: float = Field(default=3.0, gt=0, le=10)
    pytdx_daily_bar_max_attempts: int = Field(default=2, ge=1, le=5)
    pytdx_daily_bar_page_size: int = Field(default=800, ge=1, le=800)
    pytdx_daily_bar_max_pages: int = Field(default=16, ge=1, le=64)


class TodayLimitUpProviderSettings(BaseSettings):
    """Bounded public-node access for the current-day AKShare/Eastmoney pool."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    today_limit_up_timeout_seconds: float = Field(default=10.0, gt=0, le=30)
    today_limit_up_max_attempts: int = Field(default=2, ge=1, le=3)


class ApiSettings(BaseSettings):
    """External read-only API configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    fastapi_database_url: SecretStr
    fastapi_api_key: SecretStr = Field(min_length=32)
    fastapi_host: str = "127.0.0.1"
    fastapi_port: int = Field(default=8000, ge=1, le=65535)
    fastapi_tencent_quote_deadline_seconds: float = Field(default=8.0, ge=1, le=15)
    fastapi_auction_live_timeout_seconds: float = Field(default=5.0, ge=1, le=8)
    fastapi_auction_live_max_attempts: int = Field(default=2, ge=1, le=2)
    fastapi_auction_live_cache_seconds: float = Field(default=3.0, ge=0, le=5)
    fastapi_auction_live_minimum_interval_seconds: float = Field(default=1.0, ge=0.5, le=10)
    fastapi_auction_raw_root: Path = Path("./data/api-raw")

    @model_validator(mode="after")
    def require_database_url(self) -> Self:
        if not self.fastapi_database_url.get_secret_value().strip():
            raise ValueError("FASTAPI_DATABASE_URL is required")
        return self

    def resolved_database_url(self) -> str:
        return self.fastapi_database_url.get_secret_value()
