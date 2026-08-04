"""Worker environment configuration."""

from pathlib import Path
from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: SecretStr
    raw_data_root: Path = Path("data/raw")


class ApiSettings(BaseSettings):
    """External read-only API configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: SecretStr | None = None
    fastapi_database_url: SecretStr | None = None
    fastapi_api_key: SecretStr = Field(min_length=16)
    fastapi_host: str = "127.0.0.1"
    fastapi_port: int = Field(default=8000, ge=1, le=65535)

    @model_validator(mode="after")
    def require_database_url(self) -> Self:
        if self._configured_database_url() is None:
            raise ValueError("FASTAPI_DATABASE_URL or DATABASE_URL is required")
        return self

    def resolved_database_url(self) -> str:
        configured = self._configured_database_url()
        if configured is None:  # pragma: no cover - guarded by model validation
            raise RuntimeError("API database URL is unavailable")
        return configured.get_secret_value()

    def _configured_database_url(self) -> SecretStr | None:
        for configured in (self.fastapi_database_url, self.database_url):
            if configured is not None and configured.get_secret_value().strip():
                return configured
        return None
