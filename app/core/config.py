from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DF_", env_file=".env", extra="ignore")
    database_url: str = "sqlite:///./data/dataflow.db"
    storage_root: Path = Path("data/storage")
    execution_mode: Literal["local", "celery"] = "local"
    redis_url: str = "redis://localhost:6379/0"
    api_token: str = ""
    owner_id: str = "00000000-0000-0000-0000-000000000001"
    lease_seconds: int = Field(default=30, ge=3)
    retry_base_seconds: float = Field(default=2, ge=0)
    dispatch_interval: float = Field(default=1, ge=0.1)
    max_upload_bytes: int = Field(default=100 * 1024 * 1024, ge=1)
    max_rows: int = Field(default=1_000_000, ge=1)
    max_unique_keys: int = Field(default=200_000, ge=1)
    max_groups: int = Field(default=100_000, ge=1)
    max_columns: int = Field(default=200, ge=1)
    max_field_chars: int = Field(default=1_000_000, ge=1)
    orphan_grace_seconds: int = Field(default=86400, ge=3600)
    heartbeat_interval_seconds: float = Field(default=5, ge=0.2)
    heartbeat_timeout_seconds: int = Field(default=20, ge=2)
    maintenance_interval_seconds: int = Field(default=3600, ge=10)
    input_archive_days: int = Field(default=30, ge=1)
    event_archive_days: int = Field(default=30, ge=1)
    maintenance_batch_size: int = Field(default=100, ge=1, le=1000)
    alert_queue_age_seconds: int = Field(default=120, ge=1)
    alert_failure_rate: float = Field(default=0.2, ge=0, le=1)
    alert_failure_min_jobs: int = Field(default=5, ge=1)
    db_connect_timeout_seconds: int = Field(default=3, ge=1)
    db_statement_timeout_ms: int = Field(default=5000, ge=1000)


@lru_cache
def get_settings() -> Settings:
    return Settings()
