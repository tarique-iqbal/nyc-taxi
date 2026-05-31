"""
Master settings module.

All configuration is driven by environment variables (or a .env file).
Pydantic-settings validates and coerces types at startup - a missing
required variable raises immediately, not mid-run.

Usage anywhere in the codebase:
    from etl.config.settings import get_settings
    settings = get_settings()
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Sub-configs

class KafkaSettings(BaseSettings):
    """Kafka broker and topic configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
        description="Comma-separated list of broker host:port pairs.",
    )
    topic: str = Field(
        default="nyc-taxi-trips",
        alias="KAFKA_TOPIC",
    )
    dlq_topic: str = Field(
        default="nyc-taxi-trips-dlq",
        alias="KAFKA_DLQ_TOPIC",
    )
    consumer_group_id: str = Field(
        default="nyc-taxi-etl-consumer",
        alias="KAFKA_CONSUMER_GROUP_ID",
    )
    consumer_timeout_ms: int = Field(
        default=5000,
        alias="KAFKA_CONSUMER_TIMEOUT_MS",
        ge=100,
    )
    producer_acks: str = Field(
        default="all",
        alias="KAFKA_PRODUCER_ACKS",
    )
    producer_retries: int = Field(
        default=5,
        alias="KAFKA_PRODUCER_RETRIES",
        ge=0,
    )
    batch_size: int = Field(
        default=500,
        alias="KAFKA_BATCH_SIZE",
        ge=1,
        le=10_000,
        description="Number of Kafka messages to accumulate before inserting into ClickHouse.",
    )
    batch_timeout_seconds: int = Field(
        default=10,
        alias="KAFKA_BATCH_TIMEOUT_SECONDS",
        ge=1,
        description="Max seconds to wait before flushing an incomplete batch.",
    )


class ClickHouseSettings(BaseSettings):
    """ClickHouse connection and insert configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = Field(default="localhost", alias="CLICKHOUSE_HOST")
    port: int = Field(default=9000, alias="CLICKHOUSE_PORT")
    database: str = Field(default="taxi", alias="CLICKHOUSE_DB")
    user: str = Field(default="default", alias="CLICKHOUSE_USER")
    password: str = Field(default="", alias="CLICKHOUSE_PASSWORD")
    async_insert: int = Field(
        default=1,
        alias="CLICKHOUSE_ASYNC_INSERT",
        description="Enable ClickHouse async insert buffering (0 or 1).",
    )
    wait_for_async_insert: int = Field(
        default=1,
        alias="CLICKHOUSE_WAIT_FOR_ASYNC_INSERT",
        description="Wait for async insert to flush before returning (0 or 1).",
    )


class MonitoringSettings(BaseSettings):
    """Prometheus metrics and health check server configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    prometheus_port_producer: int = Field(default=9100, alias="PROMETHEUS_PORT_PRODUCER")
    prometheus_port_consumer: int = Field(default=9101, alias="PROMETHEUS_PORT_CONSUMER")
    health_port: int = Field(default=8000, alias="HEALTH_PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        upper = v.upper()
        if upper not in allowed:
            allowed_str = ", ".join(allowed)
            raise ValueError(
                f"LOG_LEVEL must be one of: {allowed_str}. Got '{v}'."
            )
        return upper


class ETLSettings(BaseSettings):
    """ETL pipeline paths and processing configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    parquet_file_path: Path = Field(
        default=Path("data/raw/yellow_tripdata_2024-01.parquet"),
        alias="PARQUET_FILE_PATH",
    )
    zone_lookup_path: Path = Field(
        default=Path("data/reference/taxi_zone_lookup.csv"),
        alias="ZONE_LOOKUP_PATH",
    )
    rejected_dir: Path = Field(
        default=Path("data/rejected"),
        alias="REJECTED_DIR",
    )
    parquet_batch_size: int = Field(
        default=1000,
        alias="PARQUET_BATCH_SIZE",
        ge=1,
        le=100_000,
        description="Number of rows per batch read from Parquet.",
    )

    @field_validator("parquet_file_path", "zone_lookup_path", mode="before")
    @classmethod
    def coerce_path(cls, v: object) -> Path:
        return Path(str(v))


# Master settings - aggregates all sub-configs

class Settings(BaseSettings):
    """
    Master settings object.

    All sub-configs are composed here. Importing code calls get_settings()
    and accesses e.g. settings.kafka.topic or settings.clickhouse.host.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    monitoring: MonitoringSettings = Field(default_factory=MonitoringSettings)
    etl: ETLSettings = Field(default_factory=ETLSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the singleton Settings instance.

    Cached after first call - safe to call from anywhere without
    re-reading the environment on every access.

    To reset in tests:
        get_settings.cache_clear()
    """
    return Settings()
