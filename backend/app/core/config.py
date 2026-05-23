from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "local"
    database_url: str = "sqlite+aiosqlite:///./dev.db"
    auto_create_tables: bool = True
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"
    connection_health_check_interval_seconds: int = 60
    cors_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:3000", "http://127.0.0.1:3000"]
    )

    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    access_token_minutes: int = 60
    refresh_token_days: int = 14

    openai_api_key: str | None = None
    openai_model: str = "gpt-5.5"
    openai_base_url: str = "https://api.openai.com/v1"

    llm_provider: str = "openai"
    openrouter_api_key: str | None = None
    openrouter_model: str = "openai/gpt-5.5"
    openrouter_base_url: str = "https://openrouter.ai/api/v1"

    magnitgpt_api_key: str | None = None
    magnitgpt_model: str = "MagnitGPT"
    magnitgpt_base_url: str = "https://llmlite.ai.corp.tander.ru/v1"
    magnitgpt_verify_ssl: bool = True
    magnitgpt_timeout_seconds: float = 120.0

    langfuse_public_key: str | None = None
    langfuse_secret_key: str | None = None
    langfuse_host: str = "http://langfuse:3000"

    airflow_base_url: str | None = "http://airflow-webserver:8080"
    airflow_public_url: str | None = None
    airflow_username: str = "airflow"
    airflow_password: str = "airflow"

    spark_master_url: str = "spark://spark-master:7077"
    spark_status_url: str = "http://spark-master:8080"

    artifact_root: str = "infra"
    artifact_git_enabled: bool = True
    artifact_git_root: str | None = None
    agent_debugger_url: str | None = None

    mcp_enabled: bool = True
    mcp_service_user_email: str = "admin@local.dev"
    mcp_npx_command: str = "npx"
    mcp_uvx_command: str = "uvx"
    mcp_database_url: str | None = None
    mcp_spark_master: str = "local[*]"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
