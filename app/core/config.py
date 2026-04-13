from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment (see `.env.example`)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str
    eia_api_key: str | None = None

    # IEA / licensed data services (optional until a concrete API is wired)
    iea_api_key: str | None = None
    iea_api_base_url: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
