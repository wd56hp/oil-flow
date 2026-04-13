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

    # UN Comtrade Plus (optional — jobs no-op without a key)
    uncomtrade_api_key: str | None = None
    # Base URL for Comtrade Plus public API (no trailing slash).
    uncomtrade_base_url: str = "https://comtradeplus.un.org/api/public/v1"
    # Logical dataset label on TradeFlowRecord rows from this connector.
    uncomtrade_dataset: str = "comtrade-hs"
    # Optional JSON object of default query parameters for GET .../data (merged with CLI).
    uncomtrade_query_json: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
