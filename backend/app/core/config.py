from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    app_name: str = "Narrative Treemap API"
    app_env: str = "development"
    backend_host: str = "0.0.0.0"
    backend_port: int = 8000
    cors_origins: str = "*"
    database_path: str = str(BASE_DIR / "data" / "narrative_treemap.db")
    seed_data_dir: str = str(BASE_DIR / "data")
    refresh_interval_seconds: int = 900
    cache_stale_seconds: int = 600
    request_timeout_seconds: float = 12.0
    user_agent: str = "NarrativeTreemapBot/0.1 (+https://localhost)"
    max_items_per_source: int = 220

    model_config = SettingsConfigDict(env_file=".env", env_prefix="NT_", extra="ignore")

    @property
    def database_file(self) -> Path:
        return Path(self.database_path)

    @property
    def seed_directory(self) -> Path:
        return Path(self.seed_data_dir)


settings = Settings()
