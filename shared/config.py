from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    raw_topic: str = "events.raw"
    dlq_topic: str = "events.dlq"
    database_url: str = "postgresql://pipeline:pipeline@localhost:5432/analytics"


settings = Settings()
