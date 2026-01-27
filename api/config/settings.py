from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from enum import Enum
from functools import lru_cache
import os


class Environment(str, Enum):
    DEV = "development"
    PROD = "production"


class MGOPipelineBaseConfiguration(BaseSettings):
    """
    Docstring for MGOPipelineBaseConfiguration
    """
    ENV: Environment = Field(
        default=Environment.DEV,
        description="The enviroment the API is running (e.g., development or production)."
    )
    APP_NAME: str = "MechaniGo LiveAgent Chat Analysis API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True

    API_PREFIX: str = "/mgo-liveagent-pipeline-api/v1"
    API_HOST: str = Field(default="0.0.0.0", description="API host")
    API_PORT: int = Field(default=8000, description="API port")
    API_RELOAD: bool = Field(default=True, description="Enable auto-reload")

    @property
    def is_dev(self) -> bool:
        return self.env == Environment.DEV

    @property
    def is_prod(self) -> bool:
        return self.env == Environment.PROD


class DevelopmentSettings(MGOPipelineBaseConfiguration):
    ENV: Environment = Environment.DEV
    model_config = SettingsConfigDict(
        env_file=".env.dev",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


class ProductionSettings(MGOPipelineBaseConfiguration):
    ENV: Environment = Environment.PROD
    API_RELOAD: bool = False
    DEBUG: bool = False
    model_config = SettingsConfigDict(
        env_file=".env.prod",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


@lru_cache
def get_settings() -> MGOPipelineBaseConfiguration:
    env = os.getenv("ENVIRONMENT", "development").lower()
    settings_map = {
        "development": DevelopmentSettings,
        "production": ProductionSettings
    }

    settings_class = settings_map.get(env, DevelopmentSettings)
    return settings_class()