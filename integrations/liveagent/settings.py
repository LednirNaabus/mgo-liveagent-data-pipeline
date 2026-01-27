from pydantic_settings import BaseSettings
from pydantic import Field


class LiveAgentClientBaseConfiguration(BaseSettings):
    """
    LiveAgent Client base configuration class.
    """
    BASE_URL: str = Field(default="https://mechanigo.ladesk.com/api/v3", description="LiveAgent Base URL.")
    MGO_SYSTEM_USER_ID: str = Field(default="system00", description="System user ID for MechaniGo LiveAgent.")
    MGO_SPECIAL_USER_ID: str = Field(default="00054iwg", description="Special user ID for MechaniGo LiveAgent.")

    MAX_PAGES: int = Field(default=5, description="Max pages to retrieve.")
    MAX_CONCURRENT_REQUESTS: int = Field(default=15, description="Max concurrent requests to LiveAgent API.")
    MAX_CONCURRENT_LIMIT: int = Field(default=15, description="Max concurrent limit.")
    THROTTLE_DELAY: float = Field(default=0.4, description="Default throttle delay.")