from openai import AsyncOpenAI, AuthenticationError, OpenAIError
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class OpenAIClient:
    """Handles OpenAI client initialization."""
    def __init__(self, api_key: str):
        self.api_key = api_key
        if not self.api_key:
            raise ValueError("API key is not set or invalid!")
        self.client = None

    async def init_async_client(self) -> AsyncOpenAI:
        try:
            client = AsyncOpenAI(api_key=self.api_key, timeout=60)
            await client.models.list()
            logging.info("OpenAI client initialized successfully.")
            self.client = client
            logging.info(f"Client: {self.client}")
            return self.client
        except (AuthenticationError, OpenAIError) as e:
            logging.error(f"OpenAI client initialization failed: {e}")
            raise