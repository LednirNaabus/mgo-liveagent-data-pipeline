from openai import AsyncOpenAI, AuthenticationError, OpenAIError
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class OpenAIClient:
    """Handles OpenAI client initialization."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key
        if not self.api_key:
            raise ValueError("API key is not set or invalid!")
        self.client = None

    def get_api_key(self) -> str:

        # passed-in key
        if self.api_key:
            return self.api_key

        # environment variable
        env_key = os.environ.get("OPENAI_API_KEY")
        if env_key:
            return env_key

        # fallback file
        api_key_file = "openai-api-key.txt"
        try:
            with open(api_key_file, "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            raise ValueError("No valid API key found.")

    async def init_async_client(self) -> AsyncOpenAI:
        """Initialize and test an AsyncOpenAI client."""
        try:
            client = AsyncOpenAI(api_key=self.api_key, timeout=60)
            await client.models.list()
            logging.info("OpenAI client initialized successfully.")
            self.client = client
            return self.client
        except (AuthenticationError, OpenAIError) as e:
            logging.error(f"OpenAI client initialization failed: {e}")
            raise