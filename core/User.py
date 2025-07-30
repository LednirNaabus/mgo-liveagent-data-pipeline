from core.LiveAgentClient import LiveAgentClient
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class User:
    """Class for `/users` LiveAgent API endpoint."""
    def __init__(self, client: LiveAgentClient):
        self.endpoint = "users"
        self.client = client

    async def get_user(self, user_id: str, session: aiohttp.ClientSession):
        return await self.client.make_request(
            session=session,
            endpoint=f"{self.endpoint}/{user_id}"
        )