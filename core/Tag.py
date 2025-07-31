from core.LiveAgentClient import LiveAgentClient
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Tag:
    """Class for `/tags` LiveAgent API endpoint."""
    def __init__(self, client: LiveAgentClient):
        self.endpoint = "tags"
        self.client = client

    async def get_tags(self, session: aiohttp.ClientSession):
        return await self.client.make_request(
            session=session,
            endpoint=self.endpoint
        )