from integrations.liveagent import LiveAgentClient
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Tag:
    """
    Docstring for Tag
    """
    def __init__(self, client: LiveAgentClient):
        self.client = client
        self.endpoint = "tags"

    async def get_tags(self, session: aiohttp.ClientSession):
        return await self.client.make_request(
            session=session,
            endpoint=self.endpoint
        )