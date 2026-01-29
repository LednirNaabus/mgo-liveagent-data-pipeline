from integrations.liveagent import LiveAgentClient, LiveAgentAPIResponse
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Tag:
    """
    LiveAgent `/tags` endpoint wrapper.

    **Responsibilities**:
    - Fetches tag lists via `LiveAgentClient.make_request`.
    - Fetches a single tag by ID via `LiveAgentClient.make_request`.

    :param client: Base client that interacts with the LiveAgent API.
    :type client: `LiveAgentClient`
    :param endpoint: API endpoint (default: `"tags"`)
    :type endpoint: str
    """
    def __init__(self, client: LiveAgentClient):
        self.client = client
        self.endpoint = "tags"

    async def get_tags(self, session: aiohttp.ClientSession) -> LiveAgentAPIResponse:
        return await self.client.make_request(
            session=session,
            endpoint=self.endpoint
        )

    async def get_tag(self, session: aiohttp.ClientSession, tag_id: str):
        return await self.client.make_request(
            session=session,
            endpoint=f"{self.endpoint}/{tag_id}"
        )