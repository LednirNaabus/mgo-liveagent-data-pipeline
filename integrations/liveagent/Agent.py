from integrations.liveagent import LiveAgentClient, LiveAgentAPIResponse
from typing import Dict, Any
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Agent:
    """
    LiveAgent `/agents` endpoint wrapper.

    **Responsibilities**:
    - Fetches paginated agent lists via `LiveAgentClient.paginate`.
    - Fetches a single agent by ID via `LiveAgentClient.make_request`.

    :param client: Base client that interacts with the LiveAgent API.
    :type client: `LiveAgentClient`
    :param endpoint: API endpoint (default: `"agents"`).
    :type endpoint: str
    """
    def __init__(self, client: LiveAgentClient):
        self.client = client
        self.endpoint = "agents"

    def _default_payload(self) -> Dict[str, Any]:
        return {
            "_page": 1,
            "_perPage": 10,
            "_sortDir": "ASC"
        }

    async def get_agents(
        self,
        session: aiohttp.ClientSession,
        max_page: int,
        per_page: int,
        payload: Dict[str, Any] = None
    ) -> LiveAgentAPIResponse:
        if payload is None:
            payload = self._default_payload()
        payload["_perPage"] = per_page
        response = await self.client.paginate(
            session,
            self.endpoint,
            payload,
            max_page
        )
        return LiveAgentAPIResponse(
            success=True,
            data=response
        )

    async def get_agent_by_id(
        self,
        session: aiohttp.ClientSession,
        agent_id: str
    ) -> LiveAgentAPIResponse:
        if not agent_id:
            raise ValueError("agent_id is required.")
        return await self.client.make_request(
            session=session,
            endpoint=f"{self.endpoint}/{agent_id}",
            method="GET"
        )