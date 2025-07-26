from api.schemas.response import ExtractionResponse
from core.LiveAgentClient import LiveAgentClient
from typing import Dict, Any
import logging
import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Agent:
    """Class for `/agents` LiveAgent API endpoint."""
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
    ) -> ExtractionResponse:
        if payload is None:
            payload = self._default_payload()
        payload["_perPage"] = per_page
        return await self.client.paginate(
            session,
            self.endpoint,
            payload,
            max_page
        )