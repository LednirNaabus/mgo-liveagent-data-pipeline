from core.LiveAgentClient import LiveAgentClient
from typing import List, Dict, Any

class Agent:
    """Class for `/agents` LiveAgent API endpoint."""
    def __init__(self, client: LiveAgentClient):
        self.endpoint = "agents"
        self.client = client

    def _default_payload(self) -> Dict[str, Any]:
        return {
            "_page": 1,
            "_perPage": 10,
            "_sortDir": "ASC"
        }

    async def get_agents(self, max_pages: int, per_page: int) -> List[Dict[str, Any]]:
        payload = self._default_payload()
        payload["_perPage"] = per_page
        try:
            return await self.client.paginate(
                payload,
                self.endpoint,
                max_pages
            )
        except Exception as e:
            print(f"Exception occurred while fetching agents: {e}")
            return []