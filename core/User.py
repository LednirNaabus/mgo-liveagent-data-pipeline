from config.constants import LIVEAGENT_MGO_SYSTEM_USER_ID
from core.LiveAgentClient import LiveAgentClient
from typing import List, Dict, Tuple

class User:
    """Class for `/users` LiveAgent API endpoint."""
    def __init__(self, client: LiveAgentClient):
        self.endpoint = "users"
        self.client = client

    async def get_user(self, user_id: str):
        return await self.client.make_request(
            endpoint=f"{self.endpoint}/{user_id}"
        )

    async def get_users_from_ids(self, user_ids: set) -> List[Dict]:
        if not user_ids:
            print(f"No user IDs collected.")
            return []

        users_data = []
        for userid in user_ids:
            try:
                if userid == LIVEAGENT_MGO_SYSTEM_USER_ID:
                    user = {
                        'id': 'system00',
                        'role': 'system'
                    }
                    users_data.append(user)
                else:
                    user = await self.get_user(userid)
                users_data.append(user)
            except Exception as e:
                print(f"Exception occurred while fetching {userid}: {e}")
                continue
        
        return users_data

    def resolve_sender(self, userid: str, owner_name: str, agentid: str, agents_cache: Dict[str, str]) -> Tuple[str, str, str, str]:
        if userid == LIVEAGENT_MGO_SYSTEM_USER_ID:
            return "system", "system", owner_name, "client"

        # 0054iwg - special user ID for MechaniGo.ph system
        if userid == "0054iwg":
            return "MechaniGo.ph", "system", owner_name, "client"

        if userid in agents_cache:
            agent_name = agents_cache[userid]
            return agent_name, "agent", owner_name, "client"
        else:
            agent_name = agents_cache.get(agentid, "Unknown Agent")
            return owner_name, "client", agent_name, "agent"