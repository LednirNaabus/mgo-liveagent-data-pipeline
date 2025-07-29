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
            print("No user IDs collected.")
            return []

        users_data = []
        for userid in user_ids:
            try:
                if userid == LIVEAGENT_MGO_SYSTEM_USER_ID:
                    user = {
                        "id": "system00",
                        "role": "system"
                    }
                    users_data.append(user)
                else:
                    user = await self.get_user(userid)
                users_data.append(user)
            except Exception as e:
                print(f"Exception occurred while fetching {userid}: {e}")
                continue

        return users_data