from config.constants import PROJECT_ID, DATASET_NAME, LIVEAGENT_MGO_SYSTEM_USER_ID, LIVEAGENT_MGO_SPECIAL_USER_ID
from core.LiveAgentClient import LiveAgentClient
from typing import Dict, List, Set, Tuple, Any
from core.BigQueryManager import BigQuery
from core.User import User
import aiohttp
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class TicketMessageProcessor:
    """Handles ticket messages and processing."""
    def __init__(self, client: LiveAgentClient):
        self.client = client
        self.user = User(self.client)
        self.bigquery_client = BigQuery()
        self.user_cache = {}
        self.agent_cache = {}

    def _extract_unique_userids(self, messages_data: List[Dict]) -> Set[str]:
        user_ids = set()

        for message in messages_data:
            if message.get("userid"):
                user_ids.add(message["userid"])

            if message.get("message_userid"):
                user_ids.add(message["message_userid"])

            if message.get("agentid"):
                user_ids.add(message["agentid"])

        return user_ids

    def get_user_cache(self) -> Dict[str, Dict[str, Any]]:
        return self.user_cache

    async def load_agents_from_bq(self):
        if not self.bigquery_client:
            logging.warning("No BigQuery client set.")
            return

        try:
            query = f"""
            SELECT id, name
            FROM `{PROJECT_ID}.{DATASET_NAME}.agents`
            """
            df = self.bigquery_client.sql_query_bq(query)
            for _, row in df.iterrows():
                agent_id = row["id"]
                self.agent_cache[agent_id] = {
                    "id": agent_id,
                    "name": row["name"]
                }
            logging.info(f"Loaded {len(self.agent_cache)} agents from BigQuery.")
        except Exception as e:
            logging.error(f"Error loading agent data from BigQuery: {e}")

    async def fetch_users_batch(
        self,
        session: aiohttp.ClientSession,
        user_ids: List[str]
    ) -> Dict[str, Dict]:
        if not user_ids:
            return {}

        non_agent_user_ids = [uid for uid in user_ids if uid not in self.agent_cache]

        if not non_agent_user_ids:
            logging.info("All user IDs are agents, no additional user data needed.")
            return {}
        logging.info(f"Fetching user data for {len(non_agent_user_ids)} non-agent user IDs.")
        semaphore = asyncio.Semaphore(10) # modify later
        async def fetch_single_user(user_id: str):
            async with semaphore:
                user_data = await self.user.get_user(
                    session=session,
                    user_id=user_id
                )
                return user_id, user_data

        try:
            tasks = [fetch_single_user(user_id) for user_id in non_agent_user_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            needed_users = {}
            successful_fetches = 0

            for result in results:
                if isinstance(result, Exception):
                    logging.error(f"Exception in user fetch task: {result}")
                    continue

                user_id, user_data = result

                if user_data is None:
                    continue

                for user_info in user_data.data:
                    name = user_info.get("name", "") or f"{user_info.get("name")}"
                    if not name:
                        name = user_info.get("email", "Unknown Name")

                    needed_users[user_id] = {
                        "id": user_id,
                        "name": name,
                        "email": user_info.get("email", ""),
                        "role": user_info.get("role", ""),
                        "avatar_url": user_info.get("avatar_url", "")
                    }
                successful_fetches += 1

            self.user_cache.update(needed_users)
            logging.info(f"Successfully fetched {successful_fetches} out of {len(non_agent_user_ids)} users.")
            return needed_users
        except Exception as e:
            import traceback
            traceback.print_exc()
            logging.info(f"Error fetching user data: {e}")
            return {}

    def _get_sender(self, message_data: Dict) -> Tuple[str, str]:
        message_userid = message_data.get("message_userid") or message_data.get("userid")
        ticket_agentid = message_data.get("agentid")
        owner_name = message_data.get("owner_name", "")

        sender_info = self.agent_cache.get(message_userid) or self.user_cache.get(message_userid)

        sender_name = owner_name # override
        if sender_info:
            if sender_info["id"] == LIVEAGENT_MGO_SYSTEM_USER_ID:
                return "System", "system"
            elif sender_info["id"] == LIVEAGENT_MGO_SPECIAL_USER_ID: # 0054iwg - special user ID for MechaniGo.ph system
                return "MechaniGo.ph", "system"
            else:
                return sender_name, sender_info.get("role", "Unknown")
        else:
            if message_userid == ticket_agentid:
                return sender_name, "agent"
            else:
                return sender_name, "client"

    def _get_receiver(self, sender_type: str, message_data: Dict) -> Tuple[str, str]:
        ticket_agentid = message_data.get("agentid")
        owner_name = message_data.get("owner_name", "Unknown User")

        if sender_type == "agent":
            return owner_name, "client"
        else:
            agent_info = self.agent_cache.get(ticket_agentid)
            if agent_info:
                if agent_info["id"] == LIVEAGENT_MGO_SPECIAL_USER_ID:
                    return "MechaniGo.ph", "system"
                return agent_info.get("name", "Unknown Agent"), "agent"
            else:
                return "Unknown Agent", "agent"
    
    def _determine_sender_receiver(self, message_data: Dict) -> Dict[str, str]:
        sender_name, sender_type = self._get_sender(message_data)
        receiver_name, receiver_type = self._get_receiver(sender_type, message_data)

        return {
            "sender_name": sender_name,
            "sender_type": sender_type,
            "receiver_name": receiver_name,
            "receiver_type": receiver_type
        }

    async def process_messages_with_metadata(
        self,
        messages_data: List[Dict],
        session: aiohttp.ClientSession
    ) -> List[Dict]:
        unique_user_ids = self._extract_unique_userids(messages_data)
        logging.info(f"Found {len(unique_user_ids)} unique user IDs")

        if not self.agent_cache:
            await self.load_agents_from_bq()

        await self.fetch_users_batch(session, list(unique_user_ids))

        processed_messages = []

        for msg in messages_data:
            sender_receiver_info = self._determine_sender_receiver(msg)

            agent_info = self.agent_cache.get(msg.get("agentid"))
            agent_name = agent_info["name"] if agent_info else "Unknown Agent"

            enhanced_msg = {
                **msg,
                **sender_receiver_info,
                "agent_name": agent_name
            }
            processed_messages.append(enhanced_msg)

        return processed_messages