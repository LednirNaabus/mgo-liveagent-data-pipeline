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

    async def preload_users_from_bq(self, user_ids: Set[str]):
        if not self.bigquery_client or not user_ids:
            return
        
        try:
            user_ids_str = "', '".join(user_ids)
            query = f"""
            SELECT DISTINCT CAST(id AS STRING) as id, name, email, role, avatar_url
            FROM `{PROJECT_ID}.{DATASET_NAME}.users`
            WHERE CAST(id AS STRING) IN ('{user_ids_str}')
            AND id IS NOT NULL
            """
            df = self.bigquery_client.sql_query_bq(query)

            if not df.empty:
                for _, row in df.iterrows():
                    user_id = str(row["id"])
                    self.user_cache[user_id] = {
                        "id": user_id,
                        "name": row.get("name", ""),
                        "email": row.get("email", ""),
                        "role": row.get("role", ""),
                        "avatar_url": row.get("avatar_url", "")
                    }
                logging.info(f"Preloaded {len(df)} users from BigQuery.")
        except Exception as e:
            logging.error(f"Error preloading users from BigQuery: {e}")

    async def fetch_users_batch(
        self,
        session: aiohttp.ClientSession,
        user_ids: List[str],
        concurrent_limit: int = 15
    ) -> Dict[str, Dict]:
        if not user_ids:
            return {}
        unique_user_ids = list(set(user_ids))
        if len(unique_user_ids) != len(user_ids):
            logging.info(f"Deduplicated {len(user_ids)} user IDs to {len(unique_user_ids)} unique IDs")

        non_agent_user_ids = [
            uid for uid in unique_user_ids
            if uid not in self.agent_cache and uid not in self.user_cache
        ]

        if not non_agent_user_ids:
            logging.info("All user IDs are either agents or already cached, no additional requests needed.")
            return {}

        semaphore = asyncio.Semaphore(concurrent_limit)

        logging.info(f"Fetching user data for {len(non_agent_user_ids)} non-agent user IDs.")
        async def fetch_single_user(user_id: str):
            async with semaphore:
                try:
                    user_data = await self.user.get_user(
                        session=session,
                        user_id=user_id
                    )
                    return user_id, user_data, None
                except Exception as e:
                    logging.warning(f"Failed to fetch user {user_id}: {e}")
                    return user_id, None, e

        try:
            tasks = [fetch_single_user(user_id) for user_id in non_agent_user_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            needed_users = {}
            successful_fetches = 0
            failed_fetches = 0
            for user_id, user_data, exception in results:
                if exception:
                    failed_fetches += 1
                    logging.error(f"Exception fetching user: {user_id}: {exception}")
                    continue

                if user_data is None or not user_data.success:
                    failed_fetches += 1
                    logging.warning(f"No valid data returned for user {user_id}")
                    continue

                if hasattr(user_data, 'data') and user_data.data:
                    for user_info in user_data.data:
                        logging.info(f"Processing user_info for {user_id}: {user_info["name"]}")

                        name = self._resolve_user_name(user_info)
                        needed_users[user_id] = {
                            "id": user_id,
                            "name": name,
                            "email": user_info.get("email", ""),
                            "role": user_info.get("role", ""),
                            "avatar_url": user_info.get("avatar_url", "")
                        }
                        successful_fetches += 1
                        break

            self.user_cache.update(needed_users)
            logging.info(f"User fetch completed: {successful_fetches} successful", f"{failed_fetches} failed out of {len(non_agent_user_ids)} total requests.")
            return needed_users
        except Exception as e:
            import traceback
            traceback.print_exc()
            logging.info(f"Error fetching user data: {e}")
            return {}

    async def fetch_user_in_chunks(
        self,
        session: aiohttp.ClientSession,
        user_ids: List[str],
        chunk_size: int = 50,
        concurrent_limit: int = 15
    ) -> Dict[str, Dict]:
        if len(user_ids) <= chunk_size:
            return await self.fetch_users_batch(session, user_ids, concurrent_limit)

        all_users = {}
        total_chunks = (len(user_ids) + chunk_size - 1) // chunk_size

        for i in range(0, len(user_ids), chunk_size):
            chunk = user_ids[i:i + chunk_size]
            chunk_num = (i // chunk_size) + 1

            logging.info(f"Processing user chunk {chunk_num}/{total_chunks} ({len(chunk)} users)")
            chunk_result = await self.fetch_users_batch(session, chunk, concurrent_limit)
            all_users.update(chunk_result)

            if chunk_num < total_chunks:
                await asyncio.sleep(0.1)

        logging.info(f"Completed processing {len(user_ids)} users in {total_chunks} chunks")
        return all_users

    def _resolve_user_name(self, user_info: Dict) -> str:
        name = user_info.get("name", "").strip()
        if name:
            return name

        email = user_info.get("email", "").strip()
        if email:
            return email

        return "Unknown Name"

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

        await self.preload_users_from_bq(unique_user_ids)

        await self.fetch_user_in_chunks(session, list(unique_user_ids), chunk_size=50, concurrent_limit=15)

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