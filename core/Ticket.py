from config.constants import LIVEAGENT_MGO_SYSTEM_USER_ID
from typing import List, Dict, Any, Tuple, Optional
from core.LiveAgentClient import LiveAgentClient
from core.User import User
import asyncio

class Ticket:
    """Class for `/tickets` LiveAgent API endpoint."""
    def __init__(self, client: LiveAgentClient, user_client: Optional[User] = None):
        self.endpoint = "tickets"
        self.client = client
        self.user_client = user_client
        self.agents_cache = {}
        self.unique_userids = set()

    def _default_payload(self) -> Dict[str, Any]:
        return {
            "includeQuotedMessages": "false",
            "_page": 1,
            "_perPage": 10,
            "_sortDir": "ASC"
        }

    def _set_user_client(self, user_client: User):
        self.user_client = user_client

    def _get_ticket_context(self, data: List[Dict[str, Any]]) -> List[Tuple[str, str, str]]:
        return [(ticket["id"], ticket["owner_name"], ticket.get("agentid")) for ticket in data]

    def _process_message_group(self, msg_grp: Dict[str, Any], ticket_id: str, owner_name: str, agent_id: str, agent_name: str) -> Dict:
        msg_grp_data = {
            'ticket_id': ticket_id,
            'owner_name': owner_name,
            'agentid': agent_id,
            'agent_name': agent_name,
            'id': msg_grp.get('id'),
            'parent_id': msg_grp.get('parent_id'),
            'userid': msg_grp.get('userid'),
            'user_full_name': msg_grp.get('user_full_name'),
            'type': msg_grp.get('type'),
            'status': msg_grp.get('status'),
            'datecreated': msg_grp.get('datecreated'),
            'datefinished': msg_grp.get('datefinished'),
            'sort_order': msg_grp.get('sort_order'),
            'mail_msg_id': msg_grp.get('mail_msg_id'),
            'pop3_msg_id': msg_grp.get('pop3_msg_id'),
            'message_id': None,
            'message_userid': None,
            'message_type': None,
            'message_datecreated': None,
            'message_format': None,
            'message': None,
            'message_visibility': None
        }
        sender_name, sender_type, receiver_name, receiver_type = self.determine_sender_receiver(
            msg_grp.get('userid'), owner_name, agent_id
        )

        msg_grp_data.update({
            'sender_name': sender_name,
            'sender_type': sender_type,
            'receiver_name': receiver_name,
            'receiver_type': receiver_type
        })
        self.unique_userids.add(msg_grp.get('userid'))
        return msg_grp_data

    def _process_nested_messages(self, msg_grp: Dict[str, Any], base_data: Dict[str, Any], owner_name: str, agent_id: str) -> List[Dict[str, Any]]:
        nested_msgs = []
        if 'messages' in msg_grp and msg_grp['messages']:
            for nested_msg in msg_grp['messages']:
                nested_msg_data = base_data.copy()
                nested_msg_data.update({
                    'message_id': nested_msg.get('id'),
                    'message_userid': nested_msg.get('userid'),
                    'message_type': nested_msg.get('type'),
                    'message_datecreated': nested_msg.get('datecreated'),
                    'message_format': nested_msg.get('format'),
                    'message': nested_msg.get('message'),
                    'message_visibility': nested_msg.get('visibility'),
                })

                sender_name, sender_type, receiver_name, receiver_type = self.determine_sender_receiver(
                    nested_msg.get('userid'), owner_name, agent_id
                )

                nested_msg_data.update({
                    'sender_name': sender_name,
                    'sender_type': sender_type,
                    'receiver_name': receiver_name,
                    'receiver_type': receiver_type
                })

                self.unique_userids.add(nested_msg.get('userid'))
                nested_msgs.append(nested_msg_data)

        return nested_msgs


    async def fetch_tickets(self, max_pages: int = 10, per_page: int = 10) -> List[Dict[str, Any]]:
        payload = self._default_payload()
        payload["_perPage"] = per_page
        try:
            ticket_data = await self.client.paginate(
                payload,
                self.endpoint,
                max_pages
            )
            for ticket in ticket_data:
                ticket['tags'] = ','.join(ticket['tags']) if ticket.get('tags') else ''
                ticket['date_due'] = ticket.get('date_due')
                ticket['date_deleted'] = ticket.get('date_deleted')
                ticket['date_resolved'] = ticket.get('date_resolved')
            return ticket_data
        except Exception as e:
            print(f"Exception occurred while fetching tickets: {e}")
            return []

    async def fetch_ticket_messages(self, ticket_id: str, max_pages: int, per_page: int) -> List[Dict[str, Any]]:
        try:
            message_payload = {
                "_page": 1,
                "_perPage": per_page
            }
            messages_data = await self.client.paginate(
                message_payload,
                endpoint=f"{self.endpoint}/{ticket_id}/messages",
                max_pages=max_pages
            )
            return messages_data
        except Exception as e:
            print(f"Exception occurred while fetching ticket messages: {e}")
            return []

    async def fetch_messages_by_ticket_contexts(self, ticket_contexts: List[Tuple[str, str, str]], max_pages: int, per_page: int) -> List[Tuple[str, str, str, List[Dict[str, Any]]]]:
        tasks = []
        for ticket_id, owner_name, agent_id in ticket_contexts:
            task = self.fetch_ticket_messages(ticket_id, max_pages, per_page)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid_results = []
        for i, result in enumerate(results):
            ticket_id, owner_name, agent_id = ticket_contexts[i]
            if isinstance(result, Exception):
                print(f"Error fetching messages for ticket {ticket_id}: {result}")
                continue
            valid_results.append((ticket_id, owner_name, agent_id, result))

        return valid_results

    async def parse_ticket_messages(self, data: List[Dict[str, Any]], message_per_page: int = None, max_pages: int = None) -> List[Dict[str, Any]]:
        try:
            ticket_contexts = self._get_ticket_context(data)
            processed_messages = []
            for ticket_id, owner_name, agent_id in ticket_contexts:
                messages = await self.fetch_ticket_messages(ticket_id, max_pages, message_per_page)
                agent_name = self.agents_cache.get(agent_id, "Unknown Agent")

                for msg_grp in messages:
                    msg_grp_data = self._process_message_group(msg_grp, ticket_id, owner_name, agent_id, agent_name)
                    nested_msgs = self._process_nested_messages(msg_grp, msg_grp_data, owner_name, agent_id)

                    if nested_msgs:
                        processed_messages.extend(nested_msgs)
                    else:
                        processed_messages.append(msg_grp_data)

            return processed_messages
        except Exception as e:
            print(f"Exception occurred while parsing ticket messages: {e}")
            return None

    async def populate_users_from_collected_ids(self) -> List[Dict]:
        if not self.unique_userids:
            print(f"No user IDs collected.")
            return []

        if not self.user_client:
            raise ValueError("User client is not set. Cannot determine ticket sender/receiver.")
        
        return await self.user_client.get_users_from_ids(self.unique_userids)
    
    def determine_sender_receiver(self, userid: str, owner_name: str, agentid: str) -> Tuple[str, str, str, str]:
        if not self.user_client:
            raise ValueError("User client is not set. Cannot determine ticket sender/receiver.")
        return self.user_client.resolve_sender(userid, owner_name, agentid, self.agents_cache)