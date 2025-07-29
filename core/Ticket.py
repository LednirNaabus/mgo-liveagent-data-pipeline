from api.schemas.response import ExtractionResponse
from core.LiveAgentClient import LiveAgentClient
from typing import Dict, List, Any
from core.User import User
import pandas as pd
import aiohttp
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Ticket:
    """Class for `/tickets` LiveAgent API endpoint."""
    def __init__(self, client: LiveAgentClient):
        self.client = client
        self.user = User(self.client)
        self.endpoint = "tickets"

        self.agents_cache = {}
        self.unique_userids = set()

    def _default_payload(self) -> Dict[str, Any]:
        return {
            "includeQuotedMessage": "false",
            "_page": 1,
            "_perPage": 10,
            "_sortDir": "ASC"
        }

    async def fetch_tickets(
        self,
        session: aiohttp.ClientSession,
        payload: Dict[str, Any] = None,
        max_pages: int = 5,
        per_page: int = 10
    ) -> ExtractionResponse:
        if payload is None:
            payload = self._default_payload()
        payload["_perPage"] = per_page
        data = await self.client.paginate(
            session,
            self.endpoint,
            payload,
            max_pages
        )

        for ticket in data:
            ticket['tags'] = ','.join(ticket['tags']) if ticket.get('tags') else ''
            ticket['date_due'] = ticket.get('date_due')
            ticket['date_deleted'] = ticket.get('date_deleted')
            ticket['date_resolved'] = ticket.get('date_resolved')

        return pd.DataFrame(data)

    async def fetch_ticket_message(
        self,
        ticket_id: str,
        max_page: int,
        per_page: int,
        session: aiohttp.ClientSession
    ) -> ExtractionResponse:
        message_payload = {
            "_page": 1,
            "_perPage": per_page
        }

        messages_data = await self.client.paginate(
            session,
            endpoint=f"{self.endpoint}/{ticket_id}/messages",
            payload=message_payload,
            max_pages=max_page
        )

        for message in messages_data:
            message['ticket_id'] = ticket_id

        return messages_data

    async def fetch_ticket_messages_batch(
        self,
        ticket_ids: List[str],
        max_page: int,
        per_page: int,
        session: aiohttp.ClientSession,
        concurrent_limit: int = 10
    ):
        """
        For fetching multiple tickets concurrently.
        """
        semaphore = asyncio.Semaphore(concurrent_limit)
        async def fetch_single_ticket_messages(ticket_id: str):
            async with semaphore:
                try:
                    logging.info(f"Fetching messages for ticket {ticket_id}")
                    return await self.fetch_ticket_message(
                        ticket_id, max_page, per_page, session
                    )
                except Exception as e:
                    logging.error(f"Error fetching messages for ticket {ticket_id}: {e}")
                    return []

        tasks = [fetch_single_ticket_messages(ticket_id) for ticket_id in ticket_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # get ticket ids
        # ticket owner name
        # ticket agent name

        flattened_messages = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logging.error(f"Failed to fetch messages for ticket {ticket_ids[i]}: {result}")

            logging.info(f"Successfully fetched messages for {len([r for r in results if not isinstance(r, Exception)])} out of {len(ticket_ids)} tickets.")
            for message in result:
                base_info = {
                    "ticket_id": ticket_ids[i],
                    "message_group_id": message.get("id"),
                    "parent_id": message.get("parent_id"),
                    "userid": message.get("userid"),
                    "user_full_name": message.get("user_full_name"),
                    "type": message.get("type"),
                    "status": message.get("status"),
                    "datecreated": message.get("datecreated"),
                    "datefinished": message.get("datefinished"),
                    "sort_order": message.get("sort_order"),
                    "mail_msg_id": message.get("mail_msg_id"),
                    "pop3_msg_id": message.get("pop3_msg_id"),
                }

                messages = message.get("messages", [])
                if messages:
                    for msg in messages:
                        flattened_message = {
                            **base_info,
                            "message_id": msg.get("id"),
                            "message_userid": msg.get("userid"),
                            "message_type": msg.get("type"),
                            "message_datecreated": msg.get("datecreated"),
                            "message_format": msg.get("format"),
                            "message_content": msg.get("message"),
                            "message_visibility": msg.get("visibility")
                        }
                        flattened_messages.append(flattened_message)
                else:
                    flattened_messages.append(base_info)
        
        return flattened_messages