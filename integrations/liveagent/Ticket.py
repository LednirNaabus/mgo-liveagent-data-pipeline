from integrations.liveagent import (
    TicketMessageProcessor,
    ExtractionResponse,
    LiveAgentClient,
    User
)
from typing import Dict, List, Any
import pandas as pd
import aiohttp
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Ticket:
    """
    Class for `/tickets` LiveAgent API endpoint.
    """
    def __init__(
        self,
        client: LiveAgentClient,
        user: User,
        message_processor: TicketMessageProcessor
    ):
        self.client = client
        self.user = user
        self.message_processor = message_processor
        self.endpoint = "tickets"
        self.ticket_metadata_cache = {}

    def _default_payload(self) -> Dict[str, Any]:
        return {
            "includeQuotedMessage": "false",
            "_page": 1,
            "_perPage": 10,
            "_sortDir": "ASC"
        }

    def get_ticket_metadata_cache(self) -> Dict[str, Dict]:
        return self.ticket_metadata_cache

    def get_user_cache(self) -> Dict[str, Dict[str, Any]]:
        return self.message_processor.get_user_cache()

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
            ticket["owner_name"] = ticket.get("owner_name", None)
            ticket["agentId"] = ticket.get("agentid", None)
            ticket["tags"] = ','.join(ticket["tags"]) if ticket.get("tags") else ""
            ticket["date_due"] = ticket.get("date_due")
            ticket["date_deleted"] = ticket.get("date_deleted")
            ticket["date_resolved"] = ticket.get("date_resolved")

            ticket_id = ticket.get("id", None)
            if ticket_id:
                self.ticket_metadata_cache[ticket_id] = {
                    "ticket_id": ticket_id,
                    "owner_name": ticket.get("owner_name", None),
                    "agentid": ticket.get("agentid", None)
                }

        return pd.DataFrame(data)

    async def fetch_ticket_message(
        self,
        ticket_id: str,
        ticket_agent_id: str,
        ticket_owner_name: str,
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

        ticket_metadata = self.ticket_metadata_cache.get(
            ticket_id, {
                "ticket_id": ticket_id,
                "agentid": ticket_agent_id,
                "owner_name": ticket_owner_name
            }
        )

        for message in messages_data:
            message.update(ticket_metadata)

        return messages_data

    async def fetch_ticket_messages_batch(
        self,
        ticket_ids: List[str],
        ticket_agentids: List[str],
        ticket_owner_names: List[str],
        max_page: int,
        per_page: int,
        session: aiohttp.ClientSession,
        concurrent_limit: int = 10
    ):
        """
        For fetching multiple tickets concurrently.
        """
        semaphore = asyncio.Semaphore(concurrent_limit)
        async def fetch_single_ticket_messages(ticket_id: str, owner_name: str, agent_id: str):
            async with semaphore:
                try:
                    logging.info(f"Fetching messages for ticket {ticket_id}")
                    return await self.fetch_ticket_message(
                        ticket_id, agent_id, owner_name, max_page, per_page, session
                    )
                except Exception as e:
                    logging.error(f"Error fetching messages for ticket {ticket_id}: {e}")
                    return []

        tasks = [
            fetch_single_ticket_messages(
                ticket_id,
                ticket_agentids[i] if ticket_agentids else None,
                ticket_owner_names[i] if ticket_owner_names else None
            )
            for i, ticket_id in enumerate(ticket_ids)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        flattened_messages = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logging.error(f"Failed to fetch messages for ticket {ticket_ids[i]}: {result}")
                continue

            for message in result:
                base_info = {
                    "ticket_id": ticket_ids[i],
                    "owner_name": ticket_owner_names[i] if ticket_owner_names and ticket_owner_names[i] is not None else None,
                    "agentid": ticket_agentids[i] if ticket_agentids and ticket_agentids[i] is not None else None,
                    "agent_name": "",
                    "id": message.get("id"),
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
                            "message": msg.get("message"),
                            "message_visibility": msg.get("visibility")
                        }
                        flattened_messages.append(flattened_message)
                else:
                    flattened_messages.append(base_info)
        
        logging.info(f"Successfully fetched messages for {len([r for r in results if not isinstance(r, Exception)])} out of {len(ticket_ids)} tickets.")
        return flattened_messages

    async def fetch_messages_with_sender_receiver(
        self,
        ticket_ids: List[str],
        ticket_agentids: List[str],
        ticket_owner_names: List[str],
        max_page: int,
        per_page: int,
        session: aiohttp.ClientSession,
        concurrent_limit: int = 10
    ) -> ExtractionResponse:
        messages_with_metadata = await self.fetch_ticket_messages_batch(
            ticket_ids, ticket_agentids, ticket_owner_names, max_page, per_page, session, concurrent_limit
        )

        final_messages = await self.message_processor.process_messages_with_metadata(
            messages_data=messages_with_metadata, session=session
        )

        return final_messages

    def clear_cache(self):
        self.ticket_metadata_cache.clear()
        logging.info("Ticket metadata cleared!")
        self.message_processor.agent_cache.clear()
        logging.info("Agent cache cleared!")
        self.message_processor.user_cache.clear()
        logging.info("User cache cleared!")