from config.constants import LIVEAGENT_MGO_SYSTEM_USER_ID
from api.schemas.response import ExtractionResponse
from core.LiveAgentClient import LiveAgentClient
from typing import Dict, List, Any
import pandas as pd
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Ticket:
    """Class for `/tickets` LiveAgent API endpoint."""
    def __init__(self, client: LiveAgentClient):
        self.client = client
        self.endpoint = "tickets"

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
        return messages_data