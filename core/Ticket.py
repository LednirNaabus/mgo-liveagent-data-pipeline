from config.constants import LIVEAGENT_MGO_SYSTEM_USER_ID
from api.schemas.response import TicketAPIResponse
from core.LiveAgentClient import LiveAgentClient
from typing import Dict, Any
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

    async def paginate(
        self,
        session: aiohttp.ClientSession,
        endpoint: str = None,
        payload: Dict[str, Any] = None,
        max_pages: int = 5
    ) -> TicketAPIResponse:
        """Generic pagination logic for `/tickets`."""
        all_data = []
        page = 1
        while page <= max_pages:
            payload["_page"] = page
            print(f"Fetching page {page} from {endpoint}")
            try:
                data = await self.client.make_request(
                    session,
                    endpoint,
                    params=payload
                )

                if not data:
                    print(f"No data returned for page {page}, stopping pagination.")
                    break

                if hasattr(data, "data"):
                    items = data.data if isinstance(data.data, list) else []
                else:
                    items = []

                if not items:
                    print(f"No items in page {page}, stopping pagination.")
                    break

                all_data.extend(items)
                page += 1
            except Exception as e:
                print(f"Error fetching page {page}: {e}")
                break

        return all_data

    async def fetch_tickets(
        self,
        session: aiohttp.ClientSession,
        max_pages: int = 5,
        per_page: int = 10
    ) -> TicketAPIResponse:
        payload = self._default_payload()
        payload["_perPage"] = per_page
        try:
            data = await self.paginate(
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
            return data
        except Exception as e:
            pass

    async def fetch_ticket_message(
        self,
        ticket_id: str,
        max_page: int,
        per_page: int,
        session: aiohttp.ClientSession
    ) -> TicketAPIResponse:
        try:
            message_payload = {
                "_page": 1,
                "_perPage": per_page
            }

            messages_data = await self.paginate(
                session,
                endpoint=f"{self.endpoint}/{ticket_id}/messages",
                payload=message_payload,
                max_pages=max_page
            )
            return messages_data
        except Exception as e:
            logging.info(f"Exception occurred while fetching ticket message: {e}")
            return []