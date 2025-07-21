from core.LiveAgentClient import LiveAgentClient
from typing import List, Dict, Any
import asyncio

class Ticket:
    """Class for `/tickets` LiveAgent API endpoint."""
    def __init__(self, client: LiveAgentClient):
        self.endpoint = "tickets"
        self.client = client

    def _default_payload(self) -> Dict[str, Any]:
        return {
            "includeQuotedMessages": "false",
            "_page": 1,
            "_perPage": 10,
            "_sortDir": "ASC"
        }

    async def paginate(self, payload: Dict[str, Any], max_pages: int = 5, endpoint: str = None) -> List[Dict[str, Any]]:
        """
        Generic pagination logic for the `/tickets` endpoint.
        """
        all_data = []
        page = 1
        while page <= max_pages:
            payload["_page"] = page
            print(f"Fetching page {page} from {endpoint}")
            try:
                data = await self.client.make_request(
                    endpoint,
                    params=payload
                )
                if not data:
                    print(f"No data returned for page {page}, stopping pagination.")
                    break

                if isinstance(data, dict) and 'data' in data:
                    items = data['data']
                else:
                    items = data if isinstance(data, list) else []
                
                if not items:
                    print(f"No items in page {page}, stopping pagination.")
                    break
                all_data.extend(items)
                page += 1
            except Exception as e:
                print(f"Exception occurred while fetching ticket page {page}: {e}")
                break
        return all_data

    async def fetch_tickets(self, max_pages: int = 10, per_page: int = 10) -> List[Dict[str, Any]]:
        payload = self._default_payload()
        payload["_perPage"] = per_page
        try:
            ticket_data = await self.paginate(
                payload,
                max_pages,
                self.endpoint
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
            messages_data = await self.paginate(
                message_payload,
                max_pages=max_pages,
                endpoint=f"{self.endpoint}/{ticket_id}/messages"
            )
            return messages_data
        except Exception as e:
            print(f"Exception occurred while fetching ticket messages: {e}")
            return {
                "error": str(e)
            }