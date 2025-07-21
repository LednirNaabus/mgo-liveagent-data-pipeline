from core.LiveAgentClient import LiveAgentClient
from typing import List, Dict, Any

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
            return {
                "error": str(e)
            }