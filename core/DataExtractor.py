from core.LiveAgentClient import LiveAgentClient
from core.Ticket import Ticket
from typing import Optional
import aiohttp

class Extractor:
    def __init__(
        self,
        api_key: str,
        max_page: int,
        per_page: int,
        ticket: Optional[Ticket] = None
    ):
        self.api_key = api_key
        self.max_page = max_page
        self.per_page = per_page
        self.ticket = ticket

    async def extract_tickets(
        self,
        session: aiohttp.ClientSession
    ):
        try:
            s = await self.ticket.fetch_tickets(session, 1, 5)
            return s
        except Exception as e:
            print(f"Exception occurred while extracting tickets: {e}")
            return None