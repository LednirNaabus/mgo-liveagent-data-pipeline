from core.LiveAgentClient import LiveAgentClient
from typing import Optional, Tuple
from core.Ticket import Ticket
import aiohttp

class Extractor:
    def __init__(
        self,
        api_key: str,
        max_page: int,
        per_page: int,
        session: aiohttp.ClientSession
    ):
        self.api_key = api_key
        self.max_page = max_page
        self.per_page = per_page
        self.client = LiveAgentClient(api_key, session)
        self.ticket = Ticket(self.client)
        self.session = session

    async def extract_tickets(self):
        try:
            return await self.ticket.fetch_tickets(self.session, self.max_page, self.per_page)
        except Exception as e:
            print(f"Exception occurred while extracting tickets: {e}")
            return None