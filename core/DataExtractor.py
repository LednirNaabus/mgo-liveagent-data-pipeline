from api.schemas.response import TicketAPIResponse, ResponseStatus
from core.LiveAgentClient import LiveAgentClient
from typing import Optional, Tuple
from core.Ticket import Ticket
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# FLOW:
# 1. Call from LiveAgentAPI/{tickets,users,agents}
# 2. Perform any parsing
# 3. Return
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

    async def extract_tickets(self) -> TicketAPIResponse:
        try:
            tickets_raw = await self.ticket.fetch_tickets(self.session, self.max_page, self.per_page)
            if not tickets_raw:
                return TicketAPIResponse(
                    status=ResponseStatus.ERROR,
                    count="0",
                    data=[],
                    message="No tickets fetched!"
                )
            return tickets_raw
        except Exception as e:
            logging.info(f"Exception occurred while extracting tickets: {e}")
            return TicketAPIResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR
            )

    async def extract_ticket_message(self):
        try:
            pass
        except Exception as e:
            logging.info(f"Exception occurred during extraction of ticket messages: {e}")
            return None