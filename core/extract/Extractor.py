from api.schemas.response import TicketAPIResponse, ResponseStatus
from core.extract.ticket_processor import process_tickets
from core.LiveAgentClient import LiveAgentClient
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
    """
    The `Extractor` class is the core of the pipeline.

    Handles the following operations:

    1. Calls each endpoint from MechaniGo LiveAgent API (`/tickets`, `/users`, `/agents`).
    2. Parses the data according to requirements and needs.
    3. Prepares the parsed data for uploading to BigQuery.
    """
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
            tickets_processed = process_tickets(tickets_raw)
            if tickets_processed.empty:
                return TicketAPIResponse(
                    status=ResponseStatus.ERROR,
                    count="0",
                    data=[],
                    message="No tickets fetched!"
                )
            tickets = tickets_processed.to_dict(orient="records")
            return TicketAPIResponse(
                status=ResponseStatus.SUCCESS,
                count=str(len(tickets)),
                data=tickets
            )
        except Exception as e:
            logging.info(f"Exception occurred while extracting tickets: {e}")
            return TicketAPIResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR
            )