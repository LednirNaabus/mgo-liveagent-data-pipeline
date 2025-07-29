from api.routes.helpers.tickets_route_helpers import resolve_extraction_date
from api.routes.helpers.file_handlers import write_to_file, read_from_file
from utils.session_utils import get_aiohttp_session
from core.factory import create_extractor
from typing import Optional
from api.common import (
    APIRouter,
    Request,
    Query
)

# Delete Later
from config.constants import TEST_MAX_PAGE, TEST_PER_PAGE

router = APIRouter()

# 1. Extract data from LiveAgent API
# 2. Parse according to requirements
# 3. Load data to BigQuery

# TO DO:
# 1. Make process-tickets 'POST'
# 2. Create endpoint for fetching parsed tickets from BigQuery
@router.post("/process-tickets/{table_name}")
async def process_tickets(
    request: Request,
    table_name: str,
    is_initial: bool = Query(False),
    date: Optional[str] = Query(default=None, description="Start-of-month date (YYYY-MM-DD)")
):
    session = get_aiohttp_session(request)
    date, filter_field = resolve_extraction_date(is_initial, date)
    extractor = create_extractor(
        max_page=TEST_MAX_PAGE,
        per_page=TEST_PER_PAGE,
        table_name=table_name,
        session=session
    )
    response = await extractor.extract_tickets(date, filter_field)
    ticket_ids = [ticket["id"] for ticket in response.data]
    write_to_file(table_name, ticket_ids)
    return {
        "status": response.status,
        "count": response.count,
        "ticket_ids": ticket_ids,
        "message": response.message if hasattr(response, "message") else None
    }

@router.post("/process-ticket-messages/{table_name}")
async def process_ticket_messages(
    request: Request,
    table_name: str,
    is_initial: bool = Query(False),
    date: Optional[str] = Query(default=None, description="Start-of-month date (YYYY-MM-DD)")
):
    session = get_aiohttp_session(request)
    extractor = create_extractor(
        max_page=TEST_MAX_PAGE,
        per_page=TEST_PER_PAGE,
        table_name=table_name,
        session=session
    )
    ticket_ids = read_from_file(table_name)
    all_messages = []
    for ticket_id in ticket_ids:
        messages = await extractor.extract_ticket_messages(ticket_id, session)
        all_messages.extend(messages)
    return {
        "status": "SUCCESS",
        "count": len(all_messages),
        "data": all_messages
    }

@router.get("/tickets")
async def get_tickets(request: Request):
    session = get_aiohttp_session(request)
    extractor = create_extractor(
        session=session
    )
    return await extractor.fetch_bq_tickets()