from api.routes.helpers.tickets_route_helpers import resolve_extraction_date
from utils.session_utils import get_aiohttp_session
from core.factory import create_extractor
from typing import Optional
import traceback
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
    return {
        "status": response.status,
        "count": response.count,
        "data": response.data,
        "message": response.message if hasattr(response, "message") else None
    }

@router.post("/process-single-ticket-messages/{table_name}")
async def process_single_ticket_messages(
    request: Request,
    table_name: str,
    ticket_id: str
):
    session = get_aiohttp_session(request)
    extractor = create_extractor(
        max_page=TEST_MAX_PAGE,
        per_page=TEST_PER_PAGE,
        table_name=table_name,
        session=session
    )
    message = await extractor.extract_single_ticket_message(ticket_id, session)
    return {
        "status": message.status,
        "count": message.count,
        "data": message.data
    }

########## FOR CLOUD SCHEDULER ##########
@router.post("/process-tickets-and-messages/{table_name}")
# Change:
# - Remove 'table_name' -> not needed
# - Hard code table creation for both tickets and ticket messages
async def process_tickets_and_messages(
    request: Request,
    is_initial: bool = Query(False),
    date: Optional[str] = Query(default=None, description="Start-of-month date (YYYY-MM-DD)")
):
    session = get_aiohttp_session(request)
    date, filter_field = resolve_extraction_date(is_initial, date)
    extractor = create_extractor(
        max_page=1,
        per_page=2,
        session=session
    )

    response = await extractor.extract_tickets_and_messages(
        date=date,
        filter_field=filter_field,
        session=session
    )

    res_data = {
        "status": response.status,
        "count": response.count,
        "data": response.data
    }
    return res_data

@router.get("/tickets")
async def get_tickets(request: Request):
    session = get_aiohttp_session(request)
    extractor = create_extractor(
        session=session
    )
    return await extractor.fetch_bq_tickets()