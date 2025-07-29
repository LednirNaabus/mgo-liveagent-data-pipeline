from api.routes.helpers.tickets_route_helpers import resolve_extraction_date
from utils.session_utils import get_aiohttp_session
from core.factory import create_extractor
from typing import Optional
from api.common import (
    APIRouter,
    Request,
    Query
)

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
    # For testing purposes (delete later)
    try:
        from config.constants import TEST_MAX_PAGE, TEST_PER_PAGE
    except Exception as e:
        return f"Exception occurred: {e}"
    session = get_aiohttp_session(request)
    date, filter_field = resolve_extraction_date(is_initial, date)
    extractor = create_extractor(
        max_page=TEST_MAX_PAGE,
        per_page=TEST_PER_PAGE,
        table_name=table_name,
        session=session
    )
    return await extractor.extract_tickets(date, filter_field)

@router.post("/process-ticket-messages/{table_name}")
async def process_ticket_messages(
    request: Request,
    table_name: str,
    is_initial: bool = Query(False),
    date: Optional[str] = Query(default=None, description="Start-of-month date (YYYY-MM-DD)")
):
    session = get_aiohttp_session(request)
    date, filter_field = resolve_extraction_date(is_initial, date)
    print(f"date: {date}, filter_field: {filter_field}")

@router.get("/tickets")
async def get_tickets(request: Request):
    # To Do here:
    # 1. Submit query (ex: "SELECT * FROM `tickets`")
    session = get_aiohttp_session(request)
    extractor = create_extractor(
        session=session
    )
    return await extractor.fetch_bq_tickets()