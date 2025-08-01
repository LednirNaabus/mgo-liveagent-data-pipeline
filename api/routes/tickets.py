from api.routes.helpers.tickets_route_helpers import resolve_extraction_date
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

########## FOR CLOUD SCHEDULER ##########
@router.post("/process-tickets-and-messages")
async def process_tickets_and_messages(
    request: Request,
    is_initial: bool = Query(False),
    date: Optional[str] = Query(default=None, description="Start-of-month date (YYYY-MM-DD)")
):
    session = get_aiohttp_session(request)
    date, filter_field = resolve_extraction_date(is_initial, date)
    extractor = create_extractor(
        max_page=TEST_MAX_PAGE,
        per_page=TEST_PER_PAGE,
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