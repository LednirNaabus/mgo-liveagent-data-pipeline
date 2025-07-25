# from api.schemas.response import TicketAPIResponse, ResponseStatus
from core.factory import create_extractor
from api.common import (
    APIRouter,
    Request,
)

router = APIRouter()
TEST_MAX_PAGE = 1
TEST_PER_PAGE = 1

# 1. Extract data from LiveAgent API
# 2. Parse according to requirements
# 3. Load data to BigQuery

# TO DO:
# 1. Make process-tickets 'POST'
# 2. Create endpoint for fetching parsed tickets from BigQuery
@router.get("/process-tickets/{table_name}")
async def process_tickets(request: Request, table_name: str):
    session = request.app.state.aiohttp_session
    extractor = create_extractor(
        request=request,
        max_page=TEST_MAX_PAGE,
        per_page=TEST_PER_PAGE,
        session=session
    )
    return await extractor.extract_tickets()