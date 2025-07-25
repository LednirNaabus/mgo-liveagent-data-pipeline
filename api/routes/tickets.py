from api.schemas.response import TicketAPIResponse, ResponseStatus
from core.factory import create_extractor
from api.common import (
    APIRouter,
    Request,
)

router = APIRouter()

# 1. Extract data from LiveAgent API
# 2. Parse according to requirements
# 3. Load data to BigQuery

# TO DO:
# 1. Make process-tickets 'POST'
# 2. Create endpoint for fetching parsed tickets from BigQuery
@router.get("/process-tickets")
async def process_tickets(request: Request):
    session = request.app.state.aiohttp_session
    extractor = create_extractor(
        request=request,
        max_page=10,
        per_page=10,
        session=session
    )
    data = await extractor.extract_tickets()
    return TicketAPIResponse(
        status=ResponseStatus.SUCCESS,
        count=str(len(data)),
        data=data
    )