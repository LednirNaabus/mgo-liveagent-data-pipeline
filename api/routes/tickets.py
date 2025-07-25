from core.factory import create_extractor
from api.common import (
    JSONResponse,
    APIRouter,
    Request,
    status
)

router = APIRouter()

# 1. Extract data from LiveAgent API
# 2. Parse according to requirements
# 3. Load data to BigQuery
@router.get("/process-tickets")
async def process_tickets(request: Request):
    try:
        session = request.app.state.aiohttp_session
        extractor = create_extractor(request, 1, 5, session)
        data = await extractor.extract_tickets()
        return data
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "message": str(e)
            }
        )