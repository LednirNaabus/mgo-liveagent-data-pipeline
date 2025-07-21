from api.common import (
    LIVEAGENT_API_KEY,
    JSONResponse,
    Extractor,
    APIRouter,
    status
)

router = APIRouter()

# 1. Extract data from LiveAgent API
# 2. Parse according to requirements
# 3. Load parsed data to BigQuery

@router.post("/process-tickets")
async def process_tickets():
    try:
        extractor = Extractor(LIVEAGENT_API_KEY, 1, 1)
        data = await extractor.extract_tickets()
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "status": "success",
                "count": len(data),
                "data": data
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "message": str(e)
            }
        )

@router.post("/process-ticket-messages")
async def process_ticket_messages():
    try:
        extractor = Extractor(LIVEAGENT_API_KEY, 1, 1)
        data = await extractor.extract_ticket_messages()
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "status": "success",
                "count": len(data),
                "data": data
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "message": str(e)
            }
        )