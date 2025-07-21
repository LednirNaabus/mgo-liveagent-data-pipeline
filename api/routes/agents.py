from api.common import (
    LIVEAGENT_API_KEY,
    JSONResponse,
    Extractor,
    APIRouter,
    status
)

router = APIRouter()

@router.get("/process-agents")
async def process_agents():
    extractor = Extractor(LIVEAGENT_API_KEY, 1, 1)
    data = await extractor.extract_agents()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "status": "succes",
            "count": len(data),
            "data": data
        }
    )