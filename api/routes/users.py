from api.common import (
    LIVEAGENT_API_KEY,
    JSONResponse,
    Extractor,
    APIRouter,
    status
)

router = APIRouter()

@router.get("/process-users")
async def process_users():
    extractor = Extractor(LIVEAGENT_API_KEY)
    # data = await extractor.