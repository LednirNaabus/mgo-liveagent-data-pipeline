from utils.session_utils import get_aiohttp_session
from core.factory import create_extractor
from api.common import (
    APIRouter,
    Request
)
router = APIRouter()

@router.post("/process-convo")
async def process_convo(request: Request):
    session = get_aiohttp_session(request)
    extractor = create_extractor(
        session=session
    )
    return await extractor.extract_conversation_analysis()