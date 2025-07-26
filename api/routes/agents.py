from utils.session_utils import get_aiohttp_session
from config.constants import MAX_VALUE
from core.factory import create_extractor
from api.common import (
    APIRouter,
    Request
)

router = APIRouter()

@router.post("/process-agents/{table_name}")
async def process_agents(
    request: Request,
    table_name: str
):
    session = get_aiohttp_session(request)
    extractor = create_extractor(
        max_page=MAX_VALUE,
        per_page=MAX_VALUE,
        table_name=table_name,
        session=session
    )
    return await extractor.extract_agents()