from utils.session_utils import get_aiohttp_session
from core.factory import create_extractor
from api.common import (
    HTTPException,
    APIRouter,
    Request,
    Query
)

router = APIRouter()

@router.get("/{table_name}")
async def get_data(
    request: Request,
    table_name: str,
    limit: int = Query(description="The number of rows queried.")
):
    if table_name not in {"tickets", "messages", "agents", "convo_analysis"}:
        raise HTTPException(status_code=404, detail="Table not found!")
    
    session = get_aiohttp_session(request)
    extractor = create_extractor(session)
    return await extractor.fetch_bq_table(table_name, limit)