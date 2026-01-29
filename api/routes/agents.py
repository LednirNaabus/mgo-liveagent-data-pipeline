from api.shared import APIRouter, Request, BackgroundTasks, JSONResponse, status
from chat_analysis import ConvoData, get_channel_gateway
from integrations.liveagent.models import AGENTS_SCHEMA
from api.services import persist_rows
from api.utils import (
    get_aiohttp_session,
    get_bq_utils
)

router = APIRouter()

@router.post("/agents")
async def process_agents(request: Request, background_tasks: BackgroundTasks) -> JSONResponse:
    session = get_aiohttp_session(request=request)
    bq_client = get_bq_utils(request=request, table_id="agents_test")
    gateway = get_channel_gateway(session=session, bq_client=bq_client)
    extractor = ConvoData(gateway=gateway)
    response = await extractor.fetch_agents(per_page=100, max_pages=100)
    rows = response.data or []

    background_tasks.add_task(
        persist_rows,
        bq_client=bq_client,
        rows=rows,
        schema=AGENTS_SCHEMA,
        key_columns=["id"] 
    )
    return JSONResponse(
        content={
            "status": response.status.value,
            "count": response.count,
            "data": response.data
        },
        status_code=status.HTTP_200_OK
    )