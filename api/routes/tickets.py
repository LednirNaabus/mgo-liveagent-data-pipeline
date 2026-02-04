from api.shared import (
    jsonable_encoder,
    BackgroundTasks,
    JSONResponse,
    APIRouter,
    Request,
    status
)
from integrations.liveagent.models import TICKETS_SCHEMA
from chat_analysis import ConvoData, get_channel_gateway
from api.services import persist_rows
from api.utils import (
    get_aiohttp_session,
    get_bq_utils
)

router = APIRouter()

@router.post("/tickets")
async def process_tickets(request: Request, background_tasks: BackgroundTasks) -> JSONResponse:
    session = get_aiohttp_session(request=request)
    bq_client = get_bq_utils(request=request, table_id="tickets_test")
    gateway = get_channel_gateway(session=session, bq_client=bq_client)
    extractor = ConvoData(gateway=gateway)
    response = await extractor.fetch_tickets(per_page=10, max_pages=2)
    rows = response.data or []

    # Do BQ stuff
    background_tasks.add_task(
        persist_rows,
        bq_client=bq_client,
        rows=rows,
        schema=TICKETS_SCHEMA,
        key_columns=["id"]
    )

    return JSONResponse(
        content=jsonable_encoder({
            "status": response.status.value,
            "count": response.count,
            "data": response.data
        }),
        status_code=status.HTTP_200_OK
    )