from config.config import LIVEAGENT_API_KEY
from fastapi.responses import JSONResponse
from core.DataExtractor import Extractor
from fastapi import APIRouter

router = APIRouter()

@router.post("/process-tickets")
async def process_tickets():
    extractor = Extractor(LIVEAGENT_API_KEY, 1, 1)
    data = await extractor.extract_tickets()
    return JSONResponse(data)

@router.post("/process-ticket-messages")
async def process_ticket_messages():
    extractor = Extractor(LIVEAGENT_API_KEY, 1, 1)
    data = await extractor.extract_ticket_messages()
    return JSONResponse(data)