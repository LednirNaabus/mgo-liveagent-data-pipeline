from config.config import LIVEAGENT_API_KEY
from core.DataExtractor import Extractor
from fastapi import Request
import aiohttp

def create_extractor(request: Request, max_page: int, per_page: int, session: aiohttp.ClientSession) -> Extractor:
    # session = request.app.state.aiohttp_session
    return Extractor(LIVEAGENT_API_KEY, max_page, per_page, session)