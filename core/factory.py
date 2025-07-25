from core.extract.Extractor import Extractor
from config.config import LIVEAGENT_API_KEY
from fastapi import Request
import aiohttp

def create_extractor(max_page: int, per_page: int, session: aiohttp.ClientSession) -> Extractor:
    return Extractor(LIVEAGENT_API_KEY, max_page, per_page, session)