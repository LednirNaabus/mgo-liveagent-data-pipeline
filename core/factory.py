from core.extract.Extractor import Extractor
from config.config import LIVEAGENT_API_KEY
import aiohttp

def create_extractor(
    max_page: int = None,
    per_page: int = None,
    table_name: str = None,
    session: aiohttp.ClientSession = None,
) -> Extractor:
    return Extractor(LIVEAGENT_API_KEY, max_page, per_page, table_name, session)