from core.extract.Extractor import Extractor
from config.config import LIVEAGENT_API_KEY
from core.BigQueryManager import BigQuery
import aiohttp

def create_extractor(
    max_page: int = None,
    per_page: int = None,
    session: aiohttp.ClientSession = None
) -> Extractor:
    bq_client = BigQuery()
    return Extractor(LIVEAGENT_API_KEY, bq_client, max_page, per_page, session)