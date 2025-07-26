from api.schemas.response import TicketAPIResponse, ResponseStatus
from core.extract.ticket_processor import process_tickets
from core.LiveAgentClient import LiveAgentClient
from core.BigQueryManager import BigQuery
from core.Ticket import Ticket
import pandas as pd
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# FLOW:
# 1. Call from LiveAgentAPI/{tickets,users,agents}
# 2. Perform any parsing
# 3. Return
class Extractor:
    """
    The `Extractor` class is the core of the pipeline.

    Handles the following operations:

    1. Calls each endpoint from MechaniGo LiveAgent API (`/tickets`, `/users`, `/agents`).
    2. Parses the data according to requirements and needs.
    3. Prepares the parsed data for uploading to BigQuery.
    """
    def __init__(
        self,
        api_key: str,
        max_page: int,
        per_page: int,
        table_name: str,
        session: aiohttp.ClientSession
    ):
        self.api_key = api_key
        self.max_page = max_page
        self.per_page = per_page
        self.table_name = table_name
        self.client = LiveAgentClient(api_key, session)
        self.ticket = Ticket(self.client)
        self.bigquery = BigQuery()
        self.session = session 

    async def _load_to_bq(self, df: pd.DataFrame) -> None:
        """Helper function for `Extractor` class that loads a DataFrame to BigQuery."""
        logging.info("Generating schema and loading data to BigQuery...")
        self.bigquery.ensure_dataset()
        schema = self.bigquery.generate_schema(df)
        self.bigquery.ensure_table(self.table_name, schema)
        self.bigquery.load_dataframe(df, self.table_name)

    async def extract_tickets(self) -> TicketAPIResponse:
        try:
            tickets_raw = await self.ticket.fetch_tickets(self.session, self.max_page, self.per_page)
            tickets_processed = process_tickets(tickets_raw)
            if tickets_processed.empty:
                return TicketAPIResponse(
                    status=ResponseStatus.ERROR,
                    count="0",
                    data=[],
                    message="No tickets fetched!"
                )
            self._load_to_bq(tickets_processed)
            tickets = (
                tickets_processed
                .where(pd.notnull(tickets_processed), None)
                .to_dict(orient="records")
            )
            return TicketAPIResponse(
                status=ResponseStatus.SUCCESS,
                count=str(len(tickets)),
                data=tickets
            )
        except Exception as e:
            logging.info(f"Exception occurred while extracting tickets: {e}")
            return TicketAPIResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR
            )

    async def fetch_bq_tickets(self) -> TicketAPIResponse:
        try:
            from config.constants import PROJECT_ID, DATASET_NAME
            query = """
            SELECT * FROM `{}.{}.{}`
            """.format(PROJECT_ID, DATASET_NAME, self.table_name)
            df = self.bigquery.sql_query_bq(query)
            df = df.to_dict(orient="records")
            return TicketAPIResponse(
                status=ResponseStatus.SUCCESS,
                count=str(len(df)),
                data=df
            )
        except Exception as e:
            logging.info(f"Exception occurred while querying tickets from BigQuery: {e}")
            return TicketAPIResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR,
                message="Table not found."
            )