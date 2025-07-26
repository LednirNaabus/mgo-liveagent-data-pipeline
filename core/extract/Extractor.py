from core.extract.helpers.extractor_bq_helpers import prepare_and_load_to_bq, upsert_to_bq_with_staging
from api.schemas.response import TicketAPIResponse, ResponseStatus
from core.extract.helpers.ticket_processing import process_tickets
from core.schemas.TicketFilter import FilterField
from core.LiveAgentClient import LiveAgentClient
from core.BigQueryManager import BigQuery
from utils.tickets_util import set_filter
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

    async def extract_tickets(
        self,
        date: pd.Timestamp,
        filter_field: FilterField = FilterField.DATE_CHANGED
    ) -> TicketAPIResponse:
        filters = set_filter(date, filter_field)
        ticket_payload = {
            "_perPage": self.per_page,
            "_filters": filters
        }

        if filter_field == FilterField.DATE_CREATED:
            ticket_payload["_sortDir"] = "ASC"
        try:
            logging.info(f"Extracting using the filer: {ticket_payload["_filters"]}")
            tickets_raw = await self.ticket.fetch_tickets(self.session, ticket_payload, self.max_page, self.per_page)
            tickets_processed = process_tickets(tickets_raw)
            if tickets_processed.empty:
                return TicketAPIResponse(
                    status=ResponseStatus.ERROR,
                    count="0",
                    data=[],
                    message="No tickets fetched!"
                )
            logging.info("Generating schema and loading data to BigQuery...")
            schema = prepare_and_load_to_bq(self.bigquery, tickets_processed, self.table_name, flag=False)
            upsert_to_bq_with_staging(self.bigquery, tickets_processed, schema, self.table_name)
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

    # fetch ticket messages
    async def extract_ticket_messages(
        self
    ) -> TicketAPIResponse:
        pass

    async def fetch_bq_tickets(self) -> TicketAPIResponse:
        # To do: make the table name static (i.e., whatever the table name is in BigQuery)
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