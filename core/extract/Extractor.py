from core.extract.helpers.extractor_bq_helpers import prepare_and_load_to_bq, upsert_to_bq_with_staging
from core.extract.helpers.extraction_helpers import process_tickets, process_agents
from api.schemas.response import ExtractionResponse, ResponseStatus
from core.schemas.TicketFilter import FilterField
from core.LiveAgentClient import LiveAgentClient
from core.BigQueryManager import BigQuery
from utils.tickets_util import set_filter
from core.Ticket import Ticket
from core.Agent import Agent
import pandas as pd
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# TODO:
# 1. (maybe) Move "fetch from BQ" query logic to a separate helper module

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
        self.agent = Agent(self.client)
        self.bigquery = BigQuery()
        self.session = session 

    async def extract_tickets(
        self,
        date: pd.Timestamp,
        filter_field: FilterField = FilterField.DATE_CHANGED
    ) -> ExtractionResponse:
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
                return ExtractionResponse(
                    status=ResponseStatus.ERROR,
                    count="0",
                    data=[],
                    message="No tickets fetched!"
                )
            # logging.info("Generating schema and loading data to BigQuery...")
            # schema = prepare_and_load_to_bq(self.bigquery, tickets_processed, self.table_name, flag=False)
            # upsert_to_bq_with_staging(self.bigquery, tickets_processed, schema, self.table_name)
            tickets = (
                tickets_processed
                .where(pd.notnull(tickets_processed), None)
                .to_dict(orient="records")
            )
            return ExtractionResponse(
                status=ResponseStatus.SUCCESS,
                count=str(len(tickets)),
                data=tickets
            )
        except Exception as e:
            logging.info(f"Exception occurred while extracting tickets: {e}")
            return ExtractionResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR
            )

    # fetch ticket messages
    async def extract_ticket_messages(
        self,
        ticket_ids,
        session: aiohttp.ClientSession
    ) -> ExtractionResponse:
        return await self.ticket.fetch_ticket_message(
            ticket_id=ticket_ids,
            max_page=self.max_page,
            per_page=self.per_page,
            session=session
        )

    async def fetch_bq_tickets(self) -> ExtractionResponse:
        # To do: make the table name static (i.e., whatever the table name is in BigQuery)
        try:
            from config.constants import PROJECT_ID, DATASET_NAME
            query = """
            SELECT * FROM `{}.{}.tickets` LIMIT 10
            """.format(PROJECT_ID, DATASET_NAME)
            df = self.bigquery.sql_query_bq(query)
            df = df.to_dict(orient="records")
            return ExtractionResponse(
                status=ResponseStatus.SUCCESS,
                count=str(len(df)),
                data=df
            )
        except Exception as e:
            logging.info(f"Exception occurred while querying tickets from BigQuery: {e}")
            return ExtractionResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR,
                message="Table not found."
            )

    async def extract_agents(
        self
    ) -> ExtractionResponse:
        try:
            import traceback
            agents_raw = await self.agent.get_agents(self.session, self.max_page, self.per_page)
            agents_processed = process_agents(agents_raw)
            logging.info(f"agents_processed: {agents_processed}")
            if agents_processed.empty:
                return ExtractionResponse(
                    status=ResponseStatus.ERROR,
                    count="0",
                    data=[],
                    message="No agents found!"
                )
            logging.info("Generating schema and loading data to BigQuery...")
            prepare_and_load_to_bq(self.bigquery, agents_processed, self.table_name, write_mode="WRITE_TRUNCATE")
            return ExtractionResponse(
                status=ResponseStatus.SUCCESS,
                count=str(len(agents_raw)),
                data=agents_raw
            )
        except Exception as e:
            traceback.print_exc()
            logging.info(f"Exception occurred while extracting agents: {e}")
            return ExtractionResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR
            )