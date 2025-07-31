from core.extract.helpers.extraction_helpers import process_tickets, process_ticket_messages, process_agents, process_tags
from core.extract.helpers.extractor_bq_helpers import prepare_and_load_to_bq, upsert_to_bq_with_staging
from api.schemas.response import ExtractionResponse, ResponseStatus
from config.constants import PROJECT_ID, DATASET_NAME
from core.schemas.TicketFilter import FilterField
from core.LiveAgentClient import LiveAgentClient
from core.BigQueryManager import BigQuery
from utils.tickets_util import set_filter
from core.Ticket import Ticket
from core.Agent import Agent
from core.Tag import Tag
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
        self.tag = Tag(self.client)
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
            logging.info(f"Extracting using the following filter: {ticket_payload["_filters"]}")
            tickets_raw = await self.ticket.fetch_tickets(self.session, ticket_payload, self.max_page, self.per_page)
            tickets_processed = process_tickets(tickets_raw)
            if tickets_processed.empty:
                return ExtractionResponse(
                    status=ResponseStatus.ERROR,
                    count="0",
                    data=[],
                    message="No tickets fetched!"
                )
            logging.info("Generating schema and loading data to BigQuery...")
            schema = prepare_and_load_to_bq(self.bigquery, tickets_processed, "tickets", load_data=False)
            upsert_to_bq_with_staging(self.bigquery, tickets_processed, schema, "tickets")
            logging.info("Done loading to BigQuery!")
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
            logging.error(f"Exception occurred while extracting tickets: {e}")
            return ExtractionResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR
            )

    async def extract_tickets_and_messages(
        self,
        date: pd.Timestamp,
        session: aiohttp.ClientSession,
        filter_field: FilterField = FilterField.DATE_CHANGED,
        concurrent_limit: int = 10
    ):
        tickets = await self.extract_tickets(date, filter_field)
        tickets_df = pd.DataFrame(tickets.data)
        if isinstance(tickets_df, pd.DataFrame):
            ticket_ids = tickets_df["id"].tolist()
            ticket_agentids = tickets_df["agentid"].tolist()
            ticket_ownernames = tickets_df["owner_name"].tolist()
        else:
            ticket_ids = [ticket.get("id") for ticket in tickets_df.data if ticket.get("id")]
            ticket_agentids = [ticket.get("agentid") for ticket in tickets_df.data if ticket.get("agentid")]
            ticket_ownernames = [ticket.get("owner_name") for ticket in tickets_df.data if ticket.get("owner_name")]

        logging.info(f"Found {len(ticket_ids)} tickets")

        if not ticket_ids:
            return ExtractionResponse(
                status=ResponseStatus.ERROR,
                count="0",
                data=[]
            )

        messages = await self.ticket.fetch_messages_with_sender_receiver(
            ticket_ids=ticket_ids,
            ticket_agentids=ticket_agentids,
            ticket_owner_names=ticket_ownernames,
            max_page=self.max_page,
            per_page=self.per_page,
            session=session,
            concurrent_limit=concurrent_limit
        )

        messages_processed = process_ticket_messages(messages)
        if messages_processed.empty:
            return ExtractionResponse(
                status=ResponseStatus.ERROR,
                count="0",
                data=[],
                message="No ticket messages fetched!"
            )

        logging.info("Extracting user cache...")
        user_data = self.ticket.get_user_cache()
        user_list = list(user_data.values())
        users_df = pd.DataFrame(user_list)

        logging.info("Generating schema and loading data to BigQuery...")
        logging.info("Loading messages...")
        prepare_and_load_to_bq(self.bigquery, messages_processed, "messages", load_data=True)
        logging.info("Loading messages...")
        schema = prepare_and_load_to_bq(self.bigquery, users_df, "users", load_data=False)
        upsert_to_bq_with_staging(self.bigquery, users_df, schema, "users")
        logging.info("Done loading to BigQuery!")
        self.clear_all_caches()

        return ExtractionResponse(
            status=ResponseStatus.SUCCESS,
            count=str(len(tickets.data) + len(messages)),
            data={
                "tickets": tickets,
                "messages": messages
            }
        )

    async def fetch_bq_table(self, table_name: str, limit: int = 10) -> ExtractionResponse:
        # Set limit to 10 for now
        try:
            query = """
            SELECT * FROM `{}.{}.{}` LIMIT {}
            """.format(PROJECT_ID, DATASET_NAME, table_name, limit)
            df = self.bigquery.sql_query_bq(query)
            df = df.to_dict(orient="records")
            return ExtractionResponse(
                status=ResponseStatus.SUCCESS,
                count=str(len(df)),
                data=df
            )
        except Exception as e:
            logging.error(f"Exception occurred while querying for {table_name}: {e}")
            return ExtractionResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR,
                message="Table not found!"
            )

    async def extract_agents(self) -> ExtractionResponse:
        try:
            agents_raw = await self.agent.get_agents(self.session, self.max_page, self.per_page)
            agents_processed = process_agents(agents_raw)
            if agents_processed.empty:
                return ExtractionResponse(
                    status=ResponseStatus.ERROR,
                    count="0",
                    data=[],
                    message="No agents found!"
                )
            logging.info("Generating schema and loading data to BigQuery...")
            prepare_and_load_to_bq(self.bigquery, agents_processed, "agents", write_mode="WRITE_TRUNCATE")
            logging.info("Done loading to BigQuery!")
            return ExtractionResponse(
                status=ResponseStatus.SUCCESS,
                count=str(len(agents_raw)),
                data=agents_raw
            )
        except Exception as e:
            logging.error(f"Exception occurred while extracting agents: {e}")
            return ExtractionResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR
            )

    async def extract_tags(self) -> ExtractionResponse:
        try:
            tags_raw = await self.tag.get_tags(self.session)
            tags_processed = process_tags(tags_raw)
            if tags_processed.empty:
                return ExtractionResponse(
                    status=ResponseStatus.ERROR,
                    count="0",
                    data=[],
                    message="No tags found!"
                )
            logging.info("Generating schema and loading data to BigQuery...")
            prepare_and_load_to_bq(self.bigquery, tags_processed, "tags", write_mode="WRITE_TRUNCATE")
            logging.info("Done loading to BigQuery!")
            return ExtractionResponse(
                status=ResponseStatus.SUCCESS,
                count=str(len(tags_raw.data)),
                data=tags_raw
            )
        except Exception as e:
            logging.error(f"Exception occurred while extracting tags: {e}")
            return ExtractionResponse(
                count="0",
                data=[],
                status=ResponseStatus.ERROR
            )

    def clear_all_caches(self):
        logging.info("Clearing caches...")
        self.ticket.clear_cache()
        logging.info("All caches cleared!")