from api.schemas.response import TicketAPIResponse, ResponseStatus
from core.extract.ticket_processor import process_tickets
from config.constants import PROJECT_ID, DATASET_NAME
from core.LiveAgentClient import LiveAgentClient
from google.cloud.bigquery import SchemaField
from core.BigQueryManager import BigQuery
from core.Ticket import Ticket
from typing  import List
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

    # Helper functions
    def _prepare_and_load_to_bq(self, df: pd.DataFrame, flag: bool = True) -> List[SchemaField]:
        """
        Helper function for `Extractor` class that loads a DataFrame to BigQuery.

        The `flag` is default to `True` which will load the DataFrame to BigQuery,
        otherwise it will create an empty table in BigQuery.

        Parameters:
            df (`pd.DataFrame`): a pandas DataFrame that will be loaded to BigQuery.
            flag (`bool`): the checker

        Returns:
            List[SchemaField]: a list of `bigquery.SchemaField`
        """
        self.bigquery.ensure_dataset()
        schema = self.bigquery.generate_schema(df)
        self.bigquery.ensure_table(self.table_name, schema)
        if flag:
            self.bigquery.load_dataframe(df, self.table_name)
        return schema

    def _upsert_to_bq_with_staging(self, df: pd.DataFrame, schema: List[SchemaField]) -> None:
        """
        Helper function for `Extractor` class that loads a DataFrame into a **staging** table, then executes a `MERGE`
        operation into the main table. For the cleanup step, drops the **staging** table.
        """
        staging_table_name = f"{self.table_name}_staging"
        logging.info(f"Table staging name: {staging_table_name}")

        self.bigquery.load_dataframe(
            df,
            staging_table_name,
            write_disposition="WRITE_TRUNCATE",
            schema=schema
        )

        update_columns = [
        'owner_contactid', 'owner_email', 'owner_name', 'departmentid', 'agentid', 
        'status', 'tags', 'code', 'channel_type', 'date_created', 'date_changed', 
        'date_resolved', 'last_activity', 'last_activity_public', 'public_access_urlcode', 
        'subject', 'custom_fields', 'date_due', 'date_deleted', 'datetime_extracted'
        ]
        all_columns = ['id'] + update_columns
        update_set_clauses = ',\n    '.join([f"{col} = source.{col}" for col in update_columns])
        insert_columns =', '.join(all_columns)
        insert_values = ', '.join([f"source.{col}" for col in all_columns])

        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET_NAME}.{self.table_name}` AS target
        USING `{PROJECT_ID}.{DATASET_NAME}.{staging_table_name}` AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                {update_set_clauses}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """
        logging.info("Merging...")
        self.bigquery.sql_query_bq(merge_query, return_data=False)
        logging.info("Done merging!")

        drop_query = f"DROP TABLE `{PROJECT_ID}.{DATASET_NAME}.{staging_table_name}`"
        self.bigquery.sql_query_bq(drop_query, return_data=False)

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
            logging.info("Generating schema and loading data to BigQuery...")
            schema = self._prepare_and_load_to_bq(tickets_processed, flag=False)
            self._upsert_to_bq_with_staging(tickets_processed, schema)
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