from config.constants import PROJECT_ID, DATASET_NAME
from google.cloud.bigquery import SchemaField
from core.BigQueryManager import BigQuery
from typing import List
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def prepare_and_load_to_bq(
    bq: BigQuery,
    df: pd.DataFrame,
    table_name: str,
    load_data: bool = True,
    write_mode: str = None
) -> List[SchemaField]:
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
    bq.ensure_dataset()
    schema = bq.generate_schema(df)
    bq.ensure_table(table_name, schema)
    if load_data:
        bq.load_dataframe(df, table_name, write_disposition=write_mode)
    return schema

def upsert_to_bq_with_staging(
    bq: BigQuery,
    df: pd.DataFrame,
    schema: List[SchemaField],
    table_name: str,
) -> None:
    """
    Helper function for `Extractor` class that loads a DataFrame into a **staging** table, then executes a `MERGE`
    operation into the main table. For the cleanup step, drops the **staging** table.
    """
    # TODO: Refactor this block later
    staging_table_name = f"{table_name}_staging"
    update_columns = []

    if table_name == "tickets":
        update_columns = [
            'owner_contactid', 'owner_email', 'owner_name', 'departmentid', 'agentid', 
            'status', 'tags', 'code', 'channel_type', 'date_created', 'date_changed', 
            'date_resolved', 'last_activity', 'last_activity_public', 'public_access_urlcode', 
            'subject', 'custom_fields', 'date_due', 'date_deleted', 'datetime_extracted'
        ]

    if table_name == "users":
        update_columns = [
            'name', 'email', 'role', 'avatar_url'
        ]

    if table_name == "convo_analysis":
        update_columns = [
            'service_category', 'summary', 'intent_rating', 'engagement_rating', 'clarity_rating',
            'resolution_rating', 'sentiment_rating', 'location', 'schedule_date', 'schedule_time',
            'car', 'inspection', 'quotation', 'tokens', 'date_extracted', 'address', 'viable', 'model'
        ]
        all_columns = ['ticket_id'] + update_columns
        identifier = "ticket_id"
        # For historical data purposes
        history = f"{table_name}_history"
        bq.load_dataframe(
            df,
            history,
            write_disposition="WRITE_APPEND",
            schema=schema
        )
    else:
        all_columns = ['id'] + update_columns
        identifier = "id"

    logging.info(f"Table staging name: {staging_table_name}")

    bq.load_dataframe(
        df,
        staging_table_name,
        write_disposition="WRITE_TRUNCATE",
        schema=schema
    )

    logging.info(f"update_columns: {update_columns}")
    update_set_clauses = ',\n    '.join([f"{col} = source.{col}" for col in update_columns])
    insert_columns =', '.join(all_columns)
    insert_values = ', '.join([f"source.{col}" for col in all_columns])

    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_NAME}.{table_name}` AS target
    USING `{PROJECT_ID}.{DATASET_NAME}.{staging_table_name}` AS source
    ON target.{identifier} = source.{identifier}
    WHEN MATCHED THEN
        UPDATE SET
            {update_set_clauses}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    logging.info("Merging...")
    bq.sql_query_bq(merge_query, return_data=False)
    logging.info("Done merging!")

    drop_query = f"DROP TABLE `{PROJECT_ID}.{DATASET_NAME}.{staging_table_name}`"
    bq.sql_query_bq(drop_query, return_data=False)