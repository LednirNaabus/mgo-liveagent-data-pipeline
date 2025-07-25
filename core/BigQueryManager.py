from config.bq_config import BQ_CLIENT, BQ_DATASET_NAME, CREDS_FILE, GCLOUD_PROJECT_ID
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from typing import List
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class BigQuery:
    def __init__(
        self,
        client: bigquery.Client = BQ_CLIENT
    ):
        self.client = client
        self.dataset_id = BQ_DATASET_NAME

    def ensure_dataset(self):
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref=dataset_ref)
        except NotFound:
            logging.info(f"Exception occurred: {NotFound}")
            dataset = bigquery.Dataset(dataset_ref=dataset_ref)
            dataset.location = "asia-southeast1"
            self.client.create_dataset(dataset)
            logging.info(f"Created dataset: {dataset}")

    def ensure_table(
        self,
        table_name: str,
        schema: List[SchemaField] = None
    ):
        table_id = f"{self.client.project}.{self.dataset_id}.{table_name}"
        try:
            self.client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        write_disposition: str = "WRITE_APPEND"
    ):
        table_id = f"{self.client.project}.{self.dataset_id}.{table_name}"

        try:
            job = self.client.load_table_from_dataframe(
                df,
                table_id,
                job_config=bigquery.LoadJobConfig(
                    write_disposition=write_disposition
                )
            )
            job.result()
        except NotFound:
            raise ValueError(f"Table {table_id} not found.")
        except Exception as e:
            raise RuntimeError(f"Failed to load data into {table_id}: {e}")

    def generate_schema(self, df: pd.DataFrame) -> List[SchemaField]:
        TYPE_MAPPING = {
            "i": "INTEGER",
            "u": "NUMERIC",
            "b": "BOOLEAN",
            "f": "FLOAT",
            "O": "STRING",
            "S": "STRING",
            "U": "STRING",
            "M": "DATETIME",
        }

        FORCE_NULLABLE = {"custom_fields"} # from /tickets

        schema = []
        for col, dtype in df.dtypes.items():
            val = df[col].iloc[0]
            is_list_of_dicts = isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict)
            force_null = col in FORCE_NULLABLE
            mode = "NULLABLE" if force_null else ("REPEATED" if isinstance(val, list) else "NULLABLE")

            fields = ()

            if isinstance(val, dict):
                fields = self.generate_schema(pd.json_normalize(val))
            elif is_list_of_dicts:
                fields = self.generate_schema(pd.json_normalize(val))

            if fields:
                field_type = "RECORD"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                field_type = "DATETIME"
            else:
                field_type = TYPE_MAPPING.get(dtype.kind, "STRING")
            schema.append(
                SchemaField(
                    name=col,
                    field_type=field_type,
                    mode=mode,
                    fields=fields
                )
            )
        return schema