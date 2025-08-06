from config.constants import PROJECT_ID, DATASET_NAME
from typing import List, Tuple, Dict, Optional
from utils.date_utils import get_start_end_str
from api.logs.Tracker import runtime_tracker
from core.BigQueryManager import BigQuery
from enum import StrEnum
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Tables(StrEnum):
    TICKETS = "tickets"
    MESSAGES = "messages"
    CONVO = "convo_analysis"

class ExtractionLogger:
    def __init__(self):
        self.bigquery = BigQuery()
        self.errors: List[str] = []

    def add_error(self, error: str):
        self.errors.append(error)
        logging.error(f"Extraction error: {error}")

    def query_table_data(
        self,
        table: Tables = None,
        columns: str = None,
        date_range: Tuple = None,
        is_distinct: bool = False
    ) -> pd.DataFrame:
        if columns is None:
            columns = "ticket_id, message_id" if table == Tables.MESSAGES else "id"

        if is_distinct:
            columns = f"DISTINCT {columns}"

        query = """
        SELECT {}
        FROM {}.{}.{}
        """.format(columns, PROJECT_ID, "conversations", table)

        if date_range:
            start_str, end_str = get_start_end_str(date_range[0]) if isinstance(date_range[0], pd.Timestamp) else date_range
            query += """WHERE datetime_extracted >= '{}' AND datetime_extracted < '{}'""".format(start_str, end_str)

        return self.bigquery.sql_query_bq(query)

    def get_from_recent_run(self, date: pd.Timestamp, table: Tables) -> pd.DataFrame:
        return self.query_table_data(
            table=table,
            date_range=(date, None),
            is_distinct=True
        )

    def get_existing(self, table: Tables) -> pd.DataFrame:
        columns = "ticket_id" if table == Tables.MESSAGES else "id"
        return self.query_table_data(
            table=table,
            columns=columns,
            is_distinct=True
        )

    def get_total_tokens(self, date: pd.Timestamp, table: Tables) -> Tuple:
        start_str, end_str = get_start_end_str(date)
        query = """
        SELECT model, SUM(tokens) AS total_tokens
        FROM {}.{}.{}
        WHERE date_extracted >= '{}' AND date_extracted < '{}'
        GROUP BY model
        """.format(PROJECT_ID, "conversations", table, start_str, end_str)
        df = self.bigquery.sql_query_bq(query)
        if df.empty:
            return 0, "N/A"
        return df["total_tokens"].iloc[0], df["model"].iloc[0]

    def get_runtime(self) -> Optional[float]:
        runtime_data = runtime_tracker.get_runtime()
        if runtime_data and runtime_data.total_duration_seconds:
            return round(runtime_data.total_duration_seconds, 2)
        return None

    def calculate_metrics(self, date: pd.Timestamp, table: Tables) -> Dict[str, int]:
        try:
            run_data = self.get_from_recent_run(date, table)
            if run_data.empty:
                return {"new": 0, "existing": 0, "total": 0}

            existing_data = self.get_existing(table)
            existing_ids = set(existing_data.iloc[:, 0].tolist()) if not existing_data.empty else set()

            id_column = "message_id" if table == Tables.MESSAGES else "id"
            run_ids = set(run_data[id_column].tolist())

            new_count = len(run_ids - existing_ids)
            existing_count = len(run_ids & existing_ids)

            existing_key = "old" if table == Tables.MESSAGES else "update"

            return {
                "new": new_count,
                existing_key: existing_count,
                "total": new_count + existing_count
            }
        except Exception as e:
            import traceback
            table_name = table.value
            self.add_error(f"Error calculating {table_name} metrics: {str(e)}")
            existing_key = "old" if table == Tables.MESSAGES else "update"
            traceback.print_exc()
            return {"new": 0, existing_key: 0, "total": 0}

    def calculate_ticket_metrics(self, date: pd.Timestamp) -> Dict[str, int]:
        metrics = self.calculate_metrics(date, Tables.TICKETS)
        if "existing" in metrics:
            metrics["update"] = metrics.pop("existing")
        return metrics

    def calculate_message_metrics(self, date: pd.Timestamp) -> Dict[str, int]:
        return self.calculate_metrics(date, Tables.MESSAGES)