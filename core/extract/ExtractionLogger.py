from core.extract.helpers.extraction_helpers import create_base_log_dataframe
from core.extract.helpers.extractor_bq_helpers import prepare_and_load_to_bq
from config.constants import PROJECT_ID, DATASET_NAME
from typing import List, Tuple, Dict, Optional, Any
from utils.date_utils import get_start_end_str
from api.logs.Tracker import runtime_tracker
from core.BigQueryManager import BigQuery
from config.config import MNL_TZ
from enum import StrEnum
import pandas as pd
import datetime
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
        """.format(PROJECT_ID, DATASET_NAME, table, start_str, end_str)
        df = self.bigquery.sql_query_bq(query)
        if df.empty:
            return 0, "N/A"
        return df["total_tokens"].iloc[0], df["model"].iloc[0]

    def get_runtime_seconds(self) -> Optional[float]:
        runtime_data = runtime_tracker.get_runtime()
        if not runtime_data:
            logging.error("No runtime available")
            return None

        current_time = datetime.datetime.now(MNL_TZ)
        elapsed_time = (current_time - runtime_data.app_start_time).total_seconds()
        return elapsed_time

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

    def extract_and_load_to_bq(self, date: pd.Timestamp) -> List[Dict[str, Any]]:
        df = create_base_log_dataframe()
        runtime = self.get_runtime_seconds()
        df["extraction_run_time"] = runtime

        logging.info(f"Processing extraction logs for date: {date}")

        ticket_metrics = self.calculate_ticket_metrics(date)
        df["no_tickets_new"] = ticket_metrics["new"]
        df["no_tickets_update"] = ticket_metrics["update"]
        df["no_tickets_total"] = ticket_metrics["total"]

        message_metrics = self.calculate_message_metrics(date)
        df["no_messages_new"] = message_metrics["new"]
        df["no_messages_old"] = message_metrics["old"]
        df["no_messages_total"] = message_metrics["total"]

        total_tokens, model = self.get_total_tokens(date, Tables.CONVO)
        df["total_tokens"] = total_tokens
        df["model"] = model

        error_msg = "; ".join(self.errors) if self.errors else "None"
        df["log_message"] = error_msg

        runtime_data = runtime_tracker.get_runtime()
        if runtime_data and runtime_data.total_errors > 0:
            runtime_errors = []
            for route in runtime_data.routes_execution:
                if route.error_message:
                    runtime_errors.append(f"{route.route}: {route.error_message}")

            if runtime_errors:
                combined_errors = error_msg if error_msg != "None" else ""
                if combined_errors:
                    combined_errors += ";" + ";".join(runtime_errors)
                else:
                    combined_errors = "; ".join(runtime_errors)

                df["log_message"] = combined_errors

        logging.info("Generating schema and loading data to BigQuery...")
        prepare_and_load_to_bq(self.bigquery, df, "logs_test", load_data=True)
        return df.to_dict(orient="records")