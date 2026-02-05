from typing import Literal
from enum import Enum
import pandas as pd
import json
import re

from integrations.bigquery import BigQueryUtils
from integrations.liveagent.utils import (
    add_extraction_timestamp,
    set_timezone
)
from configs import PH_TZ

class FilterField(str, Enum):
    DATE_CREATED = "date_created"
    DATE_CHANGED = "date_changed"

NOW = pd.Timestamp.now(tz="UTC").astimezone(tz=PH_TZ)

def prev_batch_tickets() -> pd.Timestamp:
    # 6 hour interval
    prev_batch = NOW - pd.Timedelta(hours=6)
    return prev_batch

def recent_tickets(
    bq_client: BigQueryUtils,
    table_name: Literal["tickets", "messages", "tickets_test", "messages_test"],
    date_filter: str = "datecreated",
    limit: int = 10
) -> pd.DataFrame:
    date = prev_batch_tickets()
    start = date.floor('h')
    end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")

    if table_name == "tickets" or "tickets_test":
        date_filter = "date_created"
        select_clause = "id, owner_name, agentid"
        where_conditions = []
    elif table_name == "messages" or "messages_test":
        select_clause = "DISTINCT ticket_id"
        where_conditions = ["message_format = 'T'"]
    else:
        raise ValueError(f"The table_name '{table_name}' not found.")
    where_clauses = [
        f"{date_filter} >= '{start_str}'",
        f"{date_filter} < '{end_str}'"
    ]
    where_clauses.extend(where_conditions)
    where_clause = " AND ".join(where_clauses)

    project_id = bq_client.project_id
    dateset_name = bq_client.dataset_id
    query = f"""
    SELECT {select_clause} FROM {project_id}.{dateset_name}.{table_name}
    WHERE {where_clause}
    """
    if limit is not None:
        query += f"\nLIMIT {limit}"

    return bq_client.query_to_dataframe(query)

def set_ticket_filter(date: pd.Timestamp, filter_field: FilterField.DATE_CREATED) -> str:
    if filter_field == FilterField.DATE_CREATED:
        start = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = (start + pd.offsets.MonthEnd(1)).replace(hour=23, minute=59, second=59)
    else:
        start = date.floor("h")
        end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)

    if isinstance(filter_field, str):
        return json.dumps([
            [filter_field, "D>", f"{start}"],
            [filter_field, "D<=", f"{end}"]
        ])
    return json.dumps([
        [filter_field.value, "D>", f"{start}"],
        [filter_field.value, "D<=", f"{end}"]
    ])

def extract_reference_code(message: str) -> str:
    if message is None or pd.isna(message):
        return "No reference code"
    match = re.search(r"Ref:\s*([A-Z0-9]+)\b", message)
    return match.group(1) if match else "No reference code"

def process_tickets(tickets: pd.DataFrame) -> pd.DataFrame:
    df = add_extraction_timestamp(df=tickets)
    df = set_timezone(
        df,
        "date_created",
        "date_changed",
        "last_activity",
        "last_activity_public",
        "date_due",
        "date_deleted",
        "date_resolved",
        "datetime_extracted",
        target_tz=PH_TZ
    )
    df["custom_fields"] = df["custom_fields"].apply(
        lambda x: x[0] if isinstance(x, list) and len(x) == 1 and isinstance(x[0], dict) else None
    )
    return df

def process_ticket_messages(messages: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(messages)
    try:
        df = add_extraction_timestamp(df)
        df = set_timezone(
            df,
            "datecreated",
            "datefinished",
            "message_datecreated",
            "datetime_extracted",
            target_tz=PH_TZ
        )
        # Extract reference code
        df["reference_code"] = df["message"].apply(extract_reference_code)
        return df
    except Exception as e:
        print(f"Exception occurred while processing ticket messages: {e}")
        raise