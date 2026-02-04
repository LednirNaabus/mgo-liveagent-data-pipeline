from enum import Enum
import pandas as pd
import json

from integrations.liveagent.utils import (
    add_extraction_timestamp,
    set_timezone
)
from configs import PH_TZ

class FilterField(str, Enum):
    DATE_CREATED = "date_created"
    DATE_CHANGED = "date_changed"

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

def resolve_extraction_date() -> pd.Timestamp:
    now = pd.Timestamp.now(tz="UTC").astimezone(tz=PH_TZ)
    # 6 hour interval
    prev_batch = now - pd.Timedelta(hours=6)
    return prev_batch

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