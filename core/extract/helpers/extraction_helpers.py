from api.schemas.response import ExtractionResponse
from utils.df_utils import fill_nan_values
from utils.date_utils import set_timezone
from config.config import MNL_TZ
import pandas as pd
import re

def add_extraction_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    df["datetime_extracted"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%S")
    df["datetime_extracted"] = pd.to_datetime(df["datetime_extracted"], errors="coerce")
    return df

def extract_reference_code(message: str):
    if message is None or pd.isna(message):
        return "No Reference code"

    match = re.search(r"Ref:\s*([A-Z0-9]+)\b", message)
    return match.group(1) if match else "No Reference code"

def process_tickets(tickets: ExtractionResponse) -> pd.DataFrame:
    df = add_extraction_timestamp(tickets)
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
        target_tz=MNL_TZ
    )

    # Normalize 'custom_fields'
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
            target_tz=MNL_TZ
        )
        # Extract reference code
        df["reference_code"] = df["message"].apply(extract_reference_code)
        return df
    except Exception as e:
        print(f"Exception occurred while processing ticket messages: {e}")
        raise

def process_agents(agents: ExtractionResponse) -> pd.DataFrame:
    agents_df = pd.DataFrame(agents)
    agents_df = set_timezone(
        agents_df,
        "last_pswd_change",
        target_tz=MNL_TZ
    )
    return agents_df

def process_tags(tags: ExtractionResponse) -> pd.DataFrame:
    tags_df = pd.DataFrame(tags.data)
    tags_df = fill_nan_values(tags_df)
    return tags_df