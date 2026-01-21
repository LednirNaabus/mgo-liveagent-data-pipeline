from core.extract.ConvoDataExtract import ConvoDataExtract
from api.schemas.response import ExtractionResponse
from utils.df_utils import fill_nan_values
from core.BigQueryManager import BigQuery
from utils.date_utils import set_timezone
from config.config import OPENAI_API_KEY
from core.Geocode import Geocoder
from config.config import MNL_TZ
import pandas as pd
import logging
import asyncio
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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

def recent_tickets(
    bq_client: BigQuery,
    project_id: str,
    dataset_name: str,
    table_name: str,
    date_filter: str = "datecreated",
    limit: int = 10
) -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC").astimezone(MNL_TZ)
    date = now - pd.Timedelta(hours=6)
    start = date.floor('h')
    end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")

    if table_name == "tickets":
        select_clause = "id, owner_name, agentid"
        where_conditions = []
    elif table_name == "messages":
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

    query = f"""
    SELECT {select_clause}
    FROM {project_id}.{dataset_name}.{table_name}
    WHERE {where_clause}
    """
    if limit is not None:
        query += f"\nLIMIT {limit}"
    logging.info(f"query: {query}")
    return bq_client.sql_query_bq(query, return_data=True)

async def process_single_chat(ticket_id: str, date_extracted: str, semaphore: asyncio.Semaphore) -> pd.DataFrame:
    async with semaphore:
        logging.info(f"Ticket ID: {ticket_id}")
        processor = await ConvoDataExtract.create(ticket_id, api_key=OPENAI_API_KEY)
        tokens = processor.data.get("tokens")
        model_used = processor.data.get("model", "unknown")
        
        data = processor.data.get("data", {})
        
        expected_fields = [
            'service_category', 'summary', 'intent_rating', 'engagement_rating',
            'clarity_rating', 'resolution_rating', 'sentiment_rating', 'location',
            'schedule_date', 'schedule_time', 'car', 'contact_num', 'payment',
            'inspection', 'quotation'
        ]
        
        if isinstance(data, dict):
            filtered_data = {k: v for k, v in data.items() if k in expected_fields}
            
            removed_fields = set(data.keys()) - set(filtered_data.keys())
            if removed_fields:
                logging.info(f"Removed LiteLLM metadata fields: {removed_fields}")
        else:
            filtered_data = data
        
        new_df = pd.DataFrame([filtered_data])
        tokens_df = pd.DataFrame([tokens], columns=["tokens"])
        model_df = pd.DataFrame([model_used], columns=["model"])
        combined = pd.concat([new_df, tokens_df, model_df], axis=1)
        combined['date_extracted'] = date_extracted
        combined['date_extracted'] = pd.to_datetime(combined['date_extracted'], errors='coerce')
        combined = set_timezone(combined, "date_extracted", target_tz=MNL_TZ)
        
        combined = convert_schedule_fields(combined)
        
        combined.insert(0, 'ticket_id', ticket_id)
        
        logging.info(f"Final columns for ticket {ticket_id}: {combined.columns.tolist()}")
        
        return combined

def convert_schedule_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert schedule_date and schedule_time fields to proper datetime formats for BigQuery.
    
    - schedule_date: Convert to DATETIME (YYYY-MM-DD HH:MM:SS)
    - schedule_time: Keep as STRING or convert to TIME format
    """
    if 'schedule_date' in df.columns:
        df['schedule_date'] = pd.to_datetime(df['schedule_date'], errors='coerce')
        if 'schedule_time' in df.columns:
            def combine_date_time(row):
                if pd.notna(row['schedule_date']):
                    date_part = row['schedule_date']
                    time_str = row.get('schedule_time', '')
                    
                    if time_str and pd.notna(time_str) and time_str.strip():
                        try:
                            time_str = str(time_str).strip().upper()
                            time_str = time_str.replace(' ', '')
                            
                            if 'AM' in time_str or 'PM' in time_str:
                                time_obj = pd.to_datetime(time_str, format='%I%p', errors='coerce')
                                if pd.isna(time_obj):
                                    time_obj = pd.to_datetime(time_str, format='%I:%M%p', errors='coerce')
                                
                                if pd.notna(time_obj):
                                    return pd.Timestamp(
                                        year=date_part.year,
                                        month=date_part.month,
                                        day=date_part.day,
                                        hour=time_obj.hour,
                                        minute=time_obj.minute
                                    )
                        except Exception as e:
                            logging.warning(f"Could not parse time '{time_str}': {e}")
                    
                    return pd.Timestamp(
                        year=date_part.year,
                        month=date_part.month,
                        day=date_part.day,
                        hour=0,
                        minute=0,
                        second=0
                    )
                return None
            
            df['schedule_date'] = df.apply(combine_date_time, axis=1)
        else:
            df['schedule_date'] = df['schedule_date'].apply(
                lambda x: pd.Timestamp(x.year, x.month, x.day, 0, 0, 0) if pd.notna(x) else None
            )
    
    return df

async def process_chat(ticket_ids: pd.Series):
    date_extracted = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")
    semaphore = asyncio.Semaphore(10)
    tasks = [
        process_single_chat(ticket_id, date_extracted, semaphore)
        for ticket_id in ticket_ids["ticket_id"]
    ]
    results = await asyncio.gather(*tasks)
    return pd.concat(results, ignore_index=True)

def process_address(df: pd.Series, gc: Geocoder):
    locations = []
    for i in df["location"]:
        try:
            result = gc.geocode(i)
            if result is None:
                result = {"lat": None, "lng": None, "address" : i}
        except AttributeError as e:
            logging.error(f"AttributeError: {e}")
        except Exception as e:
            result = {"lat": None, "address": i, "error": str({e})}
        locations.append(result)
    return pd.DataFrame(locations)

def process_tags(tags: ExtractionResponse) -> pd.DataFrame:
    tags_df = pd.DataFrame(tags.data)
    tags_df = fill_nan_values(tags_df)
    return tags_df

def create_base_log_dataframe() -> pd.DataFrame:
    df = pd.DataFrame({
        "extraction_date": [pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")]
    })
    df["extraction_date"] = pd.to_datetime(df["extraction_date"], errors="coerce")
    df = set_timezone(df, "extraction_date", target_tz=MNL_TZ)
    return df