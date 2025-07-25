from utils.df_utils import fill_nan_values
from utils.date_utils import set_timezone
from config.config import MNL_TZ
import pandas as pd

def process_tickets(tickets: pd.DataFrame) -> pd.DataFrame:
    tickets["datetime_extracted"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%S")
    tickets["datetime_extracted"] = pd.to_datetime(tickets["datetime_extracted"], errors="coerce")

    # set timezone
    tickets = set_timezone(
        tickets,
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
    tickets = fill_nan_values(tickets)
    return tickets