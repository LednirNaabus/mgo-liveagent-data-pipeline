from core.schemas.TicketFilter import FilterField
from typing import Optional, Tuple
from config.config import MNL_TZ
import pandas as pd

def resolve_extraction_date(
    is_initial: bool,
    date_str: Optional[str]
) -> Tuple[pd.Timestamp, FilterField]:
    if is_initial:
        date = pd.Timestamp(date_str) if date_str else pd.Timestamp("2025-01-01")
        return date, FilterField.DATE_CREATED
    else:
        now = pd.Timestamp.now(tz="UTC").astimezone(MNL_TZ)
        date = now - pd.Timedelta(hours=6)
        return date, FilterField.DATE_CHANGED