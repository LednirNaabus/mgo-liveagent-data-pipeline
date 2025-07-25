from core.schemas.TicketFilter import FilterField
import pandas as pd
import json

def set_filter(date: pd.Timestamp, filter_field: FilterField = FilterField.DATE_CREATED):
    if filter_field == FilterField.DATE_CREATED:
        start = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = (start + pd.offsets.MonthEnd(1)).replace(hour=23, minute=59, second=59)
    else:
        start = date.floor('h')
        end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
    return json.dumps([
        [filter_field.value, "D>", f"{start}"],
        [filter_field.value, "D<=", f"{end}"]
    ])