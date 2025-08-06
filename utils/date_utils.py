from typing import Tuple
import pandas as pd

def set_timezone(df: pd.DataFrame, *cols: str, target_tz: str) -> pd.DataFrame:
    for col in cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")
        else:
            df[col] = df[col].dt.tz_convert("UTC")
        df[col] = df[col].dt.tz_convert(target_tz).dt.tz_localize(None)
    return df

def get_start_end_str(date: pd.Timestamp) -> Tuple:
    """Calculate the time with a 6 hour interval."""
    date = date - pd.Timedelta(hours=6)
    start = date.floor('h')
    end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    return start_str, end_str