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