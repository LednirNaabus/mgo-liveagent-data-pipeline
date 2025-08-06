import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def fill_nan_values(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == "object":
            df[col].fillna("", inplace=True)
        else:
            df[col].fillna(0, inplace=True)
    return df

def drop_cols(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    try:
        existing = [col for col in cols if col in df.columns]
        if existing:
            df.drop(columns=existing, inplace=True)
    except Exception as e:
        logging.warning(f"Current columns: {df.columns.tolist()}")
        logging.error(f"Exception occurred while dropping columns: {e}")
    return df