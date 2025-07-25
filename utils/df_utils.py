import pandas as pd

def fill_nan_values(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == "object":
            df[col].fillna("", inplace=True)
        else:
            df[col].fillna(0, inplace=True)
    return df