from fuzzywuzzy import process
import pandas as pd
import re

def normalize_location(text: str) -> str:
    if not isinstance(text, str):
        return ""
    
    text = text.encode("latin1").decode("utf-8", "ignore")
    text = text.lower()
    text = re.sub(r'[^a-z\s]', '', text)
    text = text.replace("city of", "").replace("municipality of", "")
    text = text.replace("gen", "general").replace("sto", "santo")
    return re.sub(r'\s+', ' ', text).strip()

def viable(location: str, serviceable_list: list, threshold: int = 90) -> str:
    normalized_loc = normalize_location(location)
    match = process.extractOne(normalized_loc, serviceable_list)
    return "Yes" if match and match[1] >= threshold else "No"

def tag_viable(df: pd.DataFrame) -> pd.DataFrame:
    try:
        municipalities_df = pd.read_csv("config/mgo_serviceable.csv")
        municipalities_df['normalized'] = municipalities_df['municipality_name'].apply(normalize_location)
        normalized_serviceable = municipalities_df['normalized'].dropna().unique().tolist()

        df['viable'] = df['location'].apply(lambda loc: viable(loc, normalized_serviceable))
        return df
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"Exception occurred while tagging viable locations: {e}")