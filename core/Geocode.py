from core.BigQueryManager import BigQuery
from strsimpy.jaccard import Jaccard
from utils.geocode_utils import normalize_location, tag_viable, viable
from typing import Optional, Tuple
from time import time, sleep
import pandas as pd
import numpy as np
import requests
import logging

"""
Git commit message:
refactor: Convert geocoding logic into Geocoder class
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Geocoder:
    """Class for geocoding utilities."""
    def __init__(self, bq_client: BigQuery):
        self.bq_client = bq_client
        self.project_name = bq_client.client.project
        self.df_bq = self._load_bq_data()
        self.df_bq_munprov = self._filter_munprov()
        self.time_osm = 0

    def _load_bq_data(self) -> pd.DataFrame:
        query = "SELECT * FROM `{}.locations.address_location_psgc`".format(self.project_name)
        df = self.bq_client.sql_query_bq(query)
        df["address_cleaned"] = df["address"].map(self.clean_str)
        return df

    def _filter_munprov(self) -> pd.DataFrame:
        return self.df_bq[
            (self.df_bq["geo_level"] == "municity") | (self.df_bq["geo_level"] == "provdist")
        ].copy()

    def clean_str(self, text: Optional[str]) -> Optional[str]:
        if pd.isna(text):
            return None
        cleaned_text = text.replace("ñ", "n").replace("ã±", "n").lower()
        return cleaned_text

    def similarity(self, str1: str, str2: str, n: int) -> float:
        return Jaccard(n).similarity(str1, str2)
    
    def geocode(self, address: str) -> Optional[dict]:
        if not address:
            return None

        address_cleaned = self.clean_str(address)
        if not address_cleaned:
            return None

        n = max(5, round(np.sqrt(len(address_cleaned) / 2)))

        df_munprov = self.df_bq_munprov.copy()
        df_munprov["score"] = df_munprov["address_cleaned"].map(
            lambda row: self.similarity(address_cleaned, row, n)
        )
        df_munprov = df_munprov[df_munprov["score"] != 0]

        conds = pd.Series(False, index=self.df_bq.index)
        for row in df_munprov.itertuples():
            if row.geo_level == "municity":
                conds |= self.df_bq["municity_code"] == row.municity_code
            elif row.geo_level == "provdist":
                conds |= self.df_bq["provdist_code"] == row.provdist_code
            else:
                raise ValueError(f"Unexpected geo_level: {row.geo_level}")

        df_bq_brgy = self.df_bq[conds].copy()
        df_bq_brgy["score"] = df_bq_brgy["address_cleaned"].map(
            lambda row: self.similarity(address_cleaned, row, n)
        )
        df_bq_brgy.sort_values(by="score", ascending=False, inplace=True)

        if not df_bq_brgy.empty:
            top_match = df_bq_brgy.iloc[0].to_dict()
            result = {
                "input_address": address,
                "address": top_match.get("address"),
                "latitude": top_match.get("latitude"),
                "longitude": top_match.get("longitude"),
                "score": top_match.get("score"),
                "source": "database",
            }
            if result["score"] >= 0.1:
                return result

        return self._fallback_geocode(address)

    def _fallback_geocode(self, address: str) -> Optional[dict]:
        full_address = address + ", Philippines"

        try:
            delay = 1.25 - (time() - self.time_osm)
            if delay > 0:
                sleep(delay)
        except Exception:
            pass

        result = self._geocode_osm(full_address)
        self.time_osm = time()

        if result:
            lat, lng = result
            return {
                "input_address": address,
                "address": full_address,
                "latitude": lat,
                "longitude": lng,
                "score": 0,
                "source": "osm"
            }

        result = self._geocode_photon(full_address)
        if result:
            lat, lng = result
            return {
                "input_address": address,
                "address": full_address,
                "latitude": lat,
                "longitude": lng,
                "score": 0,
                "source": "photon"
            }

        return None

    def _geocode_osm(self, address: str) -> Optional[Tuple[float, float]]:
        try:
            resp = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": address, "format": "json", "limit": 1},
                headers={"User-Agent": "my_geocoder_app"}
            )
            resp.raise_for_status()
            data = resp.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception:
            pass
        return None

    def _geocode_photon(self, address: str) -> Optional[Tuple[float, float]]:
        try:
            resp = requests.get(
                "https://photon.komoot.io/api/",
                params={"q": address, "limit": 1},
            )
            resp.raise_for_status()
            features = resp.json().get("features")
            if features:
                coords = features[0]["geometry"]["coordinates"]
                return coords[1], coords[0]
        except Exception:
            pass
        return None