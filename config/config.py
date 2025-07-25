"""
Main configuration file for the project.
"""
from google.oauth2 import service_account
from config.constants import BASE_URL
from google.cloud import bigquery
from dotenv import load_dotenv
import pytz
import os

load_dotenv()

LIVEAGENT_API_KEY = os.getenv('')
try:
    if not LIVEAGENT_API_KEY:
        raise ValueError(f"Missing API key for '{BASE_URL}'!")
except ValueError as e:
    print(e)
    exit(1)

MNL_TZ = pytz.timezone("Asia/Manila")