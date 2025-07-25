from config.constants import PROJECT_ID, DATASET_NAME
from google.oauth2 import service_account
from google.cloud import bigquery
import json
import os

CREDS = os.getenv("CREDENTIALS")

if not CREDS:
    raise ValueError("Missing Google credentials!")

try:
    CREDS_FILE = json.loads(CREDS)
except json.JSONDecodeError as e:
    raise ValueError("Invalid JSON in the credentials env variable") from e

# Credentials and client
SCOPE = [
    'https://www.googleapis.com/auth/bigquery'
]
GOOGLE_CREDS = service_account.Credentials.from_service_account_info(
    CREDS_FILE,
    scopes=SCOPE
)
BQ_CLIENT = bigquery.Client(credentials=GOOGLE_CREDS, project=GOOGLE_CREDS.project_id)

# Dataset configuration
GCLOUD_PROJECT_ID = PROJECT_ID
BQ_DATASET_NAME = DATASET_NAME