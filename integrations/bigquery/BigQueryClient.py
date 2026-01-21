from __future__ import annotations

import datetime as dt
import json
import threading
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
from uuid import uuid4

import pandas as pd
import pyarrow as pa
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import LoadJobConfig, SchemaField
from google.cloud.bigquery_storage_v1 import types as bqs_types
from google.cloud.bigquery_storage_v1 import writer as bq_writer
from google.oauth2 import service_account

from configs.log_utils import get_logger, manila_tz
from configs.config import ENV
from utils.json_utils import clean_json_text

logger = get_logger(__name__, level="INFO")

_BQ_CLIENT_CACHE: Dict[str, bigquery.Client] = {}
_BQ_STORAGE_CACHE: Dict[str, bigquery_storage.BigQueryWriteClient] = {}
_CLIENT_LOCK = threading.Lock()

def _shared_bigquery_client(
    project_id: str,
    credentials: service_account.Credentials,
    location: Optional[str],
) -> bigquery.Client:
    cache_key = f"{project_id}:{location or 'default'}"
    with _CLIENT_LOCK:
        client = _BQ_CLIENT_CACHE.get(cache_key)
        if client is None:
            client = bigquery.Client(
                project=project_id,
                credentials=credentials,
                location=location,
            )
            _BQ_CLIENT_CACHE[cache_key] = client
    return client


def _shared_storage_client(
    credentials: service_account.Credentials,
) -> bigquery_storage.BigQueryWriteClient:
    cache_key = credentials.service_account_email
    with _CLIENT_LOCK:
        storage_client = _BQ_STORAGE_CACHE.get(cache_key)
        if storage_client is None:
            storage_client = bigquery_storage.BigQueryWriteClient(credentials=credentials)
            _BQ_STORAGE_CACHE[cache_key] = storage_client
    return storage_client


class BigQueryUtils:
    """
    Shared BigQuery helper that prefers streaming ingest.

    The helper encapsulates dataset/table creation, ingestion via the Storage
    Write API with automatic load-job fallback, and light query utilities. It
    caches google-cloud clients when none are explicitly provided.
    """
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: Optional[str] = None,
        service_account: Optional[Union[str, Dict[str, Any]]] = None,
        location: Optional[str] = "asia-southeast1",
        *,
        client: Optional[bigquery.Client] = None,
        storage_client: Optional[bigquery_storage.BigQueryWriteClient] = None,
    ) -> None:
        credentials = None
        if client is None or storage_client is None:
            status = load_credentials(service_account)
            credentials = status.get("credentials")
            if not credentials:
                raise ValueError("Error fetching service account/credentials.")

        self.client = client or _shared_bigquery_client(project_id, credentials, location)  # type: ignore[arg-type]
        self.storage_client = storage_client or _shared_storage_client(credentials)  # type: ignore[arg-type]
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.location = location
        self.dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        self.table_ref = self.dataset_ref.table(table_id) if table_id else None


def load_credentials(service_account_info: Optional[Union[str, Dict[str, Any]]] = None, path: str = "gulong-chatbot-459723-d62aa45e3803.json") -> Dict[str, Any]:
    """Load credentials from dicts, JSON strings, explicit files, or ENV."""
    info: Optional[Dict[str, Any]] = None
    source = None
    try:
        if isinstance(service_account_info, dict):
            info = service_account_info
            source = "provided mapping"
        elif isinstance(service_account_info, str) and service_account_info.strip():
            if service_account_info.strip().startswith("{"):
                info = json.loads(clean_json_text(service_account_info))
                source = "provided string"
            else:
                with open(service_account_info, "r", encoding="utf-8") as fp:
                    info = json.load(fp)
                source = service_account_info
        elif ENV.get("CREDENTIALS"):
            info = json.loads(clean_json_text(ENV["CREDENTIALS"]))
            source = "environment variable"
        else:
            with open(path, "r", encoding="utf-8") as fp:
                info = json.load(fp)
            source = path

        if info is None:
            raise ValueError("Unable to load service account credentials.")

        creds = service_account.Credentials.from_service_account_info(info)
        return {
            "status": "success",
            "message": f"Successfully loaded credentials from {source}.",
            "credentials": creds,
        }
    except Exception as exc:
        logger.error("Error loading credentials: %s", exc)
        return {"status": "error", "message": f"Error loading credentials: {exc}", "credentials": None}