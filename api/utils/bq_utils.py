from integrations.bigquery import BigQueryUtils
from api.shared import Request
from configs.config import ENV
from typing import Dict

def get_bq_utils(request: Request, table_id: str) -> BigQueryUtils:
    """
    Returns a cached `BigQueryUtils` instance for the given table.

    Uses FastAPI `app.state` to store a per-process cache keyed by `table_id`.
    Creates and stores a new `BigQueryUtils` instance on first access using BigQuery settings
    from `ENV`.
    
    :param request: FastAPI request used to access `app.state`.
    :type request: Request
    :param table_id: BigQuery table identifier to bind in the client.
    :type table_id: str
    :return: Cached or newly created client bound to `table_id`.
    :rtype: BigQueryUtils
    """
    cache: Dict[str, BigQueryUtils] = request.app.state.bq_clients

    if table_id in cache:
        return cache[table_id]

    bq = BigQueryUtils(
        project_id=ENV["BQ_PROJECT_ID"],
        dataset_id=ENV["BQ_DATASET_ID"],
        table_id=table_id,
        service_account=ENV.get("CREDENTIALS"),
        location=ENV.get("BQ_LOCATION", "asia-southeast1")
    )

    cache[table_id] = bq
    return bq