from typing import (
    Iterable,
    Optional,
    Sequence,
    Dict,
    Any
)
from google.cloud.bigquery import SchemaField
from integrations.bigquery import BigQueryUtils

def persist_rows(
    *,
    bq_client: BigQueryUtils,
    rows: Iterable[Dict[str, Any]],
    schema: Optional[Sequence[SchemaField]],
    key_columns: Sequence[str] = None,
    ensure_table: bool = True
):
    return bq_client.insert_rows(
        rows,
        schema=schema,
        key_columns=key_columns,
        ensure_table=ensure_table
    )