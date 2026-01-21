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

    # Table / Dataset management
    def dataset_exists(self) -> bool:
        """Return True when the dataset is reachable."""
        try:
            self.client.get_dataset(self.dataset_ref)
            return True
        except NotFound:
            return False

    def ensure_dataset(self, location: Optional[str] = None) -> None:
        """Create the dataset if it does not exist."""
        if not self.dataset_exists():
            ds = bigquery.Dataset(self.dataset_ref)
            if location or self.location:
                ds.location = location or self.location
            created = self.client.create_dataset(ds)
            logger.info("Created dataset: %s", created.full_dataset_id)

    def table_exists(self) -> bool:
        """Return True when the configured table exists."""
        if not self.table_ref:
            return False
        try:
            self.client.get_table(self.table_ref)
            return True
        except NotFound:
            return False

    def ensure_table(
        self,
        schema: Optional[Sequence[SchemaField]] = None,
        from_dataframe: Optional[pd.DataFrame] = None,
        location: Optional[str] = None,
        time_partitioning: Optional[bigquery.TimePartitioning] = None,
        clustering_fields: Optional[Sequence[str]] = None,
    ) -> None:
        """
        Create the table with the supplied schema when missing.

        If a DataFrame is provided and no explicit schema is supplied, its
        columns are converted to BigQuery fields.
        """
        if not self.table_id:
            raise ValueError("table_id is required to manage tables.")

        self.ensure_dataset(location=location)

        if self.table_exists():
            return

        if schema is None and from_dataframe is not None:
            schema = bigquery.Schema.from_dataframe(from_dataframe)  # type: ignore[attr-defined]
        if schema is None:
            raise ValueError("Table missing and schema not provided.")

        table = bigquery.Table(self.table_ref, schema=list(schema))
        if time_partitioning:
            table.time_partitioning = time_partitioning
        if clustering_fields:
            table.clustering_fields = list(clustering_fields)
        created = self.client.create_table(table)
        logger.info("Created table: %s", created.full_table_id)

    def _ensure_bq_schema(self, schema_obj: Sequence[Union[SchemaField, Dict[str, Any]]]) -> List[SchemaField]:
        fixed: List[SchemaField] = []
        for i, item in enumerate(schema_obj):
            if isinstance(item, SchemaField):
                fixed.append(item)
            elif isinstance(item, dict):
                fixed.append(SchemaField.from_api_repr(item))
            else:
                raise TypeError(f"Schema item #{i} must be SchemaField or dict, got {type(item)}")
        return fixed

    # Load methods
    def load_dataframe(
        self,
        df: pd.DataFrame,
        write_disposition: str = "WRITE_APPEND",
        schema: Optional[Sequence[SchemaField]] = None,
        ignore_unknown_values: bool = True,
        allow_quoted_newlines: bool = True,
        create_if_needed: bool = True,
    ) -> bigquery.LoadJob:
        """
        Load a pandas DataFrame into BigQuery via a load job.

        When `create_if_needed` is True the table will be created first when
        missing.
        """
        if create_if_needed:
            self.ensure_table(schema=schema, from_dataframe=df)

        # Ensure table reference is available for the client call (avoid passing None)
        if self.table_ref is None:
            raise ValueError("table_id is required to load dataframe.")

        job_config = LoadJobConfig(
            write_disposition=write_disposition,
            ignore_unknown_values=ignore_unknown_values,
            allow_quoted_newlines=allow_quoted_newlines,
        )
        if schema is not None:
            job_config.schema = list(schema)
        job = self.client.load_table_from_dataframe(df, self.table_ref, job_config=job_config)
        job.result()
        logger.info("Loaded DataFrame: %s rows -> %s", job.output_rows, self._fqtn())
        return job

    def load_json(
        self,
        rows: Union[List[Dict[str, Any]], Iterable[Dict[str, Any]]],
        write_disposition: str = "WRITE_APPEND",
        schema: Optional[Sequence[SchemaField]] = None,
        autodetect: bool = False,
        create_if_needed: bool = True,
        ignore_unknown_values: bool = True,
    ) -> bigquery.LoadJob:
        """
        Load JSON-like rows into BigQuery.

        Callers must provide a schema or enable autodetect when creating a new
        table.
        """
        if create_if_needed and not self.table_exists():
            if not schema and not autodetect:
                raise ValueError("Table missing. Provide `schema` or set `autodetect=True`.")
            self.ensure_table(schema=schema)

        job_config = LoadJobConfig(
            write_disposition=write_disposition,
            ignore_unknown_values=ignore_unknown_values,
            autodetect=autodetect,
        )
        if schema is not None:
            job_config.schema = list(schema)

        # Ensure table reference is available for the client call (avoid passing None)
        if self.table_ref is None:
            raise ValueError("table_id is required to load json.")

        job = self.client.load_table_from_json(list(rows), self.table_ref, job_config=job_config)
        job.result()
        logger.info("Loaded JSON: %s rows -> %s", job.output_rows, self._fqtn())
        return job
    # Streaming API
    def insert_rows(self, rows: Iterable[Dict[str, Any]], schema: Optional[Sequence[SchemaField]] = None, key_columns: Optional[Sequence[str]] = None, ensure_table: bool = True) -> Dict[str, Any]:
        """
        Stream rows using the Storage Write API, falling back to load jobs on failure.

        When `key_columns` is provided the method uses a staging table plus
        MERGE to update existing rows; otherwise rows are appended.
        """
        rows_list = list(rows)
        if not rows_list:
            return {"stream_ok": True, "fallback_used": False, "error": None}
        if schema is None:
            if not self.table_exists():
                raise ValueError("Schema required when table schema cannot be fetched.")
            
            # Ensure table reference is available for the client call (avoid passing None)
            if self.table_ref is None:
                raise ValueError("table_id is required to insert rows.")
                
            table = self.client.get_table(self.table_ref)
            schema_fields = list(table.schema)
        else:
            schema_fields = self._ensure_bq_schema(schema)
        if ensure_table:
            self.ensure_table(schema=schema_fields)
        def _row_errors_from_exc(err: Exception) -> Optional[List[Dict[str, Any]]]:
            """
            Attempt to extract row_errors from a BigQuery/Storage exception.
            """
            for attr in ("row_errors", "errors"):
                if hasattr(err, attr):
                    try:
                        found = getattr(err, attr)
                        if found:
                            return list(found)
                    except Exception:
                        pass
            result = getattr(err, "result", None)
            if result is not None:
                for attr in ("row_errors", "errors"):
                    try:
                        found = getattr(result, attr, None)
                        if found:
                            return list(found)
                    except Exception:
                        pass
            return None

        stream_ok = False
        fallback_used = False
        error_text: Optional[str] = None
        # Normalize once for both streaming and potential load_json fallback
        normalized_rows = self._prepare_rows(rows_list, schema_fields)
        try:
            self._stream_rows(self.table_id, normalized_rows, schema_fields, key_columns=key_columns)
            stream_ok = True
        except Exception as exc:
            fallback_used = True
            error_text = str(exc)
            row_errors = _row_errors_from_exc(exc)
            if row_errors:
                logger.warning(
                    "Streaming insert failed, falling back to load_json: %s | row_errors=%s",
                    exc,
                    row_errors,
                )
            else:
                logger.warning(
                    "Streaming insert failed, falling back to load_json: %s | exc_details=%s",
                    exc,
                    getattr(exc, "__dict__", {}),
                )
            self.load_json(normalized_rows, schema=schema_fields, autodetect=False, create_if_needed=True)
        return {"stream_ok": stream_ok, "fallback_used": fallback_used, "error": error_text}

    def stream_rows(self, table_id: Optional[str], rows: Iterable[Dict[str, Any]], *, schema: Sequence[SchemaField], key_columns: Optional[Sequence[str]] = None) -> None:
        """
        Stream rows into the given table using the supplied schema.

        When `key_columns` is supplied, rows are merged instead of appended.
        """
        rows_list = list(rows)
        if not rows_list:
            return
        schema_fields = self._ensure_bq_schema(schema)
        self.ensure_table(schema=schema_fields)
        self._stream_rows(table_id, rows_list, schema_fields, key_columns=key_columns)

    def _stream_rows(self, table_id: Optional[str], rows: List[Dict[str, Any]], schema_fields: Sequence[SchemaField], key_columns: Optional[Sequence[str]] = None) -> None:
        """Choose merge vs append streaming path."""
        table_ref = self._table_ref(table_id)
        if key_columns:
            self._stream_with_merge(table_ref, rows, schema_fields, key_columns)
        else:
            self._write_via_storage_api(table_ref, rows, schema_fields)

    def _table_ref(self, table_id: Optional[str] = None) -> bigquery.TableReference:
        tid = table_id or self.table_id
        if not tid:
            raise ValueError("table_id is required for streaming operations.")
        return self.dataset_ref.table(tid)

    def _stream_with_merge(self, target_ref: bigquery.TableReference, rows: List[Dict[str, Any]], schema_fields: Sequence[SchemaField], key_columns: Sequence[str]) -> None:
        """Stream rows into a staging table and MERGE into the target table."""
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        stg_name = f"{target_ref.table_id}__stg_{uuid4().hex[:8]}"
        stg_ref = dataset_ref.table(stg_name)
        stg_tbl = bigquery.Table(stg_ref, schema=list(schema_fields))
        stg_tbl.expires = dt.datetime.now(tz=manila_tz) + dt.timedelta(hours=1)
        self.client.create_table(stg_tbl)
        try:
            self._write_via_storage_api(stg_ref, rows, schema_fields)
            self._merge_tables(target_ref, stg_ref, schema_fields, key_columns)
        finally:
            self.client.delete_table(stg_ref, not_found_ok=True)
    def _write_via_storage_api(self, table_ref: bigquery.TableReference, rows: Iterable[Dict[str, Any]], schema_fields: Sequence[SchemaField]) -> None:
        """Append rows using the BigQuery Storage Write API."""
        rows_list = list(rows)
        if not rows_list:
            return
        arrow_schema = self._build_arrow_schema(schema_fields)
        parent = self.storage_client.table_path(table_ref.project, table_ref.dataset_id, table_ref.table_id)
        stream_name = f"{parent}/_default"
        request_template = bqs_types.AppendRowsRequest()
        request_template.write_stream = stream_name
        arrow_data = bqs_types.AppendRowsRequest.ArrowData()
        arrow_data.writer_schema.serialized_schema = arrow_schema.serialize().to_pybytes()
        request_template.arrow_rows = arrow_data
        append_stream = bq_writer.AppendRowsStream(self.storage_client, request_template)
        try:
            for chunk in self._chunks(rows_list, 500):
                prepared = self._prepare_rows(chunk, schema_fields)
                table = self._rows_to_arrow_table(prepared, arrow_schema)
                for batch in table.to_batches():
                    request = bqs_types.AppendRowsRequest()
                    request.arrow_rows.rows.serialized_record_batch = batch.serialize().to_pybytes()
                    append_stream.send(request).result()
        finally:
            pass
            # try:
            #     append_stream.close()
            # except RuntimeError:
            #     pass

    @staticmethod
    def _chunks(data: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
        for i in range(0, len(data), size):
            yield data[i : i + size]

    def _build_arrow_schema(self, schema_fields: Sequence[SchemaField]) -> pa.Schema:
        return pa.schema([
            pa.field(field.name, self._field_to_arrow_type(field), nullable=(field.mode or "NULLABLE").upper() != "REQUIRED")
            for field in schema_fields
        ])

    def _field_to_arrow_type(self, field: SchemaField) -> pa.DataType:
        mapping: Dict[str, pa.DataType] = {
            "STRING": pa.large_string(),
            "INT64": pa.int64(),
            "INTEGER": pa.int64(),
            "FLOAT64": pa.float64(),
            "FLOAT": pa.float64(),
            "BOOL": pa.bool_(),
            "BOOLEAN": pa.bool_(),
            "BYTES": pa.binary(),
            "TIMESTAMP": pa.timestamp("us", tz="UTC"),
            "DATE": pa.date32(),
            "TIME": pa.time64("us"),
            "DATETIME": pa.timestamp("us"),
            "NUMERIC": pa.large_string(),
            "BIGNUMERIC": pa.large_string(),
            "JSON": pa.large_string(),
        }
        field_type = field.field_type.upper()
        if field_type == "RECORD":
            children = [
                pa.field(child.name, self._field_to_arrow_type(child), nullable=(child.mode or 'NULLABLE').upper() != "REQUIRED")
                for child in field.fields
            ]
            base = pa.struct(children)
        else:
            base = mapping.get(field_type, pa.large_string())
        if (field.mode or 'NULLABLE').upper() == "REPEATED":
            return pa.list_(base)
        return base

    def _prepare_rows(self, rows: List[Dict[str, Any]], schema_fields: Sequence[SchemaField]) -> List[Dict[str, Any]]:
        prepared: List[Dict[str, Any]] = []
        for row in rows:
            converted: Dict[str, Any] = {}
            for field in schema_fields:
                converted[field.name] = self._normalize_value(field, row.get(field.name))
            prepared.append(converted)
        return prepared

    def _normalize_value(self, field: SchemaField, value: Any) -> Any:
        if (field.mode or 'NULLABLE').upper() == "REPEATED":
            if value in (None, []):
                return []
            coerced = value
            ftype = (field.field_type or "").upper()
            # For repeated scalar fields (STRING, INT, etc.) only accept JSON that decodes to a list.
            if ftype == "STRING" and isinstance(value, str):
                parsed = self._parse_jsonish_sequence(value)
                if parsed is not None and isinstance(parsed, list):
                    coerced = parsed
            # For repeated RECORD/STRUCT fields, accept a JSON list of dicts or a single dict.
            elif ftype in ("RECORD", "STRUCT") and isinstance(value, str):
                parsed = self._parse_jsonish_sequence(value)
                if parsed is not None:
                    # if dict -> wrap into list, if list -> use as-is
                    coerced = parsed if isinstance(parsed, list) else [parsed]
            # If coerced is a list of single-character fragments like ["[","'","a",...],
            # attempt to reassemble and parse again.
            if isinstance(coerced, list) and coerced and all(isinstance(ch, str) and len(ch) == 1 for ch in coerced):
                joined = "".join(coerced)
                reparsed = self._parse_jsonish_sequence(joined)
                if isinstance(reparsed, list):
                    coerced = reparsed
            seq = coerced if isinstance(coerced, list) else [coerced]
            return [self._normalize_scalar(field, item) for item in seq]
        return self._normalize_scalar(field, value)

    @staticmethod
    def _parse_jsonish_sequence(value: str) -> Optional[Union[List[Any], Dict[str, Any]]]:
        """Best-effort conversion of JSON-dumped strings to Python objects."""
        for parser in (json.loads, None):
            try:
                if parser is json.loads:
                    parsed = json.loads(clean_json_text(value))
                else:
                    import ast
                    parsed = ast.literal_eval(value)
            except Exception:
                continue
            if isinstance(parsed, (list, dict)):
                return parsed
        return None

    def _normalize_scalar(self, field: SchemaField, value: Any) -> Any:
        if value is None:
            return None
        ftype = field.field_type.upper()
        if ftype == "JSON":
            # Always return JSON-serializable text for JSON fields; wrap plain strings as JSON strings.
            if isinstance(value, str):
                try:
                    return json.dumps(json.loads(clean_json_text(value)))
                except Exception:
                    return json.dumps(value)
            return json.dumps(value)
        if ftype == "STRING" and not isinstance(value, str):
            # Storage Write API is strict about strings being bytes/str; coerce to str early.
            try:
                return str(value)
            except Exception:
                return value
        if ftype == "BYTES":
            if isinstance(value, bytes):
                return value
            if isinstance(value, str):
                return value.encode("utf-8", "ignore")
            return value
        if ftype == "RECORD":
            if not isinstance(value, dict):
                return value
            normalized: Dict[str, Any] = {}
            for child in field.fields or []:
                # Use full normalization for nested fields so repeated children are handled correctly.
                normalized[child.name] = self._normalize_value(child, value.get(child.name))
            return normalized
        if ftype == "TIMESTAMP":
            return self._normalize_timestamp_value(value)
        if ftype == "DATETIME":
            return self._normalize_datetime_value(value)
        if ftype in {"INT64", "INTEGER"} and isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return value
        if ftype in {"FLOAT64", "FLOAT"} and isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return value
        if ftype == "DATE" and isinstance(value, str):
            try:
                return dt.date.fromisoformat(value.split("T")[0])
            except ValueError:
                return value
        if ftype == "TIME" and isinstance(value, str):
            try:
                return dt.time.fromisoformat(value)
            except ValueError:
                return value
        return value

    def _normalize_timestamp_value(self, value: Any) -> Any:
        if isinstance(value, dt.datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=dt.timezone.utc)
            return value.astimezone(dt.timezone.utc)
        if isinstance(value, str):
            parsed = self._parse_datetime_string(value)
            if parsed is None:
                return value
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc)
        return value

    def _normalize_datetime_value(self, value: Any) -> Any:
        if isinstance(value, dt.datetime):
            return value.replace(tzinfo=None)
        if isinstance(value, str):
            parsed = self._parse_datetime_string(value)
            if parsed is None:
                return value
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return parsed
        return value

    @staticmethod
    def _parse_datetime_string(text: str) -> Optional[dt.datetime]:
        candidate = text.strip()
        if " " in candidate and "T" not in candidate:
            candidate = candidate.replace(" ", "T")
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            return dt.datetime.fromisoformat(candidate)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return dt.datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def _rows_to_arrow_table(self, rows: List[Dict[str, Any]], arrow_schema: pa.Schema) -> pa.Table:
        arrays = []
        for field in arrow_schema:
            column = [row.get(field.name) for row in rows]
            arrays.append(pa.array(column, type=field.type))
        return pa.Table.from_arrays(arrays, schema=arrow_schema)
    def _merge_tables(self, target: bigquery.TableReference, staging: bigquery.TableReference, schema_fields: Sequence[SchemaField], key_columns: Sequence[str]) -> None:
        target_fqn = f"`{target.project}.{target.dataset_id}.{target.table_id}`"
        staging_fqn = f"`{staging.project}.{staging.dataset_id}.{staging.table_id}`"
        on_clause = " AND ".join(f"target.`{col}` = source.`{col}`" for col in key_columns)
        all_columns = [field.name for field in schema_fields]
        update_set = ", ".join(f"`{col}` = source.`{col}`" for col in all_columns if col not in key_columns) or ", ".join(f"`{col}` = source.`{col}`" for col in key_columns)
        insert_cols = ", ".join(f"`{col}`" for col in all_columns)
        insert_vals = ", ".join(f"source.`{col}`" for col in all_columns)
        merge_sql = f"""
        MERGE {target_fqn} AS target
        USING {staging_fqn} AS source
        ON {on_clause}
        WHEN MATCHED THEN
          UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        self.client.query(merge_sql).result()

    # Upsert helpers
    def upsert_json(self, row: Dict[str, Any], schema: Sequence[SchemaField], key_cols: List[str], update_cols: Optional[List[str]] = None) -> Dict[str, Any]:
        return self.upsert_many_json([row], schema=schema, key_cols=key_cols, update_cols=update_cols)

    def upsert_many_json(self, rows: List[Dict[str, Any]], schema: Sequence[SchemaField], key_cols: List[str], update_cols: Optional[List[str]] = None) -> Dict[str, Any]:
        if not rows:
            return {'status': 'success', 'status_code': 200, 'message': 'No rows to upsert', 'data': {'affected_rows': 0}}
        schema_fields = self._ensure_bq_schema(schema)
        if not self.table_ref:
            raise ValueError("table_id is required for upsert operations.")
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        stg_name = f"{self.table_id}__stg_{uuid4().hex[:8]}"
        stg_ref = dataset_ref.table(stg_name)
        stg_tbl = bigquery.Table(stg_ref, schema=list(schema_fields))
        stg_tbl.expires = dt.datetime.now(tz=manila_tz) + dt.timedelta(hours=1)
        self.client.create_table(stg_tbl)
        try:
            load_cfg = bigquery.LoadJobConfig(schema=list(schema_fields), write_disposition="WRITE_TRUNCATE", ignore_unknown_values=True)
            self.client.load_table_from_json(rows, stg_ref, job_config=load_cfg).result()
            cols = [f.name for f in schema_fields]
            set_cols = update_cols or [c for c in cols if c not in key_cols]
            on_clause = " AND ".join([f"T.{k} = S.{k}" for k in key_cols])
            update_set = ", ".join([f"T.{c} = S.{c}" for c in set_cols])
            insert_cols = ", ".join(cols)
            insert_vals = ", ".join([f"S.{c}" for c in cols])
            merge_sql = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{self.table_id}` T
            USING `{self.project_id}.{self.dataset_id}.{stg_name}` S
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            merge_job = self.client.query(merge_sql)
            merge_result = merge_job.result()
            return {'status': 'success', 'status_code': 200, 'message': f'Upserted {len(rows)} row(s)', 'data': {'total_rows': merge_result.total_rows, 'affected_rows': merge_job.num_dml_affected_rows}}
        finally:
            self.client.delete_table(stg_ref, not_found_ok=True)

    # Query helpers
    def query_to_dataframe(self, sql: str, params: Optional[Sequence[bigquery.ScalarQueryParameter]] = None) -> pd.DataFrame:
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        job = self.client.query(sql, job_config=job_config)
        return job.result().to_dataframe()

    def query_to_json(self, sql: str, params: Optional[Sequence[bigquery.ScalarQueryParameter]] = None) -> List[Dict[str, Any]]:
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        rows = self.client.query(sql, job_config=job_config).result()
        return [dict(row.items()) for row in rows]

    def fetch_table(self, table_fqn: str, where: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        clause = f"WHERE {where}" if where else ""
        lim = f"LIMIT {limit}" if limit else ""
        sql = f"SELECT * FROM `{table_fqn}` {clause} {lim}"
        return self.query_to_json(sql)

    def claim_queue_batch(self, batch_size: int, include_failed_after_minutes: int = 0, order: str = "oldest") -> List[Dict[str, Any]]:
        """
        Claim a batch of queue entries and mark them enqueued.

        Rows are deduplicated per user_id then updated inside a multi-statement
        BigQuery script to avoid race conditions.
        """
        if not self.table_id:
            raise ValueError("table_id is required to claim queue batches.")
        sort_newest = str(order).lower() in {"newest", "desc", "recent", "latest"}
        where_clauses = ["status = 'PENDING'"]
        if include_failed_after_minutes > 0:
            where_clauses.append(
                "(status IN ('FAILED','PARTIAL') "
                f"AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(last_attempt_datetime), MINUTE) >= {int(include_failed_after_minutes)})"
            )
        where_sql = " OR ".join(where_clauses)
        sql = f"""
        BEGIN
            CREATE TEMP TABLE claimed AS
            WITH elig AS (
                SELECT user_id, user_name, added_datetime, last_user_interaction_datetime
                FROM `{self._fqtn()}`
                WHERE {where_sql}
            ),
            dedup AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY user_id
                    ORDER BY
                        (last_user_interaction_datetime IS NULL),
                        CASE WHEN @sort_newest THEN last_user_interaction_datetime END DESC,
                        CASE WHEN NOT @sort_newest THEN last_user_interaction_datetime END ASC,
                        CASE WHEN @sort_newest THEN added_datetime END DESC,
                        CASE WHEN NOT @sort_newest THEN added_datetime END ASC
                ) rn
                FROM elig
            ),
            pick AS (
                SELECT *
                FROM dedup
                WHERE rn = 1
                ORDER BY
                    (last_user_interaction_datetime IS NULL),
                    CASE WHEN @sort_newest THEN last_user_interaction_datetime END DESC,
                    CASE WHEN NOT @sort_newest THEN last_user_interaction_datetime END ASC,
                    CASE WHEN @sort_newest THEN added_datetime END DESC,
                    CASE WHEN NOT @sort_newest THEN added_datetime END ASC
                LIMIT @batch_size
            )
            SELECT
                user_id,
                user_name,
                last_user_interaction_datetime,
                CONCAT(
                    'analyze-', user_id, '-',
                    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP(), 'Asia/Manila'),
                    '-', SUBSTR(GENERATE_UUID(), 1, 6)
                ) AS task_name
            FROM pick;

            UPDATE `{self._fqtn()}` AS q
            SET
                q.status = 'ENQUEUED',
                q.enqueued_datetime = DATETIME(CURRENT_TIMESTAMP(), 'Asia/Manila'),
                q.updated_datetime = DATETIME(CURRENT_TIMESTAMP(), 'Asia/Manila'),
                q.task_name = c.task_name
            FROM claimed AS c
            WHERE q.user_id = c.user_id
              AND (
                q.status = 'PENDING' OR (
                    (q.status = 'FAILED' OR q.status = 'PARTIAL')
                    AND {include_failed_after_minutes} > 0
                    AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(q.last_attempt_datetime), MINUTE) >= {include_failed_after_minutes}
                )
              );

            SELECT user_id, user_name, last_user_interaction_datetime, task_name FROM claimed;
        END
        """
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("batch_size", "INT64", batch_size),
                    bigquery.ScalarQueryParameter("sort_newest", "BOOL", sort_newest),
                ],
                use_legacy_sql=False,
            ),
        )
        rows = list(job.result())
        return [dict(row.items()) for row in rows]

    def _fqtn(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"


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