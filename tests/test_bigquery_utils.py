from integrations.bigquery.BigQueryClient import BigQueryUtils
from google.cloud.bigquery import SchemaField
import json

def _make_utils() -> BigQueryUtils:
    return BigQueryUtils.__new__(BigQueryUtils)

def test_normalize_scalar_coerces_string_from_int():
    utils = _make_utils()
    field = SchemaField("name", "STRING")
    
    assert utils._normalize_scalar(field, 123) == "123"

def test_normalize_scalar_handles_bytes_and_json():
    utils = _make_utils()
    bytes_field = SchemaField("payload", "BYTES")
    json_field = SchemaField("meta", "JSON")

    assert utils._normalize_scalar(bytes_field, "abc") == b"abc"
    assert utils._normalize_scalar(json_field, {"k": "v"}) == json.dumps({"k": "v"})


def test_normalize_value_repeated_string_coerces_inner_values():
    utils = _make_utils()
    field = SchemaField("tags", "STRING", mode="REPEATED")

    assert utils._normalize_value(field, [1, "two", 3]) == ["1", "two", "3"]


def test_normalize_value_repeated_string_parses_json_string():
    utils = _make_utils()
    field = SchemaField("dropoff_reasons", "STRING", mode="REPEATED")

    parsed = utils._normalize_value(field, '["brand_unavailable", "stock_unavailable"]')

    assert parsed == ["brand_unavailable", "stock_unavailable"]


def test_normalize_value_repeated_string_ignores_non_list_json():
    utils = _make_utils()
    field = SchemaField("tags", "STRING", mode="REPEATED")

    parsed = utils._normalize_value(field, '{"unexpected": "map"}')

    assert parsed == ['{"unexpected": "map"}']


def test_normalize_value_repeated_record_wraps_single_dict():
    utils = _make_utils()
    field = SchemaField(
        "items",
        "RECORD",
        mode="REPEATED",
        fields=[SchemaField("name", "STRING")],
    )

    parsed = utils._normalize_value(field, '{"name": "example"}')

    assert parsed == [{"name": "example"}]


def test_prepare_rows_applies_normalization_for_multiple_fields():
    utils = _make_utils()
    schema = [
        SchemaField("name", "STRING"),
        SchemaField("tag_bytes", "BYTES"),
        SchemaField("meta", "JSON"),
    ]
    rows = [{"name": 42, "tag_bytes": "x", "meta": {"a": 1}}]

    prepared = utils._prepare_rows(rows, schema)

    assert prepared == [
        {"name": "42", "tag_bytes": b"x", "meta": json.dumps({"a": 1})}
    ]