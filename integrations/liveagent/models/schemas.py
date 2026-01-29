from google.cloud.bigquery import SchemaField

AGENTS_SCHEMA = [
    SchemaField("id", "STRING", "NULLABLE"),
    SchemaField("name", "STRING", "NULLABLE"),
    SchemaField("email", "STRING", "NULLABLE"),
    SchemaField("role", "STRING", "NULLABLE"),
    SchemaField("roleId", "STRING", "NULLABLE"),
    SchemaField("avatar_url", "STRING", "NULLABLE"),
    SchemaField("online_status", "STRING", "NULLABLE"),
    SchemaField("status", "STRING", "NULLABLE"),
    SchemaField("gender", "STRING", "NULLABLE"),
    SchemaField("last_pwd_change", "DATETIME", "NULLABLE"),
    SchemaField("twofactor_auth", "STRING", "NULLABLE"),
    SchemaField("voice_status", "STRING", "NULLABLE"),
    SchemaField("sip_phone_id", "STRING", "NULLABLE"),
    SchemaField("api_phone_id", "STRING", "NULLABLE"),
]

TAGS_SCHEMA = [
    SchemaField("id", "STRING", "NULLABLE"),
    SchemaField("name", "STRING", "NULLABLE"),
    SchemaField("color", "STRING", "NULLABLE"),
    SchemaField("background_color", "STRING", "NULLABLE"),
    SchemaField("is_public", "STRING", "NULLABLE"),
    SchemaField("is_archived", "STRING", "NULLABLE")
]