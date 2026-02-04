from google.cloud.bigquery import SchemaField

TICKETS_SCHEMA = [
    SchemaField("id", "STRING"),
    SchemaField("owner_contactid", "STRING"),
    SchemaField("owner_email", "STRING"),
    SchemaField("owner_name", "STRING"),
    SchemaField("departmentid", "STRING"),
    SchemaField("agentid", "STRING"),
    SchemaField("status", "STRING"),
    SchemaField("tags", "STRING"),
    SchemaField("code", "STRING"),
    SchemaField("channel_type", "STRING"),
    SchemaField("date_created", "DATETIME"),
    SchemaField("date_changed", "DATETIME"),
    SchemaField("date_resolved", "DATETIME"),
    SchemaField("last_activity", "DATETIME"),
    SchemaField("last_activity_public", "DATETIME"),
    SchemaField("public_access_urlcode", "STRING"),
    SchemaField("subject", "STRING"),
    SchemaField("custom_fields", "STRING"),
    SchemaField("date_due", "DATETIME"),
    SchemaField("date_deleted", "DATETIME"),
    SchemaField("datetime_extracted", "DATETIME"),
]

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