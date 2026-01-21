from integrations.bigquery.BigQueryClient import BigQueryUtils
from configs.config import ENV
import pytest
import os

PROJECT_ID = os.environ["BQ_PROJECT_ID"]
DATASET_ID = os.environ["BQ_DATASET_ID"]
CREDENTIALS = ENV.get("CREDENTIALS")

@pytest.mark.integration
def test_list_tables_from_dataset():
    utils = BigQueryUtils(
        project_id="mechanigo-liveagent",
        dataset_id="conversations_new_01_21_2026",
        service_account=CREDENTIALS
    )

    tables = list(utils.client.list_tables("conversations_new_01_21_2026"))
    assert tables is not None
    assert len(tables) > 0