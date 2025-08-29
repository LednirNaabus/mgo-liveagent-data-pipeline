"""
Static, non-sensitive constants used throughout the application.
"""
APP_VERSION = "v1"
BASE_URL = "https://mechanigo.ladesk.com/api/v3"
THROTTLE_DELAY = 0.4

LIVEAGENT_MGO_SYSTEM_USER_ID = "system00"
LIVEAGENT_MGO_SPECIAL_USER_ID = "00054iwg"

# BigQuery
PROJECT_ID = "mechanigo-liveagent"
DATASET_NAME = "conversations"

MAX_VALUE = 100
MAX_CONCURRENT_REQUESTS = 15

# For testing purposes
TEST_MAX_PAGE = 10
TEST_PER_PAGE = 10