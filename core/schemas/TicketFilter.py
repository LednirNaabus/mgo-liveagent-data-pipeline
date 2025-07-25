from enum import Enum

class FilterField(str, Enum):
    DATE_CREATED = "date_created"
    DATE_CHANGED = "date_changed"