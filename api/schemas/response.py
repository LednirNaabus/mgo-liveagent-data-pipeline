from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

class ResponseStatus(Enum):
    SUCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"

class APIResponse:
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    status: ResponseStatus = ResponseStatus.SUCCESS