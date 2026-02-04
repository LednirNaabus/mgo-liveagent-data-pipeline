from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from enum import Enum

class ResponseStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"

@dataclass
class LiveAgentAPIResponse:
    success: bool
    status: ResponseStatus = ResponseStatus.SUCCESS
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None

@dataclass
class ExtractionResponse:
    status: ResponseStatus = None
    count: int = None
    data: List[Dict[str, Any]] = None
    message: str = None