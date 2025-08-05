from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

class RouteStatus(Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class RouteExecution:
    route: str
    status: RouteStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None

@dataclass
class AppRuntime:
    app_start_time: datetime
    app_end_time: Optional[datetime] = None
    total_duration_seconds: Optional[float] = None
    routes_execution: List[RouteExecution] = None
    total_errors: int = 0
    
    def __post_init__(self):
        if self.routes_execution is None:
            self.routes_execution = [
                RouteExecution("/extract/process-agents", RouteStatus.NOT_STARTED),
                RouteExecution("/extract/process-tags", RouteStatus.NOT_STARTED),
                RouteExecution("/extract/process-tickets-and-messages", RouteStatus.NOT_STARTED),
                RouteExecution("/extract/process-convo", RouteStatus.NOT_STARTED),
            ]