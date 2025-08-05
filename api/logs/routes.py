from datetime import datetime, timezone
from .Tracker import runtime_tracker
from config.config import MNL_TZ
from .models import RouteStatus
from dataclasses import asdict
from api.common import (
    HTTPException,
    APIRouter
)

router = APIRouter()

def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: convert_datetime(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime(item) for item in obj]
    elif isinstance(obj, RouteStatus):
        return obj.value

    return obj

@router.get("/logs")
async def get_runtime_logs():
    runtime_data = runtime_tracker.get_runtime()
    if not runtime_data:
        raise HTTPException(status_code=404, detail="No runtime data available.")

    runtime_dict = convert_datetime(asdict(runtime_data))

    summary = {
        "total_routes": len(runtime_data.routes_execution),
        "completed_routes": len([r for r in runtime_data.routes_execution if r.status == RouteStatus.COMPLETED]),
        "failed_routes": len([r for r in runtime_data.routes_execution if r.status == RouteStatus.FAILED]),
        "in_progress_routes": len([r for r in runtime_data.routes_execution if r.status == RouteStatus.IN_PROGRESS]),
        "not_started_routes": len([r for r in runtime_data.routes_execution if r.status == RouteStatus.NOT_STARTED]),
        "total_errors": runtime_data.total_errors,
        "app_status": "completed" if runtime_data.app_end_time else "running"
    }

    return {
        "summary": summary,
        "runtime_details": runtime_dict,
        "timestamp": datetime.now(MNL_TZ).isoformat()
    }

@router.get("/summary")
async def get_runtime_summary():
    runtime_data = runtime_tracker.get_runtime()

    if not runtime_data:
        raise HTTPException(status_code=404, detail="No runtime data available.")

    current_time = datetime.now(MNL_TZ)
    elapsed_time = (current_time - runtime_data.app_start_time).total_seconds()

    return {
        "app_start_time": runtime_data.app_start_time.isoformat(),
        "app_end_time": runtime_data.app_end_time.isoformat() if runtime_data.app_end_time else None,
        "elapsed_time_seconds": elapsed_time,
        "total_duration_seconds": runtime_data.total_duration_seconds,
        "total_errors": runtime_data.total_errors,
        "status": "completed" if runtime_data.app_end_time else "running",
        "routes_stats": {
            r.route: r.status.value for r in runtime_data.routes_execution
        }
    }

@router.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(MNL_TZ).isoformat()
    }