from .models import AppRuntime, RouteStatus
from datetime import datetime, timezone
from config.config import MNL_TZ
from typing import Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class RuntimeTracker:
    def __init__(self):
        self._runtime: Optional[AppRuntime] = None

    def initialize(self):
        self._runtime = AppRuntime(app_start_time=datetime.now(MNL_TZ))
        logging.info(f"Runtime tracking initialized at {self._runtime.app_start_time}")

    def get_runtime(self) -> Optional[AppRuntime]:
        return self._runtime

    def start_route(self, route_path: str):
        if not self._runtime:
            return

        route_exec = next((r for r in self._runtime.routes_execution if r.route == route_path), None)
        if route_exec:
            route_exec.status = RouteStatus.IN_PROGRESS
            route_exec.start_time = datetime.now(MNL_TZ)
            logging.info(f"Started execution of {route_path} at {route_exec.start_time}")

    def complete_route(self, route_path: str):
        if not self._runtime:
            return
        
        route_exec = next((r for r in self._runtime.routes_execution if r.route == route_path), None)
        if route_exec:
            if route_exec.start_time:
                route_exec.end_time = datetime.now(MNL_TZ)
                route_exec.duration_seconds = (route_exec.end_time - route_exec.start_time).total_seconds()
                route_exec.status = RouteStatus.COMPLETED
                logging.info(f"Completed execution of {route_path} in {route_exec.duration_seconds:.2f} seconds")

                if route_path == "/extract/convo-analysis":
                    self._runtime.app_end_time = datetime.now(MNL_TZ)
                    self._runtime.total_duration_seconds = (
                        self._runtime.app_end_time - self._runtime.app_start_time
                    ).total_seconds()
                    logging.info(f"App execution completed. Total runtime: {self._runtime.total_duration_seconds:.2f} seconds")
            else:
                logging.warning(f"Route {route_path} was not properly started!")
        else:
            logging.warning(f"Route {route_path} not found in tracked routes")

    def fail_route(self, route_path: str, error: Exception):
        if not self._runtime:
            return
        
        route_exec = next((r for r in self._runtime.routes_execution if r.route == route_path), None)
        if route_exec and route_exec.start_time:
            route_exec.end_time = datetime.now(MNL_TZ)
            route_exec.duration_seconds = (route_exec.end_time - route_exec.start_time).total_seconds()
            route_exec.status = RouteStatus.FAILED
            route_exec.error_message = str(error)
            route_exec.error_details = {
                "type": type(error).__name__,
                "args": error.args if hasattr(error, 'args') else None
            }
            self._runtime.total_errors += 1

            logging.error(f"Failed execution of {route_path} after {route_exec.duration_seconds:.2f} seconds")

# Global tracker instance
runtime_tracker = RuntimeTracker()