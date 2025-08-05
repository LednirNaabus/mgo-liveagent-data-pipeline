from starlette.middleware.base import BaseHTTPMiddleware
from .Tracker import runtime_tracker
from api.common import Request
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class RuntimeMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, **kwargs):
        super().__init__(app, **kwargs)
        self.tracked_routes = [
            "/extract/process-agents",
            "/extract/process-tags",
            "/extract/process-tickets-and-messages",
            "/extract/process-convo"
        ]

    async def dispatch(self, request: Request, call_next):
        route_path = request.url.path

        print(f"MIDDLEWARE: Processing request to: {route_path} (Method: {request.method})")
        logging.info(f"Middleware processing request to: {route_path}")
        logging.info(f"Tracked routes: {self.tracked_routes}")
        logging.info(f"Is tracked route: {route_path in self.tracked_routes}")

        if route_path in self.tracked_routes:
            logging.info(f"Starting tracking for route: {route_path}")
            runtime_tracker.start_route(route_path)
            
            try:
                response = await call_next(request)
                logging.info(f"Route {route_path} completed successfully, status code: {response.status_code}")
                runtime_tracker.complete_route(route_path)
                return response
                
            except Exception as e:
                logging.info(f"Route {route_path} failed with exception: {e}")
                runtime_tracker.fail_route(route_path, e)
                raise
        else:
            response = await call_next(request)
            return response