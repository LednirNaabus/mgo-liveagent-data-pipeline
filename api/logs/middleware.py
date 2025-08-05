from .Tracker import runtime_tracker
from api.common import Request

class RuntimeMiddleware:
    def __init__(self, app):
        self.app = app
        self.tracked_routes = [
            "/extract/process-agents",
            "/extract/process-tags",
            "/extract/process-tickets-and-messages",
            "/extract/process-convo"
        ]

    async def __call__(self, request: Request, call_next):
        route_path = request.url.path

        if route_path in self.tracked_routes:
            runtime_tracker.start_route(route_path)
            
            try:
                response = await call_next(request)
                runtime_tracker.complete_route(route_path)
                return response
                
            except Exception as e:
                runtime_tracker.fail_route(route_path, e)
                raise
        else:
            response = await call_next(request)
            return response