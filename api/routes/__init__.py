from api.routes.tickets import router as tickets_router
from api.routes.agents import router as agents_router
from api.routes.tags import router as tags_router

__all__ = [
    "tickets_router",
    "agents_router",
    "tags_router"
]