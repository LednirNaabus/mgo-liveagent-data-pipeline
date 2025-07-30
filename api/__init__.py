from api.routes.tickets import router as ticket_router
from api.routes.agents import router as agent_router

__all__ = [
    "ticket_router",
    "agent_router"
]