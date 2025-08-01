from api.routes.conversations import router as conversation_router
from api.routes.tickets import router as ticket_router
from api.routes.tables import router as tables_router
from api.routes.agents import router as agent_router
from api.routes.tags import router as tag_router

__all__ = [
    "conversation_router",
    "ticket_router",
    "tables_router",
    "agent_router",
    "tag_router"
]