from api import (
    ticket_router,
    agent_router
)
from fastapi import FastAPI

app = FastAPI(title="Mechanigo LiveAgent Data Pipeline", version="0.1.0")
app.include_router(ticket_router, prefix="/extract", tags=["ticket"])
app.include_router(agent_router, prefix="/extract", tags=["agent"])

@app.get("/")
def root():
    return {
        "message": "Hello, World!"
    }