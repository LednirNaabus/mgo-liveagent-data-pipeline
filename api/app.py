from api import (
    ticket_router,
    agent_router,
    tag_router
)
from contextlib import asynccontextmanager
from config.constants import APP_VERSION
from fastapi import FastAPI
import aiohttp
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Shared session
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Creating aiohttp session...")
    logging.info("Starting app...")
    app.state.aiohttp_session = aiohttp.ClientSession()
    yield
    logging.info("Closing aiohttp session...")
    logging.info("Closing app...")
    await app.state.aiohttp_session.close()

app = FastAPI(
    lifespan=lifespan,
    title="MechaniGo LiveAgent Data Pipeline",
    version=APP_VERSION
)
app.include_router(ticket_router, prefix="/extract", tags=["ticket"])
app.include_router(agent_router, prefix="/extract", tags=["agents"])
app.include_router(tag_router, prefix="/extract", tags=["tags"])


@app.get("/")
def root():
    return "Hello, World!"