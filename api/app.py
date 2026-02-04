from contextlib import asynccontextmanager
from fastapi import FastAPI
from api import (
    tickets_router,
    agents_router,
    tags_router,
    settings
)
import aiohttp

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.aiohttp_session = aiohttp.ClientSession()
    app.state.bq_clients = {}
    yield
    await app.state.aiohttp_session.close()


app = FastAPI(
    lifespan=lifespan,
    title=settings.APP_NAME,
    version=settings.APP_VERSION
)

app.include_router(agents_router, prefix="/extract", tags=["agents"])
app.include_router(tags_router, prefix="/extract", tags=["tags"])
app.include_router(tickets_router, prefix="/extract", tags=["tickets"])

@app.get("/")
def root():
    return "Index"