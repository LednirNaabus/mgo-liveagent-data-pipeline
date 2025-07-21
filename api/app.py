from api import (extract_router)
from fastapi import FastAPI

app = FastAPI(title="Mechanigo LiveAgent Data Pipeline", version="0.1.0")
app.include_router(extract_router, prefix="/extract", tags=["extract"])

@app.get("/")
def root():
    return {
        "message": "Hello, World!"
    }