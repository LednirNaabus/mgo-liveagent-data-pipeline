from api.routes import extract_data
from fastapi import FastAPI

app = FastAPI()
app.include_router(extract_data.router)

@app.get("/")
def root():
    return {
        "message": "Hello, World!"
    }