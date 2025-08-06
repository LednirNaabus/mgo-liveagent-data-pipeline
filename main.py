import uvicorn
import os

if __name__ == "__main__":
    uvicorn.run("api.app:app", host="0.0.0.0", port=int(os.environ.get('PORT', 8080)), reload=True)