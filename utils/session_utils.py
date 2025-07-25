from fastapi import Request
import aiohttp

def get_aiohttp_session(request: Request) -> aiohttp.ClientSession:
    return request.app.state.aiohttp_session