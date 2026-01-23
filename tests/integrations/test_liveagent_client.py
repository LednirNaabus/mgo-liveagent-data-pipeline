import asyncio

import aiohttp
from aiohttp import web
import pytest

from integrations.liveagent.LiveAgentClient import LiveAgentClient


@pytest.mark.asyncio
@pytest.mark.integration
async def test_paginate_collects_multiple_pages():
    async def handler(request):
        page = int(request.query.get("_page", "1"))
        if page <= 2:
            return web.json_response([{"page": page, "value": f"item_{page}"}])
        return web.json_response([])

    app = web.Application()
    app.router.add_get("/tickets", handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]

    session = aiohttp.ClientSession()
    client = LiveAgentClient(
        api_key="fake",
        session=session,
        base_url=f"http://127.0.0.1:{port}",
        throttle_delay=0,
        max_concurrent_requests=1,
    )

    try:
        items = await client.paginate(
            session=session,
            endpoint="/tickets",
            payload={},
            max_pages=5,
        )

        assert items == [
            {"page": 1, "value": "item_1"},
            {"page": 2, "value": "item_2"},
        ]
    finally:
        await session.close()
        await runner.cleanup()
