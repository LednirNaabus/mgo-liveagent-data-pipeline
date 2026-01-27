import pytest

from integrations.liveagent.LiveAgentClient import LiveAgentClient
from integrations.liveagent.models import ResponseStatus


class DummyResponse:
    def __init__(self, status=200, payload=None, content_type="application/json"):
        self.status = status
        self._payload = payload or {}
        self.content_type = content_type

    async def json(self):
        return self._payload

    async def text(self):
        return str(self._payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # pragma: no cover - minimal helper
        return False


@pytest.mark.asyncio
async def test_make_request_success(monkeypatch):
    client = LiveAgentClient(api_key="key", session=None, throttle_delay=0)

    async def fake_throttled_request(*args, **kwargs):
        return DummyResponse(status=200, payload={"ok": True})

    monkeypatch.setattr(client, "_make_throttled_request", fake_throttled_request)

    response = await client.make_request(session=None, endpoint="ping")

    assert response.success is True
    assert response.status == ResponseStatus.SUCCESS
    assert response.status_code == 200
    assert response.data == {"ok": True}


@pytest.mark.asyncio
async def test_make_request_handles_http_error(monkeypatch):
    client = LiveAgentClient(api_key="key", session=None, throttle_delay=0)

    async def fake_throttled_request(*args, **kwargs):
        return DummyResponse(status=500, payload={"message": "boom"})

    monkeypatch.setattr(client, "_make_throttled_request", fake_throttled_request)

    response = await client.make_request(session=None, endpoint="agents")

    assert response.success is False
    assert response.status == ResponseStatus.ERROR
    assert response.status_code == 500
    assert response.data == "boom"
