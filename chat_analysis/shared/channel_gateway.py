from __future__ import annotations

from threading import Lock
from typing import (
    Protocol,
    Optional,
    List,
    Dict,
    Any
)
import aiohttp

from integrations.liveagent import (
    LiveAgentClient,
    ResponseStatus,
    Agent
)
# ManyChatSession


class ChannelAdapter(Protocol):
    """
    Adapter interface for channel-specific conversation access.
    """
    name: str

    def fetch_messages(self, user_id: str, limit: int) -> List[Dict[str, Any]]:
        ...

    def fetch_agent_data(self, user_id: str) -> Dict[str, Any]:
        ...

    def normalize_messages(
        self,
        messages: List[Dict[str, Any]],
        *,
        include_metadata: bool = True,
        collapse_bot_chunks: bool = False
    ) -> List[Dict[str, Any]]:
        ...


# Different Adapters here
class LiveAgentAdapter:
    name = "liveagent"

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        client: Optional[LiveAgentClient] = None
    ):
        self.api_key = api_key
        self.session = session
        self.client = client
        self.agent_api = Agent(client=self.client)

    async def fetch_messages(self, user_id: str, limit: int) -> List[Dict[str, Any]]:
        return []

    async def fetch_agent_data(self, user_id: str) -> Dict[str, Any]:
        response = await self.agent_api.get_agent_by_id(session=self.session, agent_id=user_id)
        if response and response.status == ResponseStatus.SUCCESS and response.data:
            return response.data[0] if isinstance(response.data, list) else response.data
        return {}

    def normalize_messages(
        self,
        messages: List[Dict[str, Any]],
        *,
        include_metadata: bool = True,
        collapse_bot_chunks: bool = False
    ) -> List[Dict[str, Any]]:
        ...


class ChannelGateway:
    """
    Registry and resolver for channel adapters.
    """
    def __init__(self, adapters: Optional[Dict[str, ChannelAdapter]] = None, default: str = "liveagent")  -> None:
        self._adapters: Dict[str, ChannelAdapter] = {}
        for name, adapter in (adapters or {}).items():
            self.register(name, adapter)
        self._default = default
        
    def register(self, name: str, adapter: ChannelAdapter) -> None:
        if not name:
            return
        self._adapters[name.lower()] = adapter

    def get_adapter(self, name: Optional[str]) -> ChannelAdapter:
        key = (name or self._default or "liveagent").lower()
        return self._adapters.get(key) or self._adapters.get(self._default) or LiveAgentAdapter()

_gateway_lock = Lock()
_gateway: Optional[ChannelGateway] = None

def build_channel_gateway() -> ChannelGateway:
    adapters = {
        "liveagent": LiveAgentAdapter()
    }
    return ChannelGateway(adapters=adapters, defualt="liveagent")

def get_channel_gateway() -> ChannelGateway:
    global _gateway
    if _gateway is None:
        with _gateway_lock:
            if _gateway is None:
                _gateway = build_channel_gateway()
    return _gateway