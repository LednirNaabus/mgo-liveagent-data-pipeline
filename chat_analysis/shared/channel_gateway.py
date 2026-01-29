from __future__ import annotations

from threading import Lock
from typing import (
    Protocol,
    Optional,
    Union,
    List,
    Dict,
    Any
)
import pandas as pd
import aiohttp

from integrations.liveagent import (
    TicketMessageProcessor,
    LiveAgentClient,
    ResponseStatus,
    FilterField,
    set_ticket_filter,
    Ticket,
    Agent,
    Tag
)

from integrations.bigquery import BigQueryUtils
from configs.config import ENV
# ManyChatSession


class ChannelAdapter(Protocol):
    """
    Adapter interface for channel-specific conversation access.
    """
    name: str

    async def fetch_tickets(self, date: pd.Timestamp, filter_field: FilterField, max_pages: int, per_page: int) -> List[Dict[str, Any]]:
        ...

    async def fetch_messages(self, ticket_id: str, user_id: str, per_page: int, max_pages: int) -> List[Dict[str, Any]]:
        ...

    async def fetch_agents(self, per_page: int, max_pages: int) -> List[Dict[str, Any]]:
        ...

    async def fetch_tags(self) -> List[Dict[str, Any]]:
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
    """
    The previous version of `LiveAgentAdapter` was loosely coupled for components
    such as `client`, `agent_api`, `message_processor`, `ticket_api`, and `tag_api` to simplify
    unit testing.

    The current design intentionally instantiates these dependencies internally.
    This adapter is tightly coupled by design, since all related classes (`Agent`, `Ticket`, `Tag`, etc.) are
    specific to and strictly interact with the LiveAgent API via `LiveAgentClient`.

    This design also keeps `build_channel_gateway()` and
    `get_channel_gateway()` intentionally minimal and predictable.
    Both functions are meant to strictly accept only:
      - `api_key`
      - `aiohttp.ClientSession`
      - an optional `BigQueryUtils` instance
    """
    name = "liveagent"

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        bq_client: Optional[BigQueryUtils] = None
    ):
        self.api_key = api_key
        self.session = session
        self.bq_client = bq_client
        self.client = LiveAgentClient(api_key=self.api_key, session=self.session)
        self.agent_api = Agent(client=self.client)
        self.message_processor = TicketMessageProcessor(client=self.client, bq_client=self.bq_client)
        self.ticket_api = Ticket(client=self.client, message_processor=self.message_processor)
        self.tag_api = Tag(client=self.client)

    async def fetch_tickets(
        self,
        date: pd.Timestamp,
        filter_field: Union[str, FilterField] = FilterField.DATE_CHANGED,
        per_page: int = 5,
        max_pages: int = 10
    ) -> List[Dict[str, Any]]:
        if isinstance(date, str):
            date = pd.Timestamp(date)

        filters = set_ticket_filter(date, filter_field)
        ticket_payload = {
            "_perPage": per_page,
            "_filters": filters
        }

        if filter_field == FilterField.DATE_CHANGED:
            ticket_payload["_sortDir"] = "ASC"
        try:
            raw = await self.ticket_api.fetch_tickets(
                session=self.session,
                payload=ticket_payload,
                max_pages=max_pages,
                per_page=per_page
            )

            if raw.empty:
                return []
            return raw.to_dict(orient="records")
        except Exception as e:
            print(f"Exception occurred: {e}")
            return None

    async def fetch_messages(
        self,
        ticket_id: str,
        user_id: str,
        per_page: int,
        max_pages: int
    ) -> List[Dict[str, Any]]:
        # TODO:
        # - internal calls to LiveAgent API /ticket to extract the ff:
        # - ticket_ids, ticket_agentids, ticket_owner_names
        return await self.ticket_api.fetch_ticket_message(
            ticket_id=ticket_id,
            ticket_agent_id=user_id,
            ticket_owner_name=None,
            max_page=max_pages,
            per_page=per_page,
            session=self.session
        )

    async def fetch_agents(self, per_page: int, max_pages: int) -> List[Dict[str, Any]]:
        response = await self.agent_api.get_agents(session=self.session, max_page=max_pages, per_page=per_page)
        if response and response.status == ResponseStatus.SUCCESS and response.data:
            return response.data
        return []

    async def fetch_tags(self) -> List[Dict[str, Any]]:
        response = await self.tag_api.get_tags(session=self.session)
        if response and response.status == ResponseStatus.SUCCESS and response.data:
            return response.data
        return []

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
        adapter = self._adapters.get(key) or self._adapters.get(self._default)
        if adapter is None:
            raise ValueError(f"No adapter registered for '{key}' and no default adapter available.")
        return adapter

_gateway_lock = Lock()
_gateway: Optional[ChannelGateway] = None

def build_channel_gateway(
    api_key: str,
    session: aiohttp.ClientSession,
    bq_client: BigQueryUtils = None
) -> ChannelGateway:
    adapters = {
        "liveagent": LiveAgentAdapter(
            api_key=api_key,
            session=session,
            bq_client=bq_client
        )
    }
    return ChannelGateway(adapters=adapters, default="liveagent")

def get_channel_gateway(
    session: aiohttp.ClientSession,
    api_key: Optional[str] = None,
    bq_client: BigQueryUtils = None
) -> ChannelGateway:
    if api_key is None:
        api_key = ENV.get("LIVEAGENT_API_KEY")
    global _gateway
    if _gateway is None:
        with _gateway_lock:
            if _gateway is None:
                _gateway = build_channel_gateway(
                    session=session,
                    api_key=api_key,
                    bq_client=bq_client
                )
    return _gateway