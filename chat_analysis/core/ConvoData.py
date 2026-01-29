from typing import Optional, Dict, List
from chat_analysis.shared.channel_gateway import ChannelGateway
from integrations.liveagent import ExtractionResponse, ResponseStatus

class ConvoData:
    def __init__(
        self,
        ticket_id: Optional[str] = None,
        user_id: Optional[str] = None,
        platform: str = "liveagent",
        *,
        gateway: Optional[ChannelGateway] = None
    ):
        self.ticket_id = ticket_id
        self.user_id = user_id
        self.platform = platform.lower() if isinstance(platform, str) else "liveagent"
        self._gateway = gateway
        self._adapter = self._gateway.get_adapter(name=self.platform)

    async def fetch_agents(self, per_page: int, max_pages: int) -> ExtractionResponse:
        agents = await self._adapter.fetch_agents(per_page, max_pages)
        return ExtractionResponse(
            status=ResponseStatus.SUCCESS,
            count=str(len(agents)),
            data=agents
        )

    # for conversation data
    async def fetch_conversation(self, per_page: int, max_pages: int) -> List:
        messages = await self._adapter.fetch_messages(
            ticket_id=self.ticket_id,
            user_id=self.user_id,
            per_page=per_page,
            max_pages=max_pages
        )

        return {
            "messages": messages
        }

    async def fetch_tags(self) -> ExtractionResponse:
        tags = await self._adapter.fetch_tags()
        return ExtractionResponse(
            status=ResponseStatus.SUCCESS,
            count=str(len(tags)),
            data=tags
        )