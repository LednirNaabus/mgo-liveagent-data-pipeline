import pandas as pd
from typing import Optional, Union
from chat_analysis.shared.channel_gateway import ChannelGateway
from integrations.liveagent import ExtractionResponse, ResponseStatus
from integrations.liveagent.utils import FilterField, process_tickets

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
            count=len(agents),
            data=agents
        )

    async def fetch_tickets(
        self,
        per_page: int,
        max_pages: int,
        filter_field: Union[str, FilterField] = FilterField.DATE_CHANGED,
    ) -> ExtractionResponse:
        raw = await self._adapter.fetch_tickets(
            filter_field=filter_field,
            max_pages=max_pages,
            per_page=per_page
        )

        processed_tickets = process_tickets(raw)
        data = (
            processed_tickets.where(pd.notnull(processed_tickets), None)
            .to_dict(orient="records")
        )
        return ExtractionResponse(
            status=ResponseStatus.SUCCESS,
            count=len(data),
            data=data
        )
        
    # for conversation data
    async def fetch_conversation(self, per_page: int, max_pages: int) -> ExtractionResponse:
        messages = await self._adapter.fetch_messages(
            ticket_id=self.ticket_id,
            user_id=self.user_id,
            per_page=per_page,
            max_pages=max_pages
        )

        # print("========== FROM CONVODATA ==========")
        # print(f"messages: {messages}")

        return ExtractionResponse(
            status=ResponseStatus.SUCCESS,
            count=len(messages),
            data=messages
        )

    async def fetch_tags(self) -> ExtractionResponse:
        tags = await self._adapter.fetch_tags()
        return ExtractionResponse(
            status=ResponseStatus.SUCCESS,
            count=len(tags),
            data=tags
        )