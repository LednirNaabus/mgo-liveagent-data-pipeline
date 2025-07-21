from typing import List, Dict, Any, Tuple, Optional
from core.LiveAgentClient import LiveAgentClient
from core.Ticket import Ticket
import asyncio

class Extractor:
    def __init__(self, api_key: str, max_page: int, per_page: int):
        self.api_key = api_key
        self.max_page = max_page
        self.per_page = per_page

    async def check_connection(self, client: LiveAgentClient) -> bool:
        success, response = await client.ping()
        if not success:
            print(f"Ping to LiveAgent API failed: {response}")
            return False
        print(f"Connection to '{client.base_url}/ping' successful!")
        return True

    async def create_clients(self) -> Tuple[Optional[LiveAgentClient], Optional[Ticket]]:
        try:
            la_client = LiveAgentClient(self.api_key)
            await la_client.__aenter__()

            if not await self.check_connection(la_client):
                await la_client.__aexit__(None, None, None)
                return None, None

            ticket_client = Ticket(la_client)
            return la_client, ticket_client
        except Exception as e:
            print(f"Failed to initialize LiveAgent client: {e}")
            return None, None

    async def extract_tickets(self) -> Optional[List[Dict[str, Any]]]:
        client, ticket_client = await self.create_clients()
        if not client or not ticket_client:
            return None
        
        try:
            tickets = await ticket_client.fetch_tickets(self.max_page, self.per_page)
            return tickets
        except Exception as e:
            print(f"Exception occurred while extracting tickets from LiveAgent API: '{e}'")
            return None
        finally:
            await client.__aexit__(None, None, None)

    async def extract_ticket_messages(self) -> Optional[List[Dict[str, Any]]]:
        client, ticket_client = await self.create_clients()
        if not client or not ticket_client:
            return None
        
        try:
            tickets = await ticket_client.fetch_tickets(self.max_page, self.per_page)
            ticket_ids = [ticket["id"] for ticket in tickets]
            print(f"Found {len(ticket_ids)} characters!")
            messages = await self._fetch_all(ticket_client, ticket_ids)
            return messages
        except Exception as e:
            print(f"Exception occurred while extracting ticket messages from LiveAgent API: '{e}'")
            return None
        finally:
            await client.__aexit__(None, None, None)

    async def _fetch_all(self, client: Ticket, ticket_ids: List[str]) -> List[Dict[str, Any]]:
        tasks = [
            client.fetch_ticket_messages(
                ticket_id=ticket_id,
                max_pages=self.max_page,
                per_page=self.per_page
            )
            for ticket_id in ticket_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Error processing ticket {ticket_ids[i]}: {result}")
            else:
                valid_results.append(result)
        return valid_results