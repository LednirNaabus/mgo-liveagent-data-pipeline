from typing import List, Dict, Any, Tuple, Optional, Type, Union
from core.LiveAgentClient import LiveAgentClient
from core.Ticket import Ticket
from core.Agent import Agent
from core.User import User
import asyncio

ClientType = Union[Type[Ticket], Type[Agent], Type[User]]
InstanceType = Union[Ticket, Agent, User]

class Extractor:
    def __init__(self, api_key: str, max_page: int = None, per_page: int = None):
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

    async def create_clients(self, *resource_classes: ClientType) -> Tuple[Optional[LiveAgentClient], List[InstanceType]]:
        try:
            la_client = LiveAgentClient(self.api_key)
            await la_client.__aenter__()

            if not await self.check_connection(la_client):
                await la_client.__aexit__(None, None, None)
                return None, None

            resources = [resource_class(la_client) for resource_class in resource_classes]
            return la_client, resources
        except Exception as e:
            print(f"Failed to initialize LiveAgent client: {e}")
            return None, None

    async def extract_tickets(self) -> Optional[List[Dict[str, Any]]]:
        client, [ticket_client] = await self.create_clients(Ticket)
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

    # async def extract_ticket_messages(self) -> Optional[List[Dict[str, Any]]]:
    #     client, [ticket_client] = await self.create_clients(Ticket)
    #     if not client or not ticket_client:
    #         return None
        
    #     try:
    #         tickets = await ticket_client.fetch_tickets(self.max_page, self.per_page)
    #         ticket_ids = [ticket["id"] for ticket in tickets]
    #         print(f"Found {len(ticket_ids)} characters!")
    #         messages = await self._fetch_all_ticket_messages(ticket_client, ticket_ids)
    #         return messages
    #     except Exception as e:
    #         print(f"Exception occurred while extracting ticket messages from LiveAgent API: '{e}'")
    #         return None
    #     finally:
    #         await client.__aexit__(None, None, None)
    async def extract_ticket_messages(self) -> Optional[List[Dict[str, Any]]]:
        client, [ticket_client, user_client] = await self.create_clients(Ticket, User)
        if not all([client, ticket_client, user_client]):
            return None

        ticket_client._set_user_client(user_client)

        try:
            tickets = await ticket_client.fetch_tickets(self.max_page, self.per_page)
            ticket_contexts = ticket_client._get_ticket_context(tickets)

            parallel_fetch = await ticket_client.fetch_messages_by_ticket_contexts(
                ticket_contexts=ticket_contexts,
                max_pages=self.max_page,
                per_page=self.per_page
            )

            all_processed_messages = []
            for ticket_id, owner_name, agent_id, messages in parallel_fetch:
                agent_name = ticket_client.agents_cache.get(agent_id, "Unknown Agent")
                for msg_grp in messages:
                    msg_grp_data = ticket_client._process_message_group(
                        msg_grp, ticket_id, owner_name, agent_id, agent_name
                    )
                    nested_msgs = ticket_client._process_nested_messages(
                        msg_grp, msg_grp_data, owner_name, agent_id
                    )

                    if nested_msgs:
                        all_processed_messages.extend(nested_msgs)
                    else:
                        all_processed_messages.append(msg_grp_data)
            return all_processed_messages
        except Exception as e:
            print(f"Exception occurred while extracting ticket messages from LiveAgent API: '{e}'")
            return None
        finally:
            await client.__aexit__(None, None, None)

    async def extract_agents(self) -> Optional[List[Dict[str, Any]]]:
        client, [agent_client] = await self.create_clients(Agent)
        if not client or not agent_client:
            return None

        try:
            agents = await agent_client.get_agents(self.max_page, self.per_page)
            return agents
        except Exception as e:
            print(f"Exception occurred while extracting agents from LiveAgent API: '{e}'")
            return None
        finally:
            await client.__aexit__(None, None, None)

    async def extract_users(self) -> Optional[List[Dict[str, Any]]]:
        client, [user_client] = await self.create_clients(User)
        if not client or not user_client:
            return None

        try:
            # users = await user_client.get_user()
            pass
        except Exception as e:
            print(f"Exception occurred while fetching users from LiveAgent API: '{e}'")
            return None
        finally:
            await client.__aexit__(None, None, None)

    async def _fetch_raw_ticket_messages(self, client: Ticket, ticket_ids: List[str]) -> List[Dict[str, Any]]:
        """**For development only**. Fetch raw ticket messages from LiveAgent API `/ticket` endpoint."""
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