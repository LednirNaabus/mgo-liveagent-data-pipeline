from config.constants import BASE_URL, THROTTLE_DELAY
from typing import Dict, Tuple, Any, Optional, List
import asyncio
import aiohttp

# TO DO:
# 1. Create logging

class LiveAgentClient:
    """Live Agent base client/class."""
    def __init__(self, api_key: str, max_concurrent_requests: int = 10):
        self.api_key = api_key
        self.base_url = BASE_URL
        self.sem = asyncio.Semaphore(max_concurrent_requests)
        self.throttle_delay = THROTTLE_DELAY
        self.session: Optional[aiohttp.ClientSession] = None

    def default_headers(self):
        return {
            'accept': 'application/json',
            'apikey': self.api_key
        }

    async def __aenter__(self):
        """
        Enter the asynchronous context for the LiveAgent API client.

        Returns:
            `self`: The instance of the LiveAgent API client with an active session.
        """
        await self.start_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the asynchronous context for the LiveAgent API client.

        Args:
            `exc_type` (Type[BaseException] | None): The exception type (if any).

            `exc_val` (BaseException | None): The exception instance (if any).

            `exc_tb` (TracebackType | None): The traceback (if any).

        Returns:
            `bool`: False to propagate the exception (if one occurred), otherwise None.
        """
        if exc_type is not None:
            print(f"Exception in LiveAgentClient context: {exc_type.__name__}: {exc_val}")
        await self.close_session()
        return False

    async def start_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def ping(self) -> Tuple[bool, Dict[str, Any]]:
        if self.session is None:
            await self.start_session()
        try:
            async with self.sem:
                async with self.session.get(
                    url=f"{self.base_url}/ping",
                    headers=self.default_headers()
                ) as response:
                    status_ok = response.status == 200
                    try:
                        response_json = await response.json()
                    except aiohttp.ContentTypeError:
                        response_json = {
                            "message": "Non-JSON response"
                        }
                    return status_ok, response_json
        except aiohttp.ClientError as e:
            print(f"ClientError: {e}")
            return False, {
                "error": str(e)
            }
        except Exception as e:
            print(f"Exception occurrred while making a request to '{self.base_url}/ping': {e}")
            return False, {
                "error": str(e)
            }
    
    async def make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        full_url = f"{self.base_url}/{endpoint}"
        print(f"Making request to: {full_url}")
        try:
            async with self.sem:
                async with self.session.get(
                    url=full_url,
                    headers=self.default_headers(),
                    params=params
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return data
        except aiohttp.ClientError as e:
            print(f"ClientError: {e}")
            return None
        except Exception as e:
            print(f"Exception occurred while making request to '{full_url}': {e}")
            return None

    async def paginate(self, payload: Dict[str, Any] = None, endpoint: str = None, max_pages: int = 5) -> List[Dict[str, Any]]:
        all_data = []
        page = 1
        while page <= max_pages:
            payload["_page"] = page
            print(f"Fetching page {page} from {endpoint}")
            try:
                data = await self.make_request(
                    endpoint,
                    params=payload
                )
                if not data:
                    print(f"No data returned for page {page}, stopping pagination.")
                    break
                if isinstance(data, dict) and 'data' in data:
                    items = data['data']
                else:
                    items = data if isinstance(data, list) else []
                if not items:
                    print(f"No items in page {page}, stopping pagination.")
                    break
                all_data.extend(items)
                page += 1
            except Exception as e:
                print(f"Exception occurred while fetching ticket page: {page}: {e}")
                break
        return all_data