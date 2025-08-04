from api.schemas.response import LiveAgentAPIResponse, ResponseStatus
from config.constants import BASE_URL, THROTTLE_DELAY
from typing import Dict, List, Optional, Any
import asyncio
import aiohttp
import logging

# TO DO:
# 1. Custom Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class LiveAgentClient:
    """Live Agent base client/class."""
    def __init__(self, api_key: str, session: aiohttp.ClientSession, max_concurrent_requests: int = 3):
        if not api_key:
            raise ValueError("API key cannot be empty.")

        self.api_key = api_key
        self.session = session
        self.base_url = BASE_URL
        self.headers = self.default_headers()
        self.throttle_delay = THROTTLE_DELAY
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.logger = logging.getLogger(__name__)

    def default_headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "apiKey": self.api_key
        }

    async def _make_throttled_request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        **kwargs
    ) -> aiohttp.ClientResponse:
        """Request with respect to LiveAgent API rate limit. The API rate limit for LiveAgent API v3 is 180 requests per minute."""
        async with self.semaphore:
            if self.throttle_delay > 0:
                await asyncio.sleep(self.throttle_delay)

            return await session.request(method, url, **kwargs)
    
    async def _handle_response(
        self,
        response: aiohttp.ClientResponse,
        endpoint: str
    ) -> LiveAgentAPIResponse:
        """API response parser and handler."""
        try:
            if response.content_type == "application/json":
                data = await response.json()
            else:
                text = await response.text()
                data = {"message": text} if text else {"message": "Empty response"}

            if 200 <= response.status < 300:
                return LiveAgentAPIResponse(
                    success=True,
                    data=data,
                    status_code=response.status,
                    status=ResponseStatus.SUCCESS
                )
            else:
                error_msg = data.get("message", f"HTTP {response.status}") if isinstance(data, dict) else str(data)
                self.logger.warning(f"API error for {endpoint}: {response.status} - {error_msg}")
                return LiveAgentAPIResponse(
                    success=False,
                    data=error_msg,
                    status_code=response.status,
                    status=ResponseStatus.ERROR
                )
        except aiohttp.ContentTypeError as e:
            error_msg = f"Invalid response from {endpoint}"
            self.logger.error(error_msg)
            return LiveAgentAPIResponse(
                success=False,
                data=error_msg,
                status_code=response.status,
                status=ResponseStatus.ERROR
            )
        except Exception as e:
            pass

    async def make_request(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
    ) -> LiveAgentAPIResponse:
        endpoint = endpoint.lstrip("/")
        url = f"{self.base_url}/{endpoint}"

        try:
            self.logger.info(f"Making {method} request to: {url}")

            request_kwargs = {
                "headers": self.headers,
                "params": params
            }

            async with await self._make_throttled_request(
                session, method, url, **request_kwargs
            ) as response:
                return await self._handle_response(response, endpoint)
        except aiohttp.ClientError as e:
            error_msg = f"Client error for {endpoint}: {str(e)}"
            self.logger.error(error_msg)
            return LiveAgentAPIResponse(
                success=False,
                error=error_msg,
                status=ResponseStatus.ERROR
            )
        except asyncio.TimeoutError:
            error_msg = f"Request to {endpoint} timed out"
            self.logger.error(error_msg)
            return LiveAgentAPIResponse(
                success=True,
                error=error_msg,
                status=ResponseStatus.TIMEOUT
            )
        except Exception as e:
            error_msg = f"Exception occurred for {endpoint}: {str(e)}"
            self.logger.error(error_msg)
            return LiveAgentAPIResponse(
                success=False,
                error=error_msg,
                status=ResponseStatus.ERROR
            )

    async def paginate(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        max_pages: int = 5
    ) -> List[Dict[str, Any]]:
        """Generic pagination utility for any LiveAgent endpoint."""
        all_data = []
        page = 1

        if payload is None:
            payload = {}

        while page <= max_pages:
            payload["_page"] = page
            self.logger.info(f"Fetching page {page} from {endpoint}")
            try:
                response = await self.make_request(
                    session=session,
                    endpoint=endpoint,
                    params=payload
                )

                if not response.success or not response.data:
                    self.logger.warning(f"No data returned or request failed at page {page}.")
                    break

                if isinstance(response.data, list):
                    items = response.data
                elif isinstance(response.data, dict) and "data" in response.data:
                    items = response.data["data"]
                else:
                    self.logger.warning(f"Unexpected data structure at page {page}.")
                    break

                if not items:
                    self.logger.info(f"No items on page {page}, stopping pagination.")
                    break

                all_data.extend(items)
                page += 1

            except Exception as e:
                self.logger.info(f"Error during pagination at page {page}: {e}")
                break

        return all_data