from typing import Dict, List, Literal, Optional, Any

from integrations.liveagent import LiveAgentAPIResponse, ResponseStatus
from configs.constants import (
    THROTTLE_DELAY, MAX_CONCURRENT_REQUESTS,
    LIVEAGENT_API_MAX_PAGES,
    BASE_URL
)

import asyncio
import aiohttp
import logging

#TODO:
# logging module

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class LiveAgentClient:
    """LiveAgent base client/class."""
    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        base_url: Optional[str] = None,
        throttle_delay: Optional[float] = None,
        max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS
    ):
        if not api_key:
            raise ValueError("API key cannot be empty.")

        self.api_key = api_key
        self.session = session
        self.base_url = base_url or BASE_URL
        self.headers = self.default_headers()
        self.throttle_delay = throttle_delay or THROTTLE_DELAY
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
        """
        Make request with respect to LiveAgent API rate limit.
        
        :param self: Description
        :param session: Description
        :type session: aiohttp.ClientSession
        :param method: Description
        :type method: str
        :param url: Description
        :type url: str
        :param kwargs: Description
        :return: Description
        :rtype: ClientResponse
        """
        # The API rate limit for LiveAgent API v3 is 180 requests per minute.
        async with self.semaphore:
            if self.throttle_delay > 0:
                await asyncio.sleep(self.throttle_delay)

            return await session.request(method, url, **kwargs)

    async def _handle_response(
        self,
        response: aiohttp.ClientResponse,
        endpoint: str
    ) -> LiveAgentAPIResponse:
        """
        LiveAgent API response parser and handler.
        
        :param self: Description
        :param response: Description
        :type response: aiohttp.ClientResponse
        :param endpoint: Description
        :type endpoint: str
        :return: Description
        :rtype: LiveAgentAPIResponse
        """
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

        except aiohttp.ContentTypeError:
            error_msg = f"Invalid response from {endpoint}"
            self.logger.error(error_msg)
            return LiveAgentAPIResponse(
                success=False,
                error=error_msg,
                status_code=response.status,
                status=ResponseStatus.ERROR
            )

    async def make_request(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        method: Literal["GET", "POST"] = "GET",
        params: Optional[Dict[str, Any]] = None
    ) -> LiveAgentAPIResponse:
        """
        Docstring for make_request
        
        :param self: Description
        :param session: Description
        :type session: aiohttp.ClientSession
        :param endpoint: Description
        :type endpoint: str
        :param method: Description
        :type method: Literal["GET", "POST"]
        :param params: Description
        :type params: Optional[Dict[str, Any]]
        :return: Description
        :rtype: LiveAgentAPIResponse
        """
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
            error_msg = f"Request to {endpoint} timed out."
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
        max_pages: int = LIVEAGENT_API_MAX_PAGES
    ) -> List[Dict[str, Any]]:
        """
        Generic pagination utility for any LiveAgent endpoint.
        
        :param self: Description
        :param session: Description
        :type session: aiohttp.ClientSession
        :param endpoint: Description
        :type endpoint: str
        :param payload: Description
        :type payload: Optional[Dict[str, Any]]
        :param max_pages: Description
        :type max_pages: int
        :return: Description
        :rtype: List[Dict[str, Any]]
        """
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