"""Base client class for energy data APIs.

Provides common functionality:
- Async HTTP client with connection pooling
- Retry logic with exponential backoff
- Rate limiting
- Logging and audit trail
"""

import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Base exception for API errors."""

    def __init__(self, message: str, status_code: int | None = None, response: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class RateLimitError(APIError):
    """Raised when API rate limit is exceeded."""

    pass


class BaseClient(ABC):
    """Base class for energy data API clients.

    Provides:
    - Async HTTP client with connection pooling
    - Automatic retries with exponential backoff
    - Request logging for audit trail
    - Common error handling
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        """Initialize the base client.

        Args:
            base_url: Base URL for the API
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "BaseClient":
        """Enter async context manager."""
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit async context manager."""
        await self.close()

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Ensure HTTP client is initialized."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                headers=self._get_default_headers(),
            )
        return self._client

    def _get_default_headers(self) -> dict[str, str]:
        """Get default headers for requests."""
        return {
            "Accept": "application/json",
            "User-Agent": "EnergyOracle/0.1.0 (https://energyoracle.io)",
        }

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make an HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Query parameters
            **kwargs: Additional arguments passed to httpx

        Returns:
            JSON response as dictionary

        Raises:
            APIError: If the request fails
            RateLimitError: If rate limit is exceeded
        """
        client = await self._ensure_client()
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}/{endpoint.lstrip('/')}"

        # Log request for audit trail
        request_time = datetime.now(UTC)
        logger.info(f"API Request: {method} {url} params={params}")

        try:
            response = await client.request(method, endpoint, params=params, **kwargs)

            # Log response
            logger.info(
                f"API Response: {response.status_code} in "
                f"{(datetime.now(UTC) - request_time).total_seconds():.2f}s"
            )

            if response.status_code == 429:
                raise RateLimitError(
                    "Rate limit exceeded",
                    status_code=429,
                    response=response.text,
                )

            if response.status_code >= 400:
                raise APIError(
                    f"API error: {response.status_code}",
                    status_code=response.status_code,
                    response=response.text,
                )

            return response.json()

        except httpx.TimeoutException as e:
            logger.error(f"Request timeout: {url}")
            raise APIError(f"Request timeout: {e}") from e
        except httpx.NetworkError as e:
            logger.error(f"Network error: {url} - {e}")
            raise APIError(f"Network error: {e}") from e

    async def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make a GET request.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            **kwargs: Additional arguments

        Returns:
            JSON response as dictionary
        """
        return await self._request("GET", endpoint, params=params, **kwargs)

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the API is healthy and accessible.

        Returns:
            True if API is accessible, False otherwise
        """
        pass
