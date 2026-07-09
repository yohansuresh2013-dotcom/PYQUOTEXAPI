"""Async HTTP browser client using httpx for Quotex API communication."""
import logging
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from httpx import Response
from typing_extensions import Self

logger = logging.getLogger("Browser")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
if not logger.handlers:
    logger.addHandler(handler)

from pyquotex.network.ssl_utils import (
    create_ssl_context,
    CIPHER_SUITE_FIREFOX
)

USER_AGENT_DEFAULT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"


def _normalize_cookie_domain(domain: str | None) -> str:
    return (domain or "").lstrip(".").lower()


def _cookie_matches_host(cookie_domain: str | None, host: str | None) -> bool:
    if not host:
        return True
    h = _normalize_cookie_domain(host)
    d = _normalize_cookie_domain(cookie_domain)
    if not d:
        return True
    return h == d or h.endswith("." + d)


def _proxy_from_config(proxies: dict[str, str] | str | None) -> str | None:
    """Return a single httpx proxy URL from either string or requests-style dict."""
    if isinstance(proxies, str):
        return proxies
    if isinstance(proxies, dict):
        return (
            proxies.get("https")
            or proxies.get("https://")
            or proxies.get("http")
            or proxies.get("http://")
            or proxies.get("all")
            or proxies.get("all://")
        )
    return None


class Browser:
    """Async HTTP client wrapping httpx.AsyncClient with TLS, cookies, 
    and proxy support."""

    def __init__(self, *args: Any, **kwargs: Any):
        self.response: httpx.Response | None = None
        self.default_headers: dict[str, str] | None = None
        self.source_address: Any = kwargs.pop('source_address', None)
        self.server_hostname: str | None = kwargs.pop('server_hostname', None)
        self.cookie_domain: str | None = kwargs.pop('cookie_domain', self.server_hostname)
        self.proxies: dict[str, str] | str | None = kwargs.pop('proxies', None)
        self.debug: bool = kwargs.pop('debug', False)

        # Build SSL context with specified cipher suite and ECDH curve
        self._ssl_context = create_ssl_context(cipher_suite=CIPHER_SUITE_FIREFOX)

        if self.server_hostname:
            self._ssl_context.check_hostname = False

        self.headers = self.get_headers()

        # Build httpx.AsyncClient
        self._client = httpx.AsyncClient(
            verify=self._ssl_context,
            timeout=30.0,
            follow_redirects=True,
            proxy=_proxy_from_config(self.proxies),
        )

        if self.debug:
            logger.setLevel(logging.DEBUG)

    def __enter__(self) -> Self:
        return self

    def __exit__(
            self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
            self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying httpx client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def get_headers(self) -> dict[str, str] | None:
        self.default_headers = {
            "User-Agent": USER_AGENT_DEFAULT,
        }
        return self.default_headers

    def set_headers(self, headers: dict[str, str] | None = None) -> None:
        if self.default_headers:
            self.headers.update(self.default_headers)
        if headers:
            self.headers.update(headers)

    def get_cookies(self, host: str | None = None) -> str:
        """Get cookies as a Cookie header, optionally filtered by host.

        Avoids httpx.CookieConflict when duplicate names exist with different
        domain/path values. For duplicate names, the most recent matching
        cookie wins.
        """
        target_host = host or self.cookie_domain
        result: dict[str, str] = {}
        jar = getattr(self._client.cookies, "jar", None)

        if jar is not None:
            for cookie in jar:
                if _cookie_matches_host(getattr(cookie, "domain", None), target_host):
                    result[cookie.name] = cookie.value
            return "; ".join(f"{name}={value}" for name, value in result.items())

        try:
            return "; ".join(
                f"{name}={value}"
                for name, value in self._client.cookies.items()
            )
        except Exception:
            return ""

    def get_soup(self) -> BeautifulSoup:
        """Parse the last response content with BeautifulSoup."""
        if self.response and self.response.status_code >= 400:
            raise RuntimeError(
                f"HTTP {self.response.status_code}: "
                f"{self.response.reason_phrase}"
            )
        return BeautifulSoup(
            self.response.content if self.response else b"",
            "html.parser"
        )

    def get_json(self) -> Any:
        """Parse the last response as JSON."""
        if self.response and self.response.status_code >= 400:
            raise RuntimeError(
                f"HTTP {self.response.status_code}: "
                f"{self.response.reason_phrase}"
            )
        try:
            return self.response.json() if self.response else None
        except Exception:
            return None

    async def send_request(
            self,
            method: str,
            url: str,
            headers: dict[str, str] | None = None,
            **kwargs: Any
    ) -> Response:
        """Send an async HTTP request using httpx.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Target URL
            headers: Optional additional headers
            **kwargs: Additional httpx request arguments (data, JSON,
                      params, etc.)

        Returns:
            httpx.Response object
        """
        merged_headers = dict(self.headers or {})
        if headers:
            merged_headers.update(headers)

        logger.debug("Using proxies: %s", self.proxies)

        self.response = await self._client.request(
            method,
            url,
            headers=merged_headers,
            **kwargs,
        )

        if self.debug:
            logger.debug(f"→ {method} {url}")
            logger.debug(f"Status: {self.response.status_code}")
            logger.debug(f"Headers enviados: {merged_headers}")
            logger.debug(f"Headers recebidos: {dict(self.response.headers)}")
            logger.debug(f"Cookies: {self.get_cookies()}")
            content_preview = (
                self.response.text[:250].strip().replace('\n', '')
            )
            logger.debug(f"Body (preview): {content_preview} [...]")

        return self.response
