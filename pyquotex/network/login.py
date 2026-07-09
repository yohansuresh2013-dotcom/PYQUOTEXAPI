import asyncio
import inspect
import re
import sys
from typing import Any
from urllib.parse import urlparse

from bs4.element import AttributeValueList

from pyquotex._api._waits import backoff_sleep
from pyquotex.config import normalize_host, update_session
from pyquotex.network.navigator import Browser
from pyquotex.utils import json_utils as json

def _cookie_matches_host(cookie_domain: str | None, host: str | None) -> bool:
    if not host:
        return True
    h = (host or "").lstrip(".").lower()
    d = (cookie_domain or "").lstrip(".").lower()
    if not d:
        return True
    return h == d or h.endswith("." + d)


def _safe_cookie_header(cookies, host: str | None = None) -> str:
    """
    Build a Cookie header without httpx.CookieConflict.

    If multiple cookies share a name, prefer the latest cookie that matches
    the selected broker host. This prevents qxbroker.com cookies from leaking
    into market-qx.trade / broker-qx.pro sessions.
    """
    jar = getattr(cookies, "jar", None)
    if jar is None:
        try:
            return "; ".join(f"{k}={v}" for k, v in cookies.items())
        except Exception:
            return ""

    result: dict[str, str] = {}
    for cookie in jar:
        if _cookie_matches_host(getattr(cookie, "domain", None), host):
            result[cookie.name] = cookie.value

    return "; ".join(f"{k}={v}" for k, v in result.items())


def _extract_settings_json(html_text: str) -> dict[str, Any] | None:
    """Extract window.settings JSON from any script tag, tolerating minified pages."""
    if not html_text:
        return None
    marker = "window.settings"
    idx = html_text.find(marker)
    while idx != -1:
        eq = html_text.find("=", idx)
        if eq == -1:
            break
        start = html_text.find("{", eq)
        if start == -1:
            idx = html_text.find(marker, idx + len(marker))
            continue
        depth = 0
        in_str = False
        esc = False
        quote = ""
        for pos in range(start, len(html_text)):
            ch = html_text[pos]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == quote:
                    in_str = False
                continue
            if ch in ('"', "'"):
                in_str = True
                quote = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    raw = html_text[start:pos + 1]
                    try:
                        return json.loads(raw)
                    except Exception:
                        break
        idx = html_text.find(marker, idx + len(marker))
    return None


class Login(Browser):
    """Class for Quotex login resource."""

    url: str = ""
    cookies: str | None = None
    ssid: str | None = None
    base_url: str = 'qxbroker.com'
    https_base_url: str = 'https://qxbroker.com'

    def __init__(self, api: Any, *args: Any, **kwargs: Any):
        super().__init__(cookie_domain=normalize_host(getattr(api, "host", None)), proxies=getattr(api, "proxies", None), *args, **kwargs)
        self.api = api
        self.html: Any = None
        self.headers: dict[str, str] = self.get_headers()
        self.base_url = normalize_host(getattr(api, "host", None))
        self.https_base_url = f"https://{self.base_url}"
        self.full_url: str = f"{self.https_base_url}/{api.lang}"

    async def get_sign_page(self):
        headers = {}
        headers["Connection"] = "keep-alive"
        headers["Accept-Encoding"] = "gzip, deflate, br"
        headers["Accept-Language"] = "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3"
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        headers["Referer"] = self.api.https_url
        headers["Upgrade-Insecure-Requests"] = "1"
        headers["Sec-Ch-Ua-Mobile"] = "?0"
        headers["Sec-Ch-Ua-Platform"] = '"Linux"'
        headers["Sec-Fetch-Site"] = "same-origin"
        headers["Sec-Fetch-User"] = "?1"
        headers["Sec-Fetch-Dest"] = "document"
        headers["Sec-Fetch-Mode"] = "navigate"
        headers["Dnt"] = "1"
        response = await self.send_request(
            method="GET",
            url=self.full_url,
            headers=headers
        )

        if not response.is_success:
            return response

        self.cookies = _safe_cookie_header(response.cookies, self.base_url)
        return response

    async def get_token(self) -> None | str | AttributeValueList:
        self.headers["Cookie"] = self.cookies or ''
        self.headers["Referer"] = f"{self.full_url}/sign-in"
        response = await self.send_request(
            "GET",
            f"{self.full_url}/sign-in/modal/"
        )
        if not response.is_success:
            return None
        html = self.get_soup()
        match = html.find(
            "input", {"name": "_token"}
        )
        token = None if not match else match.get("value")
        return token

    async def awaiting_pin(
            self,
            data: dict[str, Any],
            input_message: str
    ) -> None:
        self.headers["Content-Type"] = "application/x-www-form-urlencoded"
        self.headers["Referer"] = f"{self.full_url}/sign-in/modal"
        data["keep_code"] = 1
        try:
            if self.api.on_otp_callback:
                if inspect.iscoroutinefunction(self.api.on_otp_callback):
                    code = await self.api.on_otp_callback(input_message)
                else:
                    code = self.api.on_otp_callback(input_message)
            else:
                code = input(input_message)

            if not code or not str(code).isdigit():
                print("Please enter a valid code.")
                return await self.awaiting_pin(data, input_message)
            data["code"] = code
        except KeyboardInterrupt:
            print("\nClosing program.")
            sys.exit()

        # TODO: this is a linear settle-delay between OTP entry and POST,
        # not a counted retry. backoff_sleep(0) preserves ~1s pacing today;
        # a proper retry counter should be introduced when this method is
        # refactored to handle transient PIN-submission failures.
        await backoff_sleep(0)
        await self.send_request(
            method="POST",
            url=f"{self.full_url}/sign-in/modal",
            data=data
        )

    async def get_profile(self):
        headers = {
            "Referer": f"{self.api.https_url}/{self.api.lang}/trade",
            "Cookie": self.api.session_data.get("cookies") or ""
        }
        response = await self.send_request(
            method="GET",
            url=f"{self.api.https_url}/api/v1/cabinets/digest",
            headers=headers
        )
        if response.is_success:
            data = response.json()["data"]
            self.api.session_data["token"] = data.get("token")

        return response

    async def get_settings(self) -> tuple[Any, dict[str, Any] | None]:
        html_text = self.response.text if self.response is not None else ""
        settings_data = _extract_settings_json(html_text)
        self.cookies = self.get_cookies(self.base_url)
        if settings_data:
            self.ssid = settings_data.get("token")
            self.api.session_data["cookies"] = self.cookies
            self.api.session_data["token"] = self.ssid
            self.api.session_data["user_agent"] = self.headers.get("User-Agent", "")
            self.api.session_data["host"] = self.base_url
            update_session(self.api.username, self.api.session_data, self.base_url)
            return self.response, settings_data
        return self.response, None

    async def _post(self, data: dict[str, Any]) -> tuple[bool, str]:
        """Send post-request for Quotex API login http resource.
        :returns: The instance of: class:`httpx.Response`.
        """
        self.response = await self.send_request(
            method="POST",
            url=f"{self.full_url}/sign-in/",
            data=data
        )
        required_keep_code = self.get_soup().find(
            "input", {"name": "keep_code"}
        )
        if required_keep_code:
            auth_body = self.get_soup().find(
                "main", {"class": "auth__body"}
            )
            input_message = (
                f'{auth_body.find("p").text}: '
                if auth_body and auth_body.find("p")
                else "Insira o código PIN que acabamos "
                     "de enviar para o seu e-mail: "
            )
            await self.awaiting_pin(data, input_message)
        # TODO: linear settle-delay before reading the post-login redirect,
        # not a counted retry. backoff_sleep(0) preserves ~1s pacing today;
        # a proper retry counter should be introduced if login form
        # submission grows transient-failure handling.
        await backoff_sleep(0)
        success = await self.success_login()
        return success

    async def success_login(self) -> tuple[bool, str]:
        if self.response is None:
            return False, "No response received."

        response_url = str(self.response.url)
        if "trade" in response_url:
            await self.get_settings()
            return True, "Login successful."

        soup = self.get_soup()

        not_available = soup.select_one(
            "#tab-1 > div > div.modal-sign__not-avalible__title"
        )
        if not_available:
            return False, (
                f"Service unavailable: {not_available.get_text(strip=True)}"
            )

        error = soup.select_one("#tab-1 form > div:nth-child(2) > div")
        msg = error.get_text(strip=True) if error else "Unknown error"

        return False, f"Login failed. {msg}"

    async def __call__(
            self,
            username: str,
            password: str,
            user_data_dir: str | None = None
    ) -> tuple[bool, str]:
        """Method to get Quotex API login http request.
        :param str username: The username of a Quotex server.
        :param str password: The password of a Quotex server.
        :param str user_data_dir: The optional value for path userdata.
        :returns: The instance of: class:`httpx.Response`.
        """
        home = await self.get_sign_page()
        reason_msg = 'Access page with SSL RESOLVER'
        if not home.is_success:
            if home.status_code == 429:
                reason_msg = (
                    'HTTP 429 rate limit while opening login page. '
                    'Wait before retrying; do not loop login/connect.'
                )
            else:
                reason_msg = f'Error on access page: HTTP {home.status_code} {home.reason_phrase}'

        if getattr(self.api, "debug", False):
            print(reason_msg)
        if not home.is_success:
            return False, reason_msg

        token = await self.get_token()
        if not token:
            return False, "Login failed. CSRF token not found; selected domain may be blocked or changed."

        data = {
            "_token": token,
            "email": username,
            "password": password,
            "remember": 1,

        }
        status, msg = await self._post(data)

        return status, msg
