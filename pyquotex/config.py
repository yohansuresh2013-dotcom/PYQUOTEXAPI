import configparser
import json
import os
import sys
import threading
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    from fake_useragent import UserAgent
except Exception:  # optional dependency fallback
    UserAgent = None  # type: ignore[assignment]

USER_AGENT = (
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) "
    "Gecko/20100101 Firefox/119.0"
)

base_dir = Path.cwd()
config_path = Path(os.path.join(base_dir, "settings/config.ini"))
config = configparser.ConfigParser(interpolation=None)

session_lock = threading.Lock()


def normalize_host(host: str | None) -> str:
    """Return a clean broker hostname without scheme, path, or ws2 prefix."""
    value = (host or "qxbroker.com").strip()
    if not value:
        return "qxbroker.com"
    if "://" in value:
        parsed = urlparse(value)
        value = parsed.netloc or parsed.path
    value = value.split("/", 1)[0].split(":", 1)[0].strip().lower()
    if value.startswith("www."):
        value = value[4:]
    if value.startswith("ws2."):
        value = value[4:]
    return value or "qxbroker.com"


def session_key(email: str, host: str | None = None) -> str:
    """Session storage key scoped by email + host to avoid mirror-cookie mixups."""
    clean_email = (email or "").strip().lower()
    clean_host = normalize_host(host)
    return f"{clean_email}@{clean_host}"



def credentials() -> tuple[str, str]:
    """Get or prompt for user credentials from config file."""
    if not config_path.exists():
        config_path.parent.mkdir(exist_ok=True, parents=True)
        text_settings = (
            f"[settings]\n"
            f"email={input('Enter your account email: ')}\n"
            f"password={input('Enter your account password: ')}\n"
        )
        config_path.write_text(text_settings)

    config.read(config_path, encoding="utf-8")

    email = config.get("settings", "email")
    password = config.get("settings", "password")

    if not email or not password:
        print("Email and password cannot be left blank...")
        sys.exit()

    return email, password


def resource_path(relative_path: str | Path) -> Path:
    """Get absolute path to resource, works for dev and for PyInstaller"""
    global base_dir
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        base_dir = Path(sys._MEIPASS)
    return base_dir / relative_path


def load_session(
    email: str,
    user_agent: str | None = None,
    host: str | None = None,
) -> dict[str, Any]:
    """Load session data for a specific email+host.

    Older versions stored sessions by email only. This function keeps that
    legacy entry readable only for qxbroker.com and writes new sessions using
    an email@host key so cookies/tokens from different Quotex mirrors never
    collide.
    """
    if user_agent is None:
        try:
            user_agent = UserAgent().random if UserAgent else USER_AGENT
        except Exception:
            user_agent = USER_AGENT

    key = session_key(email, host)
    legacy_key = (email or "").strip().lower()
    output_file = Path(resource_path("session.json"))
    with session_lock:
        all_sessions: dict[str, Any] = {}
        if output_file.exists():
            try:
                loaded = json.loads(output_file.read_text())
                if isinstance(loaded, dict):
                    all_sessions = loaded
            except json.JSONDecodeError:
                pass
        else:
            output_file.parent.mkdir(exist_ok=True, parents=True)

        if key not in all_sessions:
            legacy = all_sessions.get(legacy_key)
            if normalize_host(host) == "qxbroker.com" and isinstance(legacy, dict):
                all_sessions[key] = legacy
            else:
                all_sessions[key] = {
                    "cookies": None,
                    "token": None,
                    "user_agent": user_agent,
                    "host": normalize_host(host),
                }
            output_file.write_text(json.dumps(all_sessions, indent=4))

        session = all_sessions.get(key) or {}
        session.setdefault("host", normalize_host(host))
        session.setdefault("user_agent", user_agent)
        session.setdefault("cookies", None)
        session.setdefault("token", None)
        return session


def update_session(
    email: str,
    d: dict[str, Any],
    host: str | None = None,
) -> dict[str, Any]:
    """Update and persist session data for a specific email+host."""
    key = session_key(email, host)
    data = dict(d or {})
    data["host"] = normalize_host(host)
    output_file = Path(resource_path("session.json"))
    with session_lock:
        current_sessions: dict[str, Any] = {}
        if output_file.exists():
            try:
                loaded = json.loads(output_file.read_text())
                if isinstance(loaded, dict):
                    current_sessions = loaded
            except json.JSONDecodeError:
                pass
        else:
            output_file.parent.mkdir(exist_ok=True, parents=True)

        current_sessions[key] = data
        output_file.write_text(json.dumps(current_sessions, indent=4))
        return current_sessions.get(key, data)
