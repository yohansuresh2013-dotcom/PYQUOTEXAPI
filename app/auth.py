"""
Remo API — Auth
Currently a no-op: REMO_API_KEY unset means every request passes.
Set REMO_API_KEY in the environment and every request must send it
back as `X-API-Key` — flip it on later without touching route code.
"""
from __future__ import annotations
import os
from fastapi import Header, HTTPException

REQUIRED_KEY = os.getenv("REMO_API_KEY")  # None = auth disabled


async def require_api_key(x_api_key: str | None = Header(default=None)):
    if REQUIRED_KEY is None:
        return  # auth disabled — open access
    if x_api_key != REQUIRED_KEY:
        raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key header")

