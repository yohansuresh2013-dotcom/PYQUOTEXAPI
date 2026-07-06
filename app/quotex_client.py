"""
Remo API — Quotex Client Wrapper
Thin async wrapper around pyquotex's Quotex client: connection lifecycle,
candle fetching with retry, and asset/payout listing.
"""
from __future__ import annotations
import asyncio
import logging
import os
from typing import List, Dict, Any, Optional

logger = logging.getLogger("remo.quotex")

try:
    from pyquotex.stable_api import Quotex
except ImportError:
    Quotex = None  # allows the rest of the app to import cleanly before install


class QuotexClient:
    def __init__(self, email: Optional[str] = None, password: Optional[str] = None, lang: str = "en"):
        self.email = email or os.getenv("QUOTEX_EMAIL")
        self.password = password or os.getenv("QUOTEX_PASSWORD")
        self.lang = lang
        self.client: Optional["Quotex"] = None
        self._connected = False
        self._lock = asyncio.Lock()

    async def connect(self, retries: int = 3, delay: float = 2.0) -> bool:
        if Quotex is None:
            raise RuntimeError(
                "pyquotex is not installed. Run: pip install git+https://github.com/cleitonleonel/pyquotex.git"
            )
        async with self._lock:
            if self._connected:
                return True
            self.client = Quotex(email=self.email, password=self.password, lang=self.lang)
            for attempt in range(1, retries + 1):
                try:
                    status, message = await self.client.connect()
                    if status:
                        self._connected = True
                        logger.info("Connected to Quotex")
                        return True
                    logger.warning("Connect attempt %d failed: %s", attempt, message)
                except Exception as e:
                    logger.warning("Connect attempt %d raised: %s", attempt, e)
                await asyncio.sleep(delay)
            return False

    async def ensure_connected(self):
        if not self._connected:
            ok = await self.connect()
            if not ok:
                raise RuntimeError("Unable to connect to Quotex")

    async def close(self):
        if self.client and self._connected:
            try:
                await self.client.close()
            finally:
                self._connected = False

    async def get_balance(self) -> Optional[float]:
        await self.ensure_connected()
        return await self.client.get_balance()

    async def list_assets(self) -> List[Dict[str, Any]]:
        """Return all tradable assets with symbol, display name, open/closed state, payout."""
        await self.ensure_connected()
        # pyquotex exposes instrument/asset info differently across versions;
        # this wrapper normalizes whatever is available.
        raw_assets = []
        if hasattr(self.client, "get_all_asset_name"):
            raw_assets = self.client.get_all_asset_name() or []
        elif hasattr(self.client, "api") and hasattr(self.client.api, "instruments"):
            raw_assets = self.client.api.instruments or []

        assets = []
        for item in raw_assets:
            # normalize tuple/list/dict shapes defensively
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                symbol, display = item[0], item[1]
            elif isinstance(item, dict):
                symbol, display = item.get("symbol"), item.get("name", item.get("symbol"))
            else:
                continue

            payout = await self.get_payout(symbol)
            assets.append({
                "symbol": symbol,
                "display": display,
                "payout": payout,
                "is_otc": "_otc" in symbol.lower() or "-otc" in symbol.lower(),
            })
        return assets

    async def get_payout(self, symbol: str) -> Optional[float]:
        await self.ensure_connected()
        try:
            if hasattr(self.client, "get_payout_by_asset"):
                return await self.client.get_payout_by_asset(symbol)
            if hasattr(self.client, "get_payment"):
                data = self.client.get_payment()
                asset_data = data.get(symbol) if isinstance(data, dict) else None
                if asset_data:
                    return asset_data.get("payout") or asset_data.get("profit")
        except Exception as e:
            logger.debug("Payout fetch failed for %s: %s", symbol, e)
        return None

    async def get_candles(self, symbol: str, period: int = 60, count: int = 100, attempts: int = 3) -> List[Dict[str, Any]]:
        """Fetch recent candles, retrying on transient failures."""
        await self.ensure_connected()
        amount_of_seconds = period * count

        last_err = None
        for attempt in range(1, attempts + 1):
            try:
                raw = await self.client.get_candles(symbol, amount_of_seconds, period)
                return self._normalize_candles(raw)
            except Exception as e:
                last_err = e
                logger.debug("get_candles attempt %d for %s failed: %s", attempt, symbol, e)
                await asyncio.sleep(1.0)
        logger.warning("get_candles failed for %s after %d attempts: %s", symbol, attempts, last_err)
        return []

    async def get_realtime_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch the latest tick/price for a symbol, used to build the live (unclosed) candle."""
        await self.ensure_connected()
        try:
            if hasattr(self.client, "get_realtime_candles"):
                data = self.client.get_realtime_candles(symbol)
                if data:
                    latest_ts = max(data.keys())
                    return self._normalize_candles([data[latest_ts]])[0]
        except Exception as e:
            logger.debug("realtime price fetch failed for %s: %s", symbol, e)
        return None

    @staticmethod
    def _normalize_candles(raw: List[Any]) -> List[Dict[str, Any]]:
        """Normalize whatever shape pyquotex returns into {time, open, high, low, close}."""
        out = []
        for item in raw or []:
            if isinstance(item, dict):
                t = item.get("time") or item.get("t") or item.get("from")
                o = item.get("open", item.get("o"))
                h = item.get("high", item.get("h"))
                l = item.get("low", item.get("l"))
                c = item.get("close", item.get("c"))
            elif isinstance(item, (list, tuple)) and len(item) >= 5:
                t, o, h, l, c = item[:5]
            else:
                continue
            if None in (t, o, h, l, c):
                continue
            out.append({"time": int(t), "open": float(o), "high": float(h), "low": float(l), "close": float(c)})
        out.sort(key=lambda x: x["time"])
        return out

