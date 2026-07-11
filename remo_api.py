"""
Remo API — Quotex Market Data & Trading REST/WebSocket API
============================================================

Single-file FastAPI service wrapping pyquotex (cleitonleonel/pyquotex).

Run locally:
    pip install -r requirements.txt
    export QUOTEX_EMAIL="you@example.com"
    export QUOTEX_PASSWORD="yourpassword"
    export QUOTEX_ACCOUNT="PRACTICE"   # or REAL
    uvicorn remo_api:app --host 0.0.0.0 --port 8000

Deploy on Railway/Render: see Dockerfile / Procfile alongside this file.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from pyquotex.stable_api import Quotex

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

QUOTEX_EMAIL = os.getenv("QUOTEX_EMAIL", "")
QUOTEX_PASSWORD = os.getenv("QUOTEX_PASSWORD", "")
QUOTEX_ACCOUNT = os.getenv("QUOTEX_ACCOUNT", "PRACTICE")  # PRACTICE | REAL
QUOTEX_LANG = os.getenv("QUOTEX_LANG", "pt")
DEFAULT_ASSET = os.getenv("QUOTEX_DEFAULT_ASSET", "EURUSD_otc")
API_KEY = os.getenv("API_KEY", "")  # empty string = auth disabled for now

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("remo_api")

VALID_PERIODS = [5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 86400]

START_TIME = time.time()


# --------------------------------------------------------------------------
# Quotex client wrapper — connection lifecycle, auto-reconnect, caching
# --------------------------------------------------------------------------

class QuotexManager:
    """Owns the single Quotex client instance and its connection state."""

    def __init__(self) -> None:
        self.client: Optional[Quotex] = None
        self.connected: bool = False
        self.last_error: str = ""
        self.lock = asyncio.Lock()
        self._reconnect_task: Optional[asyncio.Task] = None
        self._assets_cache: dict[str, Any] = {}
        self._assets_cache_ts: float = 0.0
        self._assets_cache_ttl: float = 30.0
        # Tracks the last time each asset's price stream was subscribed
        # to, so the watchdog can detect if a subscribed stream has gone
        # quiet (no fresh ticks) despite the connection itself being alive.
        self._subscribed_assets: dict[str, float] = {}

    async def start(self) -> None:
        if not QUOTEX_EMAIL or not QUOTEX_PASSWORD:
            self.last_error = "QUOTEX_EMAIL / QUOTEX_PASSWORD not set"
            logger.warning(self.last_error)
            return

        self.client = Quotex(
            email=QUOTEX_EMAIL,
            password=QUOTEX_PASSWORD,
            lang=QUOTEX_LANG,
            asset_default=DEFAULT_ASSET,
        )
        self.client.set_account_mode(QUOTEX_ACCOUNT)
        await self._connect()
        self._reconnect_task = asyncio.create_task(self._watchdog())

    async def _connect(self) -> None:
        async with self.lock:
            if self.client is None:
                return
            try:
                ok, reason = await self.client.connect()
                self.connected = bool(ok)
                self.last_error = "" if ok else str(reason)
                if ok:
                    logger.info("Connected to Quotex (%s account)", QUOTEX_ACCOUNT)
                else:
                    logger.error("Quotex connect failed: %s", reason)
            except Exception as exc:  # noqa: BLE001
                self.connected = False
                self.last_error = str(exc)
                logger.exception("Quotex connect raised an exception")

    def _check_stale_streams(self, threshold_seconds: float) -> list[str]:
        """Checks tracked asset subscriptions for stale data — a subscribed
        asset with no fresh tick in over `threshold_seconds`. Returns the
        list of stale asset symbols, or an empty list if all is well /
        nothing is currently tracked (e.g. no /price calls made yet)."""
        if self.client is None or not self._subscribed_assets:
            return []
        stale: list[str] = []
        now = time.time()
        for symbol, subscribed_at in list(self._subscribed_assets.items()):
            # Give a fresh subscription a grace period before judging it
            if now - subscribed_at < threshold_seconds:
                continue
            try:
                data = self.client.api.realtime_price.get(symbol, [])
                last_tick_time = data[-1].get("time", 0) if data else 0
                if now - last_tick_time > threshold_seconds:
                    stale.append(symbol)
            except Exception:  # noqa: BLE001
                stale.append(symbol)
        return stale

    async def _watchdog(self) -> None:
        """Background task: hard-pings the connection and reconnects if needed.

        Uses get_balance() as a real round-trip request rather than just
        checking a flag, since a socket can look "connected" while silently
        dead. Uses exponential backoff on repeated failures so a genuine
        block/rate-limit from Quotex isn't made worse by hammering retries
        every ~15-20s indefinitely — cooldown grows 10s -> 20s -> 40s ...
        up to a 10 minute cap, and resets to 10s the moment a connection
        actually succeeds.

        Also detects STALE DATA STREAMS: the WebSocket connection itself
        can stay alive and pass get_balance() pings while a specific
        asset's price/candle subscription silently stops receiving
        updates (observed: /price and /candles timing out for a single
        asset for several minutes, resolved only by a full manual
        restart). Since get_balance() alone can't detect this, we also
        check whether any recently-subscribed asset has gone stale and
        force a full reconnect if so — replicating what a manual restart
        does, without requiring one.
        """
        last_reconnect = 0.0
        cooldown = 10.0
        max_cooldown = 600.0  # 10 minutes
        consecutive_failures = 0
        STALE_DATA_THRESHOLD = 90.0  # seconds with zero fresh ticks on any tracked asset

        while True:
            await asyncio.sleep(15)
            try:
                if self.client is None:
                    continue
                await asyncio.wait_for(self.client.get_balance(), timeout=8)
                self.connected = True
                if consecutive_failures > 0:
                    logger.info("Quotex connection recovered after %d failed attempt(s)", consecutive_failures)
                consecutive_failures = 0
                cooldown = 10.0

                # Connection pings fine — now check if subscribed data
                # streams are actually still flowing.
                stale = self._check_stale_streams(STALE_DATA_THRESHOLD)
                if stale:
                    logger.warning(
                        "Data stream(s) stale for >%.0fs (%s) despite healthy connection — forcing full reconnect",
                        STALE_DATA_THRESHOLD, ", ".join(stale),
                    )
                    await self._connect()
                    self._subscribed_assets.clear()  # re-subscribe fresh after reconnect
            except Exception as exc:  # noqa: BLE001
                self.connected = False
                self.last_error = str(exc)
                now = time.time()
                if now - last_reconnect < cooldown:
                    continue
                last_reconnect = now
                consecutive_failures += 1
                logger.warning(
                    "Hard ping failed (%s), reconnecting... (attempt %d, next cooldown %.0fs)",
                    exc, consecutive_failures, cooldown,
                )
                await self._connect()
                # Grow the cooldown for next time regardless of outcome —
                # if _connect() succeeded, get_balance() will confirm on
                # the next loop iteration and reset it back to 10s.
                cooldown = min(cooldown * 2, max_cooldown)

    async def ensure_connected(self) -> Quotex:
        if self.client is None:
            raise HTTPException(
                status_code=503,
                detail="Quotex client not configured (missing credentials).",
            )
        if not self.connected:
            await self._connect()
        if not self.connected:
            raise HTTPException(
                status_code=503,
                detail=f"Not connected to Quotex: {self.last_error}",
            )
        return self.client

    async def get_instruments_cached(self) -> list[Any]:
        """Cached instrument list (asset metadata) to avoid hammering the WS."""
        now = time.time()
        if now - self._assets_cache_ts < self._assets_cache_ttl and self._assets_cache.get("data"):
            return self._assets_cache["data"]
        client = await self.ensure_connected()
        instruments = await client.get_instruments()
        self._assets_cache = {"data": instruments}
        self._assets_cache_ts = now
        return instruments

    async def shutdown(self) -> None:
        if self._reconnect_task:
            self._reconnect_task.cancel()
        if self.client:
            await self.client.close()


manager = QuotexManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await manager.start()
    yield
    await manager.shutdown()


app = FastAPI(
    title="Remo API",
    description="Quotex market data & trading API built on pyquotex",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _instrument_row_to_dict(row: list[Any]) -> dict[str, Any]:
    """Map a raw pyquotex instrument row to a friendly dict.

    Row layout (from pyquotex assets mixin usage):
      row[0]  = internal code
      row[1]  = asset id / symbol (e.g. EURUSD_otc)
      row[2]  = display name
      row[5]  = payment (payout %)
      row[14] = open (bool)
      row[18] = turbo payment
      row[-10] = 24H profit
      row[-9]  = 1M profit
      row[-8]  = 5M profit
    """
    try:
        return {
            "code": row[0],
            "symbol": row[1],
            "name": row[2].replace("\n", "") if isinstance(row[2], str) else row[2],
            "open": bool(row[14]) if len(row) > 14 else None,
            "payout": row[5] if len(row) > 5 else None,
            "turbo_payout": row[18] if len(row) > 18 else None,
        }
    except Exception:  # noqa: BLE001
        return {"raw": row}


def _period_query(period: int = Query(60, description="Candle period in seconds")) -> int:
    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Valid values: {VALID_PERIODS}",
        )
    return period


class BuyRequest(BaseModel):
    asset: str = Field(..., description="Asset symbol, e.g. EURUSD_otc")
    amount: float = Field(..., gt=0, description="Trade amount")
    direction: str = Field(..., description="'call' (up) or 'put' (down)")
    duration: int = Field(60, description="Duration in seconds")


class SellRequest(BaseModel):
    options_ids: list[str] = Field(..., description="List of option/order IDs to sell")


# --------------------------------------------------------------------------
# 1. Status
# --------------------------------------------------------------------------

@app.get("/")
async def root():
    return {
        "name": "Remo API",
        "status": "online",
        "quotex_connected": manager.connected,
        "uptime_seconds": round(time.time() - START_TIME, 1),
    }


@app.get("/status")
async def status():
    return {
        "connected": manager.connected,
        "account_mode": QUOTEX_ACCOUNT,
        "last_error": manager.last_error,
        "uptime_seconds": round(time.time() - START_TIME, 1),
    }


@app.get("/health")
async def health():
    return {"healthy": True}


@app.get("/ping")
async def ping():
    return {"pong": True, "time": time.time()}


# --------------------------------------------------------------------------
# 2. Assets
# --------------------------------------------------------------------------

@app.get("/assets")
async def get_assets():
    instruments = await manager.get_instruments_cached()
    return {"count": len(instruments), "assets": [_instrument_row_to_dict(r) for r in instruments]}


@app.get("/asset/{pair}")
async def get_asset(pair: str):
    client = await manager.ensure_connected()
    raw, info = await client.check_asset_open(pair)
    if raw is None:
        raise HTTPException(status_code=404, detail=f"Asset '{pair}' not found")
    return {
        "symbol": pair,
        "id": info[0],
        "name": info[1],
        "open": info[2],
    }


@app.get("/available-assets")
async def available_assets():
    instruments = await manager.get_instruments_cached()
    return {"assets": [row[1] for row in instruments if len(row) > 1]}


@app.get("/open-assets")
async def open_assets():
    instruments = await manager.get_instruments_cached()
    result = [_instrument_row_to_dict(r) for r in instruments if len(r) > 14 and r[14]]
    return {"count": len(result), "assets": result}


@app.get("/closed-assets")
async def closed_assets():
    instruments = await manager.get_instruments_cached()
    result = [_instrument_row_to_dict(r) for r in instruments if len(r) > 14 and not r[14]]
    return {"count": len(result), "assets": result}


@app.get("/otc-assets")
async def otc_assets():
    instruments = await manager.get_instruments_cached()
    result = [_instrument_row_to_dict(r) for r in instruments if len(r) > 1 and "_otc" in str(r[1]).lower()]
    return {"count": len(result), "assets": result}


# --------------------------------------------------------------------------
# 3. Payouts
# --------------------------------------------------------------------------

@app.get("/payout/{pair}")
async def payout(pair: str, timeframe: str = Query("1", description="'1', 'all' or specific timeframe key")):
    client = await manager.ensure_connected()
    await manager.get_instruments_cached()
    data = client.get_payout_by_asset(pair, timeframe=timeframe)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No payout data for '{pair}'")
    return {"asset": pair, "payout": data}


@app.get("/all-payouts")
async def all_payouts():
    client = await manager.ensure_connected()
    await manager.get_instruments_cached()
    return client.get_payment()


@app.get("/highest-payouts")
async def highest_payouts(limit: int = Query(10, ge=1, le=100), open_only: bool = Query(True)):
    client = await manager.ensure_connected()
    await manager.get_instruments_cached()
    payments = client.get_payment()
    items = list(payments.items())
    if open_only:
        items = [(k, v) for k, v in items if v.get("open")]
    ranked = sorted(
        items,
        key=lambda kv: kv[1].get("payment", 0) or 0,
        reverse=True,
    )[:limit]
    return {"top": [{"asset": k, **v} for k, v in ranked]}


# --------------------------------------------------------------------------
# 4. Market Data
# --------------------------------------------------------------------------

@app.get("/price/{pair}")
async def price(pair: str, history: bool = Query(False, description="Return full tick buffer instead of just the latest price")):
    client = await manager.ensure_connected()
    manager._subscribed_assets[pair] = time.time()
    data = await client.get_realtime_price(pair)

    # Detect a stale subscription: we have cached data, but it's old
    # enough that Quotex has likely stopped pushing updates for this
    # asset without the underlying connection actually dropping (the
    # watchdog's get_balance() ping would still succeed in that case,
    # so it wouldn't catch this on its own).
    STALE_THRESHOLD_SECONDS = 30
    is_stale = False
    if data:
        last_tick_time = data[-1].get("time", 0)
        if time.time() - last_tick_time > STALE_THRESHOLD_SECONDS:
            is_stale = True

    if not data or is_stale:
        try:
            await client.start_realtime_price(pair, 0)
        except TimeoutError:
            pass  # asset may be closed/illiquid; fall through and check again
        await asyncio.sleep(1.0)
        data = await client.get_realtime_price(pair)

    if not data:
        raise HTTPException(status_code=504, detail=f"No realtime price data available for '{pair}' (asset may be closed)")
    if history:
        return {"asset": pair, "count": len(data), "price": data}
    return {"asset": pair, "price": data[-1]}


@app.get("/prices")
async def prices(pairs: str = Query(..., description="Comma-separated list of asset symbols")):
    client = await manager.ensure_connected()
    symbols = [p.strip() for p in pairs.split(",") if p.strip()]
    result: dict[str, Any] = {}
    for sym in symbols:
        data = await client.get_realtime_price(sym)
        result[sym] = data[-1] if data else None
    return {"prices": result}


@app.get("/realtime/{pair}")
async def realtime(pair: str, period: int = 0):
    client = await manager.ensure_connected()
    data = await client.get_realtime_candles(pair)
    return {"asset": pair, "realtime": data}


@app.get("/candles/{pair}")
async def candles(
    pair: str,
    period: int = Query(60, description="Candle period in seconds"),
    count: int = Query(100, ge=1, le=5000, description="Number of candles"),
    end_time: Optional[float] = Query(None, description="Unix timestamp; defaults to now"),
):
    if period not in VALID_PERIODS:
        raise HTTPException(status_code=400, detail=f"Invalid period. Valid: {VALID_PERIODS}")
    client = await manager.ensure_connected()

    # A single get_candles() request can be silently truncated by the
    # server to far fewer candles than requested (observed: 7-10 candles
    # back regardless of a higher count). For anything beyond a small
    # request, route through get_historical_candles(), which chunks the
    # range across parallel workers and reliably fills the count.
    small_request = count <= 50 and end_time is None
    if small_request:
        data = await client.get_candles(pair, end_time, count, period, use_cache=True)
        if data:
            data = data[-count:]
    else:
        amount_of_seconds = count * period
        data = await client.get_historical_candles(pair, amount_of_seconds, period)
        if data:
            data = data[-count:]

    if data is None:
        raise HTTPException(status_code=504, detail="Timed out fetching candles")
    return {"asset": pair, "period": period, "count": len(data), "candles": data}


@app.get("/historical/{pair}")
async def historical(
    pair: str,
    period: int = Query(60, description="Candle period in seconds"),
    count: int = Query(100, ge=1, le=5000, description="Number of candles to return"),
    days: int = Query(1, ge=1, le=30, description="How many days back to fetch"),
):
    if period not in VALID_PERIODS:
        raise HTTPException(status_code=400, detail=f"Invalid period. Valid: {VALID_PERIODS}")
    client = await manager.ensure_connected()
    amount_of_seconds = days * 86400
    data = await client.get_historical_candles(pair, amount_of_seconds, period)
    if data:
        data = data[-count:]
    return {"asset": pair, "period": period, "days": days, "count": len(data) if data else 0, "candles": data}


@app.get("/server-time")
async def server_time():
    client = await manager.ensure_connected()
    ts = await client.get_server_time()
    return {"server_time": ts}


# --------------------------------------------------------------------------
# 6. Signals (based on realtime sentiment stream — no custom indicator math)
# --------------------------------------------------------------------------

@app.get("/signal/{pair}")
async def signal(pair: str):
    client = await manager.ensure_connected()
    sentiment = await client.get_realtime_sentiment(pair)
    if not sentiment:
        # Poll briefly instead of hard-waiting on a WS push event that may
        # never arrive for this pair — start_realtime_sentiment() raises
        # TimeoutError in that case, but polling just returns "no data"
        # cleanly, matching how pyquotex's own examples consume this.
        for _ in range(6):
            await asyncio.sleep(0.5)
            sentiment = await client.get_realtime_sentiment(pair)
            if sentiment:
                break
    if not sentiment:
        raise HTTPException(status_code=404, detail=f"No sentiment data available for '{pair}' (unsupported pair or not currently broadcasting).")
    buyers = sentiment.get("buy", sentiment.get("call", 0))
    sellers = sentiment.get("sell", sentiment.get("put", 0))
    direction = "call" if buyers >= sellers else "put"
    return {
        "asset": pair,
        "sentiment": sentiment,
        "suggested_direction": direction,
    }


@app.get("/sentiment/{pair}")
async def sentiment(pair: str):
    client = await manager.ensure_connected()
    data = await client.get_realtime_sentiment(pair)
    if not data:
        for _ in range(6):
            await asyncio.sleep(0.5)
            data = await client.get_realtime_sentiment(pair)
            if data:
                break
    if not data:
        raise HTTPException(status_code=404, detail=f"No sentiment data available for '{pair}' (unsupported pair or not currently broadcasting).")
    return {"asset": pair, "sentiment": data}


@app.get("/trend/{pair}")
async def trend(pair: str, period: int = 60, count: int = 20):
    """Simple trend read from last N candle closes (no indicator library)."""
    client = await manager.ensure_connected()
    data = await client.get_candles(pair, None, count, period, use_cache=True)
    if not data:
        raise HTTPException(status_code=504, detail="Timed out fetching candles")
    closes = [c.get("close") for c in data if c.get("close") is not None]
    if len(closes) < 2:
        return {"asset": pair, "trend": "unknown"}
    trend_dir = "up" if closes[-1] > closes[0] else "down" if closes[-1] < closes[0] else "flat"
    return {
        "asset": pair,
        "trend": trend_dir,
        "first_close": closes[0],
        "last_close": closes[-1],
        "samples": len(closes),
    }


# --------------------------------------------------------------------------
# 7. Account
# --------------------------------------------------------------------------

@app.get("/balance")
async def balance():
    client = await manager.ensure_connected()
    bal = await client.get_balance()
    return {"balance": bal, "account_mode": QUOTEX_ACCOUNT}


@app.get("/profile")
async def profile():
    client = await manager.ensure_connected()
    prof = await client.get_profile()
    return {"profile": prof.__dict__ if hasattr(prof, "__dict__") else prof}


@app.get("/history")
async def history():
    client = await manager.ensure_connected()
    data = await client.get_history()
    return {"count": len(data), "history": data}


@app.get("/trader-history")
async def trader_history(page: int = Query(1, ge=1)):
    client = await manager.ensure_connected()
    from pyquotex.utils.account_type import AccountType
    account_type = AccountType.DEMO if QUOTEX_ACCOUNT.upper() == "PRACTICE" else AccountType.REAL
    data = await client.get_trader_history(account_type, page_number=page)
    return {"page": page, "history": data}


@app.get("/profit")
async def profit():
    client = await manager.ensure_connected()
    return {"profit_in_operation": client.get_profit()}


# --------------------------------------------------------------------------
# 8. Trading
# --------------------------------------------------------------------------

@app.post("/buy")
async def buy(req: BuyRequest):
    if req.direction.lower() not in ("call", "put"):
        raise HTTPException(status_code=400, detail="direction must be 'call' or 'put'")
    client = await manager.ensure_connected()
    ok, result = await client.buy(req.amount, req.asset, req.direction.lower(), req.duration)
    if not ok:
        raise HTTPException(status_code=400, detail=f"Buy failed: {result}")
    return {"success": True, "order": result}


@app.post("/sell")
async def sell(req: SellRequest):
    client = await manager.ensure_connected()
    try:
        result = await asyncio.wait_for(client.sell_option(req.options_ids), timeout=15)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Sell timed out — option ID may be invalid or already closed.")
    return {"success": True, "result": result}


@app.post("/check")
async def check(order_id: str = Query(...), duration: int = Query(0)):
    client = await manager.ensure_connected()
    try:
        win, profit_amount = await asyncio.wait_for(client.check_win(order_id, duration), timeout=15)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Check timed out — order_id may be invalid or trade not yet closed.")
    return {"order_id": order_id, "result": win, "profit": profit_amount}


@app.get("/result/{trade_id}")
async def result(trade_id: str):
    client = await manager.ensure_connected()
    status_str, data = await client.get_result(trade_id)
    if status_str is None:
        raise HTTPException(status_code=404, detail=str(data))
    return {"trade_id": trade_id, "status": status_str, "data": data}


# --------------------------------------------------------------------------
# 9. Market Information
# --------------------------------------------------------------------------

@app.get("/market-status")
async def market_status():
    instruments = await manager.get_instruments_cached()
    open_count = sum(1 for r in instruments if len(r) > 14 and r[14])
    return {
        "total_assets": len(instruments),
        "open_assets": open_count,
        "closed_assets": len(instruments) - open_count,
    }


@app.get("/server-info")
async def server_info():
    return {
        "connected": manager.connected,
        "account_mode": QUOTEX_ACCOUNT,
        "language": QUOTEX_LANG,
        "default_asset": DEFAULT_ASSET,
    }


@app.get("/platform-version")
async def platform_version():
    import pyquotex
    return {"pyquotex_version": getattr(pyquotex, "__version__", "unknown")}


# --------------------------------------------------------------------------
# 12. Dashboard
# --------------------------------------------------------------------------

@app.get("/dashboard")
async def dashboard():
    client = await manager.ensure_connected()
    server_ts = await client.get_server_time()
    bal = await client.get_balance()
    instruments = await manager.get_instruments_cached()
    payouts = client.get_payment()
    return {
        "server_time": server_ts,
        "balance": bal,
        "assets": [_instrument_row_to_dict(r) for r in instruments[:50]],
        "payouts": dict(list(payouts.items())[:50]),
        "market_status": {
            "total_assets": len(instruments),
            "open_assets": sum(1 for r in instruments if len(r) > 14 and r[14]),
        },
    }


# --------------------------------------------------------------------------
# 13. Complete Market Endpoint
# --------------------------------------------------------------------------

@app.get("/market/{pair}")
async def market(pair: str, period: int = 60, count: int = 100):
    client = await manager.ensure_connected()
    server_ts = await client.get_server_time()
    price_data = await client.get_realtime_price(pair)
    price_latest = price_data[-1] if price_data else None
    candles_data = await client.get_candles(pair, None, count, period, use_cache=True)
    payout_data = client.get_payout_by_asset(pair, timeframe="1")
    sentiment_data = await client.get_realtime_sentiment(pair)
    return {
        "asset": pair,
        "price": price_latest,
        "candles": candles_data,
        "payout": payout_data,
        "sentiment": sentiment_data,
        "server_time": server_ts,
    }


# --------------------------------------------------------------------------
# 10. Live Streaming (WebSocket)
# --------------------------------------------------------------------------

@app.websocket("/ws/price")
async def ws_price(websocket: WebSocket):
    await websocket.accept()
    asset = websocket.query_params.get("asset", DEFAULT_ASSET)
    try:
        client = await manager.ensure_connected()
        await client.start_realtime_price(asset, 0)
        while True:
            data = await client.get_realtime_price(asset)
            await websocket.send_json({"asset": asset, "price": data})
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        logger.info("ws_price client disconnected (%s)", asset)
    except Exception as exc:  # noqa: BLE001
        logger.exception("ws_price error")
        try:
            await websocket.send_json({"error": str(exc)})
        except Exception:
            pass


@app.websocket("/ws/candles")
async def ws_candles(websocket: WebSocket):
    await websocket.accept()
    asset = websocket.query_params.get("asset", DEFAULT_ASSET)
    period = int(websocket.query_params.get("period", 60))
    try:
        client = await manager.ensure_connected()
        await client.start_candles_one_stream(asset, period)
        while True:
            data = await client.get_realtime_candles(asset)
            await websocket.send_json({"asset": asset, "period": period, "candles": data})
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        logger.info("ws_candles client disconnected (%s)", asset)
    except Exception as exc:  # noqa: BLE001
        logger.exception("ws_candles error")
        try:
            await websocket.send_json({"error": str(exc)})
        except Exception:
            pass


@app.websocket("/ws/signals")
async def ws_signals(websocket: WebSocket):
    await websocket.accept()
    asset = websocket.query_params.get("asset", DEFAULT_ASSET)
    try:
        client = await manager.ensure_connected()
        # Poll instead of hard-waiting on a WS push event that may never
        # arrive for this pair (matches the working pattern found in
        # pyquotex's own example bots — get_realtime_sentiment() in a loop,
        # no start_realtime_sentiment() hard-wait).
        while True:
            data = await client.get_realtime_sentiment(asset)
            await websocket.send_json({"asset": asset, "sentiment": data or None})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        logger.info("ws_signals client disconnected (%s)", asset)
    except Exception as exc:  # noqa: BLE001
        logger.exception("ws_signals error")
        try:
            await websocket.send_json({"error": str(exc)})
        except Exception:
            pass
        await websocket.close(code=1011)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
