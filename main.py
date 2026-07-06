"""
Remo API
A single API surface over your Quotex connection:

  GET  /api/assets                  -> every pair + payout + open/closed state
  GET  /api/payout?symbol=          -> payout for one pair
  GET  /api/candles/history         -> historical candles for one pair
  GET  /api/candles/live            -> latest formed + forming candle (one-shot poll)
  GET  /api/pair-info                -> everything about one pair in a single response
  WS   /ws/candles?symbol=&period=  -> push-based live candle stream

Auth is off by default. Set REMO_API_KEY in the environment to require
an `X-API-Key` header on every request (see app/auth.py).
"""
from __future__ import annotations
import asyncio
import logging
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware

from .quotex_client import QuotexClient
from .auth import require_api_key

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("remo.api")

app = FastAPI(
    title="Remo API",
    description="Live + historical candle data, and payouts, for every Quotex pair, in one place.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten once you have a specific frontend origin
    allow_methods=["*"],
    allow_headers=["*"],
)

quotex = QuotexClient()


@app.on_event("startup")
async def startup():
    ok = await quotex.connect()
    if not ok:
        logger.error("Failed to connect to Quotex on startup — check QUOTEX_EMAIL/QUOTEX_PASSWORD")


@app.on_event("shutdown")
async def shutdown():
    await quotex.close()


# ------------------------------------------------------------------ health --
@app.get("/api/health")
async def health():
    return {"status": "ok", "connected": quotex._connected}


# ------------------------------------------------------------------ assets --
@app.get("/api/assets", dependencies=[Depends(require_api_key)])
async def list_assets():
    """Every tradable pair with its symbol, display name, payout, and OTC flag."""
    assets = await quotex.list_assets()
    return {"count": len(assets), "assets": assets}


@app.get("/api/payout", dependencies=[Depends(require_api_key)])
async def get_payout(symbol: str):
    """Current payout percentage for a single pair."""
    payout = await quotex.get_payout(symbol)
    if payout is None:
        raise HTTPException(404, f"No payout data for '{symbol}' — check the symbol is correct")
    return {"symbol": symbol, "payout": payout}


# ------------------------------------------------------------------ candles --
@app.get("/api/candles/history", dependencies=[Depends(require_api_key)])
async def candles_history(
    symbol: str,
    period: int = Query(60, description="candle period in seconds (5, 15, 30, 60, 300, 900...)"),
    count: int = Query(150, le=5000, description="number of candles to return"),
):
    """Historical (closed) candles for one pair."""
    candles = await quotex.get_candles(symbol, period=period, count=count)
    if not candles:
        raise HTTPException(502, f"Could not fetch candles for '{symbol}' — check symbol/connection")
    return {"symbol": symbol, "period": period, "count": len(candles), "candles": candles}


@app.get("/api/candles/live", dependencies=[Depends(require_api_key)])
async def candles_live(symbol: str):
    """
    One-shot snapshot of the current in-progress candle (the tick that
    hasn't closed yet). Use this for simple polling; use the /ws/candles
    WebSocket below if you want push updates instead of polling.
    """
    tick = await quotex.get_realtime_price(symbol)
    if tick is None:
        raise HTTPException(502, f"Could not fetch live price for '{symbol}'")
    return {"symbol": symbol, "candle": tick}


# --------------------------------------------------------------- pair-info --
@app.get("/api/pair-info", dependencies=[Depends(require_api_key)])
async def pair_info(
    symbol: str,
    period: int = Query(60, description="candle period in seconds for the history slice returned"),
    history_count: int = Query(100, le=1000),
):
    """
    Everything about one pair in a single response: payout, latest live
    tick, and a slice of historical candles — so a client can render a
    full picture without stitching together three separate calls.
    """
    payout_task = quotex.get_payout(symbol)
    live_task = quotex.get_realtime_price(symbol)
    history_task = quotex.get_candles(symbol, period=period, count=history_count)

    payout, live_tick, history = await asyncio.gather(payout_task, live_task, history_task)

    if not history:
        raise HTTPException(502, f"Could not fetch data for '{symbol}' — check symbol/connection")

    return {
        "symbol": symbol,
        "payout": payout,
        "period": period,
        "live_candle": live_tick,
        "history": history,
        "history_count": len(history),
    }


@app.get("/api/market-overview", dependencies=[Depends(require_api_key)])
async def market_overview(min_payout: float = 0.0):
    """
    Every pair with its payout and latest close price, in one call —
    a birds-eye view across the whole market instead of one symbol.
    """
    assets = await quotex.list_assets()
    assets = [a for a in assets if (a.get("payout") or 0) >= min_payout]

    async def enrich(asset):
        tick = await quotex.get_realtime_price(asset["symbol"])
        return {**asset, "last_close": tick["close"] if tick else None, "last_time": tick["time"] if tick else None}

    enriched = await asyncio.gather(*(enrich(a) for a in assets))
    return {"count": len(enriched), "assets": list(enriched)}


# --------------------------------------------------------------- WebSocket --
@app.websocket("/ws/candles")
async def ws_candles(websocket: WebSocket, symbol: str = Query(...), period: int = Query(60)):
    """
    Push-based live candle stream for one pair — sends historical seed
    once on connect, then pushes tick updates and a `candle_closed`
    event on every period rollover. No polling required on the client side.
    """
    await websocket.accept()
    try:
        history = await quotex.get_candles(symbol, period=period, count=150)
        await websocket.send_json({"type": "seed", "symbol": symbol, "period": period, "candles": history})

        last_candle_time = history[-1]["time"] if history else None

        while True:
            tick = await quotex.get_realtime_price(symbol)
            if tick:
                if last_candle_time is None or tick["time"] > last_candle_time:
                    last_candle_time = tick["time"]
                    await websocket.send_json({"type": "candle_closed", "candle": tick})
                else:
                    await websocket.send_json({"type": "tick", "candle": tick})
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        logger.info("Client disconnected from %s stream", symbol)
    except Exception as e:
        logger.exception("WS error for %s: %s", symbol, e)
        try:
            await websocket.close()
        except Exception:
            pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)

