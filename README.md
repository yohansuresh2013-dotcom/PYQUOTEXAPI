# Remo API

A single API over your Quotex connection: live candles, historical
candles, payouts, and a combined per-pair snapshot — everything in
one place instead of stitching together separate calls.

## Endpoints

| Method | Path | What it gives you |
|---|---|---|
| GET | `/api/health` | Is the API up, is Quotex connected |
| GET | `/api/assets` | Every pair: symbol, display name, payout, OTC flag |
| GET | `/api/payout?symbol=` | Payout for one pair |
| GET | `/api/candles/history?symbol=&period=&count=` | Historical closed candles |
| GET | `/api/candles/live?symbol=` | One-shot snapshot of the current forming candle |
| GET | `/api/pair-info?symbol=&period=&history_count=` | Payout + live tick + history, all in one response |
| GET | `/api/market-overview?min_payout=` | Every pair with payout + last price, birds-eye view |
| WS | `/ws/candles?symbol=&period=` | Push-based live stream — no polling needed |

All routes except `/api/health` are behind `require_api_key`
(see Auth below — off by default).

### Example: pair-info

```
GET /api/pair-info?symbol=EURUSD_otc&period=60&history_count=100
```

```json
{
  "symbol": "EURUSD_otc",
  "payout": 87,
  "period": 60,
  "live_candle": {"time": 1751800000, "open": 1.0852, "high": 1.0855, "low": 1.0850, "close": 1.0853},
  "history": [ /* 100 candles */ ],
  "history_count": 100
}
```

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env       # fill in QUOTEX_EMAIL / QUOTEX_PASSWORD
python -m app.main         # runs on http://localhost:8000
```

Run this locally first, not straight on Railway — pyquotex's first
login may need a browser captcha/2FA step you can only complete
interactively. Once it logs in successfully it persists a session
file so future boots skip that step.

## Auth

Off by default — every endpoint is open. To lock it down:

1. Set `REMO_API_KEY=some-long-random-string` in `.env` (or Railway's Variables tab)
2. Every request must now send that value back as the `X-API-Key` header:

```bash
curl -H "X-API-Key: some-long-random-string" http://localhost:8000/api/assets
```

Unset `REMO_API_KEY` any time to reopen it — no code changes needed either way.

## Deploying to Railway

Same pattern as before — Docker build (not Nixpacks), because pyquotex
needs real Chromium:

1. Push this folder to a private GitHub repo (`.env` and session files are gitignored)
2. Railway → New Project → Deploy from GitHub repo → it picks up the Dockerfile automatically
3. Variables: `QUOTEX_EMAIL`, `QUOTEX_PASSWORD`, `REMO_API_KEY` (optional)
4. Settings → Resources: bump memory to 1GB+ (Chromium isn't light)
5. Settings → Networking → Generate Domain
6. Keep it at **1 replica** — one Quotex session, don't split it across instances

## Notes

- `quotex_client.py`'s asset/payout parsing is written defensively against
  a couple of possible pyquotex return shapes since exact attribute names
  have shifted across forks/versions — check it against whatever version
  you install.
- `/ws/candles` currently polls the underlying connection every 500ms
  internally and pushes to the client on each loop; if your pyquotex
  version exposes a true push callback, wire that in instead for lower latency.
- `/api/market-overview` fetches a live tick for every pair concurrently —
  fine for personal use, but if you have many dozens of pairs consider
  adding a concurrency limit (same pattern as `ranking.py` in the earlier
  chart-bot build, if you want to bring that back).

