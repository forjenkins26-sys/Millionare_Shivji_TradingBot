# Vol Surge v5 — 5min Bot (BTC/USD)

## Overview
WebSocket-native trading bot for **BTCUSD Perpetual** on **Delta Exchange India**.
Detects Vol Surge signals from live 5-minute candles — no TradingView webhook dependency.

## Dashboard Links

| Bot | Timeframe | Dashboard URL |
|---|---|---|
| BTC 1min | 1m | https://millionare-shivji-1min-bot.fly.dev/dashboard |
| **BTC 5min** | **5m** | **https://millionare-shivji-tradingbot.fly.dev/dashboard** |
| BTC 15min | 15m | https://millionare-shivji-15m-bot.fly.dev/dashboard |

> Gold MT5 bot runs locally — no public URL.

---

## Architecture
```
Delta WebSocket (candlestick_5m)
        ↓
  CandleFeed (candle_feed.py)       ← buffers 300 bars, Heikin-Ashi conversion
        ↓
  SignalEngine (signal_engine.py)   ← Vol Surge v5 Pine-parity logic
        ↓
  volsurge_v5_live.py               ← entry/exit execution + dashboard
        ↓
  Delta Exchange REST API           ← market entry + TP limit order
```

---

## Strategy

### Signal: Vol Surge v5
- **Candle type:** Heikin-Ashi (matches TradingView 78% WR mode)
- **Lookback:** 5 bars (`VS_LOOKBACK=5`)
- **Burst detection:** candle body ≥ `chopAvgTR × 2.0` (`VS_BURST_MULT=2.0`)
- **Cooldown:** 3 bars after signal (`VS_COOLDOWN=3`)
- **Min body filter:** 250pts (`MIN_BODY_PTS=250`)
- **Session filter:** OFF (trades 24/7)
- **EMA filter:** OFF

### SL/TP Model
| Parameter | Value | Notes |
|---|---|---|
| SL | **ATR-based** | `1.8 × ATR5(prev bar)` — dynamic, software enforced |
| TP | **1.3R** | GTC limit order placed on Delta immediately after entry |
| Mode | Dynamic (ATR) | `FIXED_SL_PTS=0`, `FIXED_TP_PTS=0` (env override available) |

### Entry
- **Type:** Market order (IOC) at bar close
- **Stale guard:** Skip if bar closed >60s ago (`MAX_SIGNAL_AGE_S=60`)
- **Limit timeout:** 270s / 30% of bar (`ENTRY_LIMIT_TIMEOUT_S=270`)
- **Idempotent orders:** `client_order_id=uuid4.hex` prevents duplicate fills

### Exit
- **TP:** GTC limit order on Delta — auto-fills when price hits level
- **SL:** Software monitor polls price every 1s — sends market close order when breached
- **SL execution:** Market close fires FIRST, TP cancel runs concurrently in background thread (minimises slippage)
- **Manual:** `/api/close` endpoint or "Close Trade" button on dashboard

---

## Files

| File | Purpose |
|---|---|
| `volsurge_v5_live.py` | Main bot — signal handling, order execution, FastAPI dashboard |
| `candle_feed.py` | Delta WebSocket 5m candle feed + REST backfill (300 bars) |
| `signal_engine.py` | Vol Surge signal logic (timeframe-agnostic, Pine-parity) |
| `Dockerfile` | Docker image — python:3.11-slim + uvicorn |
| `requirements_v5.txt` | Python deps: websockets, fastapi, uvicorn, requests |
| `fly.toml` | Fly.io config — app: `millionare-shivji-tradingbot`, region: `bom` (Mumbai) |

---

## Config (Key Constants)

```python
CANDLE_SECONDS        = 300     # 5-minute bars
MIN_BODY_PTS          = 250.0   # Min candle body to qualify as burst
MAX_SIGNAL_AGE_S      = 60      # Stale signal cutoff (seconds)
ENTRY_LIMIT_TIMEOUT_S = 270     # Cancel limit entry after 270s (30% of bar)
TP_R                  = 1.3     # TP = 1.3 × SL distance
FIXED_SL_PTS          = 0.0     # 0 = dynamic ATR-based SL
FIXED_TP_PTS          = 0.0     # 0 = dynamic 1.3R TP
PRICE_INTERVAL        = 1       # SL monitor tick every 1s
_AUTO_RESTART_AGE     = 1800.0  # 30min stale feed → self-restart
```

---

## Deployment

**Platform:** Fly.io (Mumbai — `bom` region)
**App:** `millionare-shivji-tradingbot`
**Volume:** `volsurge_5m_data` → `/data` (persistent trades/logs)
**GitHub:** https://github.com/forjenkins26-sys/Millionare_Shivji_TradingBot

### Deploy
```bash
git push
flyctl deploy --app millionare-shivji-tradingbot
```

### Check logs
```bash
flyctl logs --app millionare-shivji-tradingbot
```

### SSH into machine
```bash
flyctl ssh console --app millionare-shivji-tradingbot
```

---

## Fly.io Secrets (required)

| Secret | Description |
|---|---|
| `DELTA_API_KEY_LIVE` | Delta Exchange API key |
| `DELTA_API_SECRET_LIVE` | Delta Exchange API secret |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token for alerts |
| `TELEGRAM_CHAT_ID` | Telegram chat ID |
| `PAPER_MODE` | `true` = paper, `false` = live orders |
| `LOT_SIZE` | BTC lot size (e.g. `0.001`) |

> **IP Whitelist:** Delta API key must whitelist the Fly.io machine egress IP.
> If redeployed to new machine, get new IP via `flyctl ssh console -C "curl ifconfig.me"`.

---

## Dashboard Endpoints

| Endpoint | Description |
|---|---|
| `/dashboard` | Full HTML trading dashboard |
| `/health` | JSON health — preflight, WS status, price |
| `/api/live` | Live price + unrealised PnL |
| `/api/stream` | SSE price stream (~200ms) |
| `/api/close` | Manually close open trade |
| `/data` | Download trades CSV |

---

## Important Notes

- **Delta India rejects `stop_market_order`** — SL is enforced by software monitor, not exchange
- **Heikin-Ashi is mandatory** — switching to regular OHLC drops WR from 78% to 49%
- **ATR-based SL/TP** — dynamic levels scale with current volatility (unlike fixed-point bots)
- **`client_order_id`** on every order = idempotent — safe to retry on network failure
- **Watchdog** checks every 60s — if no candle for 30min, auto-restarts
- **SL slippage fix** — market close fires before TP cancel (background thread), saves ~300ms

---

## Future Improvements

- **Dynamic SL/TP already active** — ATR-based by default. If needed, can switch to fixed via `FIXED_SL_PTS` / `FIXED_TP_PTS` env vars
- **Min body filter tuning** — 250pts calibrated for 5m; monitor performance across volatility regimes

---

## Related Bots

| Bot | Folder | App | Timeframe | Dashboard |
|---|---|---|---|---|
| **BTC 5m** | `volsurge_5m` | `millionare-shivji-tradingbot` | **5 min** ← this bot | https://millionare-shivji-tradingbot.fly.dev/dashboard |
| BTC 1m | `volSurge_1min` | `millionare-shivji-1min-bot` | 1 min | https://millionare-shivji-1min-bot.fly.dev/dashboard |
| BTC 15m | `volsurge_15m` | `millionare-shivji-15m-bot` | 15 min | https://millionare-shivji-15m-bot.fly.dev/dashboard |
| Gold 5m | `Gold mt5_Vol Surge 5 min` | Local MT5/XM | 5 min | Local only |
