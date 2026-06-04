# Vol Surge v5 — Mummy Bot · 1min BTC/USD

## Overview
WebSocket-native live trading bot for **BTCUSD Perpetual** on **Delta Exchange India**.
Bot name: **🤖 MUMMY BOT**
Detects Vol Surge signals from live 1-minute Heikin-Ashi candles — no TradingView webhook.

---

## Dashboard & Links

| Resource | URL |
|---|---|
| **This bot dashboard** | https://millionare-shivji-tradingbot.fly.dev/dashboard |
| Anand bot dashboard | https://millionare-shivji-1min-bot.fly.dev/dashboard |
| GitHub repo | https://github.com/forjenkins26-sys/Millionare_Shivji_TradingBot |

---

## Architecture

```
Delta WebSocket (candlestick_1m)
        ↓
  CandleFeed (candle_feed.py)       ← buffers 300 bars, real-time mark price
        ↓
  _rc_to_ha() / _seed_ha_from_buffer()   ← incremental Heikin-Ashi (Pine-exact)
        ↓
  SignalEngine (signal_engine.py)   ← Vol Surge v5 Pine-parity signal logic
        ↓
  volsurge_v5_live.py               ← entry/exit execution + FastAPI dashboard
        ↓
  Delta Exchange REST API           ← bracket order (SL stop-market + TP limit)
```

---

## Strategy

### Signal: Vol Surge v5 (Pine-parity)
- **Candle type:** Heikin-Ashi (`USE_HA_CANDLES=true`) — mandatory for 78% WR
- **Lookback:** 5 bars (`VS_LOOKBACK=5`)
- **Burst detection:** HA body ≥ `chopAvgTR × 2.0` (`VS_BURST_MULT=2.0`)
- **Min body filter:** 50 pts (`MIN_BODY_PTS=50.0`) — blocks tiny-chop signals
- **Breakout context:** ON, 5 bars (`USE_BREAKOUT_CTX=true`, `BREAKOUT_CTX_BARS=5`) — burst must break prior 5-bar range
- **Cooldown:** 3 bars after signal (`VS_COOLDOWN=3`)
- **Session filter:** OFF — trades 24/7
- **EMA filter:** OFF

### SL/TP Model
| Parameter | Value | Notes |
|---|---|---|
| SL | **15 pts fixed** | Software monitor + exchange bracket stop-market |
| TP | **30 pts fixed** | Exchange bracket limit order (GTC) |
| R:R | 2:1 | TP = 2× SL |
| Entry | Breakout stop @ signal HIGH/LOW | BUY: enters only if next bar breaks above signal HA-HIGH |

### Entry Mode: Breakout Stop (matches Pine `pLimit := high`)
- Signal bar closes → bot arms `watch_breakout_entry(trigger=signal_candle.HIGH)`
- BUY entry fires only when `mark_price >= signal_HIGH` (momentum confirmed)
- SELL entry fires only when `mark_price <= signal_LOW`
- Timeout: 120s (`ENTRY_LIMIT_TIMEOUT_S=120`) → skip if no breakout in window
- Stale guard: skip if bar closed >30s ago (`MAX_SIGNAL_AGE_S=30`)

### Exit
- **TP:** Exchange bracket limit order — fills automatically when price hits TP
- **SL:** Exchange bracket stop-market (survives bot crash) + software monitor backup
- **Monitor:** Wakes on every WS mark_price tick (~50ms) — stale-price sanity guard (reject >500pt jumps)
- **Manual:** `/api/close` or dashboard "Close Trade" button

---

## Files

| File | Purpose |
|---|---|
| `volsurge_v5_live.py` | Main bot — signal handling, order execution, FastAPI dashboard |
| `candle_feed.py` | Delta WebSocket 1m candle feed + REST backfill (300 bars) |
| `signal_engine.py` | Vol Surge v5 signal logic — Pine-parity, timeframe-agnostic |
| `private_ws.py` | Delta private WebSocket — instant fill detection via orders/trades channels |
| `fly.toml` | Fly.io deploy config — env vars, volume mount, health check |
| `Dockerfile` | Docker image — python:3.11-slim + uvicorn |
| `requirements_v5.txt` | Python deps |
| `docs/pine_volsurge_v5.pine` | TradingView Pine script — visual reference + journal (SL=15, TP=30) |

---

## Live Config (fly.toml [env])

```
CANDLE_SECONDS        = 60        # 1-minute bars
FIXED_SL_PTS          = 15.0      # Fixed SL distance in points
FIXED_TP_PTS          = 30.0      # Fixed TP distance in points
MIN_BODY_PTS          = 50.0      # Min HA body to qualify as burst
USE_BREAKOUT_CTX      = true      # Burst must break prior 5-bar range
BREAKOUT_CTX_BARS     = 5         # Range lookback bars
USE_LIMIT_ENTRY       = true      # Breakout stop entry (not market)
ENTRY_LIMIT_TIMEOUT_S = 120       # Wait up to 120s for breakout
MAX_SIGNAL_AGE_S      = 30        # Skip stale signals
VS_BURST_MULT         = 2.0       # From .env
SL_MULT               = 1.8       # From .env (ATR-based SL dist, used only if FIXED_SL_PTS=0)
USE_HA_CANDLES        = true      # From .env
```

---

## Deployment

**Platform:** Fly.io — `nrt` region (Tokyo, co-located with Delta AWS Tokyo)
**App name:** `millionare-shivji-tradingbot`
**Volume:** `volsurge_5m_data` → `/data` (persistent trades + logs)
**Auto-deploy:** GitHub push → Fly.io fetches and redeploys automatically

```bash
# Manual deploy
flyctl deploy --app millionare-shivji-tradingbot

# Logs
flyctl logs --app millionare-shivji-tradingbot

# SSH
flyctl ssh console --app millionare-shivji-tradingbot

# Get machine IP (for Delta API whitelist)
flyctl ssh console -C "curl ifconfig.me" --app millionare-shivji-tradingbot
```

---

## Fly.io Secrets Required

| Secret | Description |
|---|---|
| `DELTA_API_KEY_LIVE` | Delta Exchange live API key |
| `DELTA_API_SECRET_LIVE` | Delta Exchange live API secret |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Telegram chat ID (757555299) |
| `PAPER_MODE` | `false` for live, `true` for paper |
| `LOT_SIZE` | BTC lot size (`0.001`) |

> **IP Whitelist:** Delta API key must whitelist Fly.io machine egress IP.
> On redeploy to new machine, get new IP via `flyctl ssh console -C "curl ifconfig.me"`.

---

## Dashboard Endpoints

| Endpoint | Description |
|---|---|
| `/dashboard` | Full HTML trading dashboard |
| `/health` | JSON — preflight status, WS feed health, current price |
| `/api/live` | Live price + unrealised PnL |
| `/api/stream` | SSE price stream (~200ms) |
| `/api/close` | Manually close open trade |
| `/data` | Download trades CSV |
| `/preflight` | Run pre-flight validation checks |

---

## Important Notes

- **Delta India rejects `stop_market_order`** — SL enforced by software monitor + bracket order
- **Heikin-Ashi mandatory** — regular OHLC gives 49% WR vs 78% WR on HA
- **Bracket order** — single `/v2/orders/bracket` call places both SL stop + TP limit server-side; survives bot crash
- **Stale-price guard** — position monitor rejects WS price jumps >500pts (prevents false SL on reconnect)
- **Breakout entry** — bot only enters when signal candle HIGH/LOW is broken by next bar; matches Pine `[BREAKOUT@HIGH/LOW]`
- **Pine parity** — `signal_engine.py` implements exact Pine v5 math: HA conversion, Wilder RMA ATR, chop avg TR, burst detection

---

## Related Bots

| Bot | Folder | App | Timeframe |
|---|---|---|---|
| **🤖 Mummy (this)** | `volsurge_1m_Mummy_Live` | `millionare-shivji-tradingbot` | **1 min** |
| 👤 Anand | `volSurge_1min_Anand_Live` | `millionare-shivji-1min-bot` | 1 min |

---

## Changelog

### 04-Jun-2026 ~15:00 IST
**Bug fix: Breakout trigger used real candle HIGH/LOW instead of HA HIGH/LOW**
- `_breakout_px = candle.high` → fixed to `_breakout_px = ha_candle.high`
- Same fix as Anand bot — both bots now use HA trigger to match Pine `pLimit := high`
- File: `volsurge_v5_live.py`

### 04-Jun-2026 ~13:00 IST
**Session: Parity audit + documentation update**
- Verified full parity between Mummy Pine script (`docs/pine_volsurge_v5.pine`) and live bot config (`fly.toml`) — all params match (SL=15, TP=30, MIN_BODY=50, breakout ctx=true, bars=5, limit entry=true)
- Confirmed entry mode: `watch_breakout_entry` with trigger = signal candle HIGH (BUY) / LOW (SELL) — matches Pine `pLimit := high`
- Identified that Mummy Pine title `[LIMIT@HA-CLOSE]` is stale/incorrect — code uses breakout at HIGH/LOW
- Updated `Project_Understanding.md` — corrected from 5min to 1min, corrected SL/TP from ATR-based to fixed 15/30, added full config, added changelog

<!-- deploy-test: 04-Jun-2026 14:00 IST -->
