# Vol Surge v5 — WebSocket-Native Bot: What Was Built & How to Use It

**Date built:** 2026-05-19  
**Folder:** `C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volsurge_15m\`  
**GitHub:** `forjenkins26-sys/Millionare_Shivji_TradingBot`

---

## Why v5 Was Built

### The Problem with v4 (Webhook-based)

v4 depends on TradingView → Webhook → Railway. On 18 May 2026, a BUY signal at 17:15 IST was **missed** because:

```
TradingView fired alert at 17:15 IST
  → Pine computed timenow
  → TV queued alert internally (2–5s delivery lag)
  → Webhook arrived at Railway: 5967ms after Pine fired
  → Bot had MAX_WH_LATENCY_MS=5000 → REJECTED as "stale"
  → Trade missed: BUY 77,057.8 → TP2 +358.9 pts ❌
```

Even after disabling the latency gate (`MAX_WH_LATENCY_MS=0`), the **TV delivery lag of 5–7 seconds is inherent** and cannot be eliminated. Every signal arrives late.

### The v5 Solution

v5 **removes TradingView from the signal path entirely**. The bot computes the Vol Surge signal itself, directly from Delta Exchange's own WebSocket feed.

```
v4 flow:  Pine Script → TradingView Alert → Webhook (5–7s delay) → Bot → Delta Order
v5 flow:  Delta WebSocket (15m candle) → Bot signal engine → Delta Order  (<100ms)
```

| Metric | v4 (webhook) | v5 (WebSocket) |
|---|---|---|
| Signal source | TradingView Pine | Python (Pine-parity engine) |
| Signal latency | 5–7 seconds | < 100ms |
| TV dependency | Yes — outage = missed trades | None |
| Latency gate | MAX_WH_LATENCY_MS (was causing rejections) | Not needed — removed |

---

## What Was Built

### Three new files

```
volsurge_15m/
├── volsurge_v5_live.py    ← Main bot (NEW) — WebSocket-native live/paper bot
├── signal_engine.py       ← Pine-parity signal computation (NEW)
└── candle_feed.py         ← Delta WebSocket 15m candle buffer (NEW)
```

### How they connect

```
candle_feed.py          signal_engine.py         volsurge_v5_live.py
──────────────          ────────────────         ───────────────────
CandleFeed              SignalEngine              on_candle_close()
  │                       │                        │
  │  Delta WebSocket       │  compute_indicators()  │  _process_entry() thread
  │  candlestick_15m       │  ATR5, EMA200,         │  Delta market order
  │  ─────────────►        │  ChopAvgTR, Burst      │  Fill-based SL/TP
  │                        │  ─────────────►        │
  │  300-bar buffer        │  IndicatorState        │  _position_monitor() thread
  │  (75 hours of 15m)     │  signal="BUY/SELL/""   │  TP/SL detection
  │                        └────────────────────────┘
  └──► on_candle_close callback ──►
```

---

## signal_engine.py — Pine Parity

This file is a Python port of Pine's Vol Surge logic. Every indicator matches Pine **exactly**.

### Parameters (all match `pine_volsurge_v5.pine` defaults)

| Pine input | Python (`SignalConfig`) | Default |
|---|---|---|
| `vsLookback` | `lookback` | `5` |
| `vsBurstMult` | `burst_mult` | `2.0` |
| `vsSLMult` | `sl_mult` | `1.8` |
| `vsTP2R` | `tp2_r` | `1.4` |
| `vsCooldown` | `cooldown` | `3` |
| `useEmaFilt` | `use_ema_filter` | `False` |
| `useSession` | `use_session` | `False` |

### Indicator formulas (Pine parity verified)

| Indicator | Pine formula | Python |
|---|---|---|
| True Range | `ta.tr(true)` | `max(H-L, │H-prevC│, │L-prevC│)` |
| ATR5 | `ta.atr(5)` Wilder RMA, alpha=1/5 | `compute_atr_rma(trs, period=5)` |
| EMA200 | `ta.ema(close, 200)` | `compute_ema_series(closes, 200)` |
| ChopAvgTR | `avg TR[1..lookback]` prev bars only | uses `trs[-(i+2)]` — excludes current bar |
| Burst threshold | `chopAvgTR × burstMult` | `chop_avg_tr * burst_mult` |
| SL distance | `atr5[1] × slMult` (prev bar ATR) | `atr5_prev * sl_mult` |
| Cooldown | decrement then check | `effective_cooldown = max(0, cooldown_left - 1)` |

---

## candle_feed.py — 15m WebSocket Feed

Subscribes to Delta India WebSocket, maintains a rolling buffer of 300 closed 15m candles.

### Key behaviour
- **Startup:** REST backfill of 300 candles (~75 hours) before WebSocket connects
- **Live:** WebSocket `candlestick_15m` channel — fires callback on each bar close
- **Reconnect:** Exponential backoff (1s → 60s), REST gap-fill after reconnect
- **Mark price:** Available from `mark_price` WebSocket channel — used for PAPER fills
- **Ready flag:** `feed.is_ready` = True once ≥250 bars loaded (enough for EMA200 warmup)

### How bar close is detected

Delta doesn't always send an explicit `closed=true` flag. The feed tracks by start timestamp:

```
New start timestamp arrives ≠ forming candle's start
    → Previous forming candle is definitively CLOSED
    → _emit_closed() fires
    → on_candle_close callback called
    → SignalEngine runs
```

---

## volsurge_v5_live.py — The Live Bot

### Key differences from v4

| Feature | v4 | v5 |
|---|---|---|
| Signal source | `/webhook` endpoint (POST from TV) | `on_candle_close` callback from CandleFeed |
| Signal latency | 5–7 seconds (TV delivery) | < 100ms (native WebSocket) |
| `MAX_WH_LATENCY_MS` gate | Yes (was causing rejections) | Removed — not needed |
| Telegram entry label | `[v4 Webhook]` | `[v5 WebSocket]` |
| Latency field in CSV | `webhook_latency_ms` | `signal_latency_ms` |
| State files | `state.json`, `trades.csv` | `state_v5.json`, `trades_v5.csv` |
| `/webhook` endpoint | Yes | No |
| Feed health endpoint | No | `/health` shows `ws_connected`, `feed_ready`, `buffer_size` |

### What stays identical to v4
- Delta order placement (market entry, SL stop-market, TP limit)
- Fill-based SL/TP anchored to actual Delta fill price
- `_position_monitor` thread (PAPER price comparison / LIVE position-flat detection)
- Crash recovery (`state_v5.json` → resume on Railway restart)
- All CSV schemas (+ 5 new v5 signal context fields)
- Telegram notifications
- Pre-flight checks

### signal_latency_ms — the key new metric

```
signal_latency_ms = (recv_time - bar_close_time) * 1000

Where:
  bar_close_time = sr.ts + 900  (bar start + 15m)
  recv_time      = time.time() when engine fired the signal

Expected value: 50–500ms
If > 1000ms: feed or engine is lagging — investigate
```

---

## How to Run v5 Locally (Paper Mode)

### Step 1 — Install dependencies

```bash
cd "C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volsurge_15m"
pip install fastapi uvicorn requests websockets python-dotenv
```

### Step 2 — Create a `.env` file (or just set env vars)

```env
PAPER_MODE=true
SYMBOL=BTCUSD
PRODUCT_ID=27
SL_MULT=1.8
TP_R=1.4
VS_BURST_MULT=2.0
VS_LOOKBACK=5
VS_COOLDOWN=3
USE_EMA_FILTER=false
USE_SESSION=false
SIGNAL_SAFETY_FACTOR=1.0
LOT_SIZE=0.001
TELEGRAM_BOT_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
# No need for DELTA_API_KEY_LIVE in paper mode
```

### Step 3 — Run the bot

```bash
uvicorn volsurge_v5_live:app --host 0.0.0.0 --port 8080 --workers 1
```

### Step 4 — What you should see in logs

```
======================================================================
  Vol Surge v5 LIVE | 📄 PAPER mode
  Signal source : Delta WebSocket (no TradingView webhook)
  Symbol        : BTCUSD
  TP model      : Single exit at 1.4R — fill-based
  SL model      : Fixed stop-market — never moved
  Engine        : lookback=5 burst_mult=2.0
                  sl_mult=1.8 tp2_r=1.4
======================================================================
[FEED] REST backfill: requesting 300 candles...
[FEED] Backfill complete — 287 candles loaded | buffer=287
[FEED] Connecting WebSocket -> wss://socket.india.delta.exchange
[FEED] Subscribed to candlestick_15m + mark_price [BTCUSD]

# On each bar close (every 15 minutes):
[ENGINE] ── Bar #288 · 2026-05-19 12:00 UTC (17:30 IST) ────────────
  close          :    94,250.0
  candle body    :       182.0 pts
  chop_avg_tr    :       134.5 pts  (avg TR of 5 bars before)
  burst_threshold:       269.0 pts  (chop x 2.0)
  burst          : none  (body=182.0 < thresh=269.0)
  atr5[1]        :       148.2 pts  (prev bar -- Pine atr5[1])
  sl_dist        :       266.8 pts  (= atr5[1] x 1.8)
  ema200         :    93,100.0  (ABOVE ▲)  (filter OFF)
  SIGNAL         : —

# When a signal fires:
[ENGINE] 🔥 SIGNAL BUY
  entry : 94,250.0
  sl    : 93,983.2  (−266.8 pts)
  tp2   : 94,623.5  (+373.5 pts)  ← v5 uses this

[SIGNAL] BUY | entry=94,250.0 sl=93,983.2 tp=94,623.5 sl_dist=266.8
[PAPER] fill simulated @ 94,252.0
[STATE→ENTERED] BUY fill=94,252.0 slip=+2.0pts sig_lat=87ms entry_lat=143ms
```

### Step 5 — Check health endpoint

Open browser or curl:
```
http://localhost:8080/health
```

Response:
```json
{
  "status": "healthy",
  "bot": "Vol Surge v5 Live",
  "mode": "PAPER",
  "signal_source": "WebSocket-native (no TV webhook)",
  "ws_connected": true,
  "feed_ready": true,
  "buffer_size": 287,
  "mark_price": 94250.0,
  "trade": { "in_trade": false }
}
```

---

## How to Deploy v5 to Railway (when ready)

> ⚠️ **Do not deploy until 20–50 paper trades validated locally.**  
> v4 is currently running live on Railway — do not disrupt it.

### Step 1 — Update the Procfile

```
# Current (v4):
web: uvicorn volsurge_v4:app --host 0.0.0.0 --port $PORT --workers 1

# Change to (v5):
web: uvicorn volsurge_v5_live:app --host 0.0.0.0 --port $PORT --workers 1
```

### Step 2 — Set Railway Variables

Go to Railway → Millionare_Shivji_TradingBot → Variables tab:

```
PAPER_MODE=true              ← Keep true until confident
PRODUCT_ID=27
SYMBOL=BTCUSD
SL_MULT=1.8
TP_R=1.4
VS_BURST_MULT=2.0
VS_LOOKBACK=5
VS_COOLDOWN=3
USE_EMA_FILTER=false
USE_SESSION=false
SIGNAL_SAFETY_FACTOR=1.0
LOT_SIZE=0.001
MAX_WH_LATENCY_MS=           ← Leave blank or delete (not used in v5)
```

DELTA_API_KEY_LIVE and DELTA_API_SECRET_LIVE are already set on Railway — no change needed.

### Step 3 — Commit and push the Procfile change

```bash
git add Procfile
git commit -m "Switch to Vol Surge v5 WebSocket-native bot"
git push
```

Railway will redeploy automatically.

### Step 4 — Verify on Railway after deploy

1. Check Railway logs — should see `[FEED] Subscribed to candlestick_15m`
2. Open Railway URL `/health` — should show `ws_connected: true, feed_ready: true`
3. Wait for a 15m bar close — confirm `[ENGINE] ── Bar #...` log line appears
4. Check Telegram — should receive "📄 PAPER Vol Surge v5 started" message

### Step 5 — Switch to LIVE

Only after paper validation:
1. Set `PAPER_MODE=false` in Railway Variables
2. Railway auto-restarts
3. Pre-flight runs automatically — checks API auth, price feed, balance, no open position
4. If all pass → Telegram: "✅ Pre-flight PASSED"
5. Bot is now live

---

## Files Summary

| File | Purpose |
|---|---|
| `volsurge_v5_live.py` | Main v5 bot — WebSocket-native, paper + live |
| `signal_engine.py` | Pine-parity Vol Surge signal computation |
| `candle_feed.py` | Delta WebSocket 15m candle buffer |
| `tests/parity_tests.py` | Unit tests — verify signal engine matches Pine |
| `docs/pine_volsurge_v5.pine` | Pine script reference (TradingView source of truth) |
| `data/state_v5.json` | Crash recovery — open trade state |
| `data/trades_v5.csv` | Trade log |
| `data/execution_latency_v5.csv` | Entry latency audit |
| `data/slippage_audit_v5.csv` | Slippage tracking |
| `WEBHOOK_LATENCY_FIX.md` | Root cause doc for missed trade on 18/05 |
| `V5_LIVE_AUDIT.md` | Full parameter + bug audit for v5 |
| `VOLSURGE_V5_GUIDE.md` | This file |

---

## What v4 Does vs What v5 Does (Side-by-side)

```
v4 still running on Railway (do not touch)
  ├── Receives POST /webhook from TradingView
  ├── Parses signal: BUY/SELL, entry price, SL, TP from Pine payload
  ├── Places Delta market order
  ├── Calculates fill-based SL/TP from actual fill
  └── Files: state.json, trades.csv

v5 running locally in PAPER mode (validating)
  ├── Subscribes to Delta WebSocket candlestick_15m
  ├── Computes signal in Python (no TradingView dependency)
  ├── Places simulated paper fill at mark price
  ├── Calculates fill-based SL/TP from simulated fill
  └── Files: state_v5.json, trades_v5.csv (no conflict with v4)
```

Both bots use identical execution logic (orders, monitor, CSV, Telegram, recovery).  
The only difference is **where the signal comes from**.
