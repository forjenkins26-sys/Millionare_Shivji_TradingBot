# Vol Surge v5 Live — Full Code Audit Report

**Date:** 2026-05-19  
**File audited:** `volsurge_v5_live.py`  
**Support files:** `signal_engine.py`, `candle_feed.py`  
**Pine reference:** `docs/pine_volsurge_v5.pine`  
**Folder rename required:** `volsurge_5m` → `volsurge_15m` *(do manually in Windows Explorer — OneDrive lock prevented auto-rename)*

---

## Architecture: Vol Surge v5 runs on 15-minute candles

```
Delta WebSocket (candlestick_15m)
    → CandleFeed (300-bar ring buffer, 15m bars)
        → on_candle_close callback (fires on each 15m bar close)
            → SignalEngine.on_candle_close(candle, buffer, in_trade)
                → IndicatorState (signal = "BUY" / "SELL" / "")
                    → _process_entry thread
                        → Delta market order → fill-based SL/TP
```

| Bot | Folder | CandleFeed channel | `signal_timeframe` |
|---|---|---|---|
| **Trendline** | `trendline_3m/` | `candlestick_15m` | `"15"` |
| **Vol Surge v5** | `volsurge_15m/` | `candlestick_15m` | `"15"` ✅ |

---

## All Bugs Found & Fixed in This Session

### Bug 1 — `signal_timeframe` mislabelled "15" → was changed to "5" → corrected back to "15"
**File:** `volsurge_v5_live.py` line 1135  
Caused by earlier confusion about 5m vs 15m. Confirmed: Vol Surge v5 uses **15m candles**.  
**Final value: `"15"`** ✅

---

### Bug 2 — `pine_signal_time` used bar START instead of bar CLOSE
**File:** `volsurge_v5_live.py` line 1132  
**Impact:** `signal_latency_ms` would always show ~900,000ms (15 minutes) instead of the correct ~50–500ms.

```python
# BEFORE (wrong — sr.ts is bar START timestamp)
"pine_signal_time":  sr.ts * 1000

# AFTER (correct — bar CLOSE = start + 15m = start + 900s)
"pine_signal_time":  (sr.ts + 900) * 1000
```

**Math with fix:**
```
bar starts at  : sr.ts  (Unix seconds)
bar closes at  : sr.ts + 900  (15m = 900 seconds later)
recv_time      : sr.ts + 900 + <tiny processing>  (time.time())

signal_latency_ms = (recv_time - pine_signal_time/1000) * 1000
                  = (recv_time - (sr.ts + 900)) * 1000
                  ≈  50–500 ms  ✅   (was ~900,000 ms ❌)
```

---

### Bug 3 — `signal_engine.py` defaults didn't match Pine
**File:** `signal_engine.py` — `SignalConfig` dataclass defaults  
**Impact:** Wrong SL distances and TP targets if `SignalEngine()` used directly without env var overrides.

| Parameter | Pine value | Old default | Fixed default |
|---|---|---|---|
| `sl_mult` (vsSLMult) | **1.8** | 0.75 ❌ | **1.8** ✅ |
| `tp2_r` (vsTP2R) | **1.4** | 2.0 ❌ | **1.4** ✅ |
| `safety_factor` | **1.0** (exact Pine parity) | 1.15 ❌ | **1.0** ✅ |

---

### Bug 4 — Feed watchdog stale threshold wrong for 15m bars
**File:** `volsurge_v5_live.py` — `_feed_watchdog()`  
Old threshold was 420s (7 min — tuned for 5m bars). For 15m bars, a stale feed alert should fire after 20 min.

```python
# BEFORE (wrong — 5m tuning)
if age > 420:

# AFTER (correct — 15m: warn if no bar in 20min)
if age > 1200:   # 15m bars: warn if no bar in 20min (1.3× bar period)
```

---

### candle_feed.py — All 5m references updated to 15m

| Location | Before | After |
|---|---|---|
| WebSocket subscription channel | `candlestick_5m` | `candlestick_15m` |
| REST backfill resolution | `"5m"` | `"15m"` |
| REST backfill time range | `count * 300 + 600` | `count * 900 + 1800` |
| Gap fill: next bar start | `last_closed.ts + 300` | `last_closed.ts + 900` |
| Gap fill: bars count | `gap_end - gap_start) // 300` | `(gap_end - gap_start) // 900` |
| Gap fill REST resolution | `"5m"` | `"15m"` |
| Docstring / comments | "5-minute candles" | "15-minute candles" |

---

### parity_tests.py — Bar spacing updated to 15m

```python
# BEFORE
ts = BASE_TS + i * 300   # 5-min bars

# AFTER
ts = BASE_TS + i * 900   # 15-min bars (900s each)
```

---

## Parameter Alignment Audit — Pine ↔ Python (Final State)

| Parameter | Pine (`pine_volsurge_v5.pine`) | `signal_engine.py` | `volsurge_v5_live.py` env default | ✓ |
|---|---|---|---|---|
| `vsLookback` | `5` | `lookback=5` | `VS_LOOKBACK=5` | ✅ |
| `vsBurstMult` | `2.0` | `burst_mult=2.0` | `VS_BURST_MULT=2.0` | ✅ |
| `vsSLMult` | `1.8` | `sl_mult=1.8` | `SL_MULT=1.8` | ✅ |
| `vsTP2R` | **1.4** | `tp2_r=1.4` | `TP_R=1.4` | ✅ |
| `vsCooldown` | `3` | `cooldown=3` | `VS_COOLDOWN=3` | ✅ |
| `useEmaFilt` | `false` | `use_ema_filter=False` | `USE_EMA_FILTER=false` | ✅ |
| `useSession` | `false` | `use_session=False` | `USE_SESSION=false` | ✅ |
| `safety_factor` | N/A (1.0 = exact parity) | `safety_factor=1.0` | `SIGNAL_SAFETY_FACTOR=1.0` | ✅ |
| `emaLen` | `200` | `ema_length=200` | (hardcoded) | ✅ |
| Timeframe | `15m` (Pine chart TF) | N/A | `candlestick_15m` | ✅ |
| TP R-multiple | **1.4R** | `tp2_r=1.4` | `TP_R=1.4` | ✅ |

---

## Indicators — Pine Parity (Verified)

| Indicator | Pine formula | Python | Match |
|---|---|---|---|
| True Range | `ta.tr(true)` | `compute_tr(H, L, prev_close)` | ✅ |
| ATR5 | `ta.atr(5)` Wilder RMA | `compute_atr_rma(trs, period=5)` | ✅ |
| EMA200 | `ta.ema(close, 200)` | `compute_ema_series(closes, 200)` | ✅ |
| ChopAvgTR | `avg TR[1..lookback]` (prev bars only) | `trs[-(i+2)] for i in range(lookback)` | ✅ |
| Burst threshold | `chopAvgTR × burstMult` | `chop_avg_tr * burst_mult` | ✅ |
| SL distance | `atr5[1] × slMult` (prev bar) | `atr5_prev * sl_mult` | ✅ |
| Cooldown | decrement before check | `effective_cooldown = max(0, cooldown_left - 1)` | ✅ |

---

## candle_feed.py — Final Configuration

| Check | Value | Status |
|---|---|---|
| WS URL | `wss://socket.india.delta.exchange` | ✅ |
| REST URL | `https://api.india.delta.exchange` | ✅ |
| WS subscription channel | `candlestick_15m` | ✅ |
| REST backfill resolution | `"15m"` | ✅ |
| Bar duration (seconds) | `900` | ✅ |
| Buffer size | `300` candles = ~75 hours of 15m data | ✅ |
| EMA200 warmup guard | `is_ready` true at ≥ 250 bars | ✅ |
| Reconnect | Exponential backoff 1s → 60s | ✅ |
| Gap fill on reconnect | REST backfill of missed 15m bars | ✅ |
| Heartbeat | Every 30s | ✅ |
| Mark price | Available for PAPER fill simulation | ✅ |
| Stale feed watchdog threshold | `> 1200s` (20 min) | ✅ |

---

## State / File Audit — No v4 Conflicts

| File | Path | v4 conflict? |
|---|---|---|
| State | `data/state_v5.json` | ❌ No |
| Trades | `data/trades_v5.csv` | ❌ No |
| Reconciliation | `data/reconciliation_v5.csv` | ❌ No |
| Order lifecycle | `data/order_lifecycle_v5.csv` | ❌ No |
| Latency log | `data/execution_latency_v5.csv` | ❌ No |
| Slippage audit | `data/slippage_audit_v5.csv` | ❌ No |
| Log | `logs/volsurge_v5_live.log` | ❌ No |

---

## Complete Change Log — All Files Modified This Session

| # | File | What changed | Why |
|---|---|---|---|
| 1 | `volsurge_v5_live.py` | `signal_timeframe = "15"` (final) | 15m candle feed, matches Pine TF |
| 2 | `volsurge_v5_live.py` | `pine_signal_time = (sr.ts + 900) * 1000` | Bar CLOSE not bar START (15m=900s) |
| 3 | `volsurge_v5_live.py` | Feed watchdog `age > 1200` (was 420) | 15m bar period = 900s |
| 4 | `volsurge_v5_live.py` | Docstring updated → "15m" | Accuracy |
| 5 | `signal_engine.py` | `sl_mult 0.75 → 1.8` | Match Pine vsSLMult |
| 6 | `signal_engine.py` | `tp2_r 2.0 → 1.4` | Match Pine vsTP2R = **1.4R** |
| 7 | `signal_engine.py` | `safety_factor 1.15 → 1.0` | Exact Pine parity |
| 8 | `candle_feed.py` | Subscribe `candlestick_15m` | 15m signal source |
| 9 | `candle_feed.py` | REST resolution `"15m"` | Backfill matches feed |
| 10 | `candle_feed.py` | Bar duration `300 → 900` (backfill & gap fill) | 15m = 900s per bar |
| 11 | `candle_feed.py` | Watchdog stale threshold update | 15m tuning |
| 12 | `candle_feed.py` | All comments "5-minute" → "15-minute" | Accuracy |
| 13 | `tests/parity_tests.py` | Bar ts spacing `300 → 900` | 15m bar timestamps |
| 14 | `tests/parity_tests.py` | Comment `sl_mult=0.75 → 1.8` | Accuracy |
| 15 | `tests/parity_tests.py` | Docstring path `volsurge_5m → volsurge_15m` | Folder rename |
| 16 | `V5_LIVE_AUDIT.md` | Recreated with full 15m config | This report |

---

## Pending: Manual Folder Rename

The local folder rename from `volsurge_5m` → `volsurge_15m` **could not be done automatically** because OneDrive had a file lock on the folder.

**Action required by user:**
1. Open Windows Explorer
2. Navigate to `Desktop → TradingBots`
3. Right-click `volsurge_5m` → Rename → type `volsurge_15m` → Enter

> The git remote (`Millionare_Shivji_TradingBot` on GitHub) is unaffected by the local folder name.
> Railway deploys from git — also unaffected.

---

## Before Going Live — Required Actions

1. **Rename folder** `volsurge_5m` → `volsurge_15m` in Windows Explorer
2. **Local paper test**: Run `PAPER_MODE=true` locally — confirm 15m candles arrive, signals fire at bar close with correct SL (1.8× ATR5) and TP (1.4R)
3. **Validate 20–50 paper trades** before switching `PAPER_MODE=false`
4. **Railway env vars when deploying v5:**

```
PAPER_MODE=true
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
```

5. **Update Procfile** when ready to switch from v4 to v5:
```
web: uvicorn volsurge_v5_live:app --host 0.0.0.0 --port $PORT --workers 1
```
