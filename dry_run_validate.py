#!/usr/bin/env python3
"""
dry_run_validate.py — Vol Surge v5 Parity Dry Run
===================================================
PURPOSE:
  Fetch LIVE candles from Delta Exchange REST API (same data the bot will use),
  run them through signal_engine.py (with Heikin-Ashi conversion),
  and print a comparison table you can check against TradingView's VOL SURGE status panel.

HOW TO USE:
  1. Open TradingView with your Vol Surge Pine script on BTCUSD 15m HA chart
  2. Note down the values in the VOL SURGE status panel (top-right table)
  3. Run this script:
       python dry_run_validate.py
  4. Compare the printed values with TradingView — they should match exactly

IF THEY MATCH → v5 WebSocket bot will work identically to TradingView Pine.
IF THEY DON'T MATCH → something needs fixing before going live.

Run from: C:\\Users\\ANAND SONI\\OneDrive\\Desktop\\TradingBots\\volsurge_15m\\
"""

import sys
import os
import requests
from collections import deque
from datetime import datetime, timezone, timedelta
import time

# Fix Windows terminal encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Add current folder to path so signal_engine imports work ──────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from candle_feed import Candle
from signal_engine import SignalEngine, SignalConfig, compute_ha_candles

# ── Config ────────────────────────────────────────────────────────────────────
REST_URL   = "https://api.india.delta.exchange"
SYMBOL     = "BTCUSD"
NUM_CANDLES = 300    # fetch 300 bars for EMA200 warmup
RESOLUTION  = "15m"  # 15-minute candles

IST = timezone(timedelta(hours=5, minutes=30))

def ist(ts): return datetime.fromtimestamp(ts, tz=IST).strftime("%d/%m %H:%M IST")
def utc(ts): return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M UTC")

# ── Step 1: Fetch candles from Delta REST ─────────────────────────────────────
def fetch_candles(count=300):
    print(f"\n{'='*65}")
    print(f"  STEP 1 — Fetching {count} x 15m candles from Delta Exchange REST")
    print(f"  Symbol: {SYMBOL}  |  Resolution: {RESOLUTION}")
    print(f"{'='*65}")

    end_ts   = int(time.time())
    start_ts = end_ts - count * 900 - 1800   # 900s per 15m bar + buffer

    try:
        resp = requests.get(
            f"{REST_URL}/v2/history/candles",
            params={"symbol": SYMBOL, "resolution": RESOLUTION,
                    "start": start_ts, "end": end_ts},
            timeout=20,
        )
        raw = resp.json()
    except Exception as e:
        print(f"\n❌ REST fetch failed: {e}")
        print("   Check your internet connection and that Delta Exchange is accessible.")
        sys.exit(1)

    candles_raw = raw.get("result", raw.get("data", []))
    if not candles_raw:
        print(f"\n❌ Delta returned empty candle data. Raw response preview:\n{str(raw)[:300]}")
        sys.exit(1)

    # Delta returns newest-first → reverse to oldest-first
    candles = []
    for c in reversed(candles_raw):
        ts = int(c.get("time", c.get("start", c.get("candle_start_time", 0))))
        if ts > 1_000_000_000_000:
            ts = ts // 1000
        if ts == 0:
            continue
        candles.append(Candle(
            ts=ts,
            open=float(c.get("open", 0)),
            high=float(c.get("high", 0)),
            low=float(c.get("low", 0)),
            close=float(c.get("close", 0)),
            volume=float(c.get("volume", c.get("turnover", 0))),
        ))

    print(f"\n  ✅ Fetched {len(candles)} candles")
    print(f"  Oldest : {ist(candles[0].ts)}  —  O:{candles[0].open:.1f} H:{candles[0].high:.1f} L:{candles[0].low:.1f} C:{candles[0].close:.1f}")
    print(f"  Newest : {ist(candles[-1].ts)}  —  O:{candles[-1].open:.1f} H:{candles[-1].high:.1f} L:{candles[-1].low:.1f} C:{candles[-1].close:.1f}")
    return candles


# ── Step 2: Show last 6 raw vs HA candles ─────────────────────────────────────
def show_ha_conversion(raw_candles):
    print(f"\n{'='*65}")
    print(f"  STEP 2 — Heikin-Ashi Conversion (last 6 bars)")
    print(f"  Compare these HA values with your TradingView HA chart")
    print(f"{'='*65}")

    ha_candles = compute_ha_candles(raw_candles)

    print(f"\n  {'Bar':<18} {'RAW Close':>12} {'HA Open':>10} {'HA High':>10} {'HA Low':>10} {'HA Close':>10}")
    print(f"  {'-'*73}")
    for i in range(-6, 0):
        r = raw_candles[i]
        h = ha_candles[i]
        marker = " ← LATEST CLOSED BAR" if i == -1 else ""
        print(f"  {ist(r.ts):<18} {r.close:>12.1f} {h.open:>10.1f} {h.high:>10.1f} {h.low:>10.1f} {h.close:>10.1f}{marker}")

    print(f"\n  👆 Open TradingView → right-click any HA candle → 'Data Window'")
    print(f"     The HA OHLC values should match the table above exactly.")


# ── Step 3: Run signal engine and print indicator state ───────────────────────
def run_engine(raw_candles, num_bars=5):
    print(f"\n{'='*65}")
    print(f"  STEP 3 — Signal Engine Output (last {num_bars} bars)")
    print(f"  Compare with TradingView VOL SURGE status panel (top-right)")
    print(f"{'='*65}")

    cfg = SignalConfig(
        lookback=5, burst_mult=2.0, sl_mult=1.8, tp2_r=1.4,
        cooldown=3, use_ema_filter=False, use_session=False,
        safety_factor=1.0, use_ha=True,
    )
    engine = SignalEngine(config=cfg)

    # Feed all candles except the last `num_bars` to warm up engine
    buf = deque(maxlen=300)
    results = []

    for i, c in enumerate(raw_candles):
        buf.append(c)
        if i < len(raw_candles) - num_bars:
            # Warm-up phase — run engine silently
            engine._bar_count += 1
            from signal_engine import compute_ha_candles as _cha, compute_indicators
            ha_buf = deque(maxlen=300)
            ha_buf.extend(compute_ha_candles(list(buf)))
            state = compute_indicators(buf, cfg, engine._cooldown, in_trade=False)
            if state and state.signal in ("BUY", "SELL"):
                engine._cooldown = cfg.cooldown
            elif engine._cooldown > 0:
                engine._cooldown -= 1
        else:
            # Last num_bars — capture and display
            state = engine.on_candle_close(c, buf, in_trade=False)
            if state:
                results.append((c, state))

    print(f"\n  {'Bar':<18} {'HA Close':>10} {'Chop TR':>9} {'Burst Need':>12} {'Curr Body':>11} {'ATR5[1]':>9} {'SL Dist':>9} {'Signal':>8}")
    print(f"  {'-'*95}")

    last_state = None
    for candle, state in results:
        signal_str = f"🔥 {state.signal}" if state.signal else "—"
        body_marker = " ✓ BURST!" if state.is_burst_bull or state.is_burst_bear else ""
        print(
            f"  {ist(candle.ts):<18} "
            f"{state.close:>10.1f} "
            f"{state.chop_avg_tr:>9.1f} "
            f"{state.burst_threshold:>12.1f} "
            f"{state.candle_body:>11.1f}{body_marker:<10}"
            f"{state.atr5_prev:>9.2f} "
            f"{state.sl_dist:>9.1f} "
            f"{signal_str:>8}"
        )
        last_state = state

    return last_state


# ── Step 4: Print comparison checklist ────────────────────────────────────────
def print_checklist(state):
    if not state:
        print("\n❌ No state computed — not enough bars.")
        return

    print(f"\n{'='*65}")
    print(f"  STEP 4 — Compare Latest Bar vs TradingView Status Panel")
    print(f"{'='*65}")
    print(f"""
  Open TradingView → VOL SURGE status table (top-right of chart)
  Check each row against the values below:

  +-----------------------------+--------------+------------------+
  | Field                       | Our Engine   | TradingView Pine |
  +-----------------------------+--------------+------------------+
  | Chop avg TR                 | {state.chop_avg_tr:>8.1f} pts | ____________ pts |
  | Burst needs (body)          | {state.burst_threshold:>8.1f} pts | ____________ pts |
  | Current body                | {state.candle_body:>8.1f} pts | ____________ pts |
  | ATR5 (prev bar)             | {state.atr5_prev:>8.2f} pts | ____________ pts |
  | SL distance (ATR x 1.8)     | {state.sl_dist:>8.1f} pts | ____________ pts |
  | EMA200                      | {state.ema200:>8.1f}     | ____________     |
  | Close (HA)                  | {state.close:>8.1f}     | ____________     |
  +-----------------------------+--------------+------------------+

  Fill in the TradingView column manually and compare.

  ✅ VALUES MATCH   → Engine is identical to Pine. v5 is ready.
  ❌ VALUES DIFFER  → Share the differences here and we will fix.
""")

    if state.signal:
        print(f"  🔥 SIGNAL on latest bar: {state.signal}")
        sr = engine_ref.build_signal_result(state)
        if sr:
            print(f"     Entry : {sr.entry_price:,.1f}")
            print(f"     SL    : {sr.sl:,.1f}  (−{sr.sl_dist:.1f} pts)")
            print(f"     TP    : {sr.tp2:,.1f}  (+{round(abs(sr.tp2-sr.entry_price),1):.1f} pts = 1.4R)")
    else:
        print(f"  No signal on latest bar (body {state.candle_body:.1f} < burst need {state.burst_threshold:.1f})")
        print(f"  Body needs to be {state.burst_threshold - state.candle_body:.1f} pts bigger to trigger a signal.")


# ── Step 5: WebSocket connectivity check ──────────────────────────────────────
def check_websocket():
    print(f"\n{'='*65}")
    print(f"  STEP 5 — WebSocket Connectivity Check")
    print(f"{'='*65}")
    import socket
    try:
        socket.setdefaulttimeout(5)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(("socket.india.delta.exchange", 443))
        print(f"\n  ✅ Delta WebSocket endpoint reachable: socket.india.delta.exchange:443")
        print(f"     When the bot runs, it will subscribe to candlestick_15m on this connection.")
    except Exception as e:
        print(f"\n  ⚠️  WebSocket connectivity check failed: {e}")
        print(f"     This may just be a firewall/proxy issue — the actual websockets library may still work.")


# ── Main ──────────────────────────────────────────────────────────────────────
engine_ref = None

if __name__ == "__main__":
    print("""
================================================================
  VOL SURGE v5 -- DRY RUN PARITY VALIDATION
  Delta Exchange Live Data  <->  Signal Engine Output
================================================================
  This script fetches REAL live candles from Delta Exchange,
  converts them to Heikin-Ashi, runs the Vol Surge engine,
  and prints values you can compare against TradingView.

  If numbers match -> bot is 100% ready, no TradingView needed.
""")

    # Run all steps
    raw_candles = fetch_candles(NUM_CANDLES)
    show_ha_conversion(raw_candles)

    cfg = SignalConfig(
        lookback=5, burst_mult=2.0, sl_mult=1.8, tp2_r=1.4,
        cooldown=3, use_ema_filter=False, use_session=False,
        safety_factor=1.0, use_ha=True,
    )
    engine_ref = SignalEngine(config=cfg)

    last_state = run_engine(raw_candles, num_bars=5)
    print_checklist(last_state)
    check_websocket()

    print(f"\n{'='*65}")
    print(f"  DRY RUN COMPLETE")
    print(f"  Compare Step 4 table with TradingView and report back.")
    print(f"{'='*65}\n")
