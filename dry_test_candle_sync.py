#!/usr/bin/env python3
"""
dry_test_candle_sync.py — Candle Sync Validation Tool
======================================================
Connects to Delta Exchange WebSocket and validates that the 5m candle data
received live is in sync with:
  1. Delta REST API (same instrument, same bar)
  2. TradingView (manual visual check — IST timestamps printed for comparison)

Run this LOCALLY (not on Railway) with the bot stopped.

Usage:
    python dry_test_candle_sync.py

Output:
  - Every 30s: prints the current forming bar (live tick update)
  - On each bar CLOSE: prints full comparison table (WS vs REST)
  - CSV log saved to: candle_sync_log.csv

Press Ctrl+C to stop.
"""

import asyncio
import json
import sys
import time
import csv
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
import websockets

# ── Config ────────────────────────────────────────────────────────────────────
WS_URL   = "wss://socket.india.delta.exchange"
REST_URL = "https://api.india.delta.exchange"
SYMBOL   = "BTCUSD"
IST      = timedelta(hours=5, minutes=30)
LOG_FILE = os.path.join(os.path.dirname(__file__), "candle_sync_log.csv")

# ── Helpers ───────────────────────────────────────────────────────────────────

def to_ist(unix_ts: float) -> str:
    """Convert Unix timestamp (UTC) to IST string."""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).astimezone(
        timezone(IST)
    ).strftime("%Y-%m-%d %H:%M:%S IST")

def now_ist() -> str:
    return to_ist(time.time())

def fetch_rest_candle(bar_start_ts: int) -> Optional[dict]:
    """
    Fetch the closed 5m candle from Delta REST for a given bar start timestamp.

    Delta's REST candle API has a ~60-90 second settle lag — the candle data
    continues updating for up to 90s after bar close. We poll with backoff
    until the close price stabilises (same value twice in a row) or 90s elapses.

    Returns dict with OHLCV or None on failure.
    """
    def _single_fetch():
        try:
            resp = requests.get(
                f"{REST_URL}/v2/history/candles",
                params={
                    "symbol":     SYMBOL,
                    "resolution": "5m",
                    "start":      bar_start_ts - 10,
                    "end":        bar_start_ts + 300 + 10,
                },
                timeout=10,
            )
            data = resp.json()
            candles = data.get("result", data.get("data", []))
            for c in candles:
                ts = int(c.get("time", c.get("start", c.get("candle_start_time", 0))))
                if ts > 1_000_000_000_000:
                    ts = ts // 1000
                if ts == bar_start_ts:
                    return {
                        "ts":     ts,
                        "open":   float(c.get("open",   0)),
                        "high":   float(c.get("high",   0)),
                        "low":    float(c.get("low",    0)),
                        "close":  float(c.get("close",  0)),
                        "volume": float(c.get("volume", c.get("turnover", 0))),
                    }
            if candles:
                c = candles[0]
                ts = int(c.get("time", c.get("start", 0)))
                if ts > 1_000_000_000_000:
                    ts = ts // 1000
                return {
                    "ts":     ts,
                    "open":   float(c.get("open",   0)),
                    "high":   float(c.get("high",   0)),
                    "low":    float(c.get("low",    0)),
                    "close":  float(c.get("close",  0)),
                    "volume": float(c.get("volume", c.get("turnover", 0))),
                    "_note":  f"CLOSEST (requested ts={bar_start_ts}, got ts={ts})",
                }
            return None
        except Exception as e:
            print(f"  [REST ERROR] {e}")
            return None

    # Poll with backoff until close stabilises or 90s elapsed
    # Delta REST candle API lags ~60s — retrying until close stops changing
    POLL_INTERVALS = [5, 10, 15, 20, 30]   # cumulative: 5, 15, 30, 50, 80s
    prev_close = None
    attempts   = 0

    time.sleep(5)   # initial 5s wait — give REST a head start
    for wait in POLL_INTERVALS:
        result = _single_fetch()
        attempts += 1
        cur_close = result["close"] if result else None

        if cur_close is not None:
            if prev_close is not None and abs(cur_close - prev_close) < 1.0:
                # Close has stabilised — REST has settled
                result["_settle_attempts"] = attempts
                return result
            print(f"  [REST POLL #{attempts}] close={cur_close:.1f}"
                  + (f" (Δ{abs(cur_close - prev_close):.1f} from prev — still settling...)" if prev_close else " (first read)"))
            prev_close = cur_close
        else:
            print(f"  [REST POLL #{attempts}] no data yet...")

        if wait == POLL_INTERVALS[-1]:
            # Final attempt — return whatever we have
            if result:
                result["_settle_attempts"] = attempts
                result["_note"] = f"REST may not have fully settled after {attempts} polls"
            return result
        time.sleep(wait)

    return None

def write_csv_row(row: dict):
    exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)

def sep(char="─", width=72):
    print(char * width)

# ── WebSocket candle tracker ──────────────────────────────────────────────────

class CanvasSyncTester:
    def __init__(self):
        self._forming: Optional[dict] = None
        self._last_closed: Optional[dict] = None
        self._bar_count   = 0
        self._match_count = 0
        self._last_print  = 0.0

    def on_ws_message(self, msg: dict):
        msg_type = str(msg.get("type", msg.get("channel", "")))

        if "candlestick" not in msg_type:
            return

        data = msg.get("data", msg)
        if isinstance(data, list):
            data = data[0] if data else {}
        if not isinstance(data, dict):
            return

        try:
            raw_ts = int(
                data.get("start",
                data.get("candle_start_time",
                data.get("open_time", 0)))
            )
            if raw_ts == 0:
                return
            if raw_ts > 1_000_000_000_000_000:
                raw_ts = raw_ts // 1_000_000
            elif raw_ts > 1_000_000_000_000:
                raw_ts = raw_ts // 1000

            o = float(data.get("open",   0))
            h = float(data.get("high",   0))
            l = float(data.get("low",    0))
            c = float(data.get("close",  0))
            v = float(data.get("volume", data.get("turnover", 0)))

            # Explicit closed flag
            if bool(data.get("closed", False)):
                self._handle_close(raw_ts, o, h, l, c, v)
                self._forming = None
                return

            if self._forming is None:
                self._forming = dict(ts=raw_ts, open=o, high=h, low=l, close=c, volume=v)
                print(f"\n[{now_ist()}] 📊 New bar forming: open={o:.1f} | bar start={to_ist(raw_ts)}")
                return

            if raw_ts != self._forming["ts"]:
                # New bar → previous bar closed
                f = self._forming
                self._handle_close(f["ts"], f["open"], f["high"], f["low"], f["close"], f["volume"])
                self._forming = dict(ts=raw_ts, open=o, high=h, low=l, close=c, volume=v)
                print(f"\n[{now_ist()}] 📊 New bar forming: open={o:.1f} | bar start={to_ist(raw_ts)}")
            else:
                # Same bar — update
                self._forming["high"]   = max(self._forming["high"], h)
                self._forming["low"]    = min(self._forming["low"],  l)
                self._forming["close"]  = c
                self._forming["volume"] = v
                # Print live update every 30s
                if time.time() - self._last_print > 30:
                    self._last_print = time.time()
                    bar_age = int(time.time() - raw_ts)
                    print(
                        f"  [{now_ist()}] ⏳ Forming [{bar_age}s into bar] "
                        f"O={self._forming['open']:.1f} H={self._forming['high']:.1f} "
                        f"L={self._forming['low']:.1f} C={self._forming['close']:.1f} "
                        f"V={self._forming['volume']:.0f}"
                    )

        except Exception as e:
            print(f"  [PARSE ERROR] {e} | raw: {str(data)[:120]}")

    def _handle_close(self, ts, o, h, l, c, v):
        self._bar_count += 1
        bar_num = self._bar_count

        sep()
        print(f"  ✅ BAR #{bar_num} CLOSED")
        print(f"  Bar start (IST) : {to_ist(ts)}")
        print(f"  Bar close (IST) : {to_ist(ts + 300)}")
        print(f"  WS OHLCV        : O={o:.1f}  H={h:.1f}  L={l:.1f}  C={c:.1f}  V={v:.0f}")
        print()

        # Fetch from REST for comparison
        # Delta REST candle API lags ~60-90s — we poll until close stabilises
        print("  🔍 Fetching Delta REST API... (polling until settled, up to 80s)")
        time.sleep(1.0)
        rest = fetch_rest_candle(ts)

        if rest:
            note   = rest.pop("_note", None)
            match  = abs(rest["close"] - c) < 5.0   # within 5pts = sync (BTC at 77k; REST lags ~1s after bar close)
            status = "✅ IN SYNC" if match else "⚠️  MISMATCH"

            print(f"  REST OHLCV      : O={rest['open']:.1f}  H={rest['high']:.1f}  "
                  f"L={rest['low']:.1f}  C={rest['close']:.1f}  V={rest['volume']:.0f}")
            print()
            print(f"  WS  close : {c:.1f}")
            print(f"  REST close: {rest['close']:.1f}")
            print(f"  Diff      : {abs(c - rest['close']):.1f} pts → {status}  (threshold: ±5pts — REST settles ~1s after bar close)")
            settle_polls = rest.pop("_settle_attempts", "?")
            print(f"  REST settled after {settle_polls} poll(s)")
            if note:
                print(f"  NOTE: {note}")
            if match:
                self._match_count += 1
        else:
            print("  REST: ❌ Could not fetch (API lag? try again in a few seconds)")

        print()
        print(f"  📺 TradingView CHECK (manual):")
        print(f"     Open BTCUSD Perp on TV → 5m chart")
        print(f"     Bar that started at {to_ist(ts)}")
        print(f"     Expected close ~ {c:.1f}")
        print()
        print(f"  Score so far: {self._match_count}/{bar_num} bars WS↔REST in sync")
        sep()

        # Save to CSV
        write_csv_row({
            "bar_num":        bar_num,
            "bar_start_ist":  to_ist(ts),
            "bar_close_ist":  to_ist(ts + 300),
            "ws_open":        o,
            "ws_high":        h,
            "ws_low":         l,
            "ws_close":       c,
            "ws_volume":      v,
            "rest_open":      rest["open"]   if rest else "",
            "rest_high":      rest["high"]   if rest else "",
            "rest_low":       rest["low"]    if rest else "",
            "rest_close":     rest["close"]  if rest else "",
            "rest_volume":    rest["volume"] if rest else "",
            "close_diff_pts": round(abs(c - rest["close"]), 1) if rest else "",
            "ws_rest_sync":   ("YES" if abs(c - rest["close"]) < 5.0 else "NO") if rest else "REST_FAIL",
        })
        print(f"  💾 Logged to {LOG_FILE}")


# ── Main WebSocket loop ───────────────────────────────────────────────────────

async def run():
    tester = CanvasSyncTester()
    backoff = 1.0

    sep("═")
    print("  Delta Exchange 5m Candle Sync Tester")
    print(f"  Symbol : {SYMBOL}")
    print(f"  WS     : {WS_URL}")
    print(f"  REST   : {REST_URL}")
    print(f"  Log    : {LOG_FILE}")
    print()
    print("  What this checks:")
    print("  1. WS candle close price == Delta REST candle close price (auto)")
    print("  2. TradingView 5m BTCUSD Perp close price (manual visual check)")
    print()
    print("  Keep TradingView open on BTCUSD 5m chart for comparison.")
    print("  Press Ctrl+C to stop.")
    sep("═")
    print()

    while True:
        try:
            print(f"[{now_ist()}] Connecting WebSocket...")
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                ping_timeout=None,
                max_size=2**20,
                open_timeout=15,
            ) as ws:
                sub = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "candlestick_5m", "symbols": [SYMBOL]},
                        ]
                    }
                }
                await ws.send(json.dumps(sub))
                print(f"[{now_ist()}] ✅ Subscribed to candlestick_5m — waiting for next tick...\n")
                backoff = 1.0

                async for raw_frame in ws:
                    try:
                        msg = json.loads(raw_frame)
                        tester.on_ws_message(msg)
                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        print(f"  [DISPATCH ERROR] {e}")

        except KeyboardInterrupt:
            print(f"\n[{now_ist()}] Stopped by user.")
            sep()
            print(f"  Final score : {tester._match_count}/{tester._bar_count} bars WS↔REST in sync")
            print(f"  Log saved   : {LOG_FILE}")
            sep()
            break
        except Exception as e:
            print(f"[{now_ist()}] WS error: {e!r} — retry in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    asyncio.run(run())
