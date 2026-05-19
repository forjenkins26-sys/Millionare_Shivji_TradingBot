#!/usr/bin/env python3
"""
candle_feed.py — Delta Exchange WebSocket candle feed for Vol Surge v5
======================================================================
Responsibilities (ONLY):
  - Maintain a 300-candle ring buffer of closed 15-minute candles
  - Maintain current mark price
  - REST backfill on startup and after reconnect gaps
  - Auto-reconnect with exponential backoff
  - Emit closed candles via callback

Does NOT:
  - Execute trades
  - Place orders
  - Generate signals
  - Modify any state outside this module

Usage:
    feed = CandleFeed(on_candle_close=my_callback)
    asyncio.run(feed.start())
"""

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, List, Optional

import requests
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

# ── Constants ─────────────────────────────────────────────────────────────────
WS_URL   = "wss://socket.india.delta.exchange"
REST_URL = "https://api.india.delta.exchange"
SYMBOL   = "BTCUSD"

_BACKOFF_INITIAL    = 1.0
_BACKOFF_MAX        = 60.0
_BACKOFF_MULT       = 2.0
_HEARTBEAT_INTERVAL = 30.0   # seconds between keepalive pings to Delta


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Candle:
    """One closed 15-minute candle."""
    ts:     int    # candle start (Unix seconds, UTC)
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float

    def __repr__(self) -> str:
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(self.ts, tz=timezone.utc)
        return (
            f"Candle({dt.strftime('%Y-%m-%d %H:%M UTC')}"
            f" O={self.open:.1f} H={self.high:.1f}"
            f" L={self.low:.1f} C={self.close:.1f})"
        )

    def to_dict(self) -> dict:
        return {
            "ts": self.ts, "open": self.open, "high": self.high,
            "low": self.low, "close": self.close, "volume": self.volume,
        }


# ── CandleFeed ────────────────────────────────────────────────────────────────

class CandleFeed:
    """
    Real-time buffer of closed 15-minute candles from Delta Exchange WebSocket.

    Parameters
    ----------
    symbol          : trading symbol, default "BTCUSD"
    buffer_size     : closed candles to keep in ring buffer, default 300
    on_candle_close : callback(candle: Candle, buffer: deque) on each closed bar
    logger          : optional external logger
    """

    def __init__(
        self,
        symbol:          str = SYMBOL,
        buffer_size:     int = 300,
        on_candle_close: Optional[Callable] = None,
        logger:          Optional[logging.Logger] = None,
    ):
        self.symbol      = symbol
        self.buffer:     deque = deque(maxlen=buffer_size)
        self.mark_price: Optional[float] = None
        self.mark_price_updated_at: Optional[float] = None   # time.time() of last mark_price WS update
        self.last_closed: Optional[Candle] = None
        self.connected:  bool = False

        # Observability
        self.last_frame_at:  Optional[float] = None   # time.time() of last WS frame
        self.reconnect_count: int = 0

        self._on_close   = on_candle_close
        self._forming:   Optional[dict] = None    # candle currently forming from WS
        self._warmed_up  = False
        self._running    = False
        self._raw_logged = False   # log first raw WS message for format debugging

        self.log = logger or logging.getLogger("candle_feed")

    # ── Public ────────────────────────────────────────────────────────────────

    @property
    def is_ready(self) -> bool:
        """True when backfill loaded ≥ 250 bars (enough for EMA200 warmup on 15m)."""
        return self._warmed_up and len(self.buffer) >= 250

    async def start(self):
        """
        Backfill from REST, then connect to WebSocket.
        Reconnects forever with exponential backoff on any disconnect.
        """
        self._running = True
        self.log.info(f"[FEED] Starting for {self.symbol}")

        await self._backfill(300)

        backoff = _BACKOFF_INITIAL
        while self._running:
            try:
                self.log.info(f"[FEED] Connecting WebSocket -> {WS_URL}")
                await self._ws_loop()
                backoff = _BACKOFF_INITIAL
            except (ConnectionClosed, WebSocketException, OSError) as e:
                self.connected = False
                self.log.warning(f"[FEED] WS disconnected: {e!r} — retry in {backoff:.0f}s")
            except Exception as e:
                self.connected = False
                self.log.error(f"[FEED] WS error: {e!r} — retry in {backoff:.0f}s")

            await asyncio.sleep(backoff)
            backoff = min(backoff * _BACKOFF_MULT, _BACKOFF_MAX)
            self.reconnect_count += 1

            if self.last_closed:
                self.log.info("[FEED] Reconnected — filling gap from REST...")
                await self._backfill_gap()

    def stop(self):
        self._running = False

    # ── REST backfill ─────────────────────────────────────────────────────────

    async def _backfill(self, count: int = 300):
        self.log.info(f"[FEED] REST backfill: requesting {count} candles...")
        try:
            end_ts   = int(time.time())
            start_ts = end_ts - count * 900 - 1800   # 15-min bars (900s each) + small buffer

            raw = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: requests.get(
                    f"{REST_URL}/v2/history/candles",
                    params={
                        "symbol":     self.symbol,
                        "resolution": "15m",   # Delta requires "15m" not "15"
                        "start":      start_ts,
                        "end":        end_ts,
                    },
                    timeout=20,
                ).json()
            )

            # Delta REST returns { result: [...] } newest-first (descending).
            # We must reverse to oldest-first so buffer[-1] = newest = current bar.
            candles_raw = raw.get("result", raw.get("data", []))
            if not candles_raw:
                self.log.warning(f"[FEED] Backfill returned empty. Raw preview: {str(raw)[:300]}")
                self._warmed_up = True
                return

            now = int(time.time())
            loaded = 0
            for c in reversed(candles_raw):   # oldest → newest: buffer[-1] = current
                candle = self._parse_rest_candle(c)
                # Skip the currently forming bar — bar closes at ts + 900.
                # If bar hasn't closed yet, exclude it so the WebSocket can
                # emit the real close (avoids dedup-skipping the close callback).
                if candle and candle.ts + 900 <= now:
                    self.buffer.append(candle)
                    self.last_closed = candle
                    loaded += 1

            self._warmed_up = True
            self.log.info(
                f"[FEED] Backfill complete — {loaded} candles loaded "
                f"| buffer={len(self.buffer)} "
                f"| oldest={self.buffer[0] if self.buffer else 'n/a'} "
                f"| newest={self.buffer[-1] if self.buffer else 'n/a'}"
            )

        except Exception as e:
            self.log.error(f"[FEED] Backfill error: {e}")
            self._warmed_up = True   # still proceed — indicators will warn if data is thin

    async def _backfill_gap(self):
        if not self.last_closed:
            await self._backfill(300)
            return

        gap_start  = self.last_closed.ts + 900
        gap_end    = int(time.time())
        gap_bars   = (gap_end - gap_start) // 900

        if gap_bars < 1:
            self.log.info("[FEED] Gap < 1 bar — no REST fill needed")
            return

        self.log.info(f"[FEED] Filling gap of ~{gap_bars} bars from REST...")
        try:
            raw = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: requests.get(
                    f"{REST_URL}/v2/history/candles",
                    params={
                        "symbol":     self.symbol,
                        "resolution": "15m",   # Delta requires "15m" not "15"
                        "start":      gap_start,
                        "end":        gap_end,
                    },
                    timeout=20,
                ).json()
            )
            # Delta returns newest-first — reverse for oldest-first append order
            candles_raw = raw.get("result", raw.get("data", []))
            now2 = int(time.time())
            filled = 0
            for c in reversed(candles_raw):
                candle = self._parse_rest_candle(c)
                # Only include bars that have fully closed (ts + 900 <= now)
                if candle and candle.ts > self.last_closed.ts and candle.ts + 900 <= now2:
                    self.buffer.append(candle)
                    self.last_closed = candle
                    filled += 1
            self.log.info(f"[FEED] Gap fill complete — {filled} bars added")
        except Exception as e:
            self.log.error(f"[FEED] Gap fill error: {e}")

    def _parse_rest_candle(self, raw: dict) -> Optional[Candle]:
        try:
            # Delta REST: 'time' or 'start' holds candle open timestamp
            ts = int(raw.get("time", raw.get("start", raw.get("candle_start_time", 0))))
            if ts == 0:
                return None
            # Some Delta endpoints return timestamps in ms
            if ts > 1_000_000_000_000:
                ts = ts // 1000
            return Candle(
                ts     = ts,
                open   = float(raw.get("open",  0)),
                high   = float(raw.get("high",  0)),
                low    = float(raw.get("low",   0)),
                close  = float(raw.get("close", 0)),
                volume = float(raw.get("volume", raw.get("turnover", 0))),
            )
        except Exception as e:
            self.log.debug(f"[FEED] parse_rest_candle skip: {e} | {raw}")
            return None

    # ── WebSocket loop ────────────────────────────────────────────────────────

    async def _ws_loop(self):
        async with websockets.connect(
            WS_URL,
            ping_interval=None,    # we manage heartbeats manually below
            ping_timeout=None,
            max_size=2 ** 20,
            open_timeout=15,
        ) as ws:
            self.connected = True
            self.log.info("[FEED] WebSocket connected (ok)")

            await self._subscribe(ws)
            hb_task = asyncio.create_task(self._heartbeat(ws))

            try:
                async for raw_frame in ws:
                    try:
                        if not self._raw_logged:
                            self.log.info(f"[FEED] First WS frame (format debug): {raw_frame[:300]}")
                            self._raw_logged = True
                        msg = json.loads(raw_frame)
                        self._dispatch(msg)
                    except json.JSONDecodeError:
                        self.log.debug(f"[FEED] Non-JSON frame: {raw_frame[:100]}")
                    except Exception as e:
                        self.log.warning(f"[FEED] dispatch error: {e}")
            finally:
                hb_task.cancel()
                self.connected = False

    async def _subscribe(self, ws):
        sub = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "candlestick_15m", "symbols": [self.symbol]},
                    {"name": "mark_price",      "symbols": [self.symbol]},
                ]
            }
        }
        await ws.send(json.dumps(sub))
        self.log.info(f"[FEED] Subscribed to candlestick_15m + mark_price [{self.symbol}]")

    async def _heartbeat(self, ws):
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            try:
                await ws.send(json.dumps({"type": "heartbeat"}))
                self.log.debug("[FEED] heartbeat ->")
            except Exception:
                break

    # ── Message dispatch ──────────────────────────────────────────────────────

    def _dispatch(self, msg: dict):
        self.last_frame_at = time.time()
        msg_type = str(msg.get("type", msg.get("channel", "")))

        if "candlestick" in msg_type:
            self._handle_candle(msg)
        elif "mark_price" in msg_type:
            self._handle_mark_price(msg)
        elif msg_type in ("subscriptions", "heartbeat", "info", "welcome", "connected"):
            self.log.debug(f"[FEED] ctrl: {msg_type}")
        else:
            self.log.debug(f"[FEED] unknown type={msg_type!r}: {str(msg)[:120]}")

    def _handle_candle(self, msg: dict):
        """
        Process a WebSocket candlestick update.

        Delta sends updates for the forming (open) candle multiple times.
        We track by `start` timestamp. When a NEW start time arrives, the
        previous candle is definitively closed.

        Also handles explicit `closed: true` field if Delta provides it.
        """
        # Normalize: some Delta formats nest under "data", some are flat
        data = msg.get("data", msg)
        if isinstance(data, list):
            data = data[0] if data else {}
        if not isinstance(data, dict):
            return

        try:
            # Timestamp — may be seconds or milliseconds
            raw_ts = int(
                data.get("start",
                data.get("candle_start_time",
                data.get("open_time", 0)))
            )
            if raw_ts == 0:
                return
            if raw_ts > 1_000_000_000_000_000:   # microseconds → seconds
                raw_ts = raw_ts // 1_000_000
            elif raw_ts > 1_000_000_000_000:     # milliseconds → seconds
                raw_ts = raw_ts // 1000

            o = float(data.get("open",   0))
            h = float(data.get("high",   0))
            l = float(data.get("low",    0))
            c = float(data.get("close",  0))
            v = float(data.get("volume", data.get("turnover", 0)))

            # Explicit closed flag — trust it if present
            if bool(data.get("closed", False)):
                self._emit_closed(Candle(ts=raw_ts, open=o, high=h, low=l, close=c, volume=v))
                self._forming = None
                return

            # No explicit close — track by start time
            if self._forming is None:
                self._forming = dict(ts=raw_ts, open=o, high=h, low=l, close=c, volume=v)
                self.log.debug(f"[FEED] forming ts={raw_ts}")
                return

            if raw_ts != self._forming["ts"]:
                # New bar started — previous bar is done
                f = self._forming
                self._emit_closed(
                    Candle(ts=f["ts"], open=f["open"], high=f["high"],
                           low=f["low"], close=f["close"], volume=f["volume"])
                )
                self._forming = dict(ts=raw_ts, open=o, high=h, low=l, close=c, volume=v)
            else:
                # Same bar — update with latest tick
                self._forming["high"]   = max(self._forming["high"], h)
                self._forming["low"]    = min(self._forming["low"],  l)
                self._forming["close"]  = c
                self._forming["volume"] = v

        except Exception as e:
            self.log.warning(f"[FEED] _handle_candle error: {e} | {str(msg)[:120]}")

    def _handle_mark_price(self, msg: dict):
        try:
            data  = msg.get("data", msg)
            price = data.get("mark_price", data.get("price", data.get("close", None)))
            if price:
                self.mark_price            = float(price)
                self.mark_price_updated_at = time.time()
        except Exception as e:
            self.log.debug(f"[FEED] mark_price error: {e}")

    def _emit_closed(self, candle: Candle):
        """Add to buffer, update last_closed, fire callback."""
        # Dedup guard
        if self.last_closed and candle.ts <= self.last_closed.ts:
            self.log.debug(f"[FEED] Dedup skip ts={candle.ts}")
            return

        self.buffer.append(candle)
        self.last_closed = candle
        self.log.info(f"[FEED] CLOSED {candle}")

        if self._on_close:
            try:
                self._on_close(candle, self.buffer)
            except Exception as e:
                self.log.error(f"[FEED] on_candle_close callback raised: {e}")
