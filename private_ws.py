#!/usr/bin/env python3
"""
private_ws.py — Delta Exchange Private WebSocket Feed
=====================================================
Authenticates on Delta's WebSocket and subscribes to:
  - 'orders'         : real-time order state changes (fills, cancels)
  - 'v2/user_trades' : real-time trade fills

Exposes threading.Events so _position_monitor() can wake instantly
on TP/SL hits instead of polling REST every 1s.

Auth method (Delta WS):
  Signature = HMAC-SHA256( "GET" + timestamp + "/realtime",  api_secret )
  Send:  {"method": "auth", "payload": {"api_key": ..., "signature": ..., "timestamp": ...}}

Usage:
    from private_ws import PrivateFeed
    pf = PrivateFeed(api_key="...", api_secret="...", symbol="BTCUSD")
    asyncio.create_task(pf.start())
    # In monitor thread:
    pf.order_event.wait(timeout=2)
    evt = pf.last_order   # latest order dict from WS
"""

import asyncio
import hashlib
import hmac
import json
import logging
import threading
import time
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

WS_URL            = "wss://socket.india.delta.exchange"
_BACKOFF_INITIAL  = 1.0
_BACKOFF_MAX      = 60.0
_BACKOFF_MULT     = 2.0
_HEARTBEAT_SECS   = 25.0


class PrivateFeed:
    """
    Private WebSocket feed — orders + trades channels.

    Thread-safe. Events are set from asyncio thread; monitor thread reads them.
    """

    def __init__(
        self,
        api_key:    str,
        api_secret: str,
        symbol:     str = "BTCUSD",
        logger:     Optional[logging.Logger] = None,
    ):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.symbol     = symbol
        self.log        = logger or logging.getLogger("private_ws")

        # Threading events — set when a new message arrives; caller must clear
        self.order_event: threading.Event = threading.Event()
        self.trade_event: threading.Event = threading.Event()

        # Latest message payloads — write from asyncio, read from monitor thread
        self.last_order: Optional[dict] = None   # most recent order update
        self.last_trade: Optional[dict] = None   # most recent fill

        self.authenticated: bool = False
        self.connected:     bool = False
        self._running:      bool = False
        self.reconnect_count: int = 0

    # ── Auth signature ────────────────────────────────────────────────────────

    def _sign(self) -> tuple:
        """Return (timestamp_str, signature_hex) for WS auth message."""
        ts  = str(int(time.time()))
        sig = hmac.new(
            self.api_secret.encode(),
            f"GET{ts}/realtime".encode(),
            hashlib.sha256,
        ).hexdigest()
        return ts, sig

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def start(self):
        self._running = True
        self.log.info("[PRIV_WS] Starting private WebSocket feed")
        backoff = _BACKOFF_INITIAL

        while self._running:
            try:
                await self._ws_loop()
                backoff = _BACKOFF_INITIAL
            except (ConnectionClosed, WebSocketException, OSError) as e:
                self.connected     = False
                self.authenticated = False
                self.log.warning(f"[PRIV_WS] Disconnected: {e!r} — retry in {backoff:.0f}s")
            except Exception as e:
                self.connected     = False
                self.authenticated = False
                self.log.error(f"[PRIV_WS] Error: {e!r} — retry in {backoff:.0f}s")

            await asyncio.sleep(backoff)
            backoff = min(backoff * _BACKOFF_MULT, _BACKOFF_MAX)
            self.reconnect_count += 1

    def stop(self):
        self._running = False

    async def _ws_loop(self):
        async with websockets.connect(
            WS_URL,
            ping_interval=None,
            ping_timeout=None,
            max_size=2 ** 20,
            open_timeout=15,
        ) as ws:
            self.connected = True
            self.log.info("[PRIV_WS] Connected")

            # Step 1: authenticate
            ts, sig = self._sign()
            await ws.send(json.dumps({
                "method": "auth",
                "payload": {
                    "api_key":   self.api_key,
                    "signature": sig,
                    "timestamp": ts,
                }
            }))
            self.log.info("[PRIV_WS] Auth message sent")

            # Step 2: wait for auth confirmation (first message)
            try:
                auth_raw = await asyncio.wait_for(ws.recv(), timeout=10)
                auth_msg = json.loads(auth_raw)
                # Delta may respond with {"type": "success"} or {"success": true}
                # or channel subscription acknowledgement
                if (auth_msg.get("type") == "success"
                        or auth_msg.get("success")
                        or auth_msg.get("type") == "subscriptions"):
                    self.authenticated = True
                    self.log.info(f"[PRIV_WS] Authenticated ✓ | response={auth_msg.get('type','?')}")
                else:
                    self.log.warning(f"[PRIV_WS] Unexpected auth response: {auth_msg} — proceeding anyway")
                    self.authenticated = True   # some Delta versions skip explicit ack
            except asyncio.TimeoutError:
                self.log.warning("[PRIV_WS] Auth response timeout — proceeding (Delta may not send explicit ack)")
                self.authenticated = True

            # Step 3: subscribe to private channels
            await ws.send(json.dumps({
                "method": "subscribe",
                "payload": {
                    "channels": [
                        {"name": "orders",         "symbols": [self.symbol]},
                        {"name": "v2/user_trades", "symbols": [self.symbol]},
                    ]
                }
            }))
            self.log.info("[PRIV_WS] Subscribed to orders + v2/user_trades")

            hb_task = asyncio.create_task(self._heartbeat(ws))
            try:
                async for raw_frame in ws:
                    try:
                        msg = json.loads(raw_frame)
                        self._dispatch(msg)
                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        self.log.warning(f"[PRIV_WS] dispatch error: {e}")
            finally:
                hb_task.cancel()
                self.connected     = False
                self.authenticated = False

    async def _heartbeat(self, ws):
        while True:
            await asyncio.sleep(_HEARTBEAT_SECS)
            try:
                await ws.send(json.dumps({"type": "heartbeat"}))
            except Exception:
                break

    # ── Message dispatch ──────────────────────────────────────────────────────

    def _dispatch(self, msg: dict):
        msg_type = str(msg.get("type", msg.get("channel", "")))

        if "orders" in msg_type and "user_trades" not in msg_type:
            data = msg.get("data", msg)
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict) and data:
                self.last_order = data
                self.order_event.set()   # wake position monitor
                self.log.info(
                    f"[PRIV_WS] order event | id={data.get('id')} "
                    f"state={data.get('state')} "
                    f"fill_px={data.get('average_fill_price', '?')}"
                )

        elif "user_trades" in msg_type or (
                "trade" in msg_type and "user_trades" in str(msg.get("channel", ""))):
            data = msg.get("data", msg)
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict) and data:
                self.last_trade = data
                self.trade_event.set()
                self.log.info(
                    f"[PRIV_WS] trade fill | px={data.get('fill_price', '?')} "
                    f"size={data.get('size', '?')} side={data.get('side', '?')}"
                )

        elif msg_type in ("subscriptions", "heartbeat", "info", "welcome", "connected", "success"):
            self.log.debug(f"[PRIV_WS] ctrl: {msg_type}")
        else:
            self.log.debug(f"[PRIV_WS] unknown type={msg_type!r}: {str(msg)[:120]}")
