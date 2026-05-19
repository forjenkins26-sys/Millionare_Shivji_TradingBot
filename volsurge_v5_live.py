#!/usr/bin/env python3
"""
volsurge_v5_live.py — Vol Surge Bot v5 Live (WebSocket-native, No Webhook)
===========================================================================
Architecture:
  Delta WebSocket → CandleFeed (15m) → SignalEngine → _process_entry → Delta Orders
                                                    (NO TradingView webhook dependency)

Signal source  : Python computes Vol Surge signal directly from Delta 15m WebSocket candles
Execution      : Identical to v4 (market entry + SL stop + TP limit on Delta Exchange)
SL/TP model    : Fill-based — anchored to actual Delta fill price, not Pine signal price
Lifecycle      : IDLE → ENTERED → CLOSED (same as v4)
Latency        : Signal detection < 100ms after bar close (vs 5-7s with TV webhook)

Modes:
  PAPER_MODE=true  → fills simulated at live Delta price, no real orders
  PAPER_MODE=false → real orders on Delta Exchange India LIVE

Key differences from v4:
  - No /webhook endpoint — signals come from SignalEngine, not TradingView
  - No MAX_WH_LATENCY_MS gate — signal is native, no TV delivery lag
  - SignalEngine config mirrors Pine inputs (sl_mult, burst_mult, tp2_r, etc.)
  - in_trade flag correctly passed to engine — engine never fires during open trade
  - Feed health visible on /health — additional monitoring layer

Run:
  uvicorn volsurge_v5_live:app --host 0.0.0.0 --port $PORT --workers 1
"""

# ════════════════════════════════════════════════════════════════════════
# IMPORTS
# ════════════════════════════════════════════════════════════════════════
import asyncio
import csv
import hashlib
import hmac
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse

from candle_feed import CandleFeed, Candle
from signal_engine import SignalEngine, SignalConfig, SignalResult

# ════════════════════════════════════════════════════════════════════════
# .env SUPPORT
# ════════════════════════════════════════════════════════════════════════
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ════════════════════════════════════════════════════════════════════════
# CONFIG  (override via .env or Railway Variables)
# ════════════════════════════════════════════════════════════════════════
API_KEY    = os.getenv("DELTA_API_KEY_LIVE",    "")
API_SECRET = os.getenv("DELTA_API_SECRET_LIVE", "")
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN",    "")
TG_CHAT    = os.getenv("TELEGRAM_CHAT_ID",      "")

PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"

BASE_URL   = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange")
PRODUCT_ID = int(os.getenv("PRODUCT_ID", "27"))      # BTCUSD Perpetual
SYMBOL     = os.getenv("SYMBOL", "BTCUSD")

LOT_SIZE                = float(os.getenv("LOT_SIZE", "0.001"))
DELTA_MIN_SIZE_BTC      = 0.001
DELTA_CONTRACT_SIZE_BTC = 0.001
LOT_SIZE_CONTRACTS      = int(os.getenv("LOT_SIZE_CONTRACTS", "0"))

# ── Signal engine parameters — must match Pine script inputs ──────────
# These mirror Pine's input defaults from pine_volsurge_v5.pine
VS_LOOKBACK   = int(os.getenv("VS_LOOKBACK",    "5"))     # vsLookback
VS_BURST_MULT = float(os.getenv("VS_BURST_MULT","2.0"))   # vsBurstMult
SL_MULT       = float(os.getenv("SL_MULT",      "1.8"))   # vsSLMult
VS_COOLDOWN   = int(os.getenv("VS_COOLDOWN",    "3"))      # vsCooldown
USE_EMA_FILT  = os.getenv("USE_EMA_FILTER",     "false").lower() == "true"
USE_SESSION   = os.getenv("USE_SESSION",        "false").lower() == "true"
# Safety factor: 1.0 = exact Pine match (no extra buffer). Increase to reduce false signals.
SAFETY_FACTOR = float(os.getenv("SIGNAL_SAFETY_FACTOR", "1.0"))
# Heikin-Ashi mode: MUST match TradingView chart type.
# True  → 78% WR, 46 trades (HA chart in TradingView) ← confirmed better
# False → 49% WR, 173 trades (regular candle chart)
USE_HA        = os.getenv("USE_HA_CANDLES", "true").lower() == "true"

# ── Trade parameters ──────────────────────────────────────────────────
# TP_R: must match Pine's vsTP2R (currently 1.4R)
TP_R               = float(os.getenv("TP_R", "1.4"))
MAX_SLIPPAGE_RATIO = float(os.getenv("MAX_SLIPPAGE_RATIO", "0.0"))

PRICE_INTERVAL = 2   # seconds between position monitor ticks
POS_MON_DELAY  = 3   # seconds to wait after entry before monitor starts

# ════════════════════════════════════════════════════════════════════════
# STATE CONSTANTS
# ════════════════════════════════════════════════════════════════════════
STATE_IDLE    = "IDLE"
STATE_ENTERED = "ENTERED"
STATE_CLOSED  = "CLOSED"

# ════════════════════════════════════════════════════════════════════════
# FILE PATHS
# ════════════════════════════════════════════════════════════════════════
DATA_DIR   = Path(os.getenv("DATA_DIR", "data")); DATA_DIR.mkdir(exist_ok=True)
LOG_DIR    = Path(os.getenv("LOG_DIR",  "logs")); LOG_DIR.mkdir(exist_ok=True)

# True when DATA_DIR is a persistent Railway volume (mounted at /app/data)
_DATA_PERSISTENT = DATA_DIR.is_absolute() and str(DATA_DIR).startswith("/app")
STATE_FILE     = DATA_DIR / "state_v5.json"
CSV_FILE       = DATA_DIR / "trades_v5.csv"
RECON_FILE     = DATA_DIR / "reconciliation_v5.csv"
LIFECYCLE_FILE = DATA_DIR / "order_lifecycle_v5.csv"
LATENCY_FILE   = DATA_DIR / "execution_latency_v5.csv"
SLIPPAGE_FILE  = DATA_DIR / "slippage_audit_v5.csv"
LOG_FILE       = LOG_DIR  / "volsurge_v5_live.log"

# ════════════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("volsurge_v5_live")

# ════════════════════════════════════════════════════════════════════════
# GLOBAL STATE
# ════════════════════════════════════════════════════════════════════════
open_trade:        Optional[dict] = None
_state_lock        = threading.Lock()
_entry_processing  = False
_preflight_ok      = False

def _tid() -> str:
    return open_trade.get("trade_id", "?") if open_trade else "IDLE"

def _log (msg): log.info   (f"[{_tid()}] {msg}")
def _logw(msg): log.warning(f"[{_tid()}] {msg}")
def _loge(msg): log.error  (f"[{_tid()}] {msg}")

# ════════════════════════════════════════════════════════════════════════
# GRACEFUL SHUTDOWN
# ════════════════════════════════════════════════════════════════════════
import signal as _signal

def _handle_shutdown(signum, frame):
    sig_name = "SIGTERM" if signum == _signal.SIGTERM else "SIGINT"
    log.warning(f"[SHUTDOWN] {sig_name} received")
    with _state_lock:
        if open_trade:
            open_trade["_shutdown_at"] = datetime.now().isoformat()
            save_state()
            log.warning(f"[SHUTDOWN] Open trade state saved — will auto-resume on restart")
            tg(
                f"⚠️ <b>Bot shutting down ({sig_name})</b>\n"
                f"Trade <b>{_tid()}</b> state saved\n"
                f"Will auto-resume on Railway restart ♻️"
            )
        else:
            log.info(f"[SHUTDOWN] {sig_name} — no open trade — clean shutdown")
    sys.exit(0)

_signal.signal(_signal.SIGTERM, _handle_shutdown)
_signal.signal(_signal.SIGINT,  _handle_shutdown)

# ════════════════════════════════════════════════════════════════════════
# CONTRACT SIZING
# ════════════════════════════════════════════════════════════════════════
def _btc_to_contracts(btc_size: float, ref_price: Optional[float] = None) -> int:
    if LOT_SIZE_CONTRACTS > 0:
        return LOT_SIZE_CONTRACTS
    contracts = max(1, round(btc_size / DELTA_CONTRACT_SIZE_BTC))
    log.info(f"[SIZE] {btc_size} BTC ÷ {DELTA_CONTRACT_SIZE_BTC} = {contracts} contracts")
    return contracts

# ════════════════════════════════════════════════════════════════════════
# CSV SCHEMAS
# ════════════════════════════════════════════════════════════════════════
CSV_HEADERS = [
    "trade_id", "direction", "mode",
    "signal_timeframe", "signal_tf_bar_time",
    "pine_entry_px", "fill_price", "entry_slippage_pts",
    "sl_price", "tp_price",
    "pine_signal_time", "signal_recv_time", "entry_fill_time",
    "signal_latency_ms", "entry_latency_ms",
    "exit_price", "exit_time", "exit_type", "exit_slippage_pts",
    "pts", "pnl_approx",
    "python_actual_outcome",
    "slippage_ratio", "structure_grade",
    "trade_duration_sec", "monitor_cycles_total",
    "recovery_event", "recovery_reason",
    "entry_order_id",
    "api_request_time", "api_ack_time",
    "sl_placed_time", "tp_placed_time",
    "exit_order_id", "exit_fill_px_delta",
    "entry_slippage_pct",
    # v5-specific
    "chop_avg_tr", "burst_threshold", "candle_body",
    "atr5_prev", "sl_dist_engine",
]

RECON_HEADERS = [
    "trade_id", "timestamp",
    "signal_timeframe",
    "python_actual_outcome",
    "entry_slippage_pts", "exit_slippage_pts",
    "signal_latency_ms", "entry_latency_ms",
    "pts",
    "trade_duration_sec", "monitor_cycles_total",
    "recovery_event", "recovery_reason",
]

LIFECYCLE_HEADERS = [
    "trade_id", "timestamp_ist", "unix_ts",
    "event", "order_id", "side", "qty", "price",
    "latency_from_prev_ms", "notes",
]

LATENCY_HEADERS = [
    "trade_id", "timestamp_ist", "direction", "mode",
    "pine_signal_time", "signal_recv_time", "entry_submit_time", "entry_ack_time",
    "pine_entry_px", "delta_fill_px", "entry_slippage_pts",
    "signal_latency_ms", "api_roundtrip_ms",
    "sl_price", "tp_price", "sl_order_id", "tp_order_id",
    "contracts", "entry_order_id",
]

SLIPPAGE_HEADERS = [
    "trade_id", "timestamp_ist", "direction", "mode",
    "pine_entry_px", "delta_entry_fill", "entry_slippage_pts", "entry_slippage_pct",
    "pine_exit_px", "delta_exit_fill", "exit_slippage_pts", "exit_slippage_pct",
    "pine_pts", "live_pts", "slippage_drag_pts",
    "exit_type", "signal_latency_ms", "timeframe",
]

def _init_csvs():
    for fpath, headers in [
        (CSV_FILE,       CSV_HEADERS),
        (RECON_FILE,     RECON_HEADERS),
        (LIFECYCLE_FILE, LIFECYCLE_HEADERS),
        (LATENCY_FILE,   LATENCY_HEADERS),
        (SLIPPAGE_FILE,  SLIPPAGE_HEADERS),
    ]:
        if not fpath.exists():
            with open(fpath, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=headers).writeheader()
            log.info(f"[CSV] Created {fpath.name}")

def _append_csv(fpath: Path, headers: list, row: dict):
    try:
        with open(fpath, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=headers, extrasaction="ignore").writerow(row)
    except Exception as e:
        _loge(f"CSV write error ({fpath.name}): {e}")

_lifecycle_last_ts: dict = {}

def _log_lifecycle(trade_id: str, event: str, order_id: str = "",
                   side: str = "", qty: float = 0, price: float = 0, notes: str = ""):
    now_unix = time.time()
    now_ist  = (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]
    prev_ts  = _lifecycle_last_ts.get(trade_id, now_unix)
    lat_ms   = round((now_unix - prev_ts) * 1000, 1) if trade_id in _lifecycle_last_ts else 0
    _lifecycle_last_ts[trade_id] = now_unix
    row = {
        "trade_id": trade_id, "timestamp_ist": now_ist, "unix_ts": round(now_unix, 3),
        "event": event, "order_id": order_id or "", "side": side,
        "qty": qty or "", "price": price or "",
        "latency_from_prev_ms": lat_ms, "notes": notes,
    }
    _append_csv(LIFECYCLE_FILE, LIFECYCLE_HEADERS, row)
    log.info(f"[LIFECYCLE][{trade_id}] {event} oid={order_id or '—'} price={price or '—'} +{lat_ms:.0f}ms")

# ════════════════════════════════════════════════════════════════════════
# STATE PERSISTENCE  (crash recovery)
# ════════════════════════════════════════════════════════════════════════
def save_state():
    try:
        payload = {**(open_trade or {}), "state": open_trade.get("state", STATE_IDLE) if open_trade else STATE_IDLE,
                   "_saved_at": datetime.now().isoformat()}
        STATE_FILE.write_text(json.dumps(payload, indent=2))
    except Exception as e:
        _loge(f"save_state error: {e}")

def load_state() -> Optional[dict]:
    try:
        if not STATE_FILE.exists():
            log.info("[RECOVERY] No state file — starting fresh")
            return None
        data = json.loads(STATE_FILE.read_text())
        st   = data.get("state")
        if st in (None, STATE_IDLE, STATE_CLOSED):
            log.info(f"[RECOVERY] state={st} — no resume needed")
            return None
        log.warning(
            f"[RECOVERY] ══════════════════════════════════════\n"
            f"[RECOVERY]  Active trade found — will resume\n"
            f"[RECOVERY]  trade_id   = {data.get('trade_id','?')}\n"
            f"[RECOVERY]  direction  = {data.get('direction','?')}\n"
            f"[RECOVERY]  fill_price = {data.get('fill_price','?')}\n"
            f"[RECOVERY]  sl_price   = {data.get('sl_price','?')}\n"
            f"[RECOVERY]  tp_price   = {data.get('tp_price','?')}\n"
            f"[RECOVERY]  saved_at   = {data.get('_saved_at','unknown')}\n"
            f"[RECOVERY] ══════════════════════════════════════"
        )
        return data
    except Exception as e:
        log.error(f"[RECOVERY] load_state error: {e}")
    return None

# ════════════════════════════════════════════════════════════════════════
# DELTA AUTH
# ════════════════════════════════════════════════════════════════════════
def _sign(method: str, path: str, qs: str = "", body: str = "") -> dict:
    ts  = str(int(time.time()))
    sig = hmac.new(
        API_SECRET.encode(),
        (method + ts + path + qs + body).encode(),
        hashlib.sha256,
    ).hexdigest()
    return {"api-key": API_KEY, "timestamp": ts, "signature": sig, "Content-Type": "application/json"}

def _get(path: str, params: Optional[dict] = None):
    qs = ""
    if params:
        qs = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    try:
        r = requests.get(BASE_URL + path + qs, headers=_sign("GET", path, qs), timeout=10)
        if r.status_code != 200:
            _loge(f"GET {path} HTTP {r.status_code} | {r.text[:300]}")
        return r.json()
    except Exception as e:
        _loge(f"GET {path} error: {e}")
        return None

def _post(path: str, body_dict: dict):
    body = json.dumps(body_dict)
    try:
        r = requests.post(BASE_URL + path, headers=_sign("POST", path, "", body), data=body, timeout=10)
        return r.json()
    except Exception as e:
        _loge(f"POST {path} error: {e}")
        return None

def _delete(path: str):
    try:
        r = requests.delete(BASE_URL + path, headers=_sign("DELETE", path), timeout=10)
        return r.json()
    except Exception as e:
        _loge(f"DELETE {path} error: {e}")
        return None

# ════════════════════════════════════════════════════════════════════════
# PRICE FEED  (public REST — fallback when WebSocket mark_price unavailable)
# ════════════════════════════════════════════════════════════════════════
def fetch_price() -> Optional[float]:
    # Prefer WebSocket mark price (already in feed) — fall back to REST
    if feed.mark_price and feed.mark_price > 0:
        return feed.mark_price
    try:
        r    = requests.get(f"{BASE_URL}/v2/tickers/{SYMBOL}", timeout=5)
        data = r.json()
        price = (data.get("result", {}).get("mark_price")
                 or data.get("result", {}).get("close"))
        return float(price) if price else None
    except Exception as e:
        _loge(f"fetch_price error: {e}")
        return None

# ════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ════════════════════════════════════════════════════════════════════════
def tg(msg: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=15,
        )
    except Exception:
        pass

# ════════════════════════════════════════════════════════════════════════
# EXCHANGE HELPERS
# ════════════════════════════════════════════════════════════════════════
def get_open_position() -> Optional[dict]:
    resp = _get("/v2/positions", {"product_id": str(PRODUCT_ID)})
    if not resp:
        return None
    result = resp.get("result", [])
    if isinstance(result, list):
        for p in result:
            if int(p.get("product_id", 0)) == PRODUCT_ID:
                return p if float(p.get("size", 0)) != 0 else None
    elif isinstance(result, dict):
        return result if float(result.get("size", 0)) != 0 else None
    return None

def place_market_order(side: str, size: float, reduce_only: bool = False,
                       ref_price: Optional[float] = None) -> Optional[dict]:
    contracts = _btc_to_contracts(size, ref_price)
    body = {
        "product_id": PRODUCT_ID, "size": contracts,
        "side": side.lower(), "order_type": "market_order",
        "time_in_force": "ioc", "reduce_only": reduce_only,
    }
    api_req_t = time.time()
    resp      = _post("/v2/orders", body)
    api_ack_t = time.time()
    if not resp:
        return None
    result   = resp.get("result", {})
    status   = result.get("state", resp.get("status", ""))
    unfilled = float(result.get("unfilled_size", -1))
    is_filled = (status in ("accepted", "filled", "open")
                 or (status == "closed" and unfilled == 0))
    if is_filled:
        avg = result.get("average_fill_price") or result.get("limit_price")
        log.info(f"[ORDER] Market filled | state={status} fill_px={avg}")
        return {"order_id": result.get("id"), "fill_price": float(avg) if avg else None,
                "api_request_time": api_req_t, "api_ack_time": api_ack_t}
    err_code = resp.get("error", resp.get("message", str(resp)[:300]))
    _loge(f"market order rejected | state={status} | code={err_code}")
    place_market_order._last_error = str(err_code)
    return None

def place_sl_order(close_side: str, size: float, sl_price: float,
                   contracts: Optional[int] = None) -> Optional[dict]:
    sz   = contracts if contracts else _btc_to_contracts(size)
    body = {
        "product_id": PRODUCT_ID, "size": sz,
        "side": close_side.lower(), "order_type": "stop_market_order",
        "stop_price": str(round(sl_price, 1)),
        "reduce_only": True, "time_in_force": "gtc",
    }
    resp = _post("/v2/orders", body)
    if resp and resp.get("result", {}).get("id"):
        return {"order_id": str(resp["result"]["id"]), "placed_time": time.time()}
    _loge(f"SL order failed: {resp}")
    return None

def place_tp_order(close_side: str, size: float, tp_price: float,
                   contracts: Optional[int] = None) -> Optional[dict]:
    sz   = contracts if contracts else _btc_to_contracts(size)
    body = {
        "product_id": PRODUCT_ID, "size": sz,
        "side": close_side.lower(), "order_type": "limit_order",
        "limit_price": str(round(tp_price, 1)),
        "reduce_only": True, "time_in_force": "gtc",
    }
    resp = _post("/v2/orders", body)
    if resp and resp.get("result", {}).get("id"):
        return {"order_id": str(resp["result"]["id"]), "placed_time": time.time()}
    _loge(f"TP order failed: {resp}")
    return None

def cancel_order(order_id: str, retries: int = 3, delay: float = 1.5) -> bool:
    for attempt in range(1, retries + 1):
        _delete(f"/v2/orders/{order_id}")
        time.sleep(delay)
        resp = _get(f"/v2/orders/{order_id}")
        if resp:
            st = resp.get("result", {}).get("state", "")
            if st in ("cancelled", "filled", "closed"):
                _log(f"cancel confirmed oid={order_id} state={st}")
                return True
        _logw(f"cancel attempt {attempt}/{retries} oid={order_id} unconfirmed")
    _loge(f"CANCEL_GAVE_UP oid={order_id} — check Delta UI manually")
    tg(f"⚠️ CANCEL_GAVE_UP order {order_id} — check Delta manually")
    return False

def cancel_all_open_orders():
    for state in ("open", "pending"):
        resp = _get("/v2/orders", {"product_id": str(PRODUCT_ID), "state": state})
        if not resp:
            continue
        for o in resp.get("result", []):
            oid = str(o.get("id", ""))
            if oid:
                _delete(f"/v2/orders/{oid}")
                _log(f"cancel_all: cancelled {state} order {oid}")

# ════════════════════════════════════════════════════════════════════════
# PRE-FLIGHT VALIDATION
# ════════════════════════════════════════════════════════════════════════
def run_preflight() -> dict:
    results = {}
    passed  = True

    ok = bool(API_KEY and API_SECRET)
    results["credentials"] = {"ok": ok}
    if not ok:
        passed = False
        _loge("PRE-FLIGHT FAIL: API credentials missing")

    resp = _get("/v2/profile")
    ok   = resp is not None and "result" in resp
    detail = resp.get("result", {}).get("email", "?") if ok else (
        resp.get("error", str(resp)[:200]) if resp else "no_response")
    results["api_auth"] = {"ok": ok, "detail": detail}
    if not ok:
        passed = False
        _loge(f"PRE-FLIGHT FAIL: API auth rejected — {detail}")

    price = fetch_price()
    ok    = price is not None
    results["price_feed"] = {"ok": ok, "price": price}
    if not ok:
        passed = False
        _loge("PRE-FLIGHT FAIL: price feed unavailable")

    if not PAPER_MODE:
        pos = get_open_position()
        ok  = pos is None
        results["no_open_position"] = {
            "ok": ok,
            "detail": f"size={pos.get('size')} entry={pos.get('entry_price')}" if pos else "flat",
        }
        if not ok:
            passed = False
            _loge("PRE-FLIGHT FAIL: stale open position — close manually before trading")

    stale = load_state()
    ok    = stale is None
    results["clean_state"] = {"ok": ok, "detail": stale.get("trade_id") if stale else "clean"}
    if not ok:
        passed = False
        _loge(f"PRE-FLIGHT FAIL: state file has active trade {stale.get('trade_id')}")

    if not PAPER_MODE:
        bal = _get("/v2/wallet/balances")
        ok  = bal is not None and "result" in bal
        results["balance_fetch"] = {"ok": ok}
        if not ok:
            passed = False
            _loge("PRE-FLIGHT FAIL: balance fetch failed")

    ok = LOT_SIZE >= DELTA_MIN_SIZE_BTC
    results["lot_size"] = {"ok": ok, "lot_btc": LOT_SIZE, "min_btc": DELTA_MIN_SIZE_BTC}
    if not ok:
        passed = False
        _loge(f"PRE-FLIGHT FAIL: LOT_SIZE={LOT_SIZE} < minimum={DELTA_MIN_SIZE_BTC}")

    results["all_passed"] = passed
    results["mode"]       = "PAPER" if PAPER_MODE else "LIVE"
    results["timestamp"]  = datetime.now().isoformat()

    status = "✅ ALL PASSED" if passed else "❌ FAILED"
    log.info(f"[PRE-FLIGHT] {status}")
    if not PAPER_MODE:
        summary = {k: (v.get("ok") if isinstance(v, dict) else v) for k, v in results.items()}
        tg(f"{'✅' if passed else '❌'} Pre-flight {'PASSED' if passed else 'FAILED'}\n"
           f"Mode: LIVE | {json.dumps(summary, indent=2)}")

    return results

# ════════════════════════════════════════════════════════════════════════
# ENTRY QUALITY GRADING
# ════════════════════════════════════════════════════════════════════════
def _structure_grade(slippage_ratio: float) -> str:
    if slippage_ratio < 0.25: return "INTACT"
    if slippage_ratio < 0.5:  return "MILD"
    if slippage_ratio < 1.0:  return "DEGRADED"
    if slippage_ratio < 1.5:  return "BROKEN"
    return "CRITICAL"

# ════════════════════════════════════════════════════════════════════════
# RECONCILIATION LOG
# ════════════════════════════════════════════════════════════════════════
def _write_reconciliation(trade: dict, python_outcome: str, pts: float,
                          exit_slippage: float, trade_duration_sec: float):
    row = {
        "trade_id":              trade["trade_id"],
        "timestamp":             datetime.now().isoformat(),
        "signal_timeframe":      trade.get("signal_timeframe", ""),
        "python_actual_outcome": python_outcome,
        "entry_slippage_pts":    trade.get("entry_slippage_pts", 0),
        "exit_slippage_pts":     exit_slippage,
        "signal_latency_ms":     trade.get("signal_latency_ms", 0),
        "entry_latency_ms":      trade.get("entry_latency_ms", 0),
        "pts":                   pts,
        "trade_duration_sec":    trade_duration_sec,
        "monitor_cycles_total":  trade.get("monitor_cycles", 0),
        "recovery_event":        trade.get("recovery_event",  False),
        "recovery_reason":       trade.get("recovery_reason", ""),
    }
    _append_csv(RECON_FILE, RECON_HEADERS, row)
    _log(f"RECONCILIATION | outcome={python_outcome} pts={pts:+.2f}")

# ════════════════════════════════════════════════════════════════════════
# OPEN TRADE STATE
# ════════════════════════════════════════════════════════════════════════
def _set_open_trade(
    trade_id: str, direction: str, fill_price: float, sl_dist: float,
    pine_entry_px: float, pine_tp: float, pine_sl: float,
    sl_oid: Optional[str], tp_oid: Optional[str],
    pine_signal_time: int, signal_recv_time: float, entry_fill_time: float,
    signal_timeframe: str = "", signal_tf_bar_time: int = 0,
    entry_order_id: Optional[str] = None,
    api_request_time: Optional[float] = None,
    api_ack_time: Optional[float] = None,
    sl_placed_time: Optional[float] = None,
    tp_placed_time: Optional[float] = None,
    # v5-specific signal context
    chop_avg_tr: float = 0.0, burst_threshold: float = 0.0,
    candle_body: float = 0.0, atr5_prev: float = 0.0,
):
    global open_trade
    d = direction

    # Fill-based SL/TP — anchored to actual Delta fill, not Pine signal price
    sl_price = round(fill_price - sl_dist, 1) if d == "BUY" else round(fill_price + sl_dist, 1)
    tp_price = round(fill_price + sl_dist * TP_R, 1) if d == "BUY" else round(fill_price - sl_dist * TP_R, 1)

    entry_slippage   = round(fill_price - pine_entry_px, 2) if d == "BUY" else round(pine_entry_px - fill_price, 2)
    # signal_latency_ms: time from bar close to when engine detected it (should be <500ms)
    signal_latency_ms = round((signal_recv_time - pine_signal_time / 1000) * 1000, 1)
    entry_latency_ms  = round((entry_fill_time - signal_recv_time) * 1000, 1)

    _ratio = round(abs(entry_slippage) / sl_dist, 3) if sl_dist > 0 else 0.0
    _grade = _structure_grade(_ratio)
    _slip_pct = round(abs(entry_slippage) / fill_price * 100, 4) if fill_price else 0.0

    open_trade = {
        "trade_id":           trade_id,
        "direction":          d,
        "mode":               "PAPER" if PAPER_MODE else "LIVE",
        "state":              STATE_ENTERED,
        "signal_timeframe":   signal_timeframe,
        "signal_tf_bar_time": signal_tf_bar_time,
        "monitor_cycles":     0,
        # Prices
        "fill_price":         fill_price,
        "pine_entry_px":      pine_entry_px,
        "sl_dist":            sl_dist,
        "sl_price":           sl_price,
        "tp_price":           tp_price,
        "pine_tp":            pine_tp,
        "pine_sl":            pine_sl,
        # Orders
        "sl_oid":             sl_oid,
        "tp_oid":             tp_oid,
        # Timing
        "entry_slippage_pts": entry_slippage,
        "signal_latency_ms":  signal_latency_ms,
        "entry_latency_ms":   entry_latency_ms,
        "pine_signal_time":   pine_signal_time,
        "signal_recv_time":   signal_recv_time,
        "entry_fill_time":    entry_fill_time,
        # Quality
        "slippage_ratio":     _ratio,
        "structure_grade":    _grade,
        "entry_slippage_pct": _slip_pct,
        # Recovery
        "recovery_event":     False,
        "recovery_reason":    "",
        # Order IDs
        "entry_order_id":     entry_order_id,
        "api_request_time":   api_request_time,
        "api_ack_time":       api_ack_time,
        "sl_placed_time":     sl_placed_time,
        "tp_placed_time":     tp_placed_time,
        "exit_order_id":      None,
        "exit_fill_px_delta": None,
        # v5 signal context
        "chop_avg_tr":        chop_avg_tr,
        "burst_threshold":    burst_threshold,
        "candle_body":        candle_body,
        "atr5_prev":          atr5_prev,
        "sl_dist_engine":     sl_dist,
    }

    save_state()

    _log_lifecycle(trade_id, "ENTRY_ACKED", order_id=entry_order_id or "",
                   side=d.lower(), qty=LOT_SIZE, price=fill_price,
                   notes=f"slip={entry_slippage:+.2f}pts grade={_grade}")
    if sl_oid:
        _log_lifecycle(trade_id, "SL_PLACED", order_id=sl_oid,
                       side="sell" if d == "BUY" else "buy", qty=LOT_SIZE, price=sl_price)
    if tp_oid:
        _log_lifecycle(trade_id, "TP_PLACED", order_id=tp_oid,
                       side="sell" if d == "BUY" else "buy", qty=LOT_SIZE, price=tp_price)

    _log(
        f"STATE→ENTERED | {d} fill={fill_price} slip={entry_slippage:+.2f}pts "
        f"sig_lat={signal_latency_ms:.0f}ms entry_lat={entry_latency_ms:.0f}ms "
        f"sl={sl_price} tp={tp_price} ({TP_R}R) mode={'PAPER' if PAPER_MODE else 'LIVE'}"
    )

    if _ratio >= 1.5:
        _logw(f"[STRUCTURE CRITICAL] ratio={_ratio:.3f}")
    elif _ratio >= 1.0:
        _logw(f"[STRUCTURE BROKEN] ratio={_ratio:.3f}")
    elif _ratio >= 0.5:
        _logw(f"[STRUCTURE DEGRADED] ratio={_ratio:.3f}")

    tg(
        f"{'📄 PAPER' if PAPER_MODE else '🟢 LIVE'} <b>{d} ENTERED</b> [v5 WebSocket]\n"
        f"Fill: <b>{fill_price:,.1f}</b> | Slip: {entry_slippage:+.2f}pts\n"
        f"SL: {sl_price:,.1f} | TP: {tp_price:,.1f} ({TP_R}R)\n"
        f"Signal lat: {signal_latency_ms:.0f}ms | Entry lat: {entry_latency_ms:.0f}ms\n"
        f"Structure: <b>{_grade}</b> | Chop: {chop_avg_tr:.1f} Burst: {burst_threshold:.1f}"
    )

# ════════════════════════════════════════════════════════════════════════
# CLOSE TRADE
# ════════════════════════════════════════════════════════════════════════
def _close_trade(exit_price: float, exit_type: str, exit_slippage: float = 0.0):
    global open_trade
    if not open_trade:
        return

    _log_lifecycle(open_trade["trade_id"], "EXIT_DETECTED", price=exit_price, notes=exit_type)

    trade     = open_trade
    d         = trade["direction"]
    entry_px  = trade["fill_price"]
    pts       = round((exit_price - entry_px) if d == "BUY" else (entry_px - exit_price), 2)
    pnl_approx = round(pts * LOT_SIZE, 4)
    trade_duration_sec = round(time.time() - trade.get("entry_fill_time", time.time()), 1)
    python_outcome     = "TP" if "TP" in exit_type else "SL"

    row = {
        "trade_id":              trade["trade_id"],
        "direction":             d,
        "mode":                  trade.get("mode", "?"),
        "signal_timeframe":      trade.get("signal_timeframe", ""),
        "signal_tf_bar_time":    trade.get("signal_tf_bar_time", ""),
        "pine_entry_px":         trade.get("pine_entry_px", ""),
        "fill_price":            entry_px,
        "entry_slippage_pts":    trade.get("entry_slippage_pts", ""),
        "sl_price":              trade.get("sl_price", ""),
        "tp_price":              trade.get("tp_price", ""),
        "pine_signal_time":      trade.get("pine_signal_time", ""),
        "signal_recv_time":      trade.get("signal_recv_time", ""),
        "entry_fill_time":       trade.get("entry_fill_time", ""),
        "signal_latency_ms":     trade.get("signal_latency_ms", ""),
        "entry_latency_ms":      trade.get("entry_latency_ms", ""),
        "exit_price":            exit_price,
        "exit_time":             datetime.now().isoformat(),
        "exit_type":             exit_type,
        "exit_slippage_pts":     exit_slippage,
        "pts":                   pts,
        "pnl_approx":            pnl_approx,
        "python_actual_outcome": python_outcome,
        "slippage_ratio":        trade.get("slippage_ratio", ""),
        "structure_grade":       trade.get("structure_grade", ""),
        "trade_duration_sec":    trade_duration_sec,
        "monitor_cycles_total":  trade.get("monitor_cycles", 0),
        "recovery_event":        trade.get("recovery_event", False),
        "recovery_reason":       trade.get("recovery_reason", ""),
        "entry_order_id":        trade.get("entry_order_id", ""),
        "api_request_time":      trade.get("api_request_time", ""),
        "api_ack_time":          trade.get("api_ack_time", ""),
        "sl_placed_time":        trade.get("sl_placed_time", ""),
        "tp_placed_time":        trade.get("tp_placed_time", ""),
        "exit_order_id":         trade.get("exit_order_id", ""),
        "exit_fill_px_delta":    trade.get("exit_fill_px_delta", ""),
        "entry_slippage_pct":    trade.get("entry_slippage_pct", ""),
        "chop_avg_tr":           trade.get("chop_avg_tr", ""),
        "burst_threshold":       trade.get("burst_threshold", ""),
        "candle_body":           trade.get("candle_body", ""),
        "atr5_prev":             trade.get("atr5_prev", ""),
        "sl_dist_engine":        trade.get("sl_dist_engine", ""),
    }
    _append_csv(CSV_FILE, CSV_HEADERS, row)
    _write_reconciliation(trade, python_outcome, pts, exit_slippage, trade_duration_sec)

    # Slippage audit row
    _pine_exit = trade.get("pine_tp", "") if "TP" in exit_type else trade.get("pine_sl", "")
    try:   _exit_slip_pct = round(exit_slippage / float(exit_price) * 100, 4) if exit_price else ""
    except: _exit_slip_pct = ""
    try:   _pine_pts = float(trade.get("pine_entry_px", 0)) - float(_pine_exit) if d == "SELL" else float(_pine_exit) - float(trade.get("pine_entry_px", 0))
    except: _pine_pts = ""
    _slip_row = {
        "trade_id":           trade.get("trade_id", ""),
        "timestamp_ist":      (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S"),
        "direction":          d,
        "mode":               trade.get("mode", ""),
        "pine_entry_px":      trade.get("pine_entry_px", ""),
        "delta_entry_fill":   trade.get("fill_price", ""),
        "entry_slippage_pts": trade.get("entry_slippage_pts", ""),
        "entry_slippage_pct": trade.get("entry_slippage_pct", ""),
        "pine_exit_px":       _pine_exit,
        "delta_exit_fill":    exit_price,
        "exit_slippage_pts":  exit_slippage,
        "exit_slippage_pct":  _exit_slip_pct,
        "pine_pts":           _pine_pts,
        "live_pts":           pts,
        "slippage_drag_pts":  round(float(_pine_pts) - pts, 2) if _pine_pts != "" else "",
        "exit_type":          exit_type,
        "signal_latency_ms":  trade.get("signal_latency_ms", ""),
        "timeframe":          trade.get("signal_timeframe", ""),
    }
    _append_csv(SLIPPAGE_FILE, SLIPPAGE_HEADERS, _slip_row)

    emoji = "✅" if pts > 0 else "🔴"
    _log(f"STATE→CLOSED | {d} exit={exit_price} pts={pts:+.2f} outcome={python_outcome}")
    tg(
        f"{emoji} <b>{d} CLOSED</b> [{exit_type}]\n"
        f"Entry: {entry_px:,.1f} → Exit: {exit_price:,.1f}\n"
        f"PnL: <b>{pts:+.2f}pts</b> | Outcome: <b>{python_outcome}</b>\n"
        f"Structure: {trade.get('structure_grade','?')} | Duration: {trade_duration_sec}s"
    )

    open_trade["state"] = STATE_CLOSED
    save_state()
    open_trade = None
    save_state()

# ════════════════════════════════════════════════════════════════════════
# POSITION MONITOR
# PAPER : price comparison at every 2s tick
# LIVE  : Delta position-flat detection
# ════════════════════════════════════════════════════════════════════════
def _position_monitor():
    global open_trade
    log.info("[MON] started")
    time.sleep(POS_MON_DELAY)

    while True:
        time.sleep(PRICE_INTERVAL)
        with _state_lock:
            if not open_trade:
                break

            d    = open_trade["direction"]
            sl   = open_trade["sl_price"]
            tp   = open_trade["tp_price"]
            price = fetch_price()

            if not price:
                _logw("[MON] price fetch failed — skipping tick")
                continue

            open_trade["monitor_cycles"] = open_trade.get("monitor_cycles", 0) + 1

            if PAPER_MODE:
                hit_tp = (d == "BUY" and price >= tp) or (d == "SELL" and price <= tp)
                if hit_tp:
                    slip = round(price - tp, 2) if d == "BUY" else round(tp - price, 2)
                    _log(f"[PAPER] TP hit price={price} tp={tp}")
                    _close_trade(tp, "TP_PAPER", slip)
                    break

                hit_sl = (d == "BUY" and price <= sl) or (d == "SELL" and price >= sl)
                if hit_sl:
                    slip = round(sl - price, 2) if d == "BUY" else round(price - sl, 2)
                    _log(f"[PAPER] SL hit price={price} sl={sl}")
                    _close_trade(sl, "SL_PAPER", slip)
                    break

            else:
                # LIVE: detect position flat
                pos = get_open_position()
                if pos is None:
                    _logw(f"[LIVE] Position flat @ approx price={price}")
                    _log_lifecycle(open_trade["trade_id"], "MONITOR_FLAT", notes=f"approx={price}")

                    exit_fill_px  = price
                    exit_order_id = None
                    exit_label    = "AUTO_EXIT"

                    for oid_key, label in [("tp_oid", "TP_LIVE"), ("sl_oid", "SL_LIVE")]:
                        oid = open_trade.get(oid_key)
                        if not oid:
                            continue
                        order_resp = _get(f"/v2/orders/{oid}")
                        if order_resp:
                            result_data = order_resp.get("result", {})
                            if result_data.get("state") in ("filled", "closed"):
                                raw_fill = result_data.get("average_fill_price")
                                if raw_fill:
                                    exit_fill_px  = float(raw_fill)
                                    exit_order_id = oid
                                    exit_label    = label
                                    _log(f"[LIVE] {label} confirmed oid={oid} fill={exit_fill_px}")
                                    _log_lifecycle(open_trade["trade_id"], "EXIT_CONFIRMED",
                                                   order_id=oid, price=exit_fill_px, notes=label)
                                    break

                    open_trade["exit_order_id"]      = exit_order_id
                    open_trade["exit_fill_px_delta"]  = exit_fill_px

                    for oid_key in ("sl_oid", "tp_oid"):
                        oid = open_trade.get(oid_key)
                        if oid and oid != exit_order_id:
                            _delete(f"/v2/orders/{oid}")
                            _log_lifecycle(open_trade["trade_id"],
                                           f"{oid_key.upper().replace('_OID','')}_CANCELLED", order_id=oid)

                    _close_trade(exit_fill_px, exit_label, 0.0)
                    break

    log.info("[MON] stopped")

# ════════════════════════════════════════════════════════════════════════
# ENTRY PROCESSOR  (runs in background thread)
# ════════════════════════════════════════════════════════════════════════
def _process_entry(
    signal: str,
    sl_dist: float,
    pine_entry_px: float,
    pine_tp: float,
    pine_sl: float,
    pine_signal_time: int,       # bar close time in ms (Unix)
    recv_time: float,            # time.time() when engine detected the signal
    trade_id: str,
    signal_timeframe: str = "",
    signal_tf_bar_time: int = 0,
    # v5 signal context (for CSV logging)
    chop_avg_tr: float = 0.0,
    burst_threshold: float = 0.0,
    candle_body: float = 0.0,
    atr5_prev: float = 0.0,
):
    global open_trade, _entry_processing

    try:
        d = signal
        _log(f"ENTRY_START | {d} sl_dist={sl_dist:.1f} pine_entry={pine_entry_px:,.1f}")

        fill_px = None
        sl_oid  = tp_oid = None
        entry_order_id = api_request_time = api_ack_time = None
        sl_placed_t = tp_placed_t = None

        if PAPER_MODE:
            fill_px = fetch_price()
            if not fill_px:
                _loge("PAPER fill: cannot fetch price — aborting")
                return
            fill_px = round(fill_px, 1)
            _log(f"PAPER fill simulated @ {fill_px}")

        else:
            side            = "buy" if d == "BUY" else "sell"
            entry_contracts = _btc_to_contracts(LOT_SIZE, pine_entry_px)
            _log_lifecycle(trade_id, "ENTRY_SENT", side=side, qty=entry_contracts,
                           price=pine_entry_px, notes=f"contracts={entry_contracts}")
            result = place_market_order(side, LOT_SIZE, ref_price=pine_entry_px)
            if not result:
                err = getattr(place_market_order, "_last_error", "unknown")
                _loge(f"Entry market order FAILED — aborting | Delta: {err}")
                tg(f"❌ ENTRY FAILED [{d}]\nDelta error: <code>{err}</code>")
                return

            entry_order_id   = str(result.get("order_id", ""))
            api_request_time = result.get("api_request_time")
            api_ack_time     = result.get("api_ack_time")
            fill_px          = result.get("fill_price")

            if not fill_px:
                time.sleep(1.5)
                pos = get_open_position()
                fill_px = float(pos.get("entry_price", pine_entry_px)) if pos else pine_entry_px
            fill_px = round(fill_px, 1)

            # Fill-based SL/TP
            close_side = "sell" if d == "BUY" else "buy"
            sl_price   = round(fill_px - sl_dist, 1) if d == "BUY" else round(fill_px + sl_dist, 1)
            tp_price   = round(fill_px + sl_dist * TP_R, 1) if d == "BUY" else round(fill_px - sl_dist * TP_R, 1)

            sl_result = place_sl_order(close_side, LOT_SIZE, sl_price, contracts=entry_contracts)
            tp_result = place_tp_order(close_side, LOT_SIZE, tp_price, contracts=entry_contracts)

            sl_oid      = sl_result["order_id"]   if sl_result else None
            sl_placed_t = sl_result["placed_time"] if sl_result else None
            tp_oid      = tp_result["order_id"]   if tp_result else None
            tp_placed_t = tp_result["placed_time"] if tp_result else None

            if not sl_oid:
                _loge("SL ORDER FAILED — CRITICAL: close position manually on Delta")
                tg(f"🚨 SL FAILED after {d} entry @ {fill_px} — CLOSE MANUALLY ON DELTA")

            # Write latency CSV immediately (crash-safe)
            try:
                _sig_lat = round((recv_time - pine_signal_time / 1000) * 1000, 1) if pine_signal_time else ""
                _api_rt  = round((result.get("api_ack_time", 0) - result.get("api_request_time", 0)) * 1000, 1) if result else ""
                _entry_slip = round(fill_px - pine_entry_px if d == "BUY" else pine_entry_px - fill_px, 1)
                _lat_row = {
                    "trade_id":           trade_id,
                    "timestamp_ist":      (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S"),
                    "direction":          d, "mode": "LIVE",
                    "pine_signal_time":   pine_signal_time or "",
                    "signal_recv_time":   recv_time,
                    "entry_submit_time":  result.get("api_request_time", "") if result else "",
                    "entry_ack_time":     result.get("api_ack_time", "") if result else "",
                    "pine_entry_px":      pine_entry_px,
                    "delta_fill_px":      fill_px,
                    "entry_slippage_pts": _entry_slip,
                    "signal_latency_ms":  _sig_lat,
                    "api_roundtrip_ms":   _api_rt,
                    "sl_price":           sl_price, "tp_price": tp_price,
                    "sl_order_id":        sl_oid or "", "tp_order_id": tp_oid or "",
                    "contracts":          entry_contracts, "entry_order_id": entry_order_id or "",
                }
                _append_csv(LATENCY_FILE, LATENCY_HEADERS, _lat_row)
                log.info(f"[LATENCY_CSV] Entry row written for {trade_id}")
            except Exception as _csv_err:
                _loge(f"[LATENCY_CSV] Write failed (non-fatal): {_csv_err}")

        # ── Slippage guard (PAPER + LIVE) ────────────────────────────────
        if sl_dist > 0 and MAX_SLIPPAGE_RATIO > 0:
            raw_slip   = (fill_px - pine_entry_px) if d == "BUY" else (pine_entry_px - fill_px)
            slip_ratio = round(abs(raw_slip) / sl_dist, 3)
            if slip_ratio > MAX_SLIPPAGE_RATIO:
                _logw(f"ENTRY REJECTED [SLIPPAGE] slip={raw_slip:+.1f}pts ratio={slip_ratio:.3f}")
                tg(
                    f"🚫 <b>ENTRY REJECTED [{d}]</b>\n"
                    f"Slippage {raw_slip:+.1f}pts = {slip_ratio:.2f}× sl_dist\n"
                    f"Signal: {pine_entry_px:,.1f} | Fill: {fill_px:,.1f}"
                )
                if not PAPER_MODE:
                    close_side = "sell" if d == "BUY" else "buy"
                    place_market_order(close_side, LOT_SIZE, reduce_only=True)
                    cancel_all_open_orders()
                return

        entry_fill_time = time.time()

        with _state_lock:
            _set_open_trade(
                trade_id=trade_id, direction=d, fill_price=fill_px,
                sl_dist=sl_dist, pine_entry_px=pine_entry_px,
                pine_tp=pine_tp, pine_sl=pine_sl,
                sl_oid=sl_oid, tp_oid=tp_oid,
                pine_signal_time=pine_signal_time,
                signal_recv_time=recv_time,
                entry_fill_time=entry_fill_time,
                signal_timeframe=signal_timeframe,
                signal_tf_bar_time=signal_tf_bar_time,
                entry_order_id=entry_order_id,
                api_request_time=api_request_time,
                api_ack_time=api_ack_time,
                sl_placed_time=sl_placed_t,
                tp_placed_time=tp_placed_t,
                chop_avg_tr=chop_avg_tr,
                burst_threshold=burst_threshold,
                candle_body=candle_body,
                atr5_prev=atr5_prev,
            )

        threading.Thread(target=_position_monitor, daemon=True, name="mon").start()

    except Exception as e:
        _loge(f"_process_entry exception: {e}")
        tg(f"❌ ENTRY EXCEPTION [{trade_id}]: {e}")
    finally:
        with _state_lock:
            _entry_processing = False

# ════════════════════════════════════════════════════════════════════════
# SIGNAL ENGINE + CANDLE FEED SETUP
# ════════════════════════════════════════════════════════════════════════
sig_cfg = SignalConfig(
    lookback       = VS_LOOKBACK,
    burst_mult     = VS_BURST_MULT,
    sl_mult        = SL_MULT,
    tp2_r          = TP_R,       # keep in sync with TP_R used in order placement
    cooldown       = VS_COOLDOWN,
    use_ema_filter = USE_EMA_FILT,
    use_session    = USE_SESSION,
    safety_factor  = SAFETY_FACTOR,
    use_ha         = USE_HA,     # True = Heikin-Ashi (78% WR), False = regular OHLC (49% WR)
)

engine = SignalEngine(config=sig_cfg, logger=logging.getLogger("signal_engine"))


def on_candle_close(candle: Candle, buffer: deque):
    """
    Called by CandleFeed on every confirmed 15m bar close.
    Runs in the asyncio event loop thread — must not block.
    Entry processing spawned in a daemon thread.
    """
    global _entry_processing

    # Pass in_trade=True when bot already has an open position.
    # This prevents the engine from firing a signal during an active trade.
    with _state_lock:
        currently_in_trade = open_trade is not None or _entry_processing

    state = engine.on_candle_close(candle, buffer, in_trade=currently_in_trade)
    if state is None:
        return

    if not state.signal:
        return

    # ── Signal fired ─────────────────────────────────────────────────
    recv_time = time.time()
    sr        = engine.build_signal_result(state)
    if not sr:
        return

    log.info(
        f"[SIGNAL] {sr.signal} | entry={sr.entry_price:,.1f} "
        f"sl={sr.sl:,.1f} tp={sr.tp2:,.1f} sl_dist={sr.sl_dist:.1f}"
    )

    with _state_lock:
        if open_trade or _entry_processing:
            _logw(f"[SIGNAL] {sr.signal} IGNORED — already in trade or entry processing")
            return
        if not PAPER_MODE and not _preflight_ok:
            _loge(f"[SIGNAL] {sr.signal} BLOCKED — preflight not passed")
            return
        _entry_processing = True

    trade_id = f"{sr.signal[0]}{int(recv_time * 1000)}"

    threading.Thread(
        target=_process_entry,
        kwargs={
            "signal":            sr.signal,
            "sl_dist":           sr.sl_dist,
            "pine_entry_px":     sr.entry_price,
            "pine_tp":           sr.tp2,
            "pine_sl":           sr.sl,
            "pine_signal_time":  (sr.ts + 900) * 1000,  # bar CLOSE Unix ms (start + 15m=900s)
            "recv_time":         recv_time,
            "trade_id":          trade_id,
            "signal_timeframe":  "15",       # 15m candles (CandleFeed candlestick_15m)
            "signal_tf_bar_time": sr.ts,
            # v5 signal context
            "chop_avg_tr":       state.chop_avg_tr,
            "burst_threshold":   state.burst_threshold,
            "candle_body":       state.candle_body,
            "atr5_prev":         state.atr5_prev,
        },
        daemon=True,
        name=f"entry-{trade_id}",
    ).start()


feed = CandleFeed(
    symbol          = SYMBOL,
    buffer_size     = 300,
    on_candle_close = on_candle_close,
    logger          = logging.getLogger("candle_feed"),
)

# ════════════════════════════════════════════════════════════════════════
# FASTAPI APP
# ════════════════════════════════════════════════════════════════════════
app = FastAPI(title="Vol Surge v5 Live — WebSocket-native Signal Engine")

@app.on_event("startup")
async def startup():
    global open_trade, _preflight_ok

    _init_csvs()

    log.info("=" * 70)
    log.info(f"  Vol Surge v5 LIVE | {'📄 PAPER' if PAPER_MODE else '🟢 *** LIVE ***'} mode")
    log.info(f"  Signal source : Delta WebSocket (no TradingView webhook)")
    log.info(f"  Symbol        : {SYMBOL}")
    log.info(f"  Product ID    : {PRODUCT_ID}")
    log.info(f"  LOT_SIZE      : {LOT_SIZE} BTC")
    log.info(f"  TP model      : Single exit at {TP_R}R — fill-based")
    log.info(f"  SL model      : Fixed stop-market — never moved")
    log.info(f"  Engine        : lookback={sig_cfg.lookback} burst_mult={sig_cfg.burst_mult}")
    log.info(f"                  sl_mult={sig_cfg.sl_mult} tp2_r={sig_cfg.tp2_r}")
    log.info(f"                  cooldown={sig_cfg.cooldown} safety_factor={sig_cfg.safety_factor}")
    log.info(f"  Candle type   : {'Heikin-Ashi ✓ (78% WR mode)' if sig_cfg.use_ha else 'Regular OHLC (49% WR mode)'}")
    log.info(f"  EMA filter    : {'ON' if sig_cfg.use_ema_filter else 'OFF'}")
    log.info(f"  Session filter: {'ON' if sig_cfg.use_session else 'OFF'}")
    log.info(f"  Creds         : {'SET ✓' if API_KEY else '⚠️  MISSING'}")
    log.info("=" * 70)

    if PAPER_MODE:
        _preflight_ok = True
        log.info("  Paper mode — preflight skipped, signal engine starting")
    else:
        if not API_KEY or not API_SECRET:
            log.error("LIVE mode: credentials missing — cannot run preflight")
        else:
            log.info("[PRE-FLIGHT] Running live validation checks...")
            pf = run_preflight()
            _preflight_ok = pf.get("all_passed", False)
            if not _preflight_ok:
                log.error("⚠️  PRE-FLIGHT FAILED — entries BLOCKED until resolved")
            else:
                log.info("✅ Pre-flight passed — LIVE mode ready")

    # ── Recovery: resume monitor if active trade exists in state.json ──
    stale = load_state()
    if stale and stale.get("state") == STATE_ENTERED:
        log.warning("[RECOVERY] Resuming open trade from state file")
        with _state_lock:
            open_trade = stale
            _preflight_ok = True
        tg(
            f"♻️ <b>Bot restarted — resuming trade</b>\n"
            f"Trade: {stale.get('trade_id','?')} [{stale.get('direction','?')}]\n"
            f"Fill: {stale.get('fill_price','?')} | SL: {stale.get('sl_price','?')} | TP: {stale.get('tp_price','?')}\n"
            f"Saved at: {stale.get('_saved_at','unknown')}"
        )
        threading.Thread(target=_position_monitor, daemon=True, name="mon-recovery").start()

    # Start the WebSocket candle feed
    asyncio.create_task(feed.start())
    asyncio.create_task(_feed_watchdog())

    tg(
        f"{'📄 PAPER' if PAPER_MODE else '🟢 LIVE'} <b>Vol Surge v5 started</b>\n"
        f"Signal: WebSocket-native (no TV webhook)\n"
        f"Candles: {'Heikin-Ashi ✓' if USE_HA else 'Regular OHLC'}\n"
        f"SL_MULT={SL_MULT} TP_R={TP_R} BURST_MULT={VS_BURST_MULT}"
    )


async def _feed_watchdog():
    """Warn every 5 min if feed is unhealthy."""
    await asyncio.sleep(120)
    while True:
        if not feed.connected:
            log.warning("[WATCHDOG] Feed disconnected")
            tg("⚠️ Vol Surge v5: WebSocket feed disconnected — reconnecting...")
        elif not feed.is_ready:
            log.warning("[WATCHDOG] Feed not ready — buffer thin")
        elif feed.last_closed:
            # Measure from bar CLOSE (ts + 900), not bar START (ts)
            # A 15m bar starts 900s before it closes — using ts directly
            # makes age=15min immediately after close, causing false alarms.
            age = time.time() - (feed.last_closed.ts + 900)
            if age > 1200:   # warn if no bar has CLOSED in the last 20min
                log.warning(f"[WATCHDOG] No bar in {age/60:.1f}min — stale feed")
                tg(f"⚠️ Vol Surge v5: No candle received for {age/60:.1f}min — stale feed")
        await asyncio.sleep(300)


@app.get("/")
@app.get("/health")
async def health():
    price = fetch_price()
    with _state_lock:
        if open_trade:
            d          = open_trade.get("direction")
            fill_px    = open_trade.get("fill_price", 0)
            sl_px      = open_trade.get("sl_price", 0)
            tp_px      = open_trade.get("tp_price", 0)
            unreal_pts = round((price - fill_px) if d == "BUY" else (fill_px - price), 1) if price else None
            dist_sl    = round(abs(price - sl_px), 1) if price else None
            dist_tp    = round(abs(tp_px - price), 1) if price else None
            trade_info = {
                "in_trade":        True,
                "trade_id":        open_trade.get("trade_id"),
                "direction":       d,
                "fill_price":      fill_px,
                "sl_price":        sl_px,
                "tp_price":        tp_px,
                "unrealised_pts":  unreal_pts,
                "dist_to_sl_pts":  dist_sl,
                "dist_to_tp_pts":  dist_tp,
                "entry_time":      open_trade.get("entry_time"),
                "signal_lat_ms":   open_trade.get("signal_latency_ms"),
                "lot_size":        open_trade.get("lot_size"),
            }
        else:
            trade_info = {"in_trade": False}
    return JSONResponse({
        "status":          "healthy" if feed.connected and feed.is_ready else "degraded",
        "bot":             "Vol Surge v5 Live",
        "mode":            "PAPER" if PAPER_MODE else "LIVE",
        "signal_source":   "WebSocket-native (no TV webhook)",
        "preflight_ok":    _preflight_ok,
        "ws_connected":    feed.connected,
        "feed_ready":      feed.is_ready,
        "buffer_size":     len(feed.buffer),
        "mark_price":      feed.mark_price,
        "price":           price,
        "trade":           trade_info,
        "tp_r":            TP_R,
        "sl_mult":         SL_MULT,
        "lot_size_btc":    LOT_SIZE,
        "timestamp":       datetime.now(tz=timezone.utc).isoformat(),
    })


@app.get("/status")
async def status():
    price = fetch_price()
    with _state_lock:
        trade_snap = dict(open_trade) if open_trade else None
    unrealised = None
    if trade_snap and price:
        d = trade_snap["direction"]
        unrealised = round((price - trade_snap["fill_price"]) if d == "BUY"
                           else (trade_snap["fill_price"] - price), 2)
    return JSONResponse({
        "bot":          "Vol Surge v5 Live",
        "mode":         "PAPER" if PAPER_MODE else "LIVE",
        "open_trade":   trade_snap,
        "unrealised_pts": unrealised,
        "mark_price":   price,
        "ws_connected": feed.connected,
        "feed_ready":   feed.is_ready,
        "buffer_size":  len(feed.buffer),
        "timestamp":    datetime.now(tz=timezone.utc).isoformat(),
    })


@app.get("/preflight")
async def preflight():
    global _preflight_ok
    pf = run_preflight()
    _preflight_ok = pf.get("all_passed", False)
    return JSONResponse(pf)


# ── /dashboard ────────────────────────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Live execution dashboard with TradingView-style journal toggle."""

    # ── Load trades ───────────────────────────────────────────────────────────
    trades = []
    try:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            trades = list(csv.DictReader(f))
    except Exception:
        pass

    # ── Load lifecycle events (last 50) ───────────────────────────────────────
    lifecycle_rows = []
    try:
        with open(LIFECYCLE_FILE, "r", encoding="utf-8") as f:
            lifecycle_rows = list(csv.DictReader(f))[-50:]
    except Exception:
        pass

    # ── Snapshot open trade ───────────────────────────────────────────────────
    with _state_lock:
        ot = dict(open_trade) if open_trade else None

    now_ist    = (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S IST")
    mode_label = "🟢 LIVE" if not PAPER_MODE else "📄 PAPER"
    mode_bg    = "#0a2a0a" if not PAPER_MODE else "#1a1a2a"
    mode_col   = "#4ade80" if not PAPER_MODE else "#93c5fd"

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _f(v, dec=1):
        try: return f"{float(v):,.{dec}f}"
        except: return "—"

    def _pts(v):
        try:
            f = float(v); s = "+" if f >= 0 else ""
            return f"{s}{f:.2f}"
        except: return "—"

    def _ms(v):
        try: return f"{float(v):.0f}ms"
        except: return "—"

    def _dur(v):
        try:
            s = int(float(v))
            return f"{s//60}m {s%60}s" if s >= 60 else f"{s}s"
        except: return "—"

    def _dt(v):
        try:
            return (datetime.fromisoformat(str(v)[:19]) + timedelta(seconds=19800)).strftime("%d/%m %H:%M")
        except: return "—"

    def _date(v):
        try:
            return (datetime.fromisoformat(str(v)[:19]) + timedelta(seconds=19800)).strftime("%d/%m")
        except: return "—"

    def _time_only(v):
        try:
            return (datetime.fromisoformat(str(v)[:19]) + timedelta(seconds=19800)).strftime("%H:%M")
        except: return "—"

    def _pc(v):
        try:
            f = float(v)
            c = "#4ade80" if f > 0 else "#f87171" if f < 0 else "#9ca3af"
            s = "+" if f >= 0 else ""
            return f'<span style="color:{c};font-weight:700;">{s}{f:.2f}</span>'
        except: return '<span style="color:#6b7280;">—</span>'

    def _dir(v):
        if v == "BUY":  return '<span style="color:#4ade80;font-weight:700;">▲ BUY</span>'
        if v == "SELL": return '<span style="color:#f87171;font-weight:700;">▼ SELL</span>'
        return "—"

    def _dir_arrow(v):
        if v == "BUY":  return '<span style="color:#4ade80;font-size:16px;font-weight:700;">▲</span>'
        if v == "SELL": return '<span style="color:#f87171;font-size:16px;font-weight:700;">▼</span>'
        return "—"

    def _outcome(v):
        if v == "TP": return '<span style="background:#14532d;color:#4ade80;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">TP ✓</span>'
        if v == "SL": return '<span style="background:#450a0a;color:#f87171;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">SL ✗</span>'
        return f'<span style="color:#6b7280;">{v or "—"}</span>'

    def _grade(v):
        c = {"INTACT":"#4ade80","MILD":"#facc15","DEGRADED":"#fb923c","BROKEN":"#f87171","CRITICAL":"#dc2626"}.get(v,"#6b7280")
        return f'<span style="color:{c};font-weight:600;font-size:11px;">{v or "—"}</span>'

    def _exit_type(v):
        if v == "TP_LIVE":   return '<span style="color:#4ade80;font-size:11px;">TP_LIVE ✓</span>'
        if v == "SL_LIVE":   return '<span style="color:#f87171;font-size:11px;">SL_LIVE ✗</span>'
        if v == "TP_PAPER":  return '<span style="color:#86efac;font-size:11px;">TP ✓</span>'
        if v == "SL_PAPER":  return '<span style="color:#fca5a5;font-size:11px;">SL ✗</span>'
        return f'<span style="color:#6b7280;font-size:11px;">{v or "—"}</span>'

    # ── Stats ─────────────────────────────────────────────────────────────────
    total   = len(trades)
    tp_cnt  = sum(1 for t in trades if t.get("python_actual_outcome") == "TP")
    sl_cnt  = sum(1 for t in trades if t.get("python_actual_outcome") == "SL")
    pts_list = []
    for t in trades:
        try: pts_list.append(float(t["pts"]))
        except: pass
    tot_pts = round(sum(pts_list), 2)
    avg_pts = round(sum(pts_list)/len(pts_list), 2) if pts_list else 0
    win_rt  = f"{round(tp_cnt/total*100)}%" if total else "—"

    slip_list = []
    for t in trades:
        try: slip_list.append(float(t["entry_slippage_pts"]))
        except: pass
    avg_slip = round(sum(slip_list)/len(slip_list), 2) if slip_list else 0

    sig_lat_list = []
    for t in trades:
        try: sig_lat_list.append(float(t.get("signal_latency_ms", 0) or 0))
        except: pass
    avg_sig_lat = round(sum(sig_lat_list)/len(sig_lat_list), 1) if sig_lat_list else 0

    el_list = []
    for t in trades:
        try: el_list.append(float(t["entry_latency_ms"]))
        except: pass
    avg_el = round(sum(el_list)/len(el_list), 1) if el_list else 0

    buy_trades  = [t for t in trades if t.get("direction") == "BUY"]
    sell_trades = [t for t in trades if t.get("direction") == "SELL"]
    buy_wr  = f"{round(sum(1 for t in buy_trades  if t.get('python_actual_outcome')=='TP')/len(buy_trades)*100)}%"  if buy_trades  else "—"
    sell_wr = f"{round(sum(1 for t in sell_trades if t.get('python_actual_outcome')=='TP')/len(sell_trades)*100)}%" if sell_trades else "—"

    tp_pts_l = [float(t["pts"]) for t in trades if t.get("python_actual_outcome")=="TP" and t.get("pts")]
    sl_pts_l = [float(t["pts"]) for t in trades if t.get("python_actual_outcome")=="SL" and t.get("pts")]
    avg_tp_pts = round(sum(tp_pts_l)/len(tp_pts_l), 1) if tp_pts_l else 0
    avg_sl_pts = round(sum(sl_pts_l)/len(sl_pts_l), 1) if sl_pts_l else 0

    grade_order  = ["INTACT","MILD","DEGRADED","BROKEN","CRITICAL"]
    grade_counts = {}
    grade_wr     = {}
    for t in trades:
        g = t.get("structure_grade","")
        if g: grade_counts[g] = grade_counts.get(g, 0) + 1
    for g in grade_order:
        g_trades = [t for t in trades if t.get("structure_grade") == g]
        g_tp = sum(1 for t in g_trades if t.get("python_actual_outcome") == "TP")
        if g_trades:
            grade_wr[g] = (len(g_trades), g_tp, round(g_tp/len(g_trades)*100))

    clean_trades = [t for t in trades if t.get("structure_grade") in ("INTACT","MILD","")]
    clean_tp  = sum(1 for t in clean_trades if t.get("python_actual_outcome") == "TP")
    clean_wr  = f"{round(clean_tp/len(clean_trades)*100)}%" if clean_trades else "—"

    # ── Open trade panel ──────────────────────────────────────────────────────
    open_panel = ""
    if ot:
        d        = ot.get("direction","?")
        fill_px  = ot.get("fill_price", 0)
        sl_px    = ot.get("sl_price", 0)
        tp_px    = ot.get("tp_price", 0)
        slip     = ot.get("entry_slippage_pts", 0)
        sig_lat  = ot.get("signal_latency_ms", 0)
        en_l     = ot.get("entry_latency_ms", 0)
        grade_v  = ot.get("structure_grade","?")
        sl_oid   = ot.get("sl_oid","—")
        tp_oid   = ot.get("tp_oid","—")
        en_oid   = ot.get("entry_order_id","—")
        grade_col = {"INTACT":"#4ade80","MILD":"#facc15","DEGRADED":"#fb923c","BROKEN":"#f87171","CRITICAL":"#dc2626"}.get(grade_v,"#9ca3af")
        dir_col   = "#4ade80" if d == "BUY" else "#f87171"
        elapsed   = round(time.time() - ot.get("entry_fill_time", time.time()))
        elapsed_s = f"{elapsed//60}m {elapsed%60}s" if elapsed >= 60 else f"{elapsed}s"
        live_px   = fetch_price() or 0
        unreal    = round((live_px - fill_px) if d == "BUY" else (fill_px - live_px), 1) if live_px else 0
        unreal_col = "#4ade80" if unreal >= 0 else "#f87171"
        open_panel = f"""
<div style="background:#0a1f0a;border:2px solid #166534;border-radius:10px;margin:0 24px 20px;padding:16px 20px;">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;">
    <span style="background:#166534;color:#4ade80;padding:3px 10px;border-radius:4px;font-size:12px;font-weight:700;">🔴 LIVE TRADE OPEN</span>
    <span style="color:{dir_col};font-size:18px;font-weight:700;">{'▲' if d=='BUY' else '▼'} {d}</span>
    <span style="color:#6b7280;font-size:12px;">in trade for {elapsed_s}</span>
  </div>
  <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;">
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Fill Price</div><div style="color:#f9fafb;font-size:16px;font-weight:700;">{_f(fill_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Live Price</div><div style="color:#60a5fa;font-size:16px;font-weight:700;">{_f(live_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Unrealized</div><div style="color:{unreal_col};font-size:16px;font-weight:700;">{'+' if unreal>=0 else ''}{unreal:.1f} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">SL Level</div><div style="color:#f87171;font-size:16px;">{_f(sl_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">TP Level</div><div style="color:#4ade80;font-size:16px;">{_f(tp_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Dist to SL</div><div style="color:#f87171;font-size:14px;">{round(abs(live_px-sl_px),1) if live_px else '—'} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Dist to TP</div><div style="color:#4ade80;font-size:14px;">{round(abs(tp_px-live_px),1) if live_px else '—'} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Entry Slippage</div><div style="color:#facc15;font-size:16px;">{'+' if float(slip or 0)>=0 else ''}{float(slip or 0):.2f} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Structure Grade</div><div style="color:{grade_col};font-size:16px;font-weight:700;">{grade_v}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Signal Latency</div><div style="color:#60a5fa;font-size:14px;">{float(sig_lat or 0):.0f}ms</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Entry Latency</div><div style="color:#60a5fa;font-size:14px;">{float(en_l or 0):.0f}ms</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">SL / TP Order</div><div style="color:#6b7280;font-size:11px;font-family:monospace;">{str(sl_oid)[:8] if sl_oid else '—'} / {str(tp_oid)[:8] if tp_oid else '—'}</div></div>
  </div>
</div>"""

    # ── Grade rows ────────────────────────────────────────────────────────────
    grade_rows = ""
    for g in grade_order:
        cnt = grade_counts.get(g, 0)
        pct = round(cnt/total*100) if total else 0
        wr_data = grade_wr.get(g)
        wr_str  = f"{wr_data[2]}%" if wr_data else "—"
        col = {"INTACT":"#4ade80","MILD":"#facc15","DEGRADED":"#fb923c","BROKEN":"#f87171","CRITICAL":"#dc2626"}.get(g,"#6b7280")
        bar = "█" * min(pct, 30)
        grade_rows += f"""<tr>
          <td style="color:{col};font-weight:600;">{g}</td>
          <td style="color:#e2e8f0;text-align:right;">{cnt}</td>
          <td style="color:#9ca3af;text-align:right;">{pct}%</td>
          <td style="color:#e2e8f0;text-align:right;">{wr_str}</td>
          <td style="color:{col};font-size:10px;letter-spacing:1px;">{bar}</td>
        </tr>"""

    # ── Lifecycle rows ────────────────────────────────────────────────────────
    lc_rows = ""
    for e in reversed(lifecycle_rows[-20:]):
        ev = e.get("event","")
        ev_col = "#4ade80" if "TP" in ev or "ACKED" in ev else "#f87171" if "SL" in ev or "CANCEL" in ev else "#60a5fa"
        lc_rows += f"""<tr style="border-bottom:1px solid #1f2937;">
          <td style="color:#6b7280;font-size:11px;">{e.get('timestamp_ist','')}</td>
          <td style="color:{ev_col};font-weight:600;font-size:11px;">{ev}</td>
          <td style="color:#9ca3af;font-size:11px;font-family:monospace;">{e.get('trade_id','')[:16]}</td>
          <td style="color:#e2e8f0;text-align:right;">{_f(e.get('price',''))}</td>
          <td style="color:#60a5fa;text-align:right;">{e.get('latency_from_prev_ms','')}</td>
          <td style="color:#6b7280;font-size:11px;font-family:monospace;">{str(e.get('order_id',''))[:12]}</td>
          <td style="color:#6b7280;font-size:11px;">{e.get('notes','')}</td>
        </tr>"""
    lc_empty = "" if lc_rows else '<tr><td colspan="7" style="color:#4b5563;text-align:center;padding:16px;">No lifecycle events yet</td></tr>'

    # ── Journal rows — DETAILED view (current dashboard style) ───────────────
    journal_rows = ""
    for i, t in enumerate(reversed(trades), 1):
        outcome = t.get("python_actual_outcome","")
        rbg = "#0a1a0a" if outcome=="TP" else "#1a0a0a" if outcome=="SL" else "#0d1117"
        rec = "♻️" if str(t.get("recovery_event","")).lower()=="true" else ""
        slip_pct = t.get("entry_slippage_pct","")
        journal_rows += f"""
        <tr style="background:{rbg};border-bottom:1px solid #1f2937;">
          <td style="color:#6b7280;text-align:center;">{len(trades)-i+1}</td>
          <td>{_dir(t.get('direction',''))}</td>
          <td style="color:#d1d5db;font-size:11px;">{_dt(t.get('entry_fill_time',''))}</td>
          <td style="color:#9ca3af;text-align:center;">{t.get('signal_timeframe','—')}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('fill_price',''))}</td>
          <td style="color:#34d399;text-align:right;">{_f(t.get('pine_tp',''))}</td>
          <td style="color:#f87171;text-align:right;">{_f(t.get('pine_sl',''))}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('exit_price',''))}</td>
          <td style="text-align:right;">{_pc(t.get('pts',''))}</td>
          <td style="color:#9ca3af;text-align:right;">{_pts(t.get('pnl_approx',''))}</td>
          <td style="text-align:center;">{_outcome(outcome)}</td>
          <td style="text-align:center;">{_exit_type(t.get('exit_type',''))}</td>
          <td style="text-align:right;">{_pc(t.get('entry_slippage_pts',''))}</td>
          <td style="color:#9ca3af;text-align:right;font-size:10px;">{f"{float(slip_pct):.4f}%" if slip_pct else "—"}</td>
          <td style="color:#60a5fa;text-align:right;">{_ms(t.get('signal_latency_ms',''))}</td>
          <td style="color:#9ca3af;text-align:right;">{_ms(t.get('entry_latency_ms',''))}</td>
          <td style="color:#9ca3af;text-align:right;">{_dur(t.get('trade_duration_sec',''))}</td>
          <td style="text-align:center;">{_grade(t.get('structure_grade',''))}</td>
          <td style="color:#6b7280;font-size:10px;font-family:monospace;">{str(t.get('entry_order_id','') or '')[:10] or '—'}</td>
          <td style="color:#6b7280;font-size:10px;font-family:monospace;">{str(t.get('exit_order_id','') or '')[:10] or '—'}</td>
          <td style="color:#9ca3af;text-align:center;">{rec or '—'}</td>
        </tr>"""

    journal_empty = "" if trades else '<tr><td colspan="21" style="text-align:center;color:#6b7280;padding:40px;">No trades yet — waiting for first signal</td></tr>'

    # ── TV journal rows — TRADINGVIEW style ───────────────────────────────────
    tv_rows = ""
    cumulative_pts = 0.0
    for i, t in enumerate(trades):   # oldest first for cumulative calc
        try: cumulative_pts += float(t.get("pts", 0) or 0)
        except: pass
    cumulative_pts = 0.0
    for i, t in enumerate(reversed(trades), 1):
        outcome  = t.get("python_actual_outcome","")
        rbg      = "#0a1a0a" if outcome=="TP" else "#1a0a0a" if outcome=="SL" else "#0d1117"
        pts_val  = 0.0
        try: pts_val = float(t.get("pts", 0) or 0)
        except: pass
        pnl_val  = 0.0
        try: pnl_val = float(t.get("pnl_approx", 0) or 0)
        except: pass
        lot_val  = t.get("lot_size", LOT_SIZE)
        status_str = ("TP2 ✓" if outcome=="TP" else "SL ✗" if outcome=="SL" else "—")
        status_col = "#4ade80" if outcome=="TP" else "#f87171" if outcome=="SL" else "#9ca3af"
        pts_col  = "#4ade80" if pts_val >= 0 else "#f87171"
        pnl_col  = "#4ade80" if pnl_val >= 0 else "#f87171"
        tv_rows += f"""
        <tr style="background:{rbg};border-bottom:1px solid #1f2937;">
          <td>{_dir_arrow(t.get('direction',''))}</td>
          <td style="color:#9ca3af;font-size:11px;">{_date(t.get('entry_fill_time',''))}</td>
          <td style="color:#d1d5db;font-size:11px;">{_time_only(t.get('entry_fill_time',''))}</td>
          <td style="color:#d1d5db;font-size:11px;">{_time_only(t.get('exit_time',''))}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('fill_price',''))}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('exit_price',''))}</td>
          <td style="color:#f87171;text-align:right;">{_f(t.get('pine_sl',''))}</td>
          <td style="color:{pts_col};text-align:right;font-weight:700;">{'+' if pts_val>=0 else ''}{pts_val:.1f}</td>
          <td style="color:#9ca3af;text-align:right;">{lot_val}</td>
          <td style="color:{pnl_col};text-align:right;font-weight:700;">{'+' if pnl_val>=0 else ''}{pnl_val:.2f}</td>
          <td style="color:{pnl_col};text-align:right;">{'+' if pnl_val>=0 else ''}{pnl_val:.2f}</td>
          <td style="color:{status_col};text-align:center;font-size:11px;font-weight:700;">{status_str}</td>
        </tr>"""

    tv_empty = "" if trades else '<tr><td colspan="12" style="text-align:center;color:#6b7280;padding:40px;">No trades yet — waiting for first signal</td></tr>'

    # ── Persistence warning ───────────────────────────────────────────────────
    _persist_warn = "" if _DATA_PERSISTENT else """
    <div style="background:#2a1a00;border:1px solid #f59e0b;border-radius:8px;padding:12px 20px;
                margin:0 24px 16px;display:flex;align-items:center;gap:12px;">
      <span style="font-size:18px">⚠️</span>
      <div>
        <div style="color:#f59e0b;font-weight:700;font-size:13px">DATA NOT PERSISTING — Railway Volume Not Mounted</div>
        <div style="color:#d97706;font-size:11px;margin-top:3px">
          trades_v5.csv and state_v5.json wiped on every redeploy.<br>
          Fix: Railway → Storage → Add Volume → Mount Path: <code>/app/data</code> → <code>DATA_DIR=/app/data</code>
        </div>
      </div>
    </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vol Surge v5 — Live Dashboard</title>
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{background:#080c10;color:#e2e8f0;font-family:'Segoe UI',system-ui,monospace;font-size:13px}}
  .hdr{{background:#0d1117;border-bottom:2px solid #1f2937;padding:14px 24px;display:flex;align-items:center;justify-content:space-between}}
  .hdr h1{{font-size:17px;font-weight:700;color:#f9fafb}}
  .sec{{font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:1px;padding:16px 24px 8px;display:flex;align-items:center;justify-content:space-between}}
  .stats{{display:flex;gap:10px;padding:0 24px 16px;flex-wrap:wrap}}
  .stat{{background:#0d1117;border:1px solid #1e293b;border-radius:8px;padding:12px 16px;min-width:120px}}
  .sv{{font-size:20px;font-weight:700}}
  .sl{{font-size:10px;color:#6b7280;margin-top:3px;text-transform:uppercase}}
  .panel{{background:#0d1117;border:1px solid #1e293b;border-radius:8px;margin:0 24px 16px;padding:16px}}
  .panel h3{{font-size:12px;color:#9ca3af;font-weight:600;margin-bottom:12px;text-transform:uppercase;letter-spacing:0.5px}}
  .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
  .grid3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px}}
  table{{width:100%;border-collapse:collapse;font-size:11px}}
  th{{background:#0d1117;color:#6b7280;font-weight:600;text-transform:uppercase;font-size:10px;padding:8px 10px;border-bottom:2px solid #1f2937;white-space:nowrap;position:sticky;top:0}}
  td{{padding:7px 10px;white-space:nowrap;vertical-align:middle}}
  tr:hover td{{background:#1e293b!important}}
  .tw{{padding:0 24px 24px;overflow-x:auto}}
  .kv{{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1e293b;font-size:12px}}
  .kv:last-child{{border-bottom:none}}
  .kl{{color:#6b7280}}
  .kv2{{color:#e2e8f0;font-weight:600}}
  .insight{{background:#0f1f0f;border-left:3px solid #4ade80;padding:8px 12px;border-radius:4px;font-size:12px;color:#86efac;margin:4px 0}}
  .insight.warn{{background:#1f0f0f;border-color:#f87171;color:#fca5a5}}
  .insight.info{{background:#0f1525;border-color:#60a5fa;color:#93c5fd}}
  .footer{{text-align:center;padding:16px;color:#374151;font-size:10px;border-top:1px solid #1f2937;margin-top:8px}}
  .toggle-btn{{background:#1e293b;color:#e2e8f0;border:1px solid #334155;border-radius:6px;padding:5px 14px;font-size:11px;cursor:pointer;font-family:inherit;transition:all 0.2s}}
  .toggle-btn.active{{background:#1d4ed8;color:#fff;border-color:#2563eb}}
  .toggle-group{{display:flex;gap:6px}}
  .hidden{{display:none}}
</style>
<script>
  setTimeout(()=>location.reload(),15000);
  setInterval(()=>{{document.getElementById('clk').textContent=new Date().toLocaleTimeString('en-IN',{{timeZone:'Asia/Kolkata'}})}},1000);
  window.onload=()=>document.getElementById('clk').textContent=new Date().toLocaleTimeString('en-IN',{{timeZone:'Asia/Kolkata'}});

  function switchView(mode) {{
    const det = document.getElementById('view-detailed');
    const tv  = document.getElementById('view-tv');
    const thDet = document.getElementById('th-detailed');
    const thTv  = document.getElementById('th-tv');
    const btnDet = document.getElementById('btn-detailed');
    const btnTv  = document.getElementById('btn-tv');
    if (mode === 'detailed') {{
      det.classList.remove('hidden'); tv.classList.add('hidden');
      thDet.classList.remove('hidden'); thTv.classList.add('hidden');
      btnDet.classList.add('active'); btnTv.classList.remove('active');
    }} else {{
      tv.classList.remove('hidden'); det.classList.add('hidden');
      thTv.classList.remove('hidden'); thDet.classList.add('hidden');
      btnTv.classList.add('active'); btnDet.classList.remove('active');
    }}
  }}
</script>
</head>
<body>

<!-- HEADER -->
<div class="hdr">
  <div>
    <h1>⚡ Vol Surge v5 — Live Dashboard</h1>
    <div style="color:#6b7280;font-size:11px;margin-top:3px;">BTCUSD · Delta Exchange India · WebSocket-native · auto-refresh 15s · {now_ist}</div>
  </div>
  <div style="text-align:right;display:flex;flex-direction:column;align-items:flex-end;gap:6px;">
    <span style="background:{mode_bg};color:{mode_col};padding:4px 14px;border-radius:20px;font-size:12px;font-weight:700;">{mode_label}</span>
    <span style="color:#6b7280;font-size:11px;">🕐 IST <b id="clk"></b></span>
    <span style="color:#{'4ade80' if ot else '6b7280'};font-size:11px;">{'🔴 POSITION OPEN' if ot else '⚪ IDLE'}</span>
    <span style="color:#{'4ade80' if _DATA_PERSISTENT else 'f59e0b'};font-size:10px;">{'💾 Data Persistent' if _DATA_PERSISTENT else '⚠️ Data Ephemeral'}</span>
  </div>
</div>

{_persist_warn}

<div style="padding:4px 24px 0;font-size:11px;color:#6b7280;">
  📊 Closed trades: <b style="color:#e2e8f0">{len(trades)}</b>
  &nbsp;|&nbsp; Data path: <code style="color:#60a5fa">{DATA_DIR}</code>
  &nbsp;|&nbsp; Signal source: <span style="color:#4ade80">⚡ WebSocket-native (&lt;100ms)</span>
</div>

<!-- OPEN TRADE PANEL -->
{open_panel}

<!-- PERFORMANCE STATS -->
<div class="sec">Performance Summary</div>
<div class="stats">
  <div class="stat"><div class="sv" style="color:#f9fafb">{total}</div><div class="sl">Total Trades</div></div>
  <div class="stat"><div class="sv" style="color:{'#4ade80' if tp_cnt>=sl_cnt else '#f87171'}">{win_rt}</div><div class="sl">Win Rate</div></div>
  <div class="stat"><div class="sv" style="color:#4ade80">{tp_cnt}</div><div class="sl">TP Hits</div></div>
  <div class="stat"><div class="sv" style="color:#f87171">{sl_cnt}</div><div class="sl">SL Hits</div></div>
  <div class="stat"><div class="sv" style="color:{'#4ade80' if tot_pts>=0 else '#f87171'}">{'+' if tot_pts>0 else ''}{tot_pts}</div><div class="sl">Total Pts</div></div>
  <div class="stat"><div class="sv" style="color:{'#4ade80' if avg_pts>=0 else '#f87171'}">{'+' if avg_pts>0 else ''}{avg_pts}</div><div class="sl">Avg Pts/Trade</div></div>
  <div class="stat"><div class="sv" style="color:#4ade80">{avg_tp_pts:+.1f}</div><div class="sl">Avg TP Pts</div></div>
  <div class="stat"><div class="sv" style="color:#f87171">{avg_sl_pts:+.1f}</div><div class="sl">Avg SL Pts</div></div>
  <div class="stat"><div class="sv" style="color:#facc15">{avg_slip:+.2f}</div><div class="sl">Avg Entry Slip</div></div>
  <div class="stat"><div class="sv" style="color:#60a5fa">{avg_sig_lat:.0f}ms</div><div class="sl">Avg Signal Lat</div></div>
  <div class="stat"><div class="sv" style="color:#818cf8">{avg_el:.0f}ms</div><div class="sl">Avg Entry Lat</div></div>
  <div class="stat"><div class="sv" style="color:#4ade80">{buy_wr}</div><div class="sl">BUY Win Rate</div></div>
  <div class="stat"><div class="sv" style="color:#f87171">{sell_wr}</div><div class="sl">SELL Win Rate</div></div>
</div>

<!-- ANALYSIS PANELS -->
<div class="grid2" style="padding:0 24px;gap:16px;margin-bottom:16px;">
  <div class="panel">
    <h3>🎯 Fill Quality vs Signal</h3>
    <div class="kv"><span class="kl">Avg entry slippage</span><span class="kv2">{avg_slip:+.2f} pts</span></div>
    <div class="kv"><span class="kl">BUY fill slippage avg</span><span class="kv2">{'—' if not buy_trades else f"{round(sum(float(t.get('entry_slippage_pts',0)) for t in buy_trades)/len(buy_trades),2):+.2f} pts"}</span></div>
    <div class="kv"><span class="kl">SELL fill slippage avg</span><span class="kv2">{'—' if not sell_trades else f"{round(sum(float(t.get('entry_slippage_pts',0)) for t in sell_trades)/len(sell_trades),2):+.2f} pts"}</span></div>
    <div class="kv" style="margin-top:8px"><span class="kl">Grade distribution</span><span></span></div>
    <table style="margin-top:6px">
      <thead><tr><th>Grade</th><th style="text-align:right">Count</th><th style="text-align:right">%</th><th style="text-align:right">Win Rate</th><th>Bar</th></tr></thead>
      <tbody>{grade_rows or '<tr><td colspan="5" style="color:#4b5563;padding:8px">No data yet</td></tr>'}</tbody>
    </table>
  </div>
  <div class="panel">
    <h3>💡 Profitability Insights</h3>
    <div class="kv"><span class="kl">Overall win rate</span><span class="kv2">{win_rt} ({total} trades)</span></div>
    <div class="kv"><span class="kl">INTACT+MILD only</span><span class="kv2">{clean_wr} ({len(clean_trades)} trades)</span></div>
    <div class="kv"><span class="kl">BUY win rate</span><span class="kv2">{buy_wr} ({len(buy_trades)} trades)</span></div>
    <div class="kv"><span class="kl">SELL win rate</span><span class="kv2">{sell_wr} ({len(sell_trades)} trades)</span></div>
    <div class="kv"><span class="kl">Avg pts on TP</span><span class="kv2" style="color:#4ade80">{avg_tp_pts:+.1f} pts</span></div>
    <div class="kv"><span class="kl">Avg pts on SL</span><span class="kv2" style="color:#f87171">{avg_sl_pts:+.1f} pts</span></div>
    <div class="kv"><span class="kl">Required WR break-even</span><span class="kv2">{f"{round(abs(avg_sl_pts)/(abs(avg_sl_pts)+avg_tp_pts)*100)}%" if avg_tp_pts>0 and avg_sl_pts<0 else "—"}</span></div>
    <div style="margin-top:10px">
      {"<div class='insight'>✅ INTACT entries performing well</div>" if grade_wr.get("INTACT",("","",0))[2]>60 else ""}
      {"<div class='insight warn'>⚠️ DEGRADED+ hurting win rate — consider skipping</div>" if grade_counts.get("DEGRADED",0)+grade_counts.get("BROKEN",0)+grade_counts.get("CRITICAL",0)>2 else ""}
      {"<div class='insight info'>ℹ️ Need 20+ trades for meaningful insights</div>" if total<20 else ""}
      {"<div class='insight'>✅ Sufficient data for analysis</div>" if total>=20 else ""}
    </div>
  </div>
</div>

<!-- LATENCY -->
<div class="panel" style="margin:0 24px 16px;">
  <h3>⚡ v5 WebSocket Latency (vs v4 Webhook)</h3>
  <div class="grid3">
    <div>
      <div class="kv"><span class="kl">Avg signal latency</span><span class="kv2" style="color:#4ade80">{avg_sig_lat:.0f} ms</span></div>
      <div class="kv"><span class="kl">Best signal lat</span><span class="kv2" style="color:#4ade80">{f"{min(sig_lat_list):.0f} ms" if sig_lat_list else "—"}</span></div>
      <div class="kv"><span class="kl">Worst signal lat</span><span class="kv2" style="color:#facc15">{f"{max(sig_lat_list):.0f} ms" if sig_lat_list else "—"}</span></div>
    </div>
    <div>
      <div class="kv"><span class="kl">Avg entry latency</span><span class="kv2" style="color:#818cf8">{avg_el:.0f} ms</span></div>
      <div class="kv"><span class="kl">Best entry</span><span class="kv2" style="color:#4ade80">{f"{min(el_list):.0f} ms" if el_list else "—"}</span></div>
      <div class="kv"><span class="kl">Worst entry</span><span class="kv2" style="color:#f87171">{f"{max(el_list):.0f} ms" if el_list else "—"}</span></div>
    </div>
    <div>
      <div class="kv"><span class="kl">Total avg end-to-end</span><span class="kv2" style="color:#facc15">{round(avg_sig_lat+avg_el):.0f} ms</span></div>
      <div class="kv"><span class="kl">v4 webhook was</span><span class="kv2" style="color:#f87171">5000–7000 ms</span></div>
      <div class="kv"><span class="kl">Target</span><span class="kv2" style="color:#4ade80">Signal &lt;500ms · Entry &lt;500ms</span></div>
    </div>
  </div>
</div>

<!-- ORDER LIFECYCLE -->
<div class="sec">Order Lifecycle — Last 20 Events</div>
<div class="tw">
<table>
  <thead><tr>
    <th>IST Time</th><th>Event</th><th>Trade ID</th><th style="text-align:right">Price</th>
    <th style="text-align:right">+ms</th><th>Order ID</th><th>Notes</th>
  </tr></thead>
  <tbody>{lc_rows or lc_empty}</tbody>
</table>
</div>

<!-- TRADE JOURNAL with toggle -->
<div class="sec">
  <span>Trade Journal — All Trades (newest first)</span>
  <div class="toggle-group">
    <button class="toggle-btn active" id="btn-detailed" onclick="switchView('detailed')">📋 Detailed</button>
    <button class="toggle-btn" id="btn-tv" onclick="switchView('tv')">📊 TradingView Style</button>
  </div>
</div>

<div class="tw">
<table>
  <!-- DETAILED headers -->
  <thead id="th-detailed">
    <tr>
      <th>#</th><th>Dir</th><th>Time (IST)</th><th>TF</th>
      <th style="text-align:right">Fill $</th>
      <th style="text-align:right">TP $</th>
      <th style="text-align:right">SL $</th>
      <th style="text-align:right">Exit $</th>
      <th style="text-align:right">Pts</th>
      <th style="text-align:right">P&L</th>
      <th>Result</th>
      <th>Exit Type</th>
      <th style="text-align:right">Slip pts</th>
      <th style="text-align:right">Slip %</th>
      <th style="text-align:right">Sig lat</th>
      <th style="text-align:right">En lat</th>
      <th style="text-align:right">Duration</th>
      <th>Grade</th>
      <th>Entry OID</th>
      <th>Exit OID</th>
      <th>Rec</th>
    </tr>
  </thead>
  <!-- TV-STYLE headers -->
  <thead id="th-tv" class="hidden">
    <tr>
      <th>Dir</th><th>Date</th><th>In</th><th>Out</th>
      <th style="text-align:right">Entry $</th>
      <th style="text-align:right">Exit $</th>
      <th style="text-align:right">SL $</th>
      <th style="text-align:right">Pts</th>
      <th style="text-align:right">Lots</th>
      <th style="text-align:right">P&L $</th>
      <th style="text-align:right">Fix P&L</th>
      <th style="text-align:center">Status</th>
    </tr>
  </thead>
  <tbody id="view-detailed">
    {journal_rows}
    {journal_empty}
  </tbody>
  <tbody id="view-tv" class="hidden">
    {tv_rows}
    {tv_empty}
  </tbody>
</table>
</div>

<div class="footer">
  Vol Surge v5 · WebSocket-native · HA candles (78% WR) · TP=1.4R · SL=1.8×ATR · LOT={LOT_SIZE} BTC · {'LIVE' if not PAPER_MODE else 'PAPER'}
</div>
</body>
</html>"""
    return HTMLResponse(content=html)
