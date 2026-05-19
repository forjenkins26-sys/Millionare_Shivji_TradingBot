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
        trade_info = {
            "in_trade":    open_trade is not None,
            "direction":   open_trade.get("direction") if open_trade else None,
            "fill_price":  open_trade.get("fill_price") if open_trade else None,
            "sl_price":    open_trade.get("sl_price") if open_trade else None,
            "tp_price":    open_trade.get("tp_price") if open_trade else None,
        }
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
