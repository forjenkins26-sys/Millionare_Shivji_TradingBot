#!/usr/bin/env python3
"""
volsurge_v4.py — Vol Surge Bot v4.0 (Simplified Lifecycle Build)
================================================================
Philosophy  : TradingView = Signal Intent | Python = Execution Authority | Delta = Reality
Lifecycle   : IDLE → ENTERED → CLOSED
TP model    : Single full-position exit at 2R (sl_dist × 2.0). No partials.
SL model    : Fixed stop-market at entry ± sl_dist. Never moved.

Modes:
  PAPER_MODE=true  → fills simulated at live Delta price, no real orders
  PAPER_MODE=false → real orders on Delta Exchange India LIVE

Start:
  python -m uvicorn volsurge_v4:app --host 0.0.0.0 --port 5001

Changes from v3.1:
  - Removed TP1 partial close, TP1_DONE state, blended PnL, order resizing
  - Removed TP1_HIT / TP2_HIT webhook signals (Pine sends entry only)
  - Removed Model B simulation, intrabar timing telemetry
  - Monitor: 2 branches only (TP hit / SL hit) vs 4 branches in v3
  - CSV schema: simplified to essential columns only
  - One active state only: ENTERED (no intermediate TP1_DONE state)
  - Recovery lifecycle: single path, no tp1_hit branching
  - PARTIAL_LOT removed — all orders use full LOT_SIZE
  - LOT_SIZE default reduced to 0.001 BTC (Delta minimum, no partial needed)
"""

# ════════════════════════════════════════════════════════════════════════
# IMPORTS
# ════════════════════════════════════════════════════════════════════════
import os, sys, json, time, hmac, hashlib, logging, threading, csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse

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
API_KEY        = os.getenv("DELTA_API_KEY_LIVE",    "")
API_SECRET     = os.getenv("DELTA_API_SECRET_LIVE", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET",        "abc123")
TG_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN",    "")
TG_CHAT        = os.getenv("TELEGRAM_CHAT_ID",      "")

PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"

BASE_URL   = "https://api.india.delta.exchange"
PRODUCT_ID = int(os.getenv("PRODUCT_ID", "27"))      # BTCUSD Perpetual

# Full position size. No PARTIAL_LOT needed — single full exit only.
# Default = 0.001 BTC (Delta minimum). Override via LOT_SIZE= in .env.
LOT_SIZE           = float(os.getenv("LOT_SIZE", "0.001"))
DELTA_MIN_SIZE_BTC = 0.001   # confirmed minimum for BTCUSD Perpetual (India)

TP_R = 2.0   # TP = entry ± sl_dist × 2.0  (2R)

# Entry guards — reject trade if either threshold is breached.
# MAX_SLIPPAGE_RATIO : reject if |fill - pine_entry| / sl_dist > this value.
#   0.75 = reject if slippage exceeds 75% of sl_dist (DEGRADED or worse).
#   Set to 0.0 to disable.
# MAX_WH_LATENCY_MS  : reject if webhook arrived more than this many ms late.
#   2000ms = 2 seconds. Signal price is stale beyond this.
#   Set to 0 to disable.
MAX_SLIPPAGE_RATIO = float(os.getenv("MAX_SLIPPAGE_RATIO", "0.0"))
MAX_WH_LATENCY_MS  = float(os.getenv("MAX_WH_LATENCY_MS",  "0"))

PRICE_INTERVAL = 2   # seconds between price poll ticks
POS_MON_DELAY  = 3   # seconds to wait after entry before monitor starts

# ════════════════════════════════════════════════════════════════════════
# STATE CONSTANTS  (TP1_DONE intentionally absent)
# ════════════════════════════════════════════════════════════════════════
STATE_IDLE    = "IDLE"
STATE_ENTERED = "ENTERED"
STATE_CLOSED  = "CLOSED"

# ════════════════════════════════════════════════════════════════════════
# FILE PATHS
# Railway: mount persistent volume at /app/data.
# Override via DATA_DIR= / LOG_DIR= env vars if needed.
# ════════════════════════════════════════════════════════════════════════
DATA_DIR   = Path(os.getenv("DATA_DIR", "data")); DATA_DIR.mkdir(exist_ok=True)
LOG_DIR    = Path(os.getenv("LOG_DIR",  "logs")); LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"
CSV_FILE   = DATA_DIR / "trades.csv"
RECON_FILE = DATA_DIR / "reconciliation.csv"
LOG_FILE   = LOG_DIR  / "volsurge_v4.log"

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
log = logging.getLogger("volsurge")

# ════════════════════════════════════════════════════════════════════════
# GLOBAL STATE
# ════════════════════════════════════════════════════════════════════════
open_trade:       Optional[dict] = None
_state_lock       = threading.Lock()
_entry_processing = False
_preflight_ok     = False

def _tid() -> str:
    return open_trade.get("trade_id", "?") if open_trade else "IDLE"

def _log (msg): log.info   (f"[{_tid()}] {msg}")
def _logw(msg): log.warning(f"[{_tid()}] {msg}")
def _loge(msg): log.error  (f"[{_tid()}] {msg}")

# ════════════════════════════════════════════════════════════════════════
# GRACEFUL SHUTDOWN  (Railway SIGTERM / Ctrl-C)
# Handler saves open trade state so startup() can recover it cleanly.
# ════════════════════════════════════════════════════════════════════════
import signal as _signal

def _handle_shutdown(signum, frame):
    sig_name = "SIGTERM" if signum == _signal.SIGTERM else "SIGINT"
    log.warning(f"[SHUTDOWN] {sig_name} received — initiating graceful shutdown")
    with _state_lock:
        if open_trade:
            tid = open_trade.get("trade_id", "?")
            st  = open_trade.get("state", "?")
            d   = open_trade.get("direction", "?")
            open_trade["_shutdown_at"] = datetime.now().isoformat()
            save_state()
            log.warning(
                f"[SHUTDOWN] Open trade state preserved in state.json\n"
                f"  trade_id  = {tid}\n"
                f"  state     = {st}\n"
                f"  direction = {d}\n"
                f"  Monitor will resume automatically on next startup"
            )
            tg(
                f"⚠️ <b>Bot shutting down ({sig_name})</b>\n"
                f"Trade <b>{tid}</b> [{d}] state saved\n"
                f"State: {st} | Will auto-resume on Railway restart ♻️"
            )
        else:
            log.info(f"[SHUTDOWN] {sig_name} — no open trade — clean shutdown")
    sys.exit(0)

_signal.signal(_signal.SIGTERM, _handle_shutdown)
_signal.signal(_signal.SIGINT,  _handle_shutdown)

# ════════════════════════════════════════════════════════════════════════
# TELEMETRY CSV SCHEMAS  (simplified from v3 — TP1/blended columns removed)
# ════════════════════════════════════════════════════════════════════════
CSV_HEADERS = [
    # Identity
    "trade_id", "direction", "mode",
    # Signal context
    "signal_timeframe", "signal_tf_bar_time",
    # Prices
    "pine_entry_px", "fill_price", "entry_slippage_pts",
    "sl_price", "tp_price",
    # Timestamps & latency
    "pine_signal_time", "webhook_recv_time", "entry_fill_time",
    "webhook_latency_ms", "entry_latency_ms",
    # Exit
    "exit_price", "exit_time", "exit_type", "exit_slippage_pts",
    # PnL
    "pts", "pnl_approx",
    # Outcome
    "python_actual_outcome",
    # Entry quality
    "slippage_ratio", "structure_grade",
    # Execution telemetry
    "trade_duration_sec", "monitor_cycles_total",
    # Recovery tracking
    "recovery_event", "recovery_reason",
]

RECON_HEADERS = [
    "trade_id", "timestamp",
    "signal_timeframe",
    "python_actual_outcome",
    "entry_slippage_pts", "exit_slippage_pts",
    "webhook_latency_ms", "entry_latency_ms",
    "pts",
    "trade_duration_sec", "monitor_cycles_total",
    "recovery_event", "recovery_reason",
]

def _init_csvs():
    for fpath, headers in [(CSV_FILE, CSV_HEADERS), (RECON_FILE, RECON_HEADERS)]:
        if fpath.exists():
            try:
                with open(fpath, "r", newline="") as f:
                    existing_headers = next(csv.reader(f), [])
                if existing_headers != headers:
                    backup = fpath.with_suffix(f".v3_backup_{int(time.time())}.csv")
                    fpath.rename(backup)
                    log.warning(f"[CSV] Schema mismatch in {fpath.name} — backed up to {backup.name}, creating v4.0 file")
                else:
                    continue
            except Exception as e:
                log.warning(f"[CSV] Could not check headers for {fpath.name}: {e}")
        with open(fpath, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=headers).writeheader()
        log.info(f"[CSV] Initialised {fpath.name} with v4.0 headers")

def _append_csv(fpath: Path, headers: list, row: dict):
    try:
        with open(fpath, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=headers, extrasaction="ignore").writerow(row)
    except Exception as e:
        _loge(f"CSV write error ({fpath.name}): {e}")

_init_csvs()

# ════════════════════════════════════════════════════════════════════════
# STATE PERSISTENCE  (crash recovery)
# ════════════════════════════════════════════════════════════════════════
def save_state():
    try:
        if open_trade:
            payload = {**open_trade, "_saved_at": datetime.now().isoformat()}
        else:
            payload = {"state": STATE_IDLE, "_saved_at": datetime.now().isoformat()}
        STATE_FILE.write_text(json.dumps(payload, indent=2))
    except Exception as e:
        _loge(f"save_state error: {e}")

def load_state() -> Optional[dict]:
    try:
        if not STATE_FILE.exists():
            log.info("[RECOVERY] No state.json found — starting fresh")
            return None
        raw  = STATE_FILE.read_text()
        data = json.loads(raw)
        st   = data.get("state")
        if st in (None, STATE_IDLE, STATE_CLOSED):
            log.info(f"[RECOVERY] state.json present — state={st} — no resume needed")
            return None
        log.warning(
            f"[RECOVERY] ══════════════════════════════════════════\n"
            f"[RECOVERY]  Active trade found — will resume\n"
            f"[RECOVERY]  trade_id   = {data.get('trade_id','?')}\n"
            f"[RECOVERY]  state      = {st}\n"
            f"[RECOVERY]  direction  = {data.get('direction','?')}\n"
            f"[RECOVERY]  mode       = {data.get('mode','?')}\n"
            f"[RECOVERY]  fill_price = {data.get('fill_price','?')}\n"
            f"[RECOVERY]  tp_price   = {data.get('tp_price','?')}\n"
            f"[RECOVERY]  sl_price   = {data.get('sl_price','?')}\n"
            f"[RECOVERY]  saved_at   = {data.get('_saved_at','unknown')}\n"
            f"[RECOVERY] ══════════════════════════════════════════"
        )
        return data
    except Exception as e:
        log.error(f"[RECOVERY] load_state error: {e}")
    return None

# ════════════════════════════════════════════════════════════════════════
# DELTA AUTH
# CRITICAL: query string must include leading '?' in HMAC payload
# ════════════════════════════════════════════════════════════════════════
def _sign(method: str, path: str, qs: str = "", body: str = "") -> dict:
    ts  = str(int(time.time()))
    sig = hmac.new(
        API_SECRET.encode(),
        (method + ts + path + qs + body).encode(),
        hashlib.sha256,
    ).hexdigest()
    return {
        "api-key":      API_KEY,
        "timestamp":    ts,
        "signature":    sig,
        "Content-Type": "application/json",
    }

def _get(path: str, params: Optional[dict] = None):
    qs = ""
    if params:
        qs = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    try:
        r = requests.get(BASE_URL + path + qs,
                         headers=_sign("GET", path, qs), timeout=10)
        return r.json()
    except Exception as e:
        _loge(f"GET {path} error: {e}")
        return None

def _post(path: str, body_dict: dict):
    body = json.dumps(body_dict)
    try:
        r = requests.post(BASE_URL + path,
                          headers=_sign("POST", path, "", body),
                          data=body, timeout=10)
        return r.json()
    except Exception as e:
        _loge(f"POST {path} error: {e}")
        return None

def _delete(path: str):
    try:
        r = requests.delete(BASE_URL + path,
                            headers=_sign("DELETE", path), timeout=10)
        return r.json()
    except Exception as e:
        _loge(f"DELETE {path} error: {e}")
        return None

# ════════════════════════════════════════════════════════════════════════
# PRICE FEED  (public — no auth required)
# ════════════════════════════════════════════════════════════════════════
def fetch_price() -> Optional[float]:
    try:
        r    = requests.get(f"{BASE_URL}/v2/tickers/BTCUSD", timeout=5)
        data = r.json()
        price = (
            data.get("result", {}).get("mark_price")
            or data.get("result", {}).get("close")
        )
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
                size = float(p.get("size", 0))
                return p if size != 0 else None
    elif isinstance(result, dict):
        size = float(result.get("size", 0))
        return result if size != 0 else None
    return None

def place_market_order(side: str, size: float, reduce_only: bool = False) -> Optional[dict]:
    body = {
        "product_id":    PRODUCT_ID,
        "size":          size,
        "side":          side.lower(),
        "order_type":    "market_order",
        "time_in_force": "ioc",
        "reduce_only":   reduce_only,
    }
    resp = _post("/v2/orders", body)
    if not resp:
        return None
    result = resp.get("result", {})
    status = result.get("state", resp.get("status", ""))
    if status in ("accepted", "filled", "open"):
        avg = result.get("average_fill_price") or result.get("limit_price")
        return {"order_id": result.get("id"), "fill_price": float(avg) if avg else None}
    _loge(f"market order rejected: {resp}")
    return None

def place_sl_order(close_side: str, size: float, sl_price: float) -> Optional[str]:
    body = {
        "product_id":    PRODUCT_ID,
        "size":          size,
        "side":          close_side.lower(),
        "order_type":    "stop_market_order",
        "stop_price":    str(round(sl_price, 1)),
        "reduce_only":   True,
        "time_in_force": "gtc",
    }
    resp = _post("/v2/orders", body)
    if resp and resp.get("result", {}).get("id"):
        return str(resp["result"]["id"])
    _loge(f"SL order failed: {resp}")
    return None

def place_tp_order(close_side: str, size: float, tp_price: float) -> Optional[str]:
    body = {
        "product_id":    PRODUCT_ID,
        "size":          size,
        "side":          close_side.lower(),
        "order_type":    "limit_order",
        "limit_price":   str(round(tp_price, 1)),
        "reduce_only":   True,
        "time_in_force": "gtc",
    }
    resp = _post("/v2/orders", body)
    if resp and resp.get("result", {}).get("id"):
        return str(resp["result"]["id"])
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
    """Cancel all open and pending orders for this product."""
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
        _loge("PRE-FLIGHT FAIL: API credentials missing from .env")

    resp = _get("/v2/profile")
    ok   = resp is not None and "result" in resp
    results["api_auth"] = {
        "ok":     ok,
        "detail": resp.get("result", {}).get("email", "?") if ok else str(resp),
    }
    if not ok:
        passed = False
        _loge("PRE-FLIGHT FAIL: API authentication failed")

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
            "ok":     ok,
            "detail": f"size={pos.get('size')} entry={pos.get('entry_price')}" if pos else "flat",
        }
        if not ok:
            passed = False
            _loge("PRE-FLIGHT FAIL: stale open position — close manually before trading")

    stale = load_state()
    ok    = stale is None
    results["clean_state"] = {
        "ok":     ok,
        "detail": stale.get("trade_id") if stale else "clean",
    }
    if not ok:
        passed = False
        _loge(f"PRE-FLIGHT FAIL: state file has active trade {stale.get('trade_id')} — recover or delete data/state.json")

    if not PAPER_MODE:
        bal = _get("/v2/wallet/balances")
        ok  = bal is not None and "result" in bal
        results["balance_fetch"] = {"ok": ok}
        if not ok:
            passed = False
            _loge("PRE-FLIGHT FAIL: balance fetch failed")

    ok = LOT_SIZE >= DELTA_MIN_SIZE_BTC
    results["lot_size"] = {
        "ok":                ok,
        "lot_btc":           LOT_SIZE,
        "delta_minimum_btc": DELTA_MIN_SIZE_BTC,
    }
    if not ok:
        passed = False
        _loge(f"PRE-FLIGHT FAIL: LOT_SIZE={LOT_SIZE} BTC < Delta minimum={DELTA_MIN_SIZE_BTC} BTC")

    results["all_passed"] = passed
    results["mode"]       = "PAPER" if PAPER_MODE else "LIVE"
    results["timestamp"]  = datetime.now().isoformat()

    status = "✅ ALL PASSED" if passed else "❌ FAILED — DO NOT TRADE LIVE"
    log.info(f"[PRE-FLIGHT] {status} | {results}")
    if not PAPER_MODE:
        tg(f"{'✅' if passed else '❌'} Pre-flight {'PASSED' if passed else 'FAILED'}\n"
           f"Mode: LIVE | {json.dumps({k: v.get('ok') if isinstance(v, dict) else v for k, v in results.items()}, indent=2)}")

    return results

# ════════════════════════════════════════════════════════════════════════
# ENTRY QUALITY GRADING
# ════════════════════════════════════════════════════════════════════════
def _structure_grade(slippage_ratio: float) -> str:
    if slippage_ratio < 0.25:  return "INTACT"
    if slippage_ratio < 0.5:   return "MILD"
    if slippage_ratio < 1.0:   return "DEGRADED"
    if slippage_ratio < 1.5:   return "BROKEN"
    return "CRITICAL"

# ════════════════════════════════════════════════════════════════════════
# RECONCILIATION LOG  (simplified — outcome is TP or SL only)
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
        "webhook_latency_ms":    trade.get("webhook_latency_ms", 0),
        "entry_latency_ms":      trade.get("entry_latency_ms", 0),
        "pts":                   pts,
        "trade_duration_sec":    trade_duration_sec,
        "monitor_cycles_total":  trade.get("monitor_cycles", 0),
        "recovery_event":        trade.get("recovery_event",  False),
        "recovery_reason":       trade.get("recovery_reason", ""),
    }
    _append_csv(RECON_FILE, RECON_HEADERS, row)
    _log(f"RECONCILIATION | outcome={python_outcome} pts={pts:+.2f} "
         f"entry_slip={trade.get('entry_slippage_pts', 0):+.2f}pts "
         f"wh_lat={trade.get('webhook_latency_ms', 0):.0f}ms")

# ════════════════════════════════════════════════════════════════════════
# OPEN TRADE STATE
# ════════════════════════════════════════════════════════════════════════
def _set_open_trade(
    trade_id: str, direction: str, fill_price: float, sl_dist: float,
    pine_entry_px: float, pine_tp: float, pine_sl: float,
    sl_oid: Optional[str], tp_oid: Optional[str],
    pine_signal_time: int, webhook_recv_time: float, entry_fill_time: float,
    signal_timeframe: str = "", signal_tf_bar_time: int = 0,
):
    global open_trade
    d = direction

    # Use Pine's exact SL/TP levels (close-based) so bot and Pine chart stay in sync.
    # Pine sends sl/tp2 in the webhook computed from close price — using those directly
    # means both Pine and bot trigger at the same price, regardless of fill slippage.
    sl_price = round(pine_sl, 1)
    tp_price = round(pine_tp, 1)

    entry_slippage   = round(fill_price - pine_entry_px, 2) if d == "BUY" else round(pine_entry_px - fill_price, 2)
    wh_latency_ms    = round((webhook_recv_time - pine_signal_time / 1000) * 1000, 1)
    entry_latency_ms = round((entry_fill_time - webhook_recv_time) * 1000, 1)

    _ratio = round(abs(entry_slippage) / sl_dist, 3) if sl_dist > 0 else 0.0
    _grade = _structure_grade(_ratio)

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
        # Timestamps & latency
        "entry_slippage_pts": entry_slippage,
        "webhook_latency_ms": wh_latency_ms,
        "entry_latency_ms":   entry_latency_ms,
        "pine_signal_time":   pine_signal_time,
        "webhook_recv_time":  webhook_recv_time,
        "entry_fill_time":    entry_fill_time,
        # Quality
        "slippage_ratio":     _ratio,
        "structure_grade":    _grade,
        # Recovery tracking
        "recovery_event":     False,
        "recovery_reason":    "",
    }

    save_state()

    _log(
        f"STATE→ENTERED | {d} fill={fill_price} slip={entry_slippage:+.2f}pts "
        f"wh_lat={wh_latency_ms:.0f}ms entry_lat={entry_latency_ms:.0f}ms "
        f"sl={sl_price} tp={tp_price} (2R) mode={'PAPER' if PAPER_MODE else 'LIVE'}"
    )

    if _ratio >= 1.5:
        _logw(f"[STRUCTURE CRITICAL] ratio={_ratio:.3f} — slippage {abs(entry_slippage):.1f}pts is {_ratio:.2f}× sl_dist")
    elif _ratio >= 1.0:
        _logw(f"[STRUCTURE BROKEN] ratio={_ratio:.3f} — slippage exceeds sl_dist")
    elif _ratio >= 0.5:
        _logw(f"[STRUCTURE DEGRADED] ratio={_ratio:.3f} — slippage consumed {_ratio*100:.0f}% of sl_dist")
    elif _ratio >= 0.25:
        _log(f"[STRUCTURE MILD] ratio={_ratio:.3f} — slippage within acceptable range")
    else:
        _log(f"[STRUCTURE INTACT] ratio={_ratio:.3f} — entry quality clean")

    tg(
        f"{'📄 PAPER' if PAPER_MODE else '🟢 LIVE'} <b>{d} ENTERED</b>\n"
        f"Fill: <b>{fill_price:,.1f}</b> | Slip: {entry_slippage:+.2f}pts\n"
        f"SL: {sl_price:,.1f} | TP: {tp_price:,.1f} (2R)\n"
        f"WH latency: {wh_latency_ms:.0f}ms | Entry latency: {entry_latency_ms:.0f}ms\n"
        f"Structure: <b>{_grade}</b> (ratio={_ratio:.3f})"
    )

# ════════════════════════════════════════════════════════════════════════
# CLOSE TRADE  (full position exit — writes CSV + reconciliation)
# Atomic crash-safe two-step state transition.
# ════════════════════════════════════════════════════════════════════════
def _close_trade(exit_price: float, exit_type: str, exit_slippage: float = 0.0):
    global open_trade
    if not open_trade:
        return

    trade    = open_trade
    d        = trade["direction"]
    entry_px = trade["fill_price"]

    pts        = round((exit_price - entry_px) if d == "BUY" else (entry_px - exit_price), 2)
    pnl_approx = round(pts * LOT_SIZE, 4)

    trade_duration_sec = round(time.time() - trade.get("entry_fill_time", time.time()), 1)
    monitor_cycles     = trade.get("monitor_cycles", 0)

    python_outcome = "TP" if "TP" in exit_type else "SL"

    row = {
        "trade_id":              trade["trade_id"],
        "direction":             d,
        "mode":                  trade.get("mode", "?"),
        "signal_timeframe":      trade.get("signal_timeframe", ""),
        "signal_tf_bar_time":    trade.get("signal_tf_bar_time", ""),
        "pine_entry_px":         trade.get("pine_entry_px", ""),
        "fill_price":            entry_px,
        "entry_slippage_pts":    trade.get("entry_slippage_pts", ""),
        "pine_tp":               trade.get("pine_tp", ""),
        "pine_sl":               trade.get("pine_sl", ""),
        "pine_signal_time":      trade.get("pine_signal_time", ""),
        "webhook_recv_time":     trade.get("webhook_recv_time", ""),
        "entry_fill_time":       trade.get("entry_fill_time", ""),
        "webhook_latency_ms":    trade.get("webhook_latency_ms", ""),
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
        "monitor_cycles_total":  monitor_cycles,
        "recovery_event":        trade.get("recovery_event", False),
        "recovery_reason":       trade.get("recovery_reason", ""),
    }
    _append_csv(CSV_FILE, CSV_HEADERS, row)
    _write_reconciliation(trade, python_outcome, pts, exit_slippage, trade_duration_sec)

    emoji = "✅" if pts > 0 else "🔴" if pts < 0 else "⚪"
    _log(f"STATE→CLOSED | {d} exit={exit_price} pts={pts:+.2f} outcome={python_outcome} via {exit_type}")

    _log(f"[TRADE SUMMARY] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    _log(f"[TRADE SUMMARY]  direction      : {d}")
    _log(f"[TRADE SUMMARY]  entry / exit   : {entry_px:,.1f} / {exit_price:,.1f}")
    _log(f"[TRADE SUMMARY]  pts            : {pts:+.2f}")
    _log(f"[TRADE SUMMARY]  outcome        : {python_outcome}  [{exit_type}]")
    _log(f"[TRADE SUMMARY]  slippage_ratio : {trade.get('slippage_ratio', 0):.3f}  → {trade.get('structure_grade', '?')}")
    _log(f"[TRADE SUMMARY]  duration       : {trade_duration_sec}s  |  cycles: {monitor_cycles}")
    _log(f"[TRADE SUMMARY] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    tg(
        f"{emoji} <b>{d} CLOSED</b> [{exit_type}]\n"
        f"Entry: {entry_px:,.1f} → Exit: {exit_price:,.1f}\n"
        f"PnL: <b>{pts:+.2f}pts</b> | Outcome: <b>{python_outcome}</b>\n"
        f"Structure: {trade.get('structure_grade','?')} (ratio={trade.get('slippage_ratio',0):.3f})\n"
        f"Duration: {trade_duration_sec}s | Monitor cycles: {monitor_cycles}"
    )

    # Atomic close — crash-safe two-step state transition.
    # Step 1: mark CLOSED before clearing (safe crash point).
    # Step 2: clear open_trade and write IDLE.
    open_trade["state"] = STATE_CLOSED
    save_state()
    open_trade = None
    save_state()

# ════════════════════════════════════════════════════════════════════════
# POSITION MONITOR  (simplified: TP or SL — no intermediate states)
# PAPER : price comparison at every 2s tick.
# LIVE  : position-flat detection (exchange fills TP or SL order).
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

            d     = open_trade["direction"]
            sl    = open_trade["sl_price"]
            tp    = open_trade["tp_price"]
            price = fetch_price()

            if not price:
                _logw("[MON] price fetch failed — skipping tick")
                continue

            open_trade["monitor_cycles"] = open_trade.get("monitor_cycles", 0) + 1

            if PAPER_MODE:
                hit_tp = (d == "BUY" and price >= tp) or (d == "SELL" and price <= tp)
                if hit_tp:
                    slip = round(price - tp, 2) if d == "BUY" else round(tp - price, 2)
                    _log(f"[PAPER] TP hit price={price} tp={tp} slip={slip:+.2f}")
                    _close_trade(tp, "TP_PAPER", slip)
                    break

                hit_sl = (d == "BUY" and price <= sl) or (d == "SELL" and price >= sl)
                if hit_sl:
                    slip = round(sl - price, 2) if d == "BUY" else round(price - sl, 2)
                    _log(f"[PAPER] SL hit price={price} sl={sl} slip={slip:+.2f}")
                    _close_trade(sl, "SL_PAPER", slip)
                    break

            else:
                # LIVE: detect position flat (Delta filled TP limit or SL stop)
                pos = get_open_position()
                if pos is None:
                    _logw(f"[LIVE] Position flat detected — approx_exit={price}")
                    # Cancel whichever order is still open (the one not filled)
                    for oid_key in ("sl_oid", "tp_oid"):
                        oid = open_trade.get(oid_key)
                        if oid:
                            _delete(f"/v2/orders/{oid}")
                    _close_trade(price, "AUTO_EXIT", 0.0)
                    break

    log.info("[MON] stopped")

# ════════════════════════════════════════════════════════════════════════
# ENTRY PROCESSOR  (background thread — avoids TradingView 10s timeout)
# ════════════════════════════════════════════════════════════════════════
def _process_entry(
    signal: str, sl_dist: float, pine_entry_px: float,
    pine_tp: float, pine_sl: float,
    pine_time: int, recv_time: float, trade_id: str,
    signal_timeframe: str = "", signal_tf_bar_time: int = 0,
):
    global open_trade, _entry_processing

    try:
        d = signal  # "BUY" or "SELL"
        _log(f"ENTRY_START | {d} sl_dist={sl_dist} pine_entry={pine_entry_px}")

        fill_px = None
        sl_oid  = tp_oid = None

        # Latency guard (PAPER + LIVE) — check before doing anything
        wh_latency_ms = round((recv_time - pine_time / 1000) * 1000, 1)
        if MAX_WH_LATENCY_MS > 0 and wh_latency_ms > MAX_WH_LATENCY_MS:
            _logw(f"ENTRY REJECTED [LATENCY] wh_latency={wh_latency_ms:.0f}ms > max={MAX_WH_LATENCY_MS:.0f}ms — signal too stale")
            tg(
                f"⏱ <b>ENTRY REJECTED [{d}]</b>\n"
                f"Webhook latency <b>{wh_latency_ms:.0f}ms</b> exceeds {MAX_WH_LATENCY_MS:.0f}ms limit\n"
                f"Signal price {pine_entry_px:,.1f} is stale — skipping trade"
            )
            return

        if PAPER_MODE:
            fill_px = fetch_price()
            if not fill_px:
                _loge("PAPER fill: cannot fetch live price — aborting")
                return
            fill_px = round(fill_px, 1)
            _log(f"PAPER fill simulated @ {fill_px}")

        else:
            # Live: market entry
            side   = "buy" if d == "BUY" else "sell"
            result = place_market_order(side, LOT_SIZE)
            if not result:
                _loge("Entry market order FAILED — aborting")
                tg(f"❌ ENTRY FAILED [{d}] — market order rejected by Delta")
                return

            fill_px = result.get("fill_price")
            if not fill_px:
                time.sleep(1.5)
                pos = get_open_position()
                fill_px = float(pos.get("entry_price", pine_entry_px)) if pos else pine_entry_px
            fill_px = round(fill_px, 1)

            # Place full-size SL stop + TP limit orders
            # Use Pine's exact levels (close-based) so bot exits match Pine chart exactly.
            close_side = "sell" if d == "BUY" else "buy"
            sl_price   = round(pine_sl, 1)
            tp_price   = round(pine_tp, 1)

            sl_oid = place_sl_order(close_side, LOT_SIZE, sl_price)
            tp_oid = place_tp_order(close_side, LOT_SIZE, tp_price)

            if not sl_oid:
                _loge("SL ORDER FAILED after entry — CRITICAL: close position manually")
                tg(f"🚨 SL ORDER FAILED after {d} entry @ {fill_px} — CLOSE POSITION MANUALLY ON DELTA")

        # ── Slippage guard (PAPER + LIVE) ────────────────────────────────
        # Reject if fill is too far from Pine's signal price.
        # In live mode the position is already open — close immediately.
        if sl_dist > 0 and MAX_SLIPPAGE_RATIO > 0:
            raw_slip  = (fill_px - pine_entry_px) if d == "BUY" else (pine_entry_px - fill_px)
            slip_ratio = round(abs(raw_slip) / sl_dist, 3)
            if slip_ratio > MAX_SLIPPAGE_RATIO:
                _logw(
                    f"ENTRY REJECTED [SLIPPAGE] slip={raw_slip:+.1f}pts "
                    f"ratio={slip_ratio:.3f} > max={MAX_SLIPPAGE_RATIO} — trade structure broken"
                )
                tg(
                    f"🚫 <b>ENTRY REJECTED [{d}]</b>\n"
                    f"Slippage {raw_slip:+.1f}pts = {slip_ratio:.2f}× sl_dist ({sl_dist}pts)\n"
                    f"Max allowed ratio: {MAX_SLIPPAGE_RATIO} | Signal: {pine_entry_px:,.1f} | Fill: {fill_px:,.1f}\n"
                    f"Trade skipped — waiting for next clean signal"
                )
                if not PAPER_MODE:
                    # Close the live position that was already opened
                    close_side = "sell" if d == "BUY" else "buy"
                    place_market_order(close_side, LOT_SIZE, reduce_only=True)
                    cancel_all_open_orders()
                    _log("SLIPPAGE GUARD: live position closed immediately after rejection")
                return

        entry_fill_time = time.time()

        with _state_lock:
            _set_open_trade(
                trade_id=trade_id, direction=d, fill_price=fill_px,
                sl_dist=sl_dist, pine_entry_px=pine_entry_px,
                pine_tp=pine_tp, pine_sl=pine_sl,
                sl_oid=sl_oid, tp_oid=tp_oid,
                pine_signal_time=pine_time, webhook_recv_time=recv_time,
                entry_fill_time=entry_fill_time,
                signal_timeframe=signal_timeframe,
                signal_tf_bar_time=signal_tf_bar_time,
            )

        threading.Thread(target=_position_monitor, daemon=True, name="mon").start()

    except Exception as e:
        _loge(f"_process_entry exception: {e}")
        tg(f"❌ ENTRY EXCEPTION [{trade_id}]: {e}")
    finally:
        with _state_lock:
            _entry_processing = False

# ════════════════════════════════════════════════════════════════════════
# FASTAPI APP
# ════════════════════════════════════════════════════════════════════════
app = FastAPI(title="Vol Surge Bot v4.0 — Simplified Lifecycle")

@app.on_event("startup")
async def startup():
    global open_trade, _preflight_ok

    log.info("=" * 70)
    log.info(f"  Vol Surge v4.0 | {'📄 PAPER' if PAPER_MODE else '🟢 *** LIVE ***'} mode")
    log.info(f"  Endpoint  : {BASE_URL}")
    log.info(f"  Product   : BTCUSD Perpetual (ID={PRODUCT_ID})")
    log.info(f"  LOT_SIZE  : {LOT_SIZE:.6f} BTC  (full position, single exit)")
    log.info(f"  TP model  : Single full exit at {TP_R}R — no partials, no resizing")
    log.info(f"  SL model  : Fixed stop-market at entry ± sl_dist — never moved")
    log.info(f"  Exit auth : MONITOR_ONLY — Pine sends entry signals only")
    log.info(f"  Lifecycle : IDLE → ENTERED → CLOSED  (no intermediate states)")
    log.info(f"  Creds     : {'SET ✓' if API_KEY else '⚠️  MISSING'}")
    log.info("=" * 70)

    if PAPER_MODE:
        _preflight_ok = True
        log.info("  Paper mode — preflight skipped, ready to receive signals")
    else:
        if not API_KEY or not API_SECRET:
            log.error("LIVE mode: credentials missing in .env — cannot run preflight")
        else:
            log.info("[PRE-FLIGHT] Running live validation checks...")
            pf = run_preflight()
            _preflight_ok = pf.get("all_passed", False)
            if not _preflight_ok:
                log.error("⚠️  PRE-FLIGHT FAILED — LIVE webhooks BLOCKED until /preflight passes")
            else:
                log.info("✅ Pre-flight passed — LIVE mode ready")

    log.info("=" * 70)

    # ── Crash / restart recovery ─────────────────────────────────────────
    recovered = load_state()
    if recovered:
        open_trade = recovered

        open_trade["recovery_event"] = True
        _shutdown_at_val = open_trade.pop("_shutdown_at", None)
        open_trade["recovery_reason"] = "SIGTERM" if _shutdown_at_val else "CRASH"
        save_state()

        log.warning(
            f"[RECOVERY] recovery_event=True  recovery_reason={open_trade['recovery_reason']}"
            + (f"  (_shutdown_at={_shutdown_at_val})" if _shutdown_at_val
               else "  (no _shutdown_at — crash recovery)")
        )

        tid = open_trade.get("trade_id", "?")
        st  = open_trade.get("state", "?")
        d   = open_trade.get("direction", "?")

        if PAPER_MODE:
            log.warning(f"[RECOVERY] PAPER mode — resuming monitor for {tid} (state={st})")
            tg(
                f"♻️ <b>RECOVERED (PAPER)</b>\n"
                f"Trade <b>{tid}</b> [{d}] | State: {st}\n"
                f"SL: {open_trade.get('sl_price','?')} | TP: {open_trade.get('tp_price','?')}\n"
                f"Monitor restarting"
            )
            threading.Thread(target=_position_monitor, daemon=True,
                             name="mon-recovery").start()
        else:
            log.info(f"[RECOVERY] LIVE mode — verifying position on Delta Exchange...")
            pos = get_open_position()
            if pos is None:
                log.error(
                    f"[RECOVERY] CRITICAL — no open position found on Delta for {tid}\n"
                    f"  Position may have been closed by exchange while bot was offline.\n"
                    f"  Clearing state.json → bot enters IDLE.\n"
                    f"  ACTION REQUIRED: check Delta UI and trades.csv manually."
                )
                tg(
                    f"🚨 <b>RECOVERY FAILED</b> — {tid}\n"
                    f"No open position on Delta Exchange.\n"
                    f"Trade may have closed while bot was offline.\n"
                    f"Bot is IDLE — check Delta UI and trades.csv manually."
                )
                open_trade = None
                save_state()
            else:
                exch_size  = pos.get("size", "?")
                exch_entry = pos.get("entry_price", "?")
                log.warning(
                    f"[RECOVERY] Position confirmed on Delta ✅\n"
                    f"  exchange size  = {exch_size}\n"
                    f"  exchange entry = {exch_entry}\n"
                    f"  Resuming monitor for {tid}"
                )
                tg(
                    f"♻️ <b>RECOVERED (LIVE)</b>\n"
                    f"Trade <b>{tid}</b> [{d}] | State: {st}\n"
                    f"Position confirmed on Delta ✅\n"
                    f"SL: {open_trade.get('sl_price','?')} | TP: {open_trade.get('tp_price','?')}\n"
                    f"Monitor restarting"
                )
                _preflight_ok = True
                threading.Thread(target=_position_monitor, daemon=True,
                                 name="mon-recovery").start()

# ── /webhook ──────────────────────────────────────────────────────────
@app.post("/webhook")
async def webhook(request: Request):
    global open_trade, _entry_processing

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if data.get("secret") != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Bad secret")

    signal    = str(data.get("signal", "")).upper()
    recv_time = time.time()
    pine_time = int(data.get("pine_time", recv_time * 1000))
    latency   = round((recv_time - pine_time / 1000) * 1000, 1)

    log.info(f"[WEBHOOK] signal={signal} latency={latency:.0f}ms")

    # ── ENTRY (BUY / SELL) ──
    if signal in ("BUY", "SELL"):
        if not PAPER_MODE and not _preflight_ok:
            _loge("LIVE entry BLOCKED — preflight has not passed. Hit /preflight to recheck.")
            return JSONResponse(
                {"status": "blocked", "reason": "preflight_not_passed",
                 "action": "GET /preflight to recheck"},
                status_code=503,
            )

        with _state_lock:
            if open_trade or _entry_processing:
                return JSONResponse({"status": "ignored", "reason": "already_in_trade"})

            sl_dist            = float(data.get("sl_dist", 0))
            pine_entry_px      = float(data.get("pine_entry_px", 0))
            pine_tp            = float(data.get("tp2", pine_entry_px + sl_dist * TP_R))
            pine_sl            = float(data.get("sl",  pine_entry_px - sl_dist))
            signal_timeframe   = str(data.get("timeframe", ""))
            signal_tf_bar_time = int(data.get("bar_time", 0))
            trade_id           = f"{signal[0]}{int(recv_time * 1000)}"
            _entry_processing  = True

        threading.Thread(
            target=_process_entry,
            args=(signal, sl_dist, pine_entry_px, pine_tp, pine_sl,
                  pine_time, recv_time, trade_id, signal_timeframe, signal_tf_bar_time),
            daemon=True, name=f"entry-{trade_id}",
        ).start()

        return JSONResponse({
            "status":     "accepted",
            "trade_id":   trade_id,
            "mode":       "PAPER" if PAPER_MODE else "LIVE",
            "latency_ms": latency,
        })

    # Unknown signals (TP1_HIT, TP2_HIT from old Pine alerts) — log and ignore
    _logw(f"[WEBHOOK] unhandled signal={signal} — ignored (v4 accepts BUY/SELL only)")
    return JSONResponse({"status": "ignored", "signal": signal,
                         "note": "v4 accepts BUY and SELL only"})

# ── / and /health ─────────────────────────────────────────────────────
@app.get("/")
@app.get("/health")
async def health():
    price = fetch_price()
    return JSONResponse({
        "status":        "healthy",
        "bot":           "Vol Surge v4.0",
        "mode":          "PAPER" if PAPER_MODE else "LIVE",
        "preflight_ok":  _preflight_ok,
        "price_ok":      price is not None,
        "price":         price,
        "creds_ok":      bool(API_KEY and API_SECRET),
        "lot_size_btc":  LOT_SIZE,
        "tp_r":          TP_R,
        "lifecycle":     "IDLE → ENTERED → CLOSED",
        "timestamp":     datetime.now().isoformat(),
    })

# ── /status ───────────────────────────────────────────────────────────
@app.get("/status")
async def status():
    price = fetch_price()
    unrealised = None
    if open_trade and price:
        d = open_trade["direction"]
        unrealised = round(
            (price - open_trade["fill_price"]) if d == "BUY"
            else (open_trade["fill_price"] - price), 2
        )
    return JSONResponse({
        "bot":              "Vol Surge v4.0",
        "mode":             "PAPER" if PAPER_MODE else "LIVE",
        "preflight_ok":     _preflight_ok,
        "state":            (open_trade.get("state") if open_trade
                             else ("PROCESSING" if _entry_processing else "IDLE")),
        "open_trade":       open_trade,
        "live_price":       price,
        "unrealised_pts":   unrealised,
        "entry_processing": _entry_processing,
    })

# ── /preflight ────────────────────────────────────────────────────────
@app.get("/preflight")
async def preflight_check():
    global _preflight_ok
    result = run_preflight()
    _preflight_ok = result.get("all_passed", False)
    return JSONResponse(result)

# ── /balance ──────────────────────────────────────────────────────────
@app.get("/balance")
async def balance():
    if PAPER_MODE:
        return JSONResponse({"mode": "PAPER", "note": "balance N/A in paper mode"})
    resp = _get("/v2/wallet/balances")
    return JSONResponse(resp or {"error": "fetch failed"})

# ── /history ──────────────────────────────────────────────────────────
@app.get("/history")
async def history():
    try:
        trades = []
        try:
            with open(CSV_FILE, "r") as f:
                trades = list(csv.DictReader(f))
        except Exception:
            pass
        for bf in sorted(DATA_DIR.glob("trades.v3_backup_*.csv")):
            try:
                with open(bf, "r") as f:
                    trades = list(csv.DictReader(f)) + trades
            except Exception:
                pass
        return JSONResponse({"count": len(trades), "trades": trades[-20:]})
    except Exception:
        return JSONResponse({"count": 0, "trades": []})

# ── /reconciliation ───────────────────────────────────────────────────
@app.get("/reconciliation")
async def reconciliation():
    try:
        with open(RECON_FILE, "r") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            return JSONResponse({"total": 0, "tp_count": 0, "sl_count": 0, "last_20": []})
        tp_count = sum(1 for r in rows if r.get("python_actual_outcome") == "TP")
        sl_count = sum(1 for r in rows if r.get("python_actual_outcome") == "SL")
        pts_list = [float(r.get("pts") or 0) for r in rows]
        return JSONResponse({
            "total":                  len(rows),
            "tp_count":               tp_count,
            "sl_count":               sl_count,
            "win_rate":               f"{round(tp_count / len(rows) * 100)}%",
            "avg_pts":                round(sum(pts_list) / len(pts_list), 2),
            "total_pts":              round(sum(pts_list), 2),
            "avg_entry_slippage_pts": round(
                sum(float(r.get("entry_slippage_pts") or 0) for r in rows) / len(rows), 2
            ),
            "avg_webhook_latency_ms": round(
                sum(float(r.get("webhook_latency_ms") or 0) for r in rows) / len(rows), 1
            ),
            "last_20": rows[-20:],
        })
    except Exception as e:
        return JSONResponse({"error": str(e), "total": 0})

# ── /test/fire/{side} ─────────────────────────────────────────────────
@app.get("/test/fire/{side}")
async def test_fire(side: str):
    """Inject a fake BUY or SELL entry webhook for end-to-end testing."""
    side = side.upper()
    if side not in ("BUY", "SELL"):
        raise HTTPException(400, "Use /test/fire/buy or /test/fire/sell")
    price   = fetch_price() or 80000.0
    sl_dist = 150.0
    payload = {
        "signal":        side,
        "secret":        WEBHOOK_SECRET,
        "sl_dist":       sl_dist,
        "pine_entry_px": price,
        "tp2":           round(price + sl_dist * 2.0 if side == "BUY" else price - sl_dist * 2.0, 1),
        "sl":            round(price - sl_dist       if side == "BUY" else price + sl_dist,       1),
        "pine_time":     int(time.time() * 1000),
        "timeframe":     "5",
        "bar_time":      int(time.time() * 1000),
    }
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post("http://localhost:5001/webhook", json=payload, timeout=5)
        return JSONResponse({"test": "fired", "side": side, "price": price,
                             "payload": payload, "response": r.json()})
    except Exception as e:
        return JSONResponse({"test": "error", "error": str(e)})

# ── /test/close ───────────────────────────────────────────────────────
@app.get("/test/close")
async def test_close():
    """Force-close current open trade at live price (paper testing only)."""
    with _state_lock:
        if not open_trade:
            return JSONResponse({"error": "No open trade"}, status_code=400)
        price = fetch_price() or float(open_trade.get("fill_price", 0))
        _close_trade(price, "TEST_CLOSE", 0.0)
    return JSONResponse({"test": "closed", "exit_price": price})

# ── /test/telegram ────────────────────────────────────────────────────
@app.get("/test/telegram")
async def test_telegram():
    tg("✅ Vol Surge v4.0 Telegram test — connection OK")
    return JSONResponse({"status": "sent"})

# ── /dashboard ────────────────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Trade journal dashboard — reads trades.csv + any v3 backup files."""
    trades = []
    # Read current v4 trades
    try:
        with open(CSV_FILE, "r") as f:
            trades = list(csv.DictReader(f))
    except Exception:
        pass
    # Also read any v3 backup files and merge (oldest first)
    try:
        backup_files = sorted(DATA_DIR.glob("trades.v3_backup_*.csv"))
        for bf in backup_files:
            try:
                with open(bf, "r") as f:
                    old_rows = list(csv.DictReader(f))
                trades = old_rows + trades   # old trades first, newer on top
            except Exception:
                pass
    except Exception:
        pass

    trades_reversed = list(reversed(trades))

    def fmt_price(v):
        try: return f"{float(v):,.1f}"
        except: return v or "—"

    def fmt_pts(v):
        try:
            f = float(v)
            sign = "+" if f > 0 else ""
            return f"{sign}{f:.2f}"
        except: return v or "—"

    def fmt_ms(v):
        try: return f"{float(v):.0f}"
        except: return v or "—"

    def fmt_dur(v):
        try:
            s = int(float(v))
            if s < 60: return f"{s}s"
            return f"{s//60}m {s%60}s"
        except: return v or "—"

    def fmt_dt(v):
        try:
            return (datetime.fromisoformat(v[:19]) + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M")
        except: return v or "—"

    def outcome_badge(v):
        if v == "TP":
            return '<span style="background:#1a472a;color:#4ade80;padding:2px 8px;border-radius:4px;font-weight:700;font-size:11px;">TP ✓</span>'
        if v == "SL":
            return '<span style="background:#4a1942;color:#f87171;padding:2px 8px;border-radius:4px;font-weight:700;font-size:11px;">SL ✗</span>'
        return v or "—"

    def dir_badge(v):
        if v == "BUY":
            return '<span style="color:#4ade80;font-weight:700;">▲ BUY</span>'
        if v == "SELL":
            return '<span style="color:#f87171;font-weight:700;">▼ SELL</span>'
        return v or "—"

    def grade_badge(v):
        colors = {"INTACT": "#4ade80", "MILD": "#facc15", "DEGRADED": "#fb923c",
                  "BROKEN": "#f87171", "CRITICAL": "#dc2626"}
        c = colors.get(v, "#9ca3af")
        return f'<span style="color:{c};font-size:11px;">{v or "—"}</span>'

    def pts_color(v):
        try:
            f = float(v)
            if f > 0: return "color:#4ade80"
            if f < 0: return "color:#f87171"
        except: pass
        return "color:#9ca3af"

    # Summary stats
    total   = len(trades)
    tp_cnt  = sum(1 for t in trades if t.get("python_actual_outcome") == "TP")
    sl_cnt  = sum(1 for t in trades if t.get("python_actual_outcome") == "SL")
    pts_all = [float(t.get("pts") or 0) for t in trades]
    tot_pts = round(sum(pts_all), 2)
    avg_pts = round(sum(pts_all) / len(pts_all), 2) if pts_all else 0
    win_rt  = f"{round(tp_cnt / total * 100)}%" if total else "—"
    avg_sl  = round(sum(float(t.get("entry_slippage_pts") or 0) for t in trades) / total, 2) if total else 0
    avg_wh  = round(sum(float(t.get("webhook_latency_ms") or 0) for t in trades) / total, 1) if total else 0

    rows_html = ""
    for i, t in enumerate(trades_reversed, 1):
        pts_val  = t.get("pts", "")
        outcome  = t.get("python_actual_outcome", "")
        row_bg   = "#0d1f12" if outcome == "TP" else "#1f0d0d" if outcome == "SL" else "#111827"
        rec_icon = "♻️" if str(t.get("recovery_event", "")).lower() == "true" else ""

        rows_html += f"""
        <tr style="background:{row_bg};border-bottom:1px solid #1f2937;">
          <td style="color:#6b7280;text-align:center;">{total - i + 1}</td>
          <td>{dir_badge(t.get("direction",""))}</td>
          <td style="color:#d1d5db;font-size:12px;">{fmt_dt(t.get("entry_fill_time",""))}</td>
          <td style="color:#9ca3af;text-align:center;">{t.get("signal_timeframe","—")}</td>
          <td style="color:#e5e7eb;text-align:right;">{fmt_price(t.get("fill_price",""))}</td>
          <td style="color:#9ca3af;text-align:right;">{fmt_price(t.get("pine_entry_px",""))}</td>
          <td style="color:#34d399;text-align:right;">{fmt_price(t.get("pine_tp",""))}</td>
          <td style="color:#f87171;text-align:right;">{fmt_price(t.get("pine_sl",""))}</td>
          <td style="color:#e5e7eb;text-align:right;">{fmt_price(t.get("exit_price",""))}</td>
          <td style="{pts_color(pts_val)};text-align:right;font-weight:700;">{fmt_pts(pts_val)}</td>
          <td style="{pts_color(pts_val)};text-align:right;">{fmt_pts(t.get("pnl_approx",""))}</td>
          <td style="text-align:center;">{outcome_badge(outcome)}</td>
          <td style="color:#9ca3af;font-size:11px;">{t.get("exit_type","—")}</td>
          <td style="color:#facc15;text-align:right;">{fmt_pts(t.get("entry_slippage_pts",""))}</td>
          <td style="color:#9ca3af;text-align:right;">{fmt_pts(t.get("exit_slippage_pts",""))}</td>
          <td style="color:#60a5fa;text-align:right;">{fmt_ms(t.get("webhook_latency_ms",""))}</td>
          <td style="color:#9ca3af;text-align:right;">{fmt_ms(t.get("entry_latency_ms",""))}</td>
          <td style="color:#9ca3af;text-align:right;">{fmt_dur(t.get("trade_duration_sec",""))}</td>
          <td style="text-align:center;">{grade_badge(t.get("structure_grade",""))}</td>
          <td style="color:#9ca3af;text-align:center;">{rec_icon} {t.get("recovery_reason","") if rec_icon else "—"}</td>
        </tr>"""

    empty_msg = "" if trades else '<tr><td colspan="20" style="text-align:center;color:#6b7280;padding:40px;">No trades recorded yet. Waiting for first signal...</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Vol Surge v4 — Trade Journal</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:#0a0f1a; color:#e5e7eb; font-family:'Segoe UI',system-ui,sans-serif; font-size:13px; }}
  .header {{ background:#111827; border-bottom:1px solid #1f2937; padding:16px 24px; display:flex; align-items:center; justify-content:space-between; }}
  .header h1 {{ font-size:18px; font-weight:700; color:#f9fafb; }}
  .header .mode {{ background:#1a3a1a; color:#4ade80; padding:4px 12px; border-radius:20px; font-size:12px; font-weight:600; }}
  .stats {{ display:flex; gap:12px; padding:16px 24px; flex-wrap:wrap; }}
  .stat {{ background:#111827; border:1px solid #1f2937; border-radius:8px; padding:12px 20px; min-width:130px; }}
  .stat .label {{ color:#6b7280; font-size:11px; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px; }}
  .stat .value {{ font-size:22px; font-weight:700; }}
  .stat .value.green {{ color:#4ade80; }}
  .stat .value.red {{ color:#f87171; }}
  .stat .value.blue {{ color:#60a5fa; }}
  .stat .value.yellow {{ color:#facc15; }}
  .stat .value.white {{ color:#f9fafb; }}
  .stat .value.neutral {{ color:#e5e7eb; }}
  .table-wrap {{ padding:0 24px 24px; overflow-x:auto; }}
  table {{ width:100%; border-collapse:collapse; font-size:12px; }}
  th {{ background:#111827; color:#9ca3af; font-weight:600; text-transform:uppercase; font-size:10px; letter-spacing:0.05em; padding:10px 12px; text-align:left; border-bottom:2px solid #1f2937; white-space:nowrap; position:sticky; top:0; }}
  td {{ padding:9px 12px; white-space:nowrap; }}
  tr:hover td {{ background:#1f2937 !important; }}
  .refresh {{ color:#6b7280; font-size:11px; }}
  .footer {{ text-align:center; padding:16px; color:#374151; font-size:11px; border-top:1px solid #1f2937; }}
</style>
<script>
  setTimeout(() => location.reload(), 30000);
  function updateClock() {{
    document.getElementById('clock').textContent = new Date().toLocaleTimeString();
  }}
  setInterval(updateClock, 1000);
  window.onload = updateClock;
</script>
</head>
<body>

<div class="header">
  <div>
    <h1>📊 Vol Surge v4.0 — Trade Journal</h1>
    <div style="color:#6b7280;font-size:12px;margin-top:4px;">BTCUSD Perpetual · Delta Exchange India · Auto-refreshes every 30s</div>
  </div>
  <div style="text-align:right;">
    <div class="mode">📄 PAPER MODE</div>
    <div class="refresh" style="margin-top:6px;">🕐 <span id="clock"></span></div>
  </div>
</div>

<div class="stats">
  <div class="stat"><div class="label">Total Trades</div><div class="value white">{total}</div></div>
  <div class="stat"><div class="label">Win Rate</div><div class="value {'green' if tp_cnt >= sl_cnt else 'red'}">{win_rt}</div></div>
  <div class="stat"><div class="label">TP Hits</div><div class="value green">{tp_cnt}</div></div>
  <div class="stat"><div class="label">SL Hits</div><div class="value red">{sl_cnt}</div></div>
  <div class="stat"><div class="label">Total Pts</div><div class="value {'green' if tot_pts >= 0 else 'red'}">{'+' if tot_pts > 0 else ''}{tot_pts}</div></div>
  <div class="stat"><div class="label">Avg Pts/Trade</div><div class="value {'green' if avg_pts >= 0 else 'red'}">{'+' if avg_pts > 0 else ''}{avg_pts}</div></div>
  <div class="stat"><div class="label">Avg Entry Slip</div><div class="value yellow">{avg_sl:+.2f}</div></div>
  <div class="stat"><div class="label">Avg WH Latency</div><div class="value blue">{avg_wh:.0f}ms</div></div>
</div>

<div class="table-wrap">
<table>
  <thead>
    <tr>
      <th>#</th>
      <th>Dir</th>
      <th>Date / Time</th>
      <th>TF</th>
      <th>Fill $</th>
      <th>Pine $</th>
      <th>TP $</th>
      <th>SL $</th>
      <th>Exit $</th>
      <th>Pts</th>
      <th>P&amp;L (BTC)</th>
      <th>Result</th>
      <th>Exit Type</th>
      <th>Entry Slip</th>
      <th>Exit Slip</th>
      <th>WH Lat (ms)</th>
      <th>Entry Lat (ms)</th>
      <th>Duration</th>
      <th>Grade</th>
      <th>Recovery</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
    {empty_msg}
  </tbody>
</table>
</div>

<div class="footer">
  Vol Surge v4.0 · Lifecycle: IDLE → ENTERED → CLOSED · Monitor: 2s poll · TP = 2R fixed · SL = fixed stop
</div>

</body>
</html>"""

    return HTMLResponse(content=html)
