#!/usr/bin/env python3
"""
volsurge_v4.py — Vol Surge Bot v4.0 (Simplified Lifecycle Build)
================================================================
Philosophy  : TradingView = Signal Intent | Python = Execution Authority | Delta = Reality
Lifecycle   : IDLE → ENTERED → CLOSED
TP model    : Single full-position exit at 1.4R (sl_dist × 1.4). No partials.
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

BASE_URL   = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange")
PRODUCT_ID = int(os.getenv("PRODUCT_ID", "27"))      # BTCUSD Perpetual

# Full position size. No PARTIAL_LOT needed — single full exit only.
# Default = 0.001 BTC (Delta minimum). Override via LOT_SIZE= in .env.
LOT_SIZE           = float(os.getenv("LOT_SIZE", "0.001"))
DELTA_MIN_SIZE_BTC = 0.001   # confirmed minimum for BTCUSD Perpetual (India)

# Delta BTCUSD Perpetual: 1 contract = 0.001 BTC face value.
# LOT_SIZE is kept in BTC for human readability.
# At order time: contracts = int(LOT_SIZE / 0.001), minimum 1.
# Override LOT_SIZE_CONTRACTS in Railway to pin a fixed contract count directly.
DELTA_CONTRACT_SIZE_BTC = 0.001   # 1 contract = 0.001 BTC (Delta Exchange India BTCUSD Perp)
LOT_SIZE_CONTRACTS = int(os.getenv("LOT_SIZE_CONTRACTS", "0"))  # 0 = auto-calc from BTC

def _btc_to_contracts(btc_size: float, ref_price: Optional[float] = None) -> int:
    """Convert BTC lot size to integer contracts for Delta API.
    1 contract = 0.001 BTC on BTCUSD perpetual (Delta Exchange India).
    """
    if LOT_SIZE_CONTRACTS > 0:
        return LOT_SIZE_CONTRACTS   # pinned override
    contracts = max(1, round(btc_size / DELTA_CONTRACT_SIZE_BTC))
    log.info(f"[SIZE] {btc_size} BTC ÷ {DELTA_CONTRACT_SIZE_BTC} BTC/contract = {contracts} contracts")
    return contracts

TP_R = 1.4   # TP = entry ± sl_dist × 1.4  (1.4R) — matches Pine TP2 R-multiple

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
STATE_FILE     = DATA_DIR / "state.json"
CSV_FILE       = DATA_DIR / "trades.csv"            # closed trade journal (written at exit)
RECON_FILE     = DATA_DIR / "reconciliation.csv"
LIFECYCLE_FILE = DATA_DIR / "order_lifecycle.csv"   # per-event order timeline
LATENCY_FILE   = DATA_DIR / "execution_latency.csv" # written at ENTRY (survives crashes)
SLIPPAGE_FILE  = DATA_DIR / "slippage_audit.csv"    # written at EXIT (entry+exit slippage)
LOG_FILE       = LOG_DIR  / "volsurge_v4.log"

# Detect whether data is on a persistent volume (Railway: mount at /app/data)
_DATA_PERSISTENT = str(DATA_DIR).startswith("/app/data") or os.getenv("DATA_PERSISTENT") == "true"

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
    # Entry order details (live only)
    "entry_order_id",       # Delta order ID for the market entry order
    "api_request_time",     # unix float: when market order API call was sent
    "api_ack_time",         # unix float: when Delta responded to entry order
    "sl_placed_time",       # unix float: when SL order was accepted by Delta
    "tp_placed_time",       # unix float: when TP order was accepted by Delta
    # Exit details (live)
    "exit_order_id",        # which order ID actually filled (sl_oid or tp_oid)
    "exit_fill_px_delta",   # actual fill price from Delta order (vs market price at detection)
    # Additional slippage metrics
    "entry_slippage_pct",   # entry slippage as % of fill price
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

LIFECYCLE_HEADERS = [
    "trade_id", "timestamp_ist", "unix_ts",
    "event",        # ENTRY_SENT, ENTRY_ACKED, SL_SENT, SL_ACKED, TP_SENT, TP_ACKED,
                    # SL_CANCELLED, TP_CANCELLED, EXIT_DETECTED, EXIT_CONFIRMED, MONITOR_FLAT
    "order_id",
    "side",
    "qty",
    "price",
    "latency_from_prev_ms",  # ms since previous event for this trade
    "notes",
]

# Written at ENTRY time — survives bot crash/redeploy between entry and exit.
# Gives a persistent record of every real Delta order placed, even if _close_trade() never runs.
LATENCY_HEADERS = [
    "trade_id", "timestamp_ist", "direction", "mode",
    "pine_signal_time", "webhook_recv_time", "entry_submit_time", "entry_ack_time",
    "pine_entry_px", "delta_fill_px", "entry_slippage_pts",
    "webhook_latency_ms", "api_roundtrip_ms",
    "sl_price", "tp_price", "sl_order_id", "tp_order_id",
    "contracts", "entry_order_id",
]

# Written at EXIT time alongside trades.csv — focused on slippage comparison.
SLIPPAGE_HEADERS = [
    "trade_id", "timestamp_ist", "direction", "mode",
    "pine_entry_px", "delta_entry_fill", "entry_slippage_pts", "entry_slippage_pct",
    "pine_exit_px",  "delta_exit_fill",  "exit_slippage_pts",  "exit_slippage_pct",
    "pine_pts", "live_pts", "slippage_drag_pts",
    "exit_type", "webhook_latency_ms", "timeframe",
]

def _init_csvs():
    for fpath, headers in [
        (CSV_FILE,       CSV_HEADERS),
        (RECON_FILE,     RECON_HEADERS),
        (LIFECYCLE_FILE, LIFECYCLE_HEADERS),
        (LATENCY_FILE,   LATENCY_HEADERS),
        (SLIPPAGE_FILE,  SLIPPAGE_HEADERS),
    ]:
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

_lifecycle_last_ts: dict = {}   # trade_id → last event unix_ts (for latency_from_prev)

def _log_lifecycle(trade_id: str, event: str, order_id: str = "",
                   side: str = "", qty: float = 0, price: float = 0, notes: str = ""):
    """Write one row to order_lifecycle.csv. Call for every order placement/fill/cancel event."""
    now_unix = time.time()
    now_ist  = (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]
    prev_ts  = _lifecycle_last_ts.get(trade_id, now_unix)
    lat_ms   = round((now_unix - prev_ts) * 1000, 1) if trade_id in _lifecycle_last_ts else 0
    _lifecycle_last_ts[trade_id] = now_unix

    row = {
        "trade_id":           trade_id,
        "timestamp_ist":      now_ist,
        "unix_ts":            round(now_unix, 3),
        "event":              event,
        "order_id":           order_id or "",
        "side":               side,
        "qty":                qty or "",
        "price":              price or "",
        "latency_from_prev_ms": lat_ms,
        "notes":              notes,
    }
    _append_csv(LIFECYCLE_FILE, LIFECYCLE_HEADERS, row)
    log.info(f"[LIFECYCLE][{trade_id}] {event} oid={order_id or '—'} price={price or '—'} +{lat_ms:.0f}ms")

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
        if r.status_code != 200:
            _loge(f"GET {path} HTTP {r.status_code} | body: {r.text[:300]}")
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

def place_market_order(side: str, size: float, reduce_only: bool = False, ref_price: Optional[float] = None) -> Optional[dict]:
    contracts = _btc_to_contracts(size, ref_price)
    body = {
        "product_id":    PRODUCT_ID,
        "size":          contracts,       # integer USD contracts, not BTC float
        "side":          side.lower(),
        "order_type":    "market_order",
        "time_in_force": "ioc",
        "reduce_only":   reduce_only,
    }
    api_req_t = time.time()
    resp = _post("/v2/orders", body)
    api_ack_t = time.time()
    if not resp:
        return None
    result = resp.get("result", {})
    status = result.get("state", resp.get("status", ""))
    unfilled = float(result.get("unfilled_size", -1))

    # Delta returns state="closed" for fully-filled IOC market orders.
    # Must include "closed" with unfilled_size=0 as a valid fill state.
    # unfilled_size=0 + paid_commission>0 = order fully executed.
    is_filled = (
        status in ("accepted", "filled", "open")
        or (status == "closed" and unfilled == 0)
    )
    if is_filled:
        avg = result.get("average_fill_price") or result.get("limit_price")
        log.info(f"[ORDER] Market order filled | state={status} unfilled={unfilled} fill_px={avg}")
        return {
            "order_id":         result.get("id"),
            "fill_price":       float(avg) if avg else None,
            "api_request_time": api_req_t,
            "api_ack_time":     api_ack_t,
        }
    err_code = resp.get("error", resp.get("message", str(resp)[:300]))
    _loge(f"market order rejected | state={status} unfilled={unfilled} | code={err_code} | full={resp}")
    place_market_order._last_error = str(err_code)
    return None

def place_sl_order(close_side: str, size: float, sl_price: float, contracts: Optional[int] = None) -> Optional[dict]:
    sz = contracts if contracts else _btc_to_contracts(size)
    body = {
        "product_id":    PRODUCT_ID,
        "size":          sz,
        "side":          close_side.lower(),
        "order_type":    "stop_market_order",
        "stop_price":    str(round(sl_price, 1)),
        "reduce_only":   True,
        "time_in_force": "gtc",
    }
    t_sent = time.time()
    resp = _post("/v2/orders", body)
    t_ack  = time.time()
    if resp and resp.get("result", {}).get("id"):
        return {"order_id": str(resp["result"]["id"]), "placed_time": t_ack}
    _loge(f"SL order failed: {resp}")
    return None

def place_tp_order(close_side: str, size: float, tp_price: float, contracts: Optional[int] = None) -> Optional[dict]:
    sz = contracts if contracts else _btc_to_contracts(size)
    body = {
        "product_id":    PRODUCT_ID,
        "size":          sz,
        "side":          close_side.lower(),
        "order_type":    "limit_order",
        "limit_price":   str(round(tp_price, 1)),
        "reduce_only":   True,
        "time_in_force": "gtc",
    }
    t_sent = time.time()
    resp = _post("/v2/orders", body)
    t_ack  = time.time()
    if resp and resp.get("result", {}).get("id"):
        return {"order_id": str(resp["result"]["id"]), "placed_time": t_ack}
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
    auth_detail = ""
    if ok:
        auth_detail = resp.get("result", {}).get("email", "?")
    else:
        # Log the exact Delta rejection reason (ip_not_whitelisted, invalid_signature, etc.)
        if resp is None:
            auth_detail = "no_response_or_connection_error"
        else:
            auth_detail = resp.get("error", resp.get("message", str(resp)[:200]))
        _loge(f"PRE-FLIGHT FAIL: API auth rejected — Delta says: {auth_detail}")
    results["api_auth"] = {"ok": ok, "detail": auth_detail}
    if not ok:
        passed = False

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
        bal_detail = bal.get("error", bal.get("message", "")) if bal else "no_response"
        results["balance_fetch"] = {"ok": ok, "detail": bal_detail if not ok else "ok"}
        if not ok:
            passed = False
            _loge(f"PRE-FLIGHT FAIL: balance fetch failed — Delta says: {bal_detail}")

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
        # Build summary: show ok/fail + include detail for any failed check
        summary = {}
        for k, v in results.items():
            if isinstance(v, dict):
                ok_val = v.get("ok")
                detail = v.get("detail", "")
                summary[k] = ok_val if ok_val else f"FAIL: {detail}"
            else:
                summary[k] = v
        tg(f"{'✅' if passed else '❌'} Pre-flight {'PASSED' if passed else 'FAILED'}\n"
           f"Mode: LIVE | {json.dumps(summary, indent=2)}")

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
    entry_order_id: Optional[str] = None,
    api_request_time: Optional[float] = None,
    api_ack_time: Optional[float] = None,
    sl_placed_time: Optional[float] = None,
    tp_placed_time: Optional[float] = None,
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

    _slippage_pct = round(abs(entry_slippage) / fill_price * 100, 4) if fill_price else 0.0

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
        "entry_slippage_pct": _slippage_pct,
        # Recovery tracking
        "recovery_event":     False,
        "recovery_reason":    "",
        # Entry order lifecycle fields (live only)
        "entry_order_id":     entry_order_id,
        "api_request_time":   api_request_time,
        "api_ack_time":       api_ack_time,
        "sl_placed_time":     sl_placed_time,
        "tp_placed_time":     tp_placed_time,
        # Exit fields (populated at close time)
        "exit_order_id":      None,
        "exit_fill_px_delta": None,
    }

    save_state()

    # Lifecycle events
    _log_lifecycle(trade_id, "ENTRY_ACKED", order_id=entry_order_id or "", side=d.lower(), qty=LOT_SIZE, price=fill_price, notes=f"slip={entry_slippage:+.2f}pts grade={_grade}")
    if sl_oid:
        _log_lifecycle(trade_id, "SL_PLACED", order_id=sl_oid, side="sell" if d == "BUY" else "buy", qty=LOT_SIZE, price=sl_price)
    if tp_oid:
        _log_lifecycle(trade_id, "TP_PLACED", order_id=tp_oid, side="sell" if d == "BUY" else "buy", qty=LOT_SIZE, price=tp_price)

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

    _log_lifecycle(open_trade["trade_id"], "EXIT_DETECTED", price=exit_price, notes=exit_type)

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
        "entry_order_id":        trade.get("entry_order_id", ""),
        "api_request_time":      trade.get("api_request_time", ""),
        "api_ack_time":          trade.get("api_ack_time", ""),
        "sl_placed_time":        trade.get("sl_placed_time", ""),
        "tp_placed_time":        trade.get("tp_placed_time", ""),
        "exit_order_id":         trade.get("exit_order_id", ""),
        "exit_fill_px_delta":    trade.get("exit_fill_px_delta", ""),
        "entry_slippage_pct":    trade.get("entry_slippage_pct", ""),
    }
    _append_csv(CSV_FILE, CSV_HEADERS, row)
    _write_reconciliation(trade, python_outcome, pts, exit_slippage, trade_duration_sec)

    # ── slippage_audit.csv — entry + exit slippage in one row ────────────
    _pine_exit = trade.get("pine_tp", "") if "TP" in exit_type else trade.get("pine_sl", "")
    try:   _exit_slip_pct = round(exit_slippage / float(exit_price) * 100, 4) if exit_price else ""
    except: _exit_slip_pct = ""
    try:   _entry_slip_pct = trade.get("entry_slippage_pct", "")
    except: _entry_slip_pct = ""
    try:   _pine_pts = float(trade.get("pine_entry_px", 0)) - float(_pine_exit) if d == "SELL" else float(_pine_exit) - float(trade.get("pine_entry_px", 0))
    except: _pine_pts = ""
    _slip_row = {
        "trade_id":             trade.get("trade_id", ""),
        "timestamp_ist":        (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S"),
        "direction":            d,
        "mode":                 trade.get("mode", ""),
        "pine_entry_px":        trade.get("pine_entry_px", ""),
        "delta_entry_fill":     trade.get("fill_price", ""),
        "entry_slippage_pts":   trade.get("entry_slippage_pts", ""),
        "entry_slippage_pct":   _entry_slip_pct,
        "pine_exit_px":         _pine_exit,
        "delta_exit_fill":      exit_price,
        "exit_slippage_pts":    exit_slippage,
        "exit_slippage_pct":    _exit_slip_pct,
        "pine_pts":             _pine_pts,
        "live_pts":             pts,
        "slippage_drag_pts":    round(float(_pine_pts) - pts, 2) if _pine_pts != "" else "",
        "exit_type":            exit_type,
        "webhook_latency_ms":   trade.get("webhook_latency_ms", ""),
        "timeframe":            trade.get("signal_timeframe", ""),
    }
    _append_csv(SLIPPAGE_FILE, SLIPPAGE_HEADERS, _slip_row)

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
                    _logw(f"[LIVE] Position flat detected @ approx market price={price}")
                    _log_lifecycle(open_trade["trade_id"], "MONITOR_FLAT", notes=f"approx_price={price}")

                    # Determine which order actually filled and get real fill price
                    exit_fill_px    = price   # fallback to market price
                    exit_order_id   = None
                    exit_label      = "AUTO_EXIT"

                    for oid_key, label in [("tp_oid", "TP_LIVE"), ("sl_oid", "SL_LIVE")]:
                        oid = open_trade.get(oid_key)
                        if not oid:
                            continue
                        order_resp = _get(f"/v2/orders/{oid}")
                        if order_resp:
                            result_data = order_resp.get("result", {})
                            state = result_data.get("state", "")
                            if state in ("filled", "closed"):
                                raw_fill = result_data.get("average_fill_price")
                                if raw_fill:
                                    exit_fill_px  = float(raw_fill)
                                    exit_order_id = oid
                                    exit_label    = label
                                    _log(f"[LIVE] {label} confirmed: oid={oid} fill={exit_fill_px}")
                                    _log_lifecycle(open_trade["trade_id"], "EXIT_CONFIRMED",
                                                   order_id=oid, price=exit_fill_px, notes=label)
                                break  # found the filled order

                    if exit_order_id is None:
                        _logw(f"[LIVE] Could not confirm which order filled — using market price {exit_fill_px}")

                    # Store exit_order_id in trade before closing
                    open_trade["exit_order_id"]      = exit_order_id
                    open_trade["exit_fill_px_delta"]  = exit_fill_px

                    # Cancel the remaining open order
                    for oid_key in ("sl_oid", "tp_oid"):
                        oid = open_trade.get(oid_key)
                        if oid and oid != exit_order_id:
                            _delete(f"/v2/orders/{oid}")
                            _log_lifecycle(open_trade["trade_id"], f"{oid_key.upper().replace('_OID', '')}_CANCELLED", order_id=oid)

                    _close_trade(exit_fill_px, exit_label, 0.0)
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
        entry_order_id = api_request_time = api_ack_time = None
        sl_placed_t = tp_placed_t = None

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
            entry_contracts = _btc_to_contracts(LOT_SIZE, pine_entry_px)
            _log_lifecycle(trade_id, "ENTRY_SENT", side=side, qty=entry_contracts, price=pine_entry_px, notes=f"pine_ref={pine_entry_px} contracts={entry_contracts}")
            result = place_market_order(side, LOT_SIZE, ref_price=pine_entry_px)
            if not result:
                err = getattr(place_market_order, "_last_error", "unknown")
                _loge(f"Entry market order FAILED — aborting | Delta error: {err}")
                tg(f"❌ ENTRY FAILED [{d}]\nDelta error: <code>{err}</code>\nproduct_id={PRODUCT_ID} contracts={entry_contracts} side={side}")
                return

            if result:
                entry_order_id   = str(result.get("order_id", ""))
                api_request_time = result.get("api_request_time")
                api_ack_time     = result.get("api_ack_time")
            else:
                entry_order_id = api_request_time = api_ack_time = None

            fill_px = result.get("fill_price")
            if not fill_px:
                time.sleep(1.5)
                pos = get_open_position()
                fill_px = float(pos.get("entry_price", pine_entry_px)) if pos else pine_entry_px
            fill_px = round(fill_px, 1)

            # Place full-size SL stop + TP limit orders
            # Fill-based: SL/TP calculated from actual fill price to preserve 2:1 R:R.
            close_side = "sell" if d == "BUY" else "buy"
            sl_price   = round(fill_px - sl_dist, 1) if d == "BUY" else round(fill_px + sl_dist, 1)
            tp_price   = round(fill_px + sl_dist * TP_R, 1) if d == "BUY" else round(fill_px - sl_dist * TP_R, 1)

            sl_result = place_sl_order(close_side, LOT_SIZE, sl_price, contracts=entry_contracts)
            tp_result = place_tp_order(close_side, LOT_SIZE, tp_price, contracts=entry_contracts)

            sl_oid      = sl_result["order_id"]    if sl_result else None
            sl_placed_t = sl_result["placed_time"]  if sl_result else None
            tp_oid      = tp_result["order_id"]    if tp_result else None
            tp_placed_t = tp_result["placed_time"]  if tp_result else None

            if not sl_oid:
                _loge("SL ORDER FAILED after entry — CRITICAL: close position manually")
                tg(f"🚨 SL ORDER FAILED after {d} entry @ {fill_px} — CLOSE POSITION MANUALLY ON DELTA")

            # ── Write execution_latency.csv at ENTRY time (crash-safe record) ──
            # This row is written immediately after SL/TP placed — persists even if
            # bot crashes or redeploys before _close_trade() is called.
            # Wrapped in try/except so a CSV write failure can NEVER kill the entry flow.
            if not PAPER_MODE:
                try:
                    _entry_slip = round(fill_px - pine_entry_px if d == "BUY" else pine_entry_px - fill_px, 1)
                    _wh_lat = round((recv_time - pine_time / 1000) * 1000, 1) if pine_time else ""
                    _api_rt  = round((result.get("api_ack_time", 0) - result.get("api_request_time", 0)) * 1000, 1) if result else ""
                    _lat_row = {
                        "trade_id":            trade_id,
                        "timestamp_ist":       (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S"),
                        "direction":           d,
                        "mode":                "LIVE",
                        "pine_signal_time":    pine_time or "",
                        "webhook_recv_time":   recv_time,
                        "entry_submit_time":   result.get("api_request_time", "") if result else "",
                        "entry_ack_time":      result.get("api_ack_time", "") if result else "",
                        "pine_entry_px":       pine_entry_px,
                        "delta_fill_px":       fill_px,
                        "entry_slippage_pts":  _entry_slip,
                        "webhook_latency_ms":  _wh_lat,
                        "api_roundtrip_ms":    _api_rt,
                        "sl_price":            sl_price,
                        "tp_price":            tp_price,
                        "sl_order_id":         sl_oid or "",
                        "tp_order_id":         tp_oid or "",
                        "contracts":           entry_contracts,
                        "entry_order_id":      entry_order_id or "",
                    }
                    _append_csv(LATENCY_FILE, LATENCY_HEADERS, _lat_row)
                    log.info(f"[LATENCY_CSV] Entry row written for {trade_id} slip={_entry_slip:+.1f}pts wh={_wh_lat}ms")
                except Exception as _csv_err:
                    _loge(f"[LATENCY_CSV] Write failed (non-fatal) — entry flow continues: {_csv_err}")

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
                entry_order_id=entry_order_id,
                api_request_time=api_request_time,
                api_ack_time=api_ack_time,
                sl_placed_time=sl_placed_t,
                tp_placed_time=tp_placed_t,
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

# ── /diagnose ─────────────────────────────────────────────────────────
# Hits Delta API directly and returns raw responses — helps debug order
# rejections without waiting for a live signal.
@app.get("/diagnose")
async def diagnose():
    if PAPER_MODE:
        return JSONResponse({"mode": "PAPER", "note": "diagnose N/A in paper mode"})
    out = {}
    # 1. Wallet balance — check USD available_balance
    bal_resp = _get("/v2/wallet/balances")
    if bal_resp and "result" in bal_resp:
        out["balances"] = {
            r["asset_symbol"]: r["available_balance"]
            for r in bal_resp["result"]
            if float(r.get("available_balance", 0)) > 0
        }
    else:
        out["balances"] = bal_resp

    # 2. Fetch product 27 directly to confirm what it actually is
    prod27 = _get(f"/v2/products/{PRODUCT_ID}")
    if prod27 and "result" in prod27:
        p = prod27["result"]
        out["product_27_detail"] = {
            "id": p.get("id"),
            "symbol": p.get("symbol"),
            "description": p.get("description"),
            "contract_type": p.get("contract_type"),
            "contract_unit_currency": p.get("contract_unit_currency"),
            "tick_size": p.get("tick_size"),
            "min_size": p.get("min_size"),
            "state": p.get("state"),
        }
    else:
        out["product_27_detail"] = prod27

    # 3. Search all perpetual futures for BTC — no state filter
    prods = _get("/v2/products", {"contract_type": "perpetual_futures"})
    if prods and "result" in prods:
        btc_perps = [p for p in prods["result"] if "BTC" in p.get("symbol", "").upper() or "BTC" in p.get("description", "").upper()]
        out["btc_perpetuals"] = [{"id": p["id"], "symbol": p["symbol"], "description": p.get("description",""), "min_size": p.get("min_size"), "state": p.get("state")} for p in btc_perps[:10]]
    else:
        out["btc_perpetuals"] = prods

    # 4. Current position
    out["position_product_27"] = _get("/v2/positions", {"product_id": str(PRODUCT_ID)})
    out["configured_product_id"] = PRODUCT_ID
    out["configured_lot_size"] = LOT_SIZE
    out["base_url"] = BASE_URL
    return JSONResponse(out)

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
    """Live execution dashboard — Pine parity, latency, profitability insights, trade journal."""
    # ── Load trades ──────────────────────────────────────────────────────
    trades = []
    try:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            trades = list(csv.DictReader(f))
    except Exception:
        pass
    try:
        for bf in sorted(DATA_DIR.glob("trades.v3_backup_*.csv")):
            try:
                with open(bf, "r", encoding="utf-8") as f:
                    trades = list(csv.DictReader(f)) + trades
            except Exception:
                pass
    except Exception:
        pass

    # ── Load lifecycle events (last 50) ──────────────────────────────────
    lifecycle_rows = []
    try:
        with open(LIFECYCLE_FILE, "r", encoding="utf-8") as f:
            lifecycle_rows = list(csv.DictReader(f))[-50:]
    except Exception:
        pass

    # ── Snapshot current open trade ───────────────────────────────────────
    with _state_lock:
        ot = dict(open_trade) if open_trade else None

    now_ist = (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S IST")
    mode_label = "🟢 LIVE" if not PAPER_MODE else "📄 PAPER"
    mode_bg    = "#0a2a0a" if not PAPER_MODE else "#1a1a2a"
    mode_col   = "#4ade80" if not PAPER_MODE else "#93c5fd"

    # ── Helpers ───────────────────────────────────────────────────────────
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
        if v == "TP_PAPER":  return '<span style="color:#86efac;font-size:11px;">TP_PAPER</span>'
        if v == "SL_PAPER":  return '<span style="color:#fca5a5;font-size:11px;">SL_PAPER</span>'
        return f'<span style="color:#6b7280;font-size:11px;">{v or "—"}</span>'

    # ── Stats computation ─────────────────────────────────────────────────
    total    = len(trades)
    tp_cnt   = sum(1 for t in trades if t.get("python_actual_outcome") == "TP")
    sl_cnt   = sum(1 for t in trades if t.get("python_actual_outcome") == "SL")
    pts_list = []
    for t in trades:
        try: pts_list.append(float(t["pts"]))
        except: pass
    tot_pts  = round(sum(pts_list), 2)
    avg_pts  = round(sum(pts_list) / len(pts_list), 2) if pts_list else 0
    win_rt   = f"{round(tp_cnt/total*100)}%" if total else "—"

    slip_list = []
    for t in trades:
        try: slip_list.append(float(t["entry_slippage_pts"]))
        except: pass
    avg_slip = round(sum(slip_list)/len(slip_list), 2) if slip_list else 0

    wh_list = []
    for t in trades:
        try: wh_list.append(float(t["webhook_latency_ms"]))
        except: pass
    avg_wh = round(sum(wh_list)/len(wh_list), 1) if wh_list else 0

    el_list = []
    for t in trades:
        try: el_list.append(float(t["entry_latency_ms"]))
        except: pass
    avg_el = round(sum(el_list)/len(el_list), 1) if el_list else 0

    # ── Pine parity stats ─────────────────────────────────────────────────
    slip_abs = [abs(s) for s in slip_list]
    worst_slip = sorted(slip_abs, reverse=True)[:3]
    grade_counts = {}
    for t in trades:
        g = t.get("structure_grade","")
        if g: grade_counts[g] = grade_counts.get(g, 0) + 1
    grade_order = ["INTACT","MILD","DEGRADED","BROKEN","CRITICAL"]

    # Win rate by structure grade
    grade_wr = {}
    for g in grade_order:
        g_trades = [t for t in trades if t.get("structure_grade") == g]
        g_tp = sum(1 for t in g_trades if t.get("python_actual_outcome") == "TP")
        if g_trades:
            grade_wr[g] = (len(g_trades), g_tp, round(g_tp/len(g_trades)*100))

    # Win rate by direction
    buy_trades  = [t for t in trades if t.get("direction") == "BUY"]
    sell_trades = [t for t in trades if t.get("direction") == "SELL"]
    buy_wr  = f"{round(sum(1 for t in buy_trades  if t.get('python_actual_outcome')=='TP')/len(buy_trades)*100)}%"  if buy_trades  else "—"
    sell_wr = f"{round(sum(1 for t in sell_trades if t.get('python_actual_outcome')=='TP')/len(sell_trades)*100)}%" if sell_trades else "—"

    # Avg pts on TP vs SL
    tp_pts = [float(t["pts"]) for t in trades if t.get("python_actual_outcome")=="TP" and t.get("pts")]
    sl_pts = [float(t["pts"]) for t in trades if t.get("python_actual_outcome")=="SL" and t.get("pts")]
    avg_tp_pts = round(sum(tp_pts)/len(tp_pts), 1) if tp_pts else 0
    avg_sl_pts = round(sum(sl_pts)/len(sl_pts), 1) if sl_pts else 0

    # Hypothetical: what if we skipped DEGRADED+ entries?
    clean_trades = [t for t in trades if t.get("structure_grade") in ("INTACT","MILD","")]
    clean_tp  = sum(1 for t in clean_trades if t.get("python_actual_outcome") == "TP")
    clean_wr  = f"{round(clean_tp/len(clean_trades)*100)}%" if clean_trades else "—"

    # ── Open trade panel ─────────────────────────────────────────────────
    open_panel = ""
    if ot:
        d         = ot.get("direction","?")
        fill_px   = ot.get("fill_price", 0)
        pine_px   = ot.get("pine_entry_px", 0)
        sl_px     = ot.get("sl_price", 0)
        tp_px     = ot.get("tp_price", 0)
        slip      = ot.get("entry_slippage_pts", 0)
        wh_l      = ot.get("webhook_latency_ms", 0)
        en_l      = ot.get("entry_latency_ms", 0)
        grade_v   = ot.get("structure_grade","?")
        sl_oid    = ot.get("sl_oid","—")
        tp_oid    = ot.get("tp_oid","—")
        en_oid    = ot.get("entry_order_id","—")
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
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Pine Ref</div><div style="color:#9ca3af;font-size:16px;">{_f(pine_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Live Price</div><div style="color:#60a5fa;font-size:16px;font-weight:700;">{_f(live_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Unrealized</div><div style="color:{unreal_col};font-size:16px;font-weight:700;">{'+' if unreal>=0 else ''}{unreal:.1f} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">SL Level</div><div style="color:#f87171;font-size:16px;">{_f(sl_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">TP Level</div><div style="color:#4ade80;font-size:16px;">{_f(tp_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Entry Slippage</div><div style="color:#facc15;font-size:16px;">{'+' if float(slip or 0)>=0 else ''}{float(slip or 0):.2f} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Structure Grade</div><div style="color:{grade_col};font-size:16px;font-weight:700;">{grade_v}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">WH Latency</div><div style="color:#60a5fa;font-size:14px;">{float(wh_l or 0):.0f}ms</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Entry Latency</div><div style="color:#60a5fa;font-size:14px;">{float(en_l or 0):.0f}ms</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Entry Order ID</div><div style="color:#6b7280;font-size:11px;font-family:monospace;">{en_oid or '—'}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">SL / TP Order</div><div style="color:#6b7280;font-size:11px;font-family:monospace;">{str(sl_oid)[:8] if sl_oid else '—'} / {str(tp_oid)[:8] if tp_oid else '—'}</div></div>
  </div>
</div>"""

    # ── Pine parity panel ─────────────────────────────────────────────────
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

    worst_slip_str = " / ".join([f"{w:.1f}pts" for w in worst_slip]) if worst_slip else "—"
    clean_note = f"If INTACT+MILD only → WR {clean_wr} ({len(clean_trades)} trades)" if clean_trades else "No data yet"

    # ── Lifecycle events table ────────────────────────────────────────────
    lc_rows = ""
    for e in reversed(lifecycle_rows[-20:]):
        ev   = e.get("event","")
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

    lc_empty = "" if lc_rows else '<tr><td colspan="7" style="color:#4b5563;text-align:center;padding:16px;">No lifecycle events yet — starts recording on first live entry</td></tr>'

    # ── Trade journal rows ────────────────────────────────────────────────
    journal_rows = ""
    for i, t in enumerate(reversed(trades), 1):
        outcome = t.get("python_actual_outcome","")
        rbg = "#0a1a0a" if outcome=="TP" else "#1a0a0a" if outcome=="SL" else "#0d1117"
        rec = "♻️" if str(t.get("recovery_event","")).lower()=="true" else ""
        en_oid_s = str(t.get("entry_order_id","") or "")
        ex_oid_s = str(t.get("exit_order_id","") or "")
        ex_delta = t.get("exit_fill_px_delta","")
        slip_pct = t.get("entry_slippage_pct","")
        journal_rows += f"""
        <tr style="background:{rbg};border-bottom:1px solid #1f2937;">
          <td style="color:#6b7280;text-align:center;">{len(trades)-i+1}</td>
          <td>{_dir(t.get('direction',''))}</td>
          <td style="color:#d1d5db;font-size:11px;">{_dt(t.get('entry_fill_time',''))}</td>
          <td style="color:#9ca3af;text-align:center;">{t.get('signal_timeframe','—')}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('fill_price',''))}</td>
          <td style="color:#9ca3af;text-align:right;">{_f(t.get('pine_entry_px',''))}</td>
          <td style="color:#34d399;text-align:right;">{_f(t.get('pine_tp',''))}</td>
          <td style="color:#f87171;text-align:right;">{_f(t.get('pine_sl',''))}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('exit_price',''))}</td>
          <td style="color:#9ca3af;text-align:right;font-size:10px;">{_f(ex_delta) if ex_delta else '—'}</td>
          <td style="text-align:right;">{_pc(t.get('pts',''))}</td>
          <td style="color:#9ca3af;text-align:right;">{_pts(t.get('pnl_approx',''))}</td>
          <td style="text-align:center;">{_outcome(outcome)}</td>
          <td style="text-align:center;">{_exit_type(t.get('exit_type',''))}</td>
          <td style="text-align:right;">{_pc(t.get('entry_slippage_pts',''))}</td>
          <td style="color:#9ca3af;text-align:right;font-size:10px;">{f"{float(slip_pct):.4f}%" if slip_pct else "—"}</td>
          <td style="color:#60a5fa;text-align:right;">{_ms(t.get('webhook_latency_ms',''))}</td>
          <td style="color:#9ca3af;text-align:right;">{_ms(t.get('entry_latency_ms',''))}</td>
          <td style="color:#9ca3af;text-align:right;">{_dur(t.get('trade_duration_sec',''))}</td>
          <td style="text-align:center;">{_grade(t.get('structure_grade',''))}</td>
          <td style="color:#6b7280;font-size:10px;font-family:monospace;">{en_oid_s[:10] or '—'}</td>
          <td style="color:#6b7280;font-size:10px;font-family:monospace;">{ex_oid_s[:10] or '—'}</td>
          <td style="color:#9ca3af;text-align:center;">{rec or '—'}</td>
        </tr>"""

    empty_msg = "" if trades else '<tr><td colspan="23" style="text-align:center;color:#6b7280;padding:40px;">No trades yet — waiting for first signal</td></tr>'

    # Persistence warning — shown prominently if no Railway volume is mounted
    _persist_warn = "" if _DATA_PERSISTENT else """
    <div style="background:#2a1a00;border:1px solid #f59e0b;border-radius:8px;padding:12px 20px;
                margin:0 24px 16px;display:flex;align-items:center;gap:12px;">
      <span style="font-size:18px">⚠️</span>
      <div>
        <div style="color:#f59e0b;font-weight:700;font-size:13px">DATA NOT PERSISTING — Railway Volume Not Mounted</div>
        <div style="color:#d97706;font-size:11px;margin-top:3px">
          trades.csv and state.json are wiped on every redeploy. Live trades will NOT appear here after bot restarts.<br>
          Fix: Railway → Storage → Add Volume → Mount Path: <code>/app/data</code> → Set env var <code>DATA_DIR=/app/data</code>
        </div>
      </div>
    </div>"""

    _latency_count = 0
    try:
        with open(LATENCY_FILE, "r") as _lf:
            _latency_count = sum(1 for _ in _lf) - 1  # subtract header
    except Exception:
        pass

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vol Surge v4 — Live Dashboard</title>
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{background:#080c10;color:#e2e8f0;font-family:'Segoe UI',system-ui,monospace;font-size:13px}}
  .hdr{{background:#0d1117;border-bottom:2px solid #1f2937;padding:14px 24px;display:flex;align-items:center;justify-content:space-between}}
  .hdr h1{{font-size:17px;font-weight:700;color:#f9fafb}}
  .sec{{font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:1px;padding:16px 24px 8px}}
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
</style>
<script>
  setTimeout(()=>location.reload(),15000);
  setInterval(()=>{{document.getElementById('clk').textContent=new Date().toLocaleTimeString('en-IN',{{timeZone:'Asia/Kolkata'}})}},1000);
  window.onload=()=>document.getElementById('clk').textContent=new Date().toLocaleTimeString('en-IN',{{timeZone:'Asia/Kolkata'}});
</script>
</head>
<body>

<!-- HEADER -->
<div class="hdr">
  <div>
    <h1>⚡ Vol Surge v4.0 — Live Dashboard</h1>
    <div style="color:#6b7280;font-size:11px;margin-top:3px;">BTCUSD · Delta Exchange India · auto-refresh 15s · {now_ist}</div>
  </div>
  <div style="text-align:right;display:flex;flex-direction:column;align-items:flex-end;gap:6px;">
    <span style="background:{mode_bg};color:{mode_col};padding:4px 14px;border-radius:20px;font-size:12px;font-weight:700;">{mode_label}</span>
    <span style="color:#6b7280;font-size:11px;">🕐 IST <b id="clk"></b></span>
    <span style="color:#{'4ade80' if ot else '6b7280'};font-size:11px;">{'🔴 POSITION OPEN' if ot else '⚪ IDLE'}</span>
    <span style="color:#{'4ade80' if _DATA_PERSISTENT else 'f59e0b'};font-size:10px;">{'💾 Data Persistent' if _DATA_PERSISTENT else '⚠️ Data Ephemeral'}</span>
  </div>
</div>

{_persist_warn}

<!-- ENTRY LATENCY COUNTER -->
<div style="padding:4px 24px 0;font-size:11px;color:#6b7280;">
  📊 Entries recorded in execution_latency.csv: <b style="color:#e2e8f0">{_latency_count}</b>
  &nbsp;|&nbsp; Closed trades in trades.csv: <b style="color:#e2e8f0">{len(trades)}</b>
  &nbsp;|&nbsp; Data path: <code style="color:#60a5fa">{DATA_DIR}</code>
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
  <div class="stat"><div class="sv" style="color:#60a5fa">{avg_wh:.0f}ms</div><div class="sl">Avg WH Latency</div></div>
  <div class="stat"><div class="sv" style="color:#818cf8">{avg_el:.0f}ms</div><div class="sl">Avg Entry Latency</div></div>
  <div class="stat"><div class="sv" style="color:#4ade80">{buy_wr}</div><div class="sl">BUY Win Rate</div></div>
  <div class="stat"><div class="sv" style="color:#f87171">{sell_wr}</div><div class="sl">SELL Win Rate</div></div>
</div>

<!-- ANALYSIS PANELS -->
<div class="grid2" style="padding:0 24px;gap:16px;margin-bottom:16px;">

  <!-- Pine Parity / Fill Quality -->
  <div class="panel">
    <h3>🎯 Pine Parity — Fill Quality vs Signal</h3>
    <div class="kv"><span class="kl">Avg entry slippage</span><span class="kv2">{avg_slip:+.2f} pts</span></div>
    <div class="kv"><span class="kl">Worst fills (top 3)</span><span class="kv2">{worst_slip_str}</span></div>
    <div class="kv"><span class="kl">BUY fill vs Pine</span><span class="kv2">{'—' if not buy_trades else f"{round(sum(float(t.get('entry_slippage_pts',0)) for t in buy_trades)/len(buy_trades),2):+.2f} pts avg"}</span></div>
    <div class="kv"><span class="kl">SELL fill vs Pine</span><span class="kv2">{'—' if not sell_trades else f"{round(sum(float(t.get('entry_slippage_pts',0)) for t in sell_trades)/len(sell_trades),2):+.2f} pts avg"}</span></div>
    <div class="kv" style="margin-top:8px"><span class="kl">Grade distribution</span><span></span></div>
    <table style="margin-top:6px">
      <thead><tr><th>Grade</th><th style="text-align:right">Count</th><th style="text-align:right">%</th><th style="text-align:right">Win Rate</th><th>Bar</th></tr></thead>
      <tbody>{grade_rows or '<tr><td colspan="5" style="color:#4b5563;padding:8px">No data yet</td></tr>'}</tbody>
    </table>
  </div>

  <!-- Profitability Insights -->
  <div class="panel">
    <h3>💡 Profitability Insights</h3>
    <div class="kv"><span class="kl">Overall win rate</span><span class="kv2">{win_rt} ({total} trades)</span></div>
    <div class="kv"><span class="kl">INTACT+MILD only</span><span class="kv2">{clean_wr} ({len(clean_trades)} trades)</span></div>
    <div class="kv"><span class="kl">BUY win rate</span><span class="kv2">{buy_wr} ({len(buy_trades)} trades)</span></div>
    <div class="kv"><span class="kl">SELL win rate</span><span class="kv2">{sell_wr} ({len(sell_trades)} trades)</span></div>
    <div class="kv"><span class="kl">Avg pts on TP</span><span class="kv2" style="color:#4ade80">{avg_tp_pts:+.1f} pts</span></div>
    <div class="kv"><span class="kl">Avg pts on SL</span><span class="kv2" style="color:#f87171">{avg_sl_pts:+.1f} pts</span></div>
    <div class="kv"><span class="kl">Required WR to break even</span><span class="kv2">{f"{round(abs(avg_sl_pts)/(abs(avg_sl_pts)+avg_tp_pts)*100)}%" if avg_tp_pts>0 and avg_sl_pts<0 else "—"}</span></div>
    <div style="margin-top:10px">
      {"<div class='insight'>✅ INTACT entries performing well — keep filtering</div>" if grade_wr.get("INTACT",("","",0))[2]>60 else ""}
      {"<div class='insight warn'>⚠️ DEGRADED+ entries hurting win rate — consider skipping</div>" if grade_counts.get("DEGRADED",0)+grade_counts.get("BROKEN",0)+grade_counts.get("CRITICAL",0)>2 else ""}
      {"<div class='insight info'>ℹ️ Not enough data yet — need 20+ trades for meaningful insights</div>" if total<20 else ""}
      {"<div class='insight'>✅ Sufficient data for analysis</div>" if total>=20 else ""}
    </div>
  </div>
</div>

<!-- LATENCY BREAKDOWN -->
<div class="panel" style="margin:0 24px 16px;">
  <h3>⏱ Execution Latency Breakdown</h3>
  <div class="grid3">
    <div>
      <div class="kv"><span class="kl">Avg webhook latency</span><span class="kv2" style="color:#60a5fa">{avg_wh:.0f} ms</span></div>
      <div class="kv"><span class="kl">Best webhook</span><span class="kv2" style="color:#4ade80">{f"{min(wh_list):.0f} ms" if wh_list else "—"}</span></div>
      <div class="kv"><span class="kl">Worst webhook</span><span class="kv2" style="color:#f87171">{f"{max(wh_list):.0f} ms" if wh_list else "—"}</span></div>
    </div>
    <div>
      <div class="kv"><span class="kl">Avg entry latency</span><span class="kv2" style="color:#818cf8">{avg_el:.0f} ms</span></div>
      <div class="kv"><span class="kl">Best entry</span><span class="kv2" style="color:#4ade80">{f"{min(el_list):.0f} ms" if el_list else "—"}</span></div>
      <div class="kv"><span class="kl">Worst entry</span><span class="kv2" style="color:#f87171">{f"{max(el_list):.0f} ms" if el_list else "—"}</span></div>
    </div>
    <div>
      <div class="kv"><span class="kl">Total avg end-to-end</span><span class="kv2" style="color:#facc15">{round(avg_wh+avg_el):.0f} ms</span></div>
      <div class="kv"><span class="kl">Pine signal → fill</span><span class="kv2">WH + entry combined</span></div>
      <div class="kv"><span class="kl">Target</span><span class="kv2" style="color:#4ade80">WH &lt;1500ms · Entry &lt;500ms</span></div>
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

<!-- TRADE JOURNAL -->
<div class="sec">Trade Journal — All Trades (newest first)</div>
<div class="tw">
<table>
  <thead><tr>
    <th>#</th><th>Dir</th><th>Time (IST)</th><th>TF</th>
    <th style="text-align:right">Fill $</th>
    <th style="text-align:right">Pine $</th>
    <th style="text-align:right">TP $</th>
    <th style="text-align:right">SL $</th>
    <th style="text-align:right">Exit $</th>
    <th style="text-align:right">Δ Exit</th>
    <th style="text-align:right">Pts</th>
    <th style="text-align:right">P&L</th>
    <th>Result</th>
    <th>Exit Type</th>
    <th style="text-align:right">Slip pts</th>
    <th style="text-align:right">Slip %</th>
    <th style="text-align:right">WH lat</th>
    <th style="text-align:right">En lat</th>
    <th style="text-align:right">Duration</th>
    <th>Grade</th>
    <th>Entry OID</th>
    <th>Exit OID</th>
    <th>Rec</th>
  </tr></thead>
  <tbody>
    {journal_rows}
    {empty_msg}
  </tbody>
</table>
</div>

<div class="footer">
  Vol Surge v4.0 · IDLE → ENTERED → CLOSED · TP=2R fixed · SL=fixed · Monitor=2s poll · LOT={LOT_SIZE} BTC · {'LIVE' if not PAPER_MODE else 'PAPER'}
</div>
</body>
</html>"""

    return HTMLResponse(content=html)
