#!/usr/bin/env python3
"""
volsurge_v3.py — Vol Surge Bot v3.1 (Live Validation Build)
============================================================
Philosophy  : TradingView = Signal Intent | Python = Execution Authority | Delta = Reality
Lifecycle   : IDLE → ENTERED → TP1_DONE → CLOSED   (no trailing, no BE movement)
TP1 model   : 50% partial close at TP1 price. Original SL price NEVER changes.
Remaining   : 50% runs to TP2 or original SL — whichever monitor detects first.

Modes:
  PAPER_MODE=true  → fills simulated at live Delta price, no real orders
  PAPER_MODE=false → real orders on Delta Exchange India LIVE

Start:
  python -m uvicorn volsurge_v3:app --host 0.0.0.0 --port 5001

Changes from v3.0:
  - TP1 executes 50% PARTIAL CLOSE (was: BE/trail move)
  - Original SL never moves after entry (was: moved to BE on TP1)
  - Trail logic completely removed
  - Monitor owns lifecycle at 2s polling (was: 5s)
  - Pre-flight validation (mandatory for LIVE, auto-runs on startup)
  - Duplicate TP1 exit guard (tp1_hit flag)
  - remaining_size tracking (LOT_SIZE → LOT_SIZE/2 after TP1)
  - Blended PnL: 0.5×TP1pts + 0.5×finalPts (matches Pine)
  - Full telemetry CSV: slippage, latency, timestamps, reconciliation
  - Reconciliation log per trade (Pine expected vs Python actual)
  - TP1_HIT webhook reads tp1_price (was: be_price — old Pine format)
  - Fail-loud: LIVE webhooks blocked until preflight passes
  - /preflight and /reconciliation endpoints added
"""

# ════════════════════════════════════════════════════════════════════════
# IMPORTS
# ════════════════════════════════════════════════════════════════════════
import os, sys, json, time, hmac, hashlib, logging, threading, csv
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

# ════════════════════════════════════════════════════════════════════════
# .env SUPPORT
# ════════════════════════════════════════════════════════════════════════
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ════════════════════════════════════════════════════════════════════════
# CONFIG  (override via .env file)
# ════════════════════════════════════════════════════════════════════════
API_KEY        = os.getenv("DELTA_API_KEY_LIVE",    "")
API_SECRET     = os.getenv("DELTA_API_SECRET_LIVE", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET",        "abc123")
TG_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN",    "")
TG_CHAT        = os.getenv("TELEGRAM_CHAT_ID",      "")

PAPER_MODE  = os.getenv("PAPER_MODE", "true").lower() == "true"

# Exit authority control
# false (default) → monitor is sole exit authority; Pine TP/SL webhooks logged as telemetry only
# true            → hybrid mode; Pine TP1_HIT/TP2_HIT webhooks can also trigger exits (old behaviour)
USE_PINE_EXIT_WEBHOOKS = os.getenv("USE_PINE_EXIT_WEBHOOKS", "false").lower() == "true"

BASE_URL    = "https://api.india.delta.exchange"
PRODUCT_ID  = int(os.getenv("PRODUCT_ID", "27"))      # BTCUSD Perpetual

# ── Lot sizing (Delta Exchange India — BTCUSD Perpetual) ──────────────
# Contracts are in BTC. Delta minimum order size = 0.001 BTC (1 contract).
# LOT_SIZE    = total position size in BTC
# PARTIAL_LOT = 50% of position, used for TP1 partial close
#
# Constraint: PARTIAL_LOT must be >= 0.001 BTC (Delta minimum)
# Therefore:  LOT_SIZE must be >= 0.002 BTC
#
# Recommended minimum for this bot:
#   LOT_SIZE = 0.002  →  PARTIAL_LOT = 0.001 BTC  ✅ meets minimum
#
# Default is set to 0.002 to enforce this constraint out of the box.
# Override via LOT_SIZE= in your .env file.
LOT_SIZE    = float(os.getenv("LOT_SIZE", "0.002"))   # BTC — full position size
PARTIAL_LOT = round(LOT_SIZE * 0.5, 6)                # BTC — 50% close at TP1

# Delta Exchange India confirmed minimum order size
DELTA_MIN_SIZE_BTC = 0.001   # 0.001 BTC per order (verified 10 May 2026)

TP1_R = 1.0   # TP1 = entry ± sl_dist × 1.0
TP2_R = 2.0   # TP2 = entry ± sl_dist × 2.0

PRICE_INTERVAL = 2    # seconds between price poll ticks
POS_MON_DELAY  = 3    # seconds to wait after entry before monitor starts

# Fail-loud safety — NEVER allow silent paper fallback in live mode
ALLOW_PAPER_FALLBACK = False

# ════════════════════════════════════════════════════════════════════════
# STATE CONSTANTS
# ════════════════════════════════════════════════════════════════════════
STATE_IDLE     = "IDLE"
STATE_ENTERED  = "ENTERED"
STATE_TP1_DONE = "TP1_DONE"
STATE_CLOSED   = "CLOSED"

# ════════════════════════════════════════════════════════════════════════
# FILE PATHS
# Railway: mount persistent volumes at /app/data and /app/logs.
# Override via DATA_DIR= / LOG_DIR= env vars if needed.
# Relative paths ("data", "logs") resolve correctly when Railway runs
# from /app (its default working directory).
# ════════════════════════════════════════════════════════════════════════
DATA_DIR   = Path(os.getenv("DATA_DIR", "data")); DATA_DIR.mkdir(exist_ok=True)
LOG_DIR    = Path(os.getenv("LOG_DIR",  "logs")); LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"
CSV_FILE   = DATA_DIR / "trades.csv"
RECON_FILE = DATA_DIR / "reconciliation.csv"
LOG_FILE   = LOG_DIR  / "volsurge_v3.log"

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
_preflight_ok     = False   # must pass before LIVE trading allowed

def _tid() -> str:
    return open_trade.get("trade_id", "?") if open_trade else "IDLE"

def _log (msg): log.info   (f"[{_tid()}] {msg}")
def _logw(msg): log.warning(f"[{_tid()}] {msg}")
def _loge(msg): log.error  (f"[{_tid()}] {msg}")

# ════════════════════════════════════════════════════════════════════════
# GRACEFUL SHUTDOWN  (Railway SIGTERM / Ctrl-C)
# Railway sends SIGTERM before stopping a container (redeploy / restart).
# Handler saves open trade state so startup() can recover it cleanly.
# SIGINT (Ctrl-C) uses the same handler for local dev parity.
# ════════════════════════════════════════════════════════════════════════
import signal as _signal

def _handle_shutdown(signum, frame):
    sig_name = "SIGTERM" if signum == _signal.SIGTERM else "SIGINT"
    log.warning(f"[SHUTDOWN] {sig_name} received — initiating graceful shutdown")
    with _state_lock:
        if open_trade:
            tid  = open_trade.get("trade_id", "?")
            st   = open_trade.get("state", "?")
            d    = open_trade.get("direction", "?")
            tp1h = open_trade.get("tp1_hit", False)
            open_trade["_shutdown_at"] = datetime.now().isoformat()
            save_state()
            log.warning(
                f"[SHUTDOWN] Open trade state preserved in state.json\n"
                f"  trade_id  = {tid}\n"
                f"  state     = {st}\n"
                f"  direction = {d}\n"
                f"  tp1_hit   = {tp1h}\n"
                f"  Monitor will resume automatically on next startup"
            )
            tg(
                f"⚠️ <b>Bot shutting down ({sig_name})</b>\n"
                f"Trade <b>{tid}</b> [{d}] state saved to state.json\n"
                f"TP1 hit: {'Yes' if tp1h else 'No'} | State: {st}\n"
                f"Will auto-resume on Railway restart ♻️"
            )
        else:
            log.info(f"[SHUTDOWN] {sig_name} — no open trade — clean shutdown")
    sys.exit(0)

_signal.signal(_signal.SIGTERM, _handle_shutdown)
_signal.signal(_signal.SIGINT,  _handle_shutdown)

# ════════════════════════════════════════════════════════════════════════
# TELEMETRY CSV SCHEMAS
# ════════════════════════════════════════════════════════════════════════
CSV_HEADERS = [
    # Identity
    "trade_id", "direction", "mode",
    # Signal context
    "signal_timeframe", "signal_tf_bar_time",
    # Entry prices
    "pine_entry_px", "fill_price", "entry_slippage_pts",
    "pine_tp1", "pine_tp2", "pine_sl",
    # Entry timestamps
    "pine_signal_time", "webhook_recv_time", "entry_fill_time",
    "webhook_latency_ms", "entry_latency_ms",
    # TP1 partial
    "tp1_hit", "tp1_fill_px", "tp1_time", "tp1_pts_50pct",
    # Exit
    "exit_price", "exit_time", "exit_type", "exit_slippage_pts",
    # PnL
    "blended_pts", "blended_pnl_approx",
    # Reconciliation
    "pine_expected_outcome", "python_actual_outcome", "lifecycle_match",
    # Entry quality metrics
    "slippage_ratio", "structure_grade",
    "bot_tp2_reached", "captured_vs_intended_pct",
    # Execution authority (who actually made each exit decision)
    "execution_authority", "lifecycle_source",
    # Execution telemetry
    "trade_duration_sec", "monitor_cycles_to_tp1", "monitor_cycles_total",
    # Model B simulation (analytics only — zero execution impact)
    # Model B: no TP1, full position held to 2R target or original SL
    "sim_b_outcome", "sim_b_exit_price", "sim_b_exit_time",
    "sim_b_pts", "sim_b_duration_sec", "comparison_delta_pts",
    # Intrabar timing telemetry (analytics only — no execution impact)
    # Tracks when monitor FIRST detected TP levels vs when Pine webhook arrived
    "tp1_first_touch_time", "tp2_first_touch_time",
    "tp1_touch_to_webhook_ms", "tp2_touch_to_webhook_ms",
    "tp1_touch_to_close_ms", "tp2_touch_to_close_ms",
    "intrabar_touch_detected", "intrabar_reversal_before_close",
    # Recovery tracking — set only on trades that resumed after restart/crash
    "recovery_event", "recovery_reason",
]

RECON_HEADERS = [
    "trade_id", "timestamp",
    "signal_timeframe",
    "pine_expected_outcome", "python_actual_outcome", "lifecycle_match",
    "entry_slippage_pts", "exit_slippage_pts",
    "webhook_latency_ms", "entry_latency_ms",
    "divergence_reason",
    "trade_duration_sec", "monitor_cycles_total",
    "recovery_event", "recovery_reason",
]

def _init_csvs():
    for fpath, headers in [(CSV_FILE, CSV_HEADERS), (RECON_FILE, RECON_HEADERS)]:
        if fpath.exists():
            # Check if existing CSV has the correct v3.1 headers
            try:
                with open(fpath, "r", newline="") as f:
                    existing_headers = next(csv.reader(f), [])
                if existing_headers != headers:
                    # Old schema detected — back it up and start fresh
                    backup = fpath.with_suffix(f".v3_backup_{int(time.time())}.csv")
                    fpath.rename(backup)
                    log.warning(f"[CSV] Schema mismatch in {fpath.name} — backed up to {backup.name}, creating fresh v3.1 file")
                else:
                    continue  # Headers match — no action needed
            except Exception as e:
                log.warning(f"[CSV] Could not check headers for {fpath.name}: {e}")
        with open(fpath, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=headers).writeheader()
        log.info(f"[CSV] Initialised {fpath.name} with v3.1 headers")

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
    """Persist current lifecycle state to state.json.
    Always adds _saved_at timestamp for crash diagnostics.
    Called after every state transition and on SIGTERM.
    """
    try:
        if open_trade:
            payload = {**open_trade, "_saved_at": datetime.now().isoformat()}
        else:
            payload = {"state": STATE_IDLE, "_saved_at": datetime.now().isoformat()}
        STATE_FILE.write_text(json.dumps(payload, indent=2))
    except Exception as e:
        _loge(f"save_state error: {e}")

def load_state() -> Optional[dict]:
    """Read state.json. Returns the active trade dict if one exists,
    or None if state is IDLE/CLOSED/missing.
    Logs full detail of every field needed for recovery decisions.
    """
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
        # Active trade found — log every relevant field for operator visibility
        log.warning(
            f"[RECOVERY] ══════════════════════════════════════════\n"
            f"[RECOVERY]  Active trade found — will resume\n"
            f"[RECOVERY]  trade_id    = {data.get('trade_id','?')}\n"
            f"[RECOVERY]  state       = {st}\n"
            f"[RECOVERY]  direction   = {data.get('direction','?')}\n"
            f"[RECOVERY]  mode        = {data.get('mode','?')}\n"
            f"[RECOVERY]  fill_price  = {data.get('fill_price','?')}\n"
            f"[RECOVERY]  tp1_hit     = {data.get('tp1_hit', False)}\n"
            f"[RECOVERY]  tp1_price   = {data.get('tp1_price','?')}\n"
            f"[RECOVERY]  tp2_price   = {data.get('tp2_price','?')}\n"
            f"[RECOVERY]  sl_price    = {data.get('original_sl_price','?')}\n"
            f"[RECOVERY]  saved_at    = {data.get('_saved_at','unknown')}\n"
            f"[RECOVERY] ══════════════════════════════════════════"
        )
        return data
    except Exception as e:
        log.error(f"[RECOVERY] load_state error: {e}")
    return None

# ════════════════════════════════════════════════════════════════════════
# DELTA AUTH
# CRITICAL: query string must include leading '?' in HMAC payload
# Signed payload = METHOD + timestamp + path + ?query_string + body
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
            timeout=5,
        )
    except Exception:
        pass

# ════════════════════════════════════════════════════════════════════════
# EXCHANGE HELPERS
# ════════════════════════════════════════════════════════════════════════
def get_open_position() -> Optional[dict]:
    """Return current open position on Delta, or None if flat."""
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
    """Market order. Returns {order_id, fill_price} or None."""
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
    """Stop-market SL order. Returns order_id or None."""
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

def place_tp2_order(close_side: str, size: float, tp2_price: float) -> Optional[str]:
    """Limit TP2 order (reduce_only). Returns order_id or None."""
    body = {
        "product_id":    PRODUCT_ID,
        "size":          size,
        "side":          close_side.lower(),
        "order_type":    "limit_order",
        "limit_price":   str(round(tp2_price, 1)),
        "reduce_only":   True,
        "time_in_force": "gtc",
    }
    resp = _post("/v2/orders", body)
    if resp and resp.get("result", {}).get("id"):
        return str(resp["result"]["id"])
    _loge(f"TP2 order failed: {resp}")
    return None

def cancel_order(order_id: str, retries: int = 3, delay: float = 1.5) -> bool:
    """Cancel with verify. Returns True when Delta confirms gone."""
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

# ════════════════════════════════════════════════════════════════════════
# PRE-FLIGHT VALIDATION
# Must pass before LIVE mode accepts any entry webhook.
# ════════════════════════════════════════════════════════════════════════
def run_preflight() -> dict:
    results = {}
    passed  = True

    # 1. Credentials present
    ok = bool(API_KEY and API_SECRET)
    results["credentials"] = {"ok": ok}
    if not ok:
        passed = False
        _loge("PRE-FLIGHT FAIL: API credentials missing from .env")

    # 2. API authentication
    resp = _get("/v2/profile")
    ok   = resp is not None and "result" in resp
    results["api_auth"] = {
        "ok":     ok,
        "detail": resp.get("result", {}).get("email", "?") if ok else str(resp),
    }
    if not ok:
        passed = False
        _loge("PRE-FLIGHT FAIL: API authentication failed")

    # 3. Price feed reachable
    price = fetch_price()
    ok    = price is not None
    results["price_feed"] = {"ok": ok, "price": price}
    if not ok:
        passed = False
        _loge("PRE-FLIGHT FAIL: price feed unavailable")

    # 4. No stale open position on exchange
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

    # 5. No stale state file with active trade
    stale = load_state()
    ok    = stale is None
    results["clean_state"] = {
        "ok":     ok,
        "detail": stale.get("trade_id") if stale else "clean",
    }
    if not ok:
        passed = False
        _loge(f"PRE-FLIGHT FAIL: state file has active trade {stale.get('trade_id')} — recover or delete data/state.json")

    # 6. Balance fetch
    if not PAPER_MODE:
        bal = _get("/v2/wallet/balances")
        ok  = bal is not None and "result" in bal
        results["balance_fetch"] = {"ok": ok}
        if not ok:
            passed = False
            _loge("PRE-FLIGHT FAIL: balance fetch failed")

    # 7. Partial lot sanity (non-zero)
    ok = PARTIAL_LOT > 0
    results["partial_lot"] = {"ok": ok, "lot": LOT_SIZE, "partial": PARTIAL_LOT}
    if not ok:
        passed = False
        _loge("PRE-FLIGHT FAIL: PARTIAL_LOT is zero")

    # 8. Delta minimum order size for partial exits
    # Confirmed: Delta Exchange India BTCUSD Perpetual minimum = 0.001 BTC per order.
    # PARTIAL_LOT must be >= 0.001 BTC or the reduce_only TP1 close will be rejected.
    # Required: LOT_SIZE >= 0.002 BTC so PARTIAL_LOT = 0.001 BTC (the minimum).
    ok = PARTIAL_LOT >= DELTA_MIN_SIZE_BTC
    results["partial_lot_min_size"] = {
        "ok":               ok,
        "partial_lot_btc":  PARTIAL_LOT,
        "lot_size_btc":     LOT_SIZE,
        "delta_minimum_btc": DELTA_MIN_SIZE_BTC,
        "fix":              f"Set LOT_SIZE=0.002 in .env → PARTIAL_LOT={LOT_SIZE * 0.5:.6f} BTC" if not ok else "OK",
    }
    if not ok:
        passed = False
        _loge(
            f"PRE-FLIGHT FAIL: PARTIAL_LOT={PARTIAL_LOT} BTC < Delta minimum={DELTA_MIN_SIZE_BTC} BTC. "
            f"TP1 partial close WILL be rejected by Delta Exchange. "
            f"Set LOT_SIZE=0.002 in .env — PARTIAL_LOT becomes 0.001 BTC (confirmed minimum)."
        )

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
# BLENDED PNL  (matches Pine Script calculation)
# TP1 hit : 0.5 × (TP1fill - entry) + 0.5 × (finalExit - entry)
# No TP1  : full position at final exit
# ════════════════════════════════════════════════════════════════════════
def _calc_blended_pts(direction: str, entry_px: float,
                      tp1_fill_px: float, final_px: float, tp1_hit: bool) -> float:
    if direction == "BUY":
        if tp1_hit:
            return round(0.5 * (tp1_fill_px - entry_px) + 0.5 * (final_px - entry_px), 2)
        return round(final_px - entry_px, 2)
    else:  # SELL
        if tp1_hit:
            return round(0.5 * (entry_px - tp1_fill_px) + 0.5 * (entry_px - final_px), 2)
        return round(entry_px - final_px, 2)

# ════════════════════════════════════════════════════════════════════════
# ENTRY QUALITY GRADING
# Classifies trade structure based on entry_slippage / sl_dist ratio.
# Higher ratio = slippage consumed more of the intended risk/reward envelope.
# ════════════════════════════════════════════════════════════════════════
def _structure_grade(slippage_ratio: float) -> str:
    if slippage_ratio < 0.25:  return "INTACT"
    if slippage_ratio < 0.5:   return "MILD"
    if slippage_ratio < 1.0:   return "DEGRADED"
    if slippage_ratio < 1.5:   return "BROKEN"
    return "CRITICAL"

# ════════════════════════════════════════════════════════════════════════
# RECONCILIATION LOG  (Pine expected vs Python actual per trade)
# ════════════════════════════════════════════════════════════════════════
def _write_reconciliation(trade: dict, python_outcome: str, exit_slippage: float = 0.0,
                          trade_duration_sec: float = 0.0):
    pine_expected  = trade.get("pine_expected_outcome", "UNKNOWN")
    match          = pine_expected == python_outcome
    diverge_reason = "" if match else "OHLC_AMBIGUITY_OR_TIMING"
    row = {
        "trade_id":               trade["trade_id"],
        "timestamp":              datetime.now().isoformat(),
        "signal_timeframe":       trade.get("signal_timeframe", ""),
        "pine_expected_outcome":  pine_expected,
        "python_actual_outcome":  python_outcome,
        "lifecycle_match":        match,
        "entry_slippage_pts":     trade.get("entry_slippage_pts", 0),
        "exit_slippage_pts":      exit_slippage,
        "webhook_latency_ms":     trade.get("webhook_latency_ms", 0),
        "entry_latency_ms":       trade.get("entry_latency_ms", 0),
        "divergence_reason":      diverge_reason,
        "trade_duration_sec":     trade_duration_sec,
        "monitor_cycles_total":   trade.get("monitor_cycles", 0),
        "recovery_event":         trade.get("recovery_event",  False),
        "recovery_reason":        trade.get("recovery_reason", ""),
    }
    _append_csv(RECON_FILE, RECON_HEADERS, row)
    marker = "✅ MATCH" if match else "⚠️  DIVERGED"
    _log(f"RECONCILIATION {marker} | pine={pine_expected} python={python_outcome} "
         f"entry_slip={trade.get('entry_slippage_pts', 0):+.2f}pts "
         f"wh_lat={trade.get('webhook_latency_ms', 0):.0f}ms")

# ════════════════════════════════════════════════════════════════════════
# OPEN TRADE STATE
# ════════════════════════════════════════════════════════════════════════
def _set_open_trade(
    trade_id: str, direction: str, fill_price: float, sl_dist: float,
    pine_entry_px: float, pine_tp1: float, pine_tp2: float, pine_sl: float,
    sl_oid: Optional[str], tp2_oid: Optional[str],
    pine_signal_time: int, webhook_recv_time: float, entry_fill_time: float,
    signal_timeframe: str = "", signal_tf_bar_time: int = 0,
):
    global open_trade
    d = direction

    sl_price  = round(fill_price - sl_dist, 1) if d == "BUY" else round(fill_price + sl_dist, 1)
    tp1_price = round(fill_price + sl_dist * TP1_R, 1) if d == "BUY" else round(fill_price - sl_dist * TP1_R, 1)
    tp2_price = round(fill_price + sl_dist * TP2_R, 1) if d == "BUY" else round(fill_price - sl_dist * TP2_R, 1)

    entry_slippage  = round(fill_price - pine_entry_px, 2) if d == "BUY" else round(pine_entry_px - fill_price, 2)
    wh_latency_ms   = round((webhook_recv_time - pine_signal_time / 1000) * 1000, 1)
    entry_latency_ms = round((entry_fill_time - webhook_recv_time) * 1000, 1)

    open_trade = {
        "trade_id":              trade_id,
        "direction":             d,
        "mode":                  "PAPER" if PAPER_MODE else "LIVE",
        "state":                 STATE_ENTERED,
        # Signal context
        "signal_timeframe":      signal_timeframe,
        "signal_tf_bar_time":    signal_tf_bar_time,
        # Monitor cycle counters (telemetry only — no execution impact)
        "monitor_cycles":        0,
        "monitor_cycles_to_tp1": None,
        # Prices
        "fill_price":            fill_price,
        "pine_entry_px":         pine_entry_px,
        "sl_dist":               sl_dist,
        "sl_price":              sl_price,         # used by monitor — stays at original
        "original_sl_price":     sl_price,         # immutable reference
        "tp1_price":             tp1_price,
        "tp2_price":             tp2_price,
        "pine_tp1":              pine_tp1,
        "pine_tp2":              pine_tp2,
        "pine_sl":               pine_sl,
        # Orders
        "sl_oid":                sl_oid,
        "tp2_oid":               tp2_oid,
        # Sizing
        "remaining_size":        LOT_SIZE,
        # TP1 state (dedup guard + telemetry)
        "tp1_hit":               False,
        "tp1_fill_px":           None,
        "tp1_time":              None,
        # Timestamps & latency
        "entry_slippage_pts":    entry_slippage,
        "webhook_latency_ms":    wh_latency_ms,
        "entry_latency_ms":      entry_latency_ms,
        "pine_signal_time":      pine_signal_time,
        "webhook_recv_time":     webhook_recv_time,
        "entry_fill_time":       entry_fill_time,
        # Pine expected outcome (updated if Pine sends signal via webhook)
        "pine_expected_outcome": "UNKNOWN",
        # Model B simulation state (analytics only — never executed)
        # Model B: full position, no TP1, exits at fill ± sl_dist×2R or original SL
        "sim_b_done":         False,
        "sim_b_outcome":      None,
        "sim_b_exit_px":      None,
        "sim_b_exit_time":    None,
        "sim_b_duration_sec": None,
        # Intrabar timing telemetry (analytics only)
        # Epoch timestamps used for ms-precision deltas; ISO for CSV readability
        "tp1_first_touch_epoch": None,   # time.time() when monitor first saw price >= tp1
        "tp1_first_touch_time":  None,   # ISO
        "tp2_first_touch_epoch": None,   # time.time() when monitor first saw price >= tp2
        "tp2_first_touch_time":  None,   # ISO
        "tp1_wh_recv_epoch":     None,   # time.time() when TP1_HIT webhook arrived
        "tp2_wh_recv_epoch":     None,   # time.time() when TP2_HIT webhook arrived
        # Recovery tracking — False/empty for normal entries; set in startup() on recovery
        "recovery_event":        False,
        "recovery_reason":       "",
    }

    # Entry quality — computed once at entry, stored for /status and close summary
    _ratio = round(abs(entry_slippage) / sl_dist, 3) if sl_dist > 0 else 0.0
    _grade = _structure_grade(_ratio)
    open_trade["slippage_ratio"]   = _ratio
    open_trade["structure_grade"]  = _grade

    save_state()

    _log(
        f"STATE→ENTERED | {d} fill={fill_price} slip={entry_slippage:+.2f}pts "
        f"wh_lat={wh_latency_ms:.0f}ms entry_lat={entry_latency_ms:.0f}ms "
        f"sl={sl_price} tp1={tp1_price} tp2={tp2_price} "
        f"mode={'PAPER' if PAPER_MODE else 'LIVE'}"
    )

    # Runtime structure warning — emitted immediately after every entry
    if _ratio >= 1.5:
        _logw(f"[STRUCTURE CRITICAL] slippage_ratio={_ratio:.3f} grade=CRITICAL | "
              f"slippage ({abs(entry_slippage):.1f}pts) is {_ratio:.2f}× sl_dist ({sl_dist:.1f}pts) — "
              f"trade structure severely compromised at entry")
    elif _ratio >= 1.0:
        _logw(f"[STRUCTURE BROKEN] slippage_ratio={_ratio:.3f} grade=BROKEN | "
              f"slippage ({abs(entry_slippage):.1f}pts) exceeds sl_dist ({sl_dist:.1f}pts) — "
              f"TP2 requires market to travel beyond intended range from fill")
    elif _ratio >= 0.5:
        _logw(f"[STRUCTURE DEGRADED] slippage_ratio={_ratio:.3f} grade=DEGRADED | "
              f"slippage consumed {_ratio*100:.0f}% of sl_dist — trade entered with reduced margin")
    elif _ratio >= 0.25:
        _log(f"[STRUCTURE MILD] slippage_ratio={_ratio:.3f} grade=MILD | "
             f"slippage within acceptable range")
    else:
        _log(f"[STRUCTURE INTACT] slippage_ratio={_ratio:.3f} grade=INTACT | "
             f"entry quality clean")

    tg(
        f"{'📄 PAPER' if PAPER_MODE else '🟢 LIVE'} <b>{d} ENTERED</b>\n"
        f"Fill: <b>{fill_price:,.1f}</b> | Slip: {entry_slippage:+.2f}pts\n"
        f"SL: {sl_price:,.1f} | TP1: {tp1_price:,.1f} | TP2: {tp2_price:,.1f}\n"
        f"WH latency: {wh_latency_ms:.0f}ms | Entry latency: {entry_latency_ms:.0f}ms\n"
        f"Structure: <b>{_grade}</b> (ratio={_ratio:.3f})"
    )

# ════════════════════════════════════════════════════════════════════════
# CLOSE TRADE  (final exit — writes CSV + reconciliation)
# ════════════════════════════════════════════════════════════════════════
def _close_trade(exit_price: float, exit_type: str, exit_slippage: float = 0.0):
    global open_trade
    if not open_trade:
        return

    trade    = open_trade
    d        = trade["direction"]
    entry_px = trade["fill_price"]
    tp1_hit  = trade.get("tp1_hit", False)
    tp1_fill = trade.get("tp1_fill_px") or entry_px

    blended_pts        = _calc_blended_pts(d, entry_px, tp1_fill, exit_price, tp1_hit)
    blended_pnl_approx = round(blended_pts * LOT_SIZE, 4)

    trade_duration_sec  = round(time.time() - trade.get("entry_fill_time", time.time()), 1)
    monitor_cycles_tot  = trade.get("monitor_cycles", 0)
    monitor_cycles_tp1  = trade.get("monitor_cycles_to_tp1", "")

    # ── Model B simulation resolution ──────────────────────────────────
    # If sim_b resolved in the monitor loop, use those values.
    # If trade closed (e.g. via webhook) before monitor detected sim_b level,
    # fall back to the actual exit: same TP/SL levels so outcome is equivalent.
    if trade.get("sim_b_done"):
        sim_b_outcome      = trade["sim_b_outcome"]
        sim_b_exit_px      = trade["sim_b_exit_px"]
        sim_b_exit_time    = trade["sim_b_exit_time"]
        sim_b_duration_sec = trade["sim_b_duration_sec"]
    else:
        # Trade closed before sim_b resolved in the poll loop (e.g. TP2_WEBHOOK)
        # Infer: if actual exit was TP type → sim_b would also have TP'd (same level)
        sim_b_exit_px      = exit_price
        sim_b_exit_time    = datetime.now().isoformat()
        sim_b_duration_sec = trade_duration_sec
        sim_b_outcome      = "TP_FULL" if "TP" in exit_type else "SL_FULL"
        _log(f"[SIM_B] Resolved at close (undetected in loop): {sim_b_outcome} @ {sim_b_exit_px}")

    # sim_b_pts: full-position, no TP1 — entry to sim_b_exit_px
    if d == "BUY":
        sim_b_pts = round(sim_b_exit_px - entry_px, 2)
    else:
        sim_b_pts = round(entry_px - sim_b_exit_px, 2)
    comparison_delta_pts = round(sim_b_pts - blended_pts, 2)

    _log(f"[SIM_B] outcome={sim_b_outcome} pts={sim_b_pts:+.2f} "
         f"vs actual={blended_pts:+.2f} delta={comparison_delta_pts:+.2f}pts")

    # ── Intrabar timing telemetry computation ──────────────────────────
    close_epoch = time.time()

    tp1_touch_epoch = trade.get("tp1_first_touch_epoch")
    tp2_touch_epoch = trade.get("tp2_first_touch_epoch")
    tp1_wh_epoch    = trade.get("tp1_wh_recv_epoch")
    tp2_wh_epoch    = trade.get("tp2_wh_recv_epoch")

    # ms from monitor's first touch to Pine's webhook arrival (positive = monitor was first)
    tp1_touch_to_webhook_ms = (
        round((tp1_wh_epoch - tp1_touch_epoch) * 1000, 1)
        if tp1_touch_epoch and tp1_wh_epoch else ""
    )
    tp2_touch_to_webhook_ms = (
        round((tp2_wh_epoch - tp2_touch_epoch) * 1000, 1)
        if tp2_touch_epoch and tp2_wh_epoch else ""
    )

    # ms from monitor's first touch to trade close
    tp1_touch_to_close_ms = (
        round((close_epoch - tp1_touch_epoch) * 1000, 1)
        if tp1_touch_epoch else ""
    )
    tp2_touch_to_close_ms = (
        round((close_epoch - tp2_touch_epoch) * 1000, 1)
        if tp2_touch_epoch else ""
    )

    intrabar_touch_detected = bool(tp1_touch_epoch or tp2_touch_epoch)

    # Reversal signal: monitor detected TP2 (price touched the level) but Pine
    # never sent TP2_HIT webhook → price reversed before candle close
    intrabar_reversal_before_close = bool(tp2_touch_epoch and not tp2_wh_epoch)

    _log(
        f"[INTRABAR] tp1_touch={trade.get('tp1_first_touch_time','—')} "
        f"tp2_touch={trade.get('tp2_first_touch_time','—')} "
        f"tp1_wh_delta={tp1_touch_to_webhook_ms}ms "
        f"tp2_wh_delta={tp2_touch_to_webhook_ms}ms "
        f"reversal={intrabar_reversal_before_close}"
    )

    # ── Entry quality metrics ───────────────────────────────────────────
    sl_dist          = trade.get("sl_dist", 0) or 1   # guard against zero
    slippage_ratio   = trade.get("slippage_ratio",  round(abs(trade.get("entry_slippage_pts", 0)) / sl_dist, 3))
    structure_grade  = trade.get("structure_grade", _structure_grade(slippage_ratio))

    # bot_tp2_reached: did monitor ever detect price at bot's recalculated TP2?
    # Derived from intrabar telemetry — tp2_first_touch_epoch set when monitor
    # first saw price cross tp2_price (regardless of what closed the trade)
    bot_tp2_reached  = bool(trade.get("tp2_first_touch_epoch"))

    # captured_vs_intended_pct: blended_pts as % of theoretical max (TP1+TP2 model)
    # Theoretical max = 0.5×1R + 0.5×2R = 1.5×sl_dist
    # Negative values indicate losses (SL trades)
    intended_max     = sl_dist * 1.5
    captured_vs_intended_pct = round(blended_pts / intended_max * 100, 1) if intended_max > 0 else 0.0

    # ── Execution authority derivation ─────────────────────────────────
    # Derives who made each exit decision from stored source fields.
    # tp1_exec_source: set in _execute_tp1 ("MONITOR_PAPER", "MONITOR_LIVE", "WEBHOOK")
    # exit_type: encodes final exit source ("TP2_PAPER"/"SL_PAPER" vs "TP2_WEBHOOK")
    tp1_exec_source  = trade.get("tp1_exec_source", "")
    tp1_by_monitor   = "MONITOR" in tp1_exec_source
    tp1_by_pine      = "WEBHOOK" in tp1_exec_source
    final_by_pine    = "WEBHOOK" in exit_type

    if not tp1_hit:
        # Single exit decision only
        execution_authority = "PINE" if final_by_pine else "MONITOR"
    else:
        if tp1_by_monitor and not final_by_pine:
            execution_authority = "MONITOR"
        elif tp1_by_pine and final_by_pine:
            execution_authority = "PINE"
        else:
            execution_authority = "HYBRID"  # TP1 from one source, final from other

    lifecycle_source = (
        "MONITOR_ONLY"    if execution_authority == "MONITOR" else
        "PINE_CONFIRMED"  if execution_authority == "PINE"    else
        "HYBRID"
    )

    _log(f"[AUTHORITY] execution_authority={execution_authority} "
         f"lifecycle_source={lifecycle_source} "
         f"tp1_src={tp1_exec_source or 'N/A'} exit_type={exit_type}")

    # Determine Python's actual outcome label
    if tp1_hit:
        python_outcome = "TP1 + TP2" if "TP2" in exit_type else "TP1 + SL"
    else:
        python_outcome = "TP2" if "TP2" in exit_type else "SL X"

    # TP1 pts on 50% leg
    if tp1_hit:
        tp1_pts_50 = round(((tp1_fill - entry_px) if d == "BUY" else (entry_px - tp1_fill)) * 0.5, 2)
    else:
        tp1_pts_50 = ""

    # Write main CSV
    row = {
        "trade_id":               trade["trade_id"],
        "direction":              d,
        "mode":                   trade.get("mode", "?"),
        "signal_timeframe":       trade.get("signal_timeframe", ""),
        "signal_tf_bar_time":     trade.get("signal_tf_bar_time", ""),
        "pine_entry_px":          trade.get("pine_entry_px", ""),
        "fill_price":             entry_px,
        "entry_slippage_pts":     trade.get("entry_slippage_pts", ""),
        "pine_tp1":               trade.get("pine_tp1", ""),
        "pine_tp2":               trade.get("pine_tp2", ""),
        "pine_sl":                trade.get("pine_sl", ""),
        "pine_signal_time":       trade.get("pine_signal_time", ""),
        "webhook_recv_time":      trade.get("webhook_recv_time", ""),
        "entry_fill_time":        trade.get("entry_fill_time", ""),
        "webhook_latency_ms":     trade.get("webhook_latency_ms", ""),
        "entry_latency_ms":       trade.get("entry_latency_ms", ""),
        "tp1_hit":                tp1_hit,
        "tp1_fill_px":            trade.get("tp1_fill_px", ""),
        "tp1_time":               trade.get("tp1_time", ""),
        "tp1_pts_50pct":          tp1_pts_50,
        "exit_price":             exit_price,
        "exit_time":              datetime.now().isoformat(),
        "exit_type":              exit_type,
        "exit_slippage_pts":      exit_slippage,
        "blended_pts":            blended_pts,
        "blended_pnl_approx":     blended_pnl_approx,
        "pine_expected_outcome":      trade.get("pine_expected_outcome", "UNKNOWN"),
        "python_actual_outcome":      python_outcome,
        "lifecycle_match":            trade.get("pine_expected_outcome", "UNKNOWN") == python_outcome,
        "slippage_ratio":             slippage_ratio,
        "structure_grade":            structure_grade,
        "bot_tp2_reached":            bot_tp2_reached,
        "captured_vs_intended_pct":   captured_vs_intended_pct,
        "execution_authority":        execution_authority,
        "lifecycle_source":           lifecycle_source,
        "trade_duration_sec":     trade_duration_sec,
        "monitor_cycles_to_tp1":  monitor_cycles_tp1,
        "monitor_cycles_total":   monitor_cycles_tot,
        # Model B simulation
        "sim_b_outcome":          sim_b_outcome,
        "sim_b_exit_price":       sim_b_exit_px,
        "sim_b_exit_time":        sim_b_exit_time,
        "sim_b_pts":              sim_b_pts,
        "sim_b_duration_sec":     sim_b_duration_sec,
        "comparison_delta_pts":   comparison_delta_pts,
        # Intrabar timing telemetry
        "tp1_first_touch_time":          trade.get("tp1_first_touch_time", ""),
        "tp2_first_touch_time":          trade.get("tp2_first_touch_time", ""),
        "tp1_touch_to_webhook_ms":       tp1_touch_to_webhook_ms,
        "tp2_touch_to_webhook_ms":       tp2_touch_to_webhook_ms,
        "tp1_touch_to_close_ms":         tp1_touch_to_close_ms,
        "tp2_touch_to_close_ms":         tp2_touch_to_close_ms,
        "intrabar_touch_detected":       intrabar_touch_detected,
        "intrabar_reversal_before_close": intrabar_reversal_before_close,
        # Recovery tracking
        "recovery_event":                trade.get("recovery_event",  False),
        "recovery_reason":               trade.get("recovery_reason", ""),
    }
    _append_csv(CSV_FILE, CSV_HEADERS, row)
    _write_reconciliation(trade, python_outcome, exit_slippage, trade_duration_sec)

    emoji = "✅" if blended_pts > 0 else "🔴" if blended_pts < 0 else "⚪"
    _log(f"STATE→CLOSED | {d} exit={exit_price} blended={blended_pts:+.2f}pts "
         f"outcome={python_outcome} via {exit_type}")

    # ── Rolling operational summary (printed after every completed trade) ──
    _log(
        f"[TRADE SUMMARY] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    _log(f"[TRADE SUMMARY]  direction          : {d}")
    _log(f"[TRADE SUMMARY]  slippage_ratio     : {slippage_ratio:.3f}  →  {structure_grade}")
    _log(f"[TRADE SUMMARY]  execution_authority: {execution_authority}  ({lifecycle_source})")
    _log(f"[TRADE SUMMARY]  bot_tp2_reached    : {bot_tp2_reached}")
    _log(f"[TRADE SUMMARY]  captured_vs_intend : {captured_vs_intended_pct:+.1f}%  (blended={blended_pts:+.2f}pts, max={round(sl_dist*1.5,1)}pts)")
    _log(f"[TRADE SUMMARY]  trade_duration_sec : {trade_duration_sec}s")
    _log(f"[TRADE SUMMARY]  outcome            : {python_outcome}")
    _log(
        f"[TRADE SUMMARY] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )

    tg(
        f"{emoji} <b>{d} CLOSED</b> [{exit_type}]\n"
        f"Exit: {exit_price:,.1f} | Blended PnL: <b>{blended_pts:+.2f}pts</b>\n"
        f"TP1 hit: {'✅ Yes' if tp1_hit else 'No'} | Outcome: {python_outcome}\n"
        f"Structure: {structure_grade} (ratio={slippage_ratio:.3f}) | "
        f"Captured: {captured_vs_intended_pct:+.1f}% of intended"
    )

    # ── Atomic close — crash-safe two-step state transition ────────────
    # Step 1: mark as CLOSED in state.json FIRST.
    #         If process crashes here, load_state() sees STATE_CLOSED
    #         and skips recovery → no ghost trade on restart.
    # Step 2: clear open_trade and write IDLE.
    #         Normal completion path.
    open_trade["state"] = STATE_CLOSED
    save_state()      # state.json = STATE_CLOSED (safe to crash here)
    open_trade = None
    save_state()      # state.json = STATE_IDLE (clean)

# ════════════════════════════════════════════════════════════════════════
# TP1 PARTIAL EXIT
# Executes 50% close. Original SL price NEVER changes.
# Duplicate-safe: tp1_hit flag checked before execution.
# Called by: position monitor (primary) or TP1_HIT webhook (secondary).
# ════════════════════════════════════════════════════════════════════════
def _execute_tp1(source: str = "MONITOR"):
    global open_trade
    if not open_trade:
        _logw(f"TP1 [{source}] — no open trade")
        return
    if open_trade.get("tp1_hit"):
        _logw(f"TP1 [{source}] — already executed (dedup guard active)")
        return

    d           = open_trade["direction"]
    tp1_price   = open_trade["tp1_price"]
    orig_sl     = open_trade["original_sl_price"]   # immutable
    tp2_price   = open_trade["tp2_price"]
    close_side  = "sell" if d == "BUY" else "buy"

    # Set dedup flag + state IMMEDIATELY (before any network call)
    # remaining_size is set ONLY after confirming partial close succeeded (live)
    open_trade["tp1_hit"]         = True
    open_trade["state"]           = STATE_TP1_DONE
    open_trade["tp1_time"]        = datetime.now().isoformat()
    open_trade["tp1_exec_source"] = source   # "MONITOR_PAPER/LIVE" or "WEBHOOK"

    tp1_fill_px = None

    if PAPER_MODE:
        tp1_fill_px = fetch_price() or tp1_price
        open_trade["remaining_size"] = PARTIAL_LOT   # paper always succeeds
        _log(f"[PAPER] TP1 partial close @ {tp1_fill_px} (50% of {LOT_SIZE})")
    else:
        # Live: execute 50% market close (reduce_only)
        result = place_market_order(close_side, PARTIAL_LOT, reduce_only=True)
        if result:
            tp1_fill_px = result.get("fill_price") or tp1_price
            open_trade["remaining_size"] = PARTIAL_LOT   # confirmed: 50% closed
            _log(f"[LIVE] TP1 partial fill @ {tp1_fill_px} size={PARTIAL_LOT}")

            # ── Resize orders: place NEW orders FIRST, then cancel OLD ones ──
            # This ensures position is NEVER left without SL protection during
            # the cancel/replace window. Belt-and-suspenders order of operations.
            old_sl_oid  = open_trade.get("sl_oid")
            old_tp2_oid = open_trade.get("tp2_oid")

            new_sl_oid  = place_sl_order (close_side, PARTIAL_LOT, orig_sl)
            new_tp2_oid = place_tp2_order(close_side, PARTIAL_LOT, tp2_price)

            # Cancel old full-size orders AFTER new partial orders are live
            if old_sl_oid:
                cancel_order(old_sl_oid)
            if old_tp2_oid:
                cancel_order(old_tp2_oid)

            open_trade["sl_oid"]  = new_sl_oid
            open_trade["tp2_oid"] = new_tp2_oid
            _log(f"[LIVE] Resized orders — SL oid={new_sl_oid} @ {orig_sl} | "
                 f"TP2 oid={new_tp2_oid} @ {tp2_price} (both size={PARTIAL_LOT})")
        else:
            # ── CRITICAL: Partial close FAILED ──
            # Do NOT cancel existing full-size orders — position is still full size.
            # Do NOT reduce remaining_size — position has not changed.
            # Keep existing SL/TP2 orders fully active to protect full position.
            open_trade["remaining_size"] = LOT_SIZE   # not reduced — partial failed
            _loge("TP1 partial close FAILED — existing full-size SL/TP2 orders PRESERVED")
            tg(f"🚨 TP1 PARTIAL FAILED [{d}] @ {tp1_price}\n"
               f"Full-size SL/TP2 still active. Manual check recommended on Delta UI.")
            tp1_fill_px = tp1_price  # fallback for telemetry only

    open_trade["tp1_fill_px"] = tp1_fill_px
    save_state()

    tp1_pts = round(
        ((tp1_fill_px - open_trade["fill_price"]) if d == "BUY"
         else (open_trade["fill_price"] - tp1_fill_px)) * 0.5, 2
    )
    _log(f"STATE→TP1_DONE | fill={tp1_fill_px} 50pct_pts={tp1_pts:+.2f} source={source}")
    tg(
        f"🔵 <b>TP1 HIT</b> [{source}] {'📄' if PAPER_MODE else '🟢'}\n"
        f"50% closed @ {tp1_fill_px:,.1f} | Pts on 50%: {tp1_pts:+.2f}\n"
        f"Remaining 50% → SL: {orig_sl:,.1f} (unchanged) | TP2: {tp2_price:,.1f}"
    )

# ════════════════════════════════════════════════════════════════════════
# POSITION MONITOR  (Python lifecycle authority)
# Polls price every PRICE_INTERVAL seconds.
# PAPER : full simulation of TP1/TP2/SL via price comparison.
# LIVE  : price-based TP1 detection + position-flat detection for SL/TP2.
# No trailing. No BE movement. original_sl_price is immutable.
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

            d       = open_trade["direction"]
            sl      = open_trade["original_sl_price"]   # never changes
            tp1     = open_trade["tp1_price"]
            tp2     = open_trade["tp2_price"]
            tp1_hit = open_trade.get("tp1_hit", False)

            price = fetch_price()
            if not price:
                _logw("[MON] price fetch failed — skipping tick")
                continue

            # Increment monitor cycle counter (telemetry only)
            open_trade["monitor_cycles"] = open_trade.get("monitor_cycles", 0) + 1

            if PAPER_MODE:
                # ── TP1: 50% partial close ──
                if not tp1_hit:
                    hit = (d == "BUY" and price >= tp1) or (d == "SELL" and price <= tp1)
                    if hit:
                        # Record first touch before execution (intrabar telemetry)
                        if not open_trade.get("tp1_first_touch_epoch"):
                            _t = time.time()
                            open_trade["tp1_first_touch_epoch"] = _t
                            open_trade["tp1_first_touch_time"]  = datetime.fromtimestamp(_t).isoformat()
                            _log(f"[INTRABAR] tp1_first_touch @ {price} (monitor)")
                        open_trade["monitor_cycles_to_tp1"] = open_trade["monitor_cycles"]
                        _execute_tp1(source="MONITOR_PAPER")
                        tp1_hit = True
                        continue   # re-evaluate remaining 50% on next tick

                # ── TP2: close remaining 50% ──
                hit_tp2 = (d == "BUY" and price >= tp2) or (d == "SELL" and price <= tp2)
                if hit_tp2:
                    # Record first touch before close (intrabar telemetry)
                    if not open_trade.get("tp2_first_touch_epoch"):
                        _t = time.time()
                        open_trade["tp2_first_touch_epoch"] = _t
                        open_trade["tp2_first_touch_time"]  = datetime.fromtimestamp(_t).isoformat()
                        _log(f"[INTRABAR] tp2_first_touch @ {price} (monitor)")
                    slip = round(price - tp2, 2) if d == "BUY" else round(tp2 - price, 2)
                    _log(f"[PAPER] TP2 hit price={price} tp2={tp2} slip={slip:+.2f}")
                    _close_trade(tp2, "TP2_PAPER", slip)
                    break

                # ── SL: close remaining (original SL — never moved) ──
                hit_sl = (d == "BUY" and price <= sl) or (d == "SELL" and price >= sl)
                if hit_sl:
                    etype = "TP1_SL_PAPER" if tp1_hit else "SL_PAPER"
                    slip  = round(sl - price, 2) if d == "BUY" else round(price - sl, 2)
                    _log(f"[PAPER] SL hit price={price} sl={sl} slip={slip:+.2f}")
                    _close_trade(sl, etype, slip)
                    break

            else:
                # ── LIVE: price-based TP1 detection ──
                if not tp1_hit:
                    hit = (d == "BUY" and price >= tp1) or (d == "SELL" and price <= tp1)
                    if hit:
                        if not open_trade.get("tp1_first_touch_epoch"):
                            _t = time.time()
                            open_trade["tp1_first_touch_epoch"] = _t
                            open_trade["tp1_first_touch_time"]  = datetime.fromtimestamp(_t).isoformat()
                            _log(f"[INTRABAR] tp1_first_touch @ {price} (monitor-live)")
                        open_trade["monitor_cycles_to_tp1"] = open_trade["monitor_cycles"]
                        _execute_tp1(source="MONITOR_LIVE")
                        tp1_hit = True
                        continue

                # ── LIVE: position-flat detection (SL stop or TP2 limit filled on Delta) ──
                pos = get_open_position()
                if pos is None:
                    # Exchange filled one of our reduce_only orders
                    etype  = "TP1_AUTO_EXIT" if tp1_hit else "AUTO_EXIT"
                    exit_px = price
                    _logw(f"[LIVE] Position flat — type={etype} approx_exit={exit_px}")
                    # Quick cancel of any remaining orders (the unfilled counterpart)
                    for oid_key in ("sl_oid", "tp2_oid"):
                        oid = open_trade.get(oid_key)
                        if oid:
                            _delete(f"/v2/orders/{oid}")
                    _close_trade(exit_px, etype, 0.0)
                    break

            # ── Model B simulation (PAPER + LIVE — analytics only, no execution) ──
            # Full-position no-TP1 model: exits at 2R target or original SL.
            # Target and SL levels are identical to bot's tp2/sl — only the lifecycle differs.
            if not open_trade.get("sim_b_done"):
                sim_target = open_trade["tp2_price"]   # same 2R level as bot
                sim_sl     = open_trade["original_sl_price"]
                sim_now    = time.time()
                if (d == "BUY" and price >= sim_target) or (d == "SELL" and price <= sim_target):
                    open_trade["sim_b_done"]         = True
                    open_trade["sim_b_outcome"]      = "TP_FULL"
                    open_trade["sim_b_exit_px"]      = sim_target
                    open_trade["sim_b_exit_time"]    = datetime.now().isoformat()
                    open_trade["sim_b_duration_sec"] = round(sim_now - open_trade["entry_fill_time"], 1)
                    _log(f"[SIM_B] TP_FULL resolved @ {sim_target} (analytics only)")
                elif (d == "BUY" and price <= sim_sl) or (d == "SELL" and price >= sim_sl):
                    open_trade["sim_b_done"]         = True
                    open_trade["sim_b_outcome"]      = "SL_FULL"
                    open_trade["sim_b_exit_px"]      = sim_sl
                    open_trade["sim_b_exit_time"]    = datetime.now().isoformat()
                    open_trade["sim_b_duration_sec"] = round(sim_now - open_trade["entry_fill_time"], 1)
                    _log(f"[SIM_B] SL_FULL resolved @ {sim_sl} (analytics only)")

    log.info("[MON] stopped")

# ════════════════════════════════════════════════════════════════════════
# ENTRY PROCESSOR  (background thread — avoids TradingView 10s timeout)
# ════════════════════════════════════════════════════════════════════════
def _process_entry(
    signal: str, sl_dist: float, pine_entry_px: float,
    pine_tp1: float, pine_tp2: float, pine_sl: float,
    pine_time: int, recv_time: float, trade_id: str,
    signal_timeframe: str = "", signal_tf_bar_time: int = 0,
):
    global open_trade, _entry_processing

    try:
        d = signal  # "BUY" or "SELL"
        _log(f"ENTRY_START | {d} sl_dist={sl_dist} pine_entry={pine_entry_px}")

        fill_px = None
        sl_oid  = tp2_oid = None

        if PAPER_MODE:
            fill_px = fetch_price()
            if not fill_px:
                _loge("PAPER fill: cannot fetch live price — aborting")
                return
            fill_px = round(fill_px, 1)
            _log(f"PAPER fill simulated @ {fill_px}")
        else:
            # Live: place market entry
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

            # Place full-size SL + TP2 orders at entry
            sl_price  = round(fill_px - sl_dist, 1) if d == "BUY" else round(fill_px + sl_dist, 1)
            tp2_price = round(fill_px + sl_dist * TP2_R, 1) if d == "BUY" else round(fill_px - sl_dist * TP2_R, 1)
            close_side = "sell" if d == "BUY" else "buy"
            sl_oid  = place_sl_order (close_side, LOT_SIZE, sl_price)
            tp2_oid = place_tp2_order(close_side, LOT_SIZE, tp2_price)

            if not sl_oid:
                _loge("SL ORDER FAILED after entry — CRITICAL: close position manually")
                tg(f"🚨 SL ORDER FAILED after {d} entry @ {fill_px} — CLOSE POSITION MANUALLY ON DELTA")

        entry_fill_time = time.time()

        with _state_lock:
            _set_open_trade(
                trade_id=trade_id, direction=d, fill_price=fill_px,
                sl_dist=sl_dist, pine_entry_px=pine_entry_px,
                pine_tp1=pine_tp1, pine_tp2=pine_tp2, pine_sl=pine_sl,
                sl_oid=sl_oid, tp2_oid=tp2_oid,
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
app = FastAPI(title="Vol Surge Bot v3.1 — Live Validation")

@app.on_event("startup")
async def startup():
    global open_trade, _preflight_ok
    # ── Lot size validity check (for startup log) ──
    _partial_valid = PARTIAL_LOT >= DELTA_MIN_SIZE_BTC
    _partial_status = (
        f"✅ {PARTIAL_LOT:.6f} BTC >= {DELTA_MIN_SIZE_BTC} BTC minimum"
        if _partial_valid
        else f"❌ {PARTIAL_LOT:.6f} BTC < {DELTA_MIN_SIZE_BTC} BTC minimum — "
             f"TP1 partial close WILL FAIL. Set LOT_SIZE=0.002 in .env."
    )

    log.info("=" * 70)
    log.info(f"  Vol Surge v3.1 | {'📄 PAPER' if PAPER_MODE else '🟢 *** LIVE ***'} mode")
    log.info(f"  Endpoint      : {BASE_URL}")
    log.info(f"  Product       : BTCUSD Perpetual (ID={PRODUCT_ID})")
    log.info(f"  ─── Lot sizing ──────────────────────────────────────────")
    log.info(f"  LOT_SIZE      : {LOT_SIZE:.6f} BTC  (full position)")
    log.info(f"  PARTIAL_LOT   : {PARTIAL_LOT:.6f} BTC  (50% close at TP1)")
    log.info(f"  Delta minimum : {DELTA_MIN_SIZE_BTC:.6f} BTC  (confirmed BTCUSD Perpetual)")
    log.info(f"  Partial valid : {_partial_status}")
    log.info(f"  ─────────────────────────────────────────────────────────")
    log.info(f"  Creds         : {'SET ✓' if API_KEY else '⚠️  MISSING'}")
    log.info(f"  TP1 model     : 50% partial close | SL immutable | No trail")
    log.info(f"  ─── Exit authority ──────────────────────────────────────")
    if USE_PINE_EXIT_WEBHOOKS:
        log.info(f"  Exit authority: HYBRID — Pine TP1/TP2 webhooks CAN execute exits")
        log.info(f"                  Set USE_PINE_EXIT_WEBHOOKS=false for monitor-only")
    else:
        log.info(f"  Exit authority: MONITOR_ONLY — Pine TP/SL webhooks are TELEMETRY ONLY")
        log.info(f"                  Monitor is sole execution authority for all exits")
    log.info(f"  ─────────────────────────────────────────────────────────")
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
    # load_state() already logged full trade detail if a trade was found.
    recovered = load_state()
    if recovered:
        open_trade = recovered

        # ── Stamp recovery metadata ──────────────────────────────────────
        # recovery_event = True on every resumed trade (filters CSV for analysis).
        # recovery_reason derived from saved state:
        #   _shutdown_at present  → SIGTERM fired before stop (Railway redeploy / manual restart)
        #   _shutdown_at absent   → CRASH (process killed without SIGTERM handler running)
        open_trade["recovery_event"] = True
        _shutdown_at_val = open_trade.pop("_shutdown_at", None)  # capture before clearing
        if _shutdown_at_val:
            open_trade["recovery_reason"] = "SIGTERM"
        else:
            open_trade["recovery_reason"] = "CRASH"
        save_state()   # persist recovery metadata so it survives a second restart

        log.warning(
            f"[RECOVERY] recovery_event=True  recovery_reason={open_trade['recovery_reason']}"
            + (f"  (_shutdown_at={_shutdown_at_val})" if _shutdown_at_val else "  (no _shutdown_at — crash recovery)")
        )

        tid  = open_trade.get("trade_id", "?")
        st   = open_trade.get("state", "?")
        d    = open_trade.get("direction", "?")
        tp1h = open_trade.get("tp1_hit", False)

        if PAPER_MODE:
            # Paper: no exchange to verify — just resume the monitor
            log.warning(f"[RECOVERY] PAPER mode — resuming monitor for {tid} "
                        f"(state={st} tp1_hit={tp1h})")
            tg(
                f"♻️ <b>RECOVERED (PAPER)</b>\n"
                f"Trade <b>{tid}</b> [{d}] | State: {st}\n"
                f"TP1 hit: {'Yes ✅' if tp1h else 'No'} | Monitor restarting"
            )
            threading.Thread(target=_position_monitor, daemon=True,
                             name="mon-recovery").start()
        else:
            # Live: verify position still exists on exchange before resuming.
            # If the exchange closed the position while bot was offline
            # (margin call, manual close, exchange maintenance), resuming the
            # monitor would track a ghost trade and produce a corrupt CSV row.
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
                # _preflight_ok stays False — operator must hit /preflight
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
                    f"TP1 hit: {'Yes ✅' if tp1h else 'No'}\n"
                    f"Position confirmed on Delta | Monitor restarting"
                )
                _preflight_ok = True
                # CRITICAL: preflight failed the stale-state check — now that we've
                # verified the trade legitimately, unblock. Operator should hit
                # /preflight after this trade closes to re-validate for the next session.
                log.info("[RECOVERY] _preflight_ok restored — "
                         "run /preflight after this trade closes to re-validate")
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
        # LIVE: fail-loud — block if preflight not passed
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
            pine_tp1           = float(data.get("tp1", pine_entry_px + sl_dist))
            pine_tp2           = float(data.get("tp2", pine_entry_px + sl_dist * 2))
            pine_sl            = float(data.get("sl",  pine_entry_px - sl_dist))
            signal_timeframe   = str(data.get("timeframe", ""))
            signal_tf_bar_time = int(data.get("bar_time", 0))
            trade_id           = f"{signal[0]}{int(recv_time * 1000)}"
            _entry_processing  = True

        threading.Thread(
            target=_process_entry,
            args=(signal, sl_dist, pine_entry_px, pine_tp1, pine_tp2, pine_sl,
                  pine_time, recv_time, trade_id, signal_timeframe, signal_tf_bar_time),
            daemon=True, name=f"entry-{trade_id}",
        ).start()

        return JSONResponse({
            "status":     "accepted",
            "trade_id":   trade_id,
            "mode":       "PAPER" if PAPER_MODE else "LIVE",
            "latency_ms": latency,
        })

    # ── TP1_HIT (Pine candle-close confirmation) ──
    elif signal == "TP1_HIT":
        direction = str(data.get("direction", "")).upper()
        tp1_price = float(data.get("tp1_price", 0))   # v5 Pine format (was be_price)
        with _state_lock:
            if open_trade and open_trade["direction"] == direction:
                # Always stamp arrival time for intrabar telemetry regardless of authority
                if not open_trade.get("tp1_wh_recv_epoch"):
                    open_trade["tp1_wh_recv_epoch"] = recv_time

                if not USE_PINE_EXIT_WEBHOOKS:
                    # MONITOR_ONLY mode — Pine alert is telemetry only, never executes
                    already = "already executed" if open_trade.get("tp1_hit") else "pending monitor"
                    _log(f"TP1_HIT webhook [TELEMETRY ONLY] tp1_price={tp1_price} "
                         f"tp1_status={already} | USE_PINE_EXIT_WEBHOOKS=false")
                else:
                    # HYBRID mode — Pine webhook can execute if monitor hasn't already
                    if not open_trade.get("tp1_hit"):
                        _execute_tp1(source="WEBHOOK")
                    else:
                        _log("TP1_HIT webhook — already executed by monitor (dedup OK)")
            else:
                _logw(f"TP1_HIT webhook — no matching open trade (dir={direction})")
        authority_mode = "TELEMETRY" if not USE_PINE_EXIT_WEBHOOKS else "HYBRID"
        return JSONResponse({"status": "ok", "tp1_price": tp1_price,
                             "source": "webhook", "authority": authority_mode})

    # ── TP2_HIT (Pine candle-close confirmation) ──
    elif signal == "TP2_HIT":
        direction = str(data.get("direction", "")).upper()
        tp2_price = float(data.get("tp2_price", 0))
        with _state_lock:
            if open_trade and open_trade["direction"] == direction:
                # Always stamp arrival time for intrabar telemetry regardless of authority
                if not open_trade.get("tp2_wh_recv_epoch"):
                    open_trade["tp2_wh_recv_epoch"] = recv_time

                if not USE_PINE_EXIT_WEBHOOKS:
                    # MONITOR_ONLY mode — Pine alert is telemetry only, never executes
                    _log(f"TP2_HIT webhook [TELEMETRY ONLY] tp2_price={tp2_price} "
                         f"| USE_PINE_EXIT_WEBHOOKS=false — monitor will close at bot's tp2")
                else:
                    # HYBRID mode — Pine webhook executes the TP2 exit
                    if not PAPER_MODE:
                        sl_oid = open_trade.get("sl_oid")
                        if sl_oid:
                            _delete(f"/v2/orders/{sl_oid}")   # quick cancel, no retry needed
                    _close_trade(tp2_price, "TP2_WEBHOOK")
            else:
                _logw(f"TP2_HIT webhook — no matching open trade (dir={direction})")
        authority_mode = "TELEMETRY" if not USE_PINE_EXIT_WEBHOOKS else "HYBRID"
        return JSONResponse({"status": "ok", "tp2_price": tp2_price,
                             "authority": authority_mode})

    return JSONResponse({"status": "ignored", "signal": signal})

# ── / and /health ─────────────────────────────────────────────────────
@app.get("/")
@app.get("/health")
async def health():
    price = fetch_price()
    return JSONResponse({
        "status":              "healthy",
        "bot":                 "Vol Surge v3.1",
        "mode":                "PAPER" if PAPER_MODE else "LIVE",
        "preflight_ok":        _preflight_ok,
        "price_ok":            price is not None,
        "price":               price,
        "creds_ok":            bool(API_KEY and API_SECRET),
        "lot_size_btc":        LOT_SIZE,
        "partial_lot_btc":     PARTIAL_LOT,
        "delta_min_btc":       DELTA_MIN_SIZE_BTC,
        "partial_lot_valid":   PARTIAL_LOT >= DELTA_MIN_SIZE_BTC,
        "timestamp":           datetime.now().isoformat(),
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
        "bot":              "Vol Surge v3.1",
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
async def preflight():
    """Re-run pre-flight checks. In LIVE mode, bot unblocks if all pass."""
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
        with open(CSV_FILE, "r") as f:
            trades = list(csv.DictReader(f))
        return JSONResponse({"count": len(trades), "trades": trades[-20:]})
    except Exception:
        return JSONResponse({"count": 0, "trades": []})

# ── /reconciliation ───────────────────────────────────────────────────
@app.get("/reconciliation")
async def reconciliation():
    """Show Pine expected vs Python actual outcomes per trade."""
    try:
        with open(RECON_FILE, "r") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            return JSONResponse({"total": 0, "matches": 0, "diverged": 0, "last_20": []})
        matches  = sum(1 for r in rows if str(r.get("lifecycle_match")).lower() == "true")
        diverged = len(rows) - matches
        return JSONResponse({
            "total":      len(rows),
            "matches":    matches,
            "diverged":   diverged,
            "match_rate": f"{round(matches / len(rows) * 100)}%",
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
    """Inject a fake BUY or SELL webhook for end-to-end testing."""
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
        "tp1":           round(price + sl_dist       if side == "BUY" else price - sl_dist,       1),
        "tp2":           round(price + sl_dist * 2.0 if side == "BUY" else price - sl_dist * 2.0, 1),
        "sl":            round(price - sl_dist       if side == "BUY" else price + sl_dist,       1),
        "pine_time":     int(time.time() * 1000),
    }
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post("http://localhost:5001/webhook", json=payload, timeout=5)
        return JSONResponse({"test": "fired", "side": side, "price": price,
                             "payload": payload, "response": r.json()})
    except Exception as e:
        return JSONResponse({"test": "error", "error": str(e)})

# ── /test/tp1 ─────────────────────────────────────────────────────────
@app.get("/test/tp1")
async def test_tp1():
    """Simulate TP1_HIT for the current open trade."""
    if not open_trade:
        return JSONResponse({"error": "No open trade"}, status_code=400)
    direction = open_trade["direction"]
    tp1_price = open_trade["tp1_price"]
    payload = {
        "signal":    "TP1_HIT",
        "secret":    WEBHOOK_SECRET,
        "direction": direction,
        "tp1_price": tp1_price,
        "pine_time": int(time.time() * 1000),
    }
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post("http://localhost:5001/webhook", json=payload, timeout=5)
        return JSONResponse({"test": "TP1_HIT fired", "direction": direction,
                             "tp1_price": tp1_price, "response": r.json()})
    except Exception as e:
        with _state_lock:
            _execute_tp1(source="TEST_DIRECT")
        return JSONResponse({"test": "TP1_HIT applied direct", "tp1_price": tp1_price,
                             "note": str(e)})

# ── /test/tp2 ─────────────────────────────────────────────────────────
@app.get("/test/tp2")
async def test_tp2():
    """Simulate TP2_HIT for the current open trade."""
    if not open_trade:
        return JSONResponse({"error": "No open trade"}, status_code=400)
    direction = open_trade["direction"]
    tp2_price = open_trade["tp2_price"]
    payload = {
        "signal":    "TP2_HIT",
        "secret":    WEBHOOK_SECRET,
        "direction": direction,
        "tp2_price": tp2_price,
        "pine_time": int(time.time() * 1000),
    }
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post("http://localhost:5001/webhook", json=payload, timeout=5)
        return JSONResponse({"test": "TP2_HIT fired", "direction": direction,
                             "tp2_price": tp2_price, "response": r.json()})
    except Exception as e:
        with _state_lock:
            if open_trade:
                _close_trade(tp2_price, "TP2_TEST")
        return JSONResponse({"test": "TP2 applied direct", "tp2_price": tp2_price,
                             "note": str(e)})

# ── /test/telegram ────────────────────────────────────────────────────
@app.get("/test/telegram")
async def test_telegram():
    tg("✅ Vol Surge v3.1 Telegram test — connection OK")
    return JSONResponse({"status": "sent"})
