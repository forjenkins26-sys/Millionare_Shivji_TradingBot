#!/usr/bin/env python3
"""
volsurge_v5.py — Vol Surge Bot v5 (WebSocket-native, Paper Mode Only)
======================================================================
Phase 2: Signal engine validation. No live execution.

Architecture:
  Delta WebSocket → CandleFeed → SignalEngine → log + CSV
                                              (NO orders, NO trades)

This file:
  - Wires CandleFeed and SignalEngine together
  - Logs every signal to data-v5/signals.csv for parity review
  - Exposes /status and /health endpoints for remote monitoring
  - PAPER MODE ONLY — will refuse to start if PAPER_MODE != true

Run locally:
  python volsurge_v5.py

Or with uvicorn (for health endpoint):
  uvicorn volsurge_v5:app --host 0.0.0.0 --port 5002
"""

import asyncio
import csv
import json
import logging
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel

from candle_feed import CandleFeed, Candle
from signal_engine import SignalEngine, SignalConfig, IndicatorState, SignalResult
from parity_tracker import ParityTracker

# ── Config ────────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"

if not PAPER_MODE:
    print("ERROR: volsurge_v5.py is PAPER MODE ONLY in Phase 2. Set PAPER_MODE=true.")
    sys.exit(1)

SYMBOL          = os.getenv("SYMBOL", "BTCUSD")
PARITY_SECRET   = os.getenv("PARITY_SECRET", "volsurge-parity-token")  # set a real secret in prod
SAFETY_FACTOR   = float(os.getenv("SIGNAL_SAFETY_FACTOR", "1.15"))  # body must be > threshold × factor

DATA_DIR = Path(os.getenv("DATA_DIR_V5", "data-v5"))
LOG_DIR  = Path(os.getenv("LOG_DIR_V5",  "logs-v5"))
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

SIGNALS_CSV  = DATA_DIR / "signals.csv"
PARITY_CSV   = DATA_DIR / "parity_log.csv"
LOG_FILE     = LOG_DIR  / "volsurge_v5.log"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)-7s | %(message)s",
    handlers = [
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(stream=open(sys.stdout.fileno(), mode="w",
                                          encoding="utf-8", buffering=1)),
    ],
)
log = logging.getLogger("volsurge_v5")

# ── Signals CSV ───────────────────────────────────────────────────────────────
SIGNAL_HEADERS = [
    "ts_bar", "ts_received", "signal",
    "entry_price", "sl", "tp1", "tp2", "sl_dist",
    "candle_body", "chop_avg_tr", "burst_threshold",
    "atr5", "atr5_prev", "ema200",
    "session_ok", "cooldown_left", "bars_in_buffer",
    "warmup_warning",
]

def _init_csv():
    if not SIGNALS_CSV.exists():
        with open(SIGNALS_CSV, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=SIGNAL_HEADERS).writeheader()
        log.info(f"[V5] Created signals CSV: {SIGNALS_CSV}")

def _append_signal(result: SignalResult):
    row = {
        "ts_bar":          result.ts,
        "ts_received":     datetime.now(tz=timezone.utc).isoformat(),
        "signal":          result.signal,
        "entry_price":     result.entry_price,
        "sl":              result.sl,
        "tp1":             result.tp1,
        "tp2":             result.tp2,
        "sl_dist":         result.sl_dist,
        "candle_body":     result.state.candle_body,
        "chop_avg_tr":     result.state.chop_avg_tr,
        "burst_threshold": result.state.burst_threshold,
        "atr5":            round(result.state.atr5, 2),
        "atr5_prev":       round(result.state.atr5_prev, 2),
        "ema200":          round(result.state.ema200, 2),
        "session_ok":      result.state.session_ok,
        "cooldown_left":   result.state.cooldown_left,
        "bars_in_buffer":  result.state.bars_in_buffer,
        "warmup_warning":  result.state.warmup_warning,
    }
    try:
        with open(SIGNALS_CSV, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=SIGNAL_HEADERS, extrasaction="ignore").writerow(row)
    except Exception as e:
        log.error(f"[V5] CSV write error: {e}")

# ── Global state ──────────────────────────────────────────────────────────────
_last_state: dict = {}
_signal_count     = 0
_bar_count        = 0
_started_at_ts    = time.time()
_started_at       = datetime.now(tz=timezone.utc).isoformat()
_last_signal_time: Optional[float] = None   # time.time() when last signal fired


# ── Health helpers ────────────────────────────────────────────────────────────

def _health_status() -> str:
    """HEALTHY / DEGRADED / DISCONNECTED — used by dashboard badge and /health."""
    if not feed.connected:
        return "DISCONNECTED"
    now = time.time()
    last_bar_age = (now - feed.last_closed.ts) if feed.last_closed else 9999
    if not feed.is_ready or len(feed.buffer) < 300 or last_bar_age > 420:
        return "DEGRADED"
    return "HEALTHY"


def _active_warnings() -> list:
    """Return list of human-readable warning strings for current feed state."""
    warns = []
    if not feed.connected:
        warns.append("WebSocket disconnected")
    if not feed.is_ready:
        warns.append("Feed not ready — buffer thin or backfill failed")
    if len(feed.buffer) < 300:
        warns.append(f"Buffer size {len(feed.buffer)} / 300")
    if feed.last_closed:
        age = time.time() - feed.last_closed.ts
        if age > 420:
            warns.append(f"No bar received for {age / 60:.1f} min (stale feed)")
    return warns


def _metrics() -> dict:
    """Snapshot of live operational metrics."""
    now       = time.time()
    uptime_s  = now - _started_at_ts
    uptime_h  = uptime_s / 3600
    bph       = round(_bar_count / uptime_h, 1) if uptime_h > 0.05 else 0

    frame_age = round(now - feed.last_frame_at, 1) if feed.last_frame_at else None
    bar_age   = round(now - feed.last_closed.ts, 1) if feed.last_closed else None

    if _last_signal_time:
        sig_dt  = datetime.fromtimestamp(_last_signal_time, tz=timezone.utc)
        sig_str = sig_dt.strftime("%H:%M UTC")
    else:
        sig_str = "none yet"

    h = int(uptime_s // 3600)
    m = int((uptime_s % 3600) // 60)

    return {
        "uptime":             f"{h}h {m}m",
        "bars_processed":     _bar_count,
        "bars_per_hour":      bph,
        "reconnect_count":    feed.reconnect_count,
        "last_frame_age_s":   frame_age,
        "last_bar_age_s":     bar_age,
        "last_signal":        sig_str,
        "buffer_size":        len(feed.buffer),
        "mark_price":         feed.mark_price,
    }

# ── Signal engine ─────────────────────────────────────────────────────────────
cfg    = SignalConfig(safety_factor=SAFETY_FACTOR)   # Pine defaults — all filters OFF
engine = SignalEngine(config=cfg, logger=logging.getLogger("signal_engine"))

# ── Parity tracker ────────────────────────────────────────────────────────────
tracker = ParityTracker(log_path=PARITY_CSV, logger=logging.getLogger("parity_tracker"))

# ── Candle close callback ─────────────────────────────────────────────────────

def on_candle_close(candle: Candle, buffer: deque):
    """Called by CandleFeed on every confirmed bar close."""
    global _bar_count, _signal_count, _last_state, _last_signal_time

    _bar_count += 1

    state = engine.on_candle_close(candle, buffer, in_trade=False)
    if state is None:
        return

    # ── Console bar-close block ───────────────────────────────────────────────
    bar_dt     = datetime.fromtimestamp(state.ts, tz=timezone.utc).strftime("%H:%M UTC")
    sig_label  = state.signal if state.signal else "NONE"
    _SEP       = "-" * 48
    log.info(_SEP)
    log.info(f"BAR CLOSED | {bar_dt}")
    log.info(f"SIGNAL         : {sig_label}")
    log.info(f"chop_avg_tr    : {state.chop_avg_tr:.1f}")
    log.info(f"burst_threshold: {state.burst_threshold:.1f}")
    log.info(f"atr5           : {state.atr5:.2f}")
    log.info(f"sl_dist        : {state.sl_dist:.1f}")
    log.info(f"cooldown_left  : {state.cooldown_left}")
    log.info(f"parity_logged  : {len(tracker._rows)}")
    log.info(_SEP)

    # Update last state for /status
    _last_state = {
        "ts":              state.ts,
        "close":           state.close,
        "candle_body":     round(state.candle_body, 1),
        "chop_avg_tr":     round(state.chop_avg_tr, 1),
        "burst_threshold": round(state.burst_threshold, 1),
        "is_burst_bull":   state.is_burst_bull,
        "is_burst_bear":   state.is_burst_bear,
        "atr5":            round(state.atr5, 2),
        "atr5_prev":       round(state.atr5_prev, 2),
        "sl_dist":         state.sl_dist,
        "ema200":          round(state.ema200, 1),
        "above_ema":       state.above_ema,
        "session_ok":      state.session_ok,
        "cooldown_ok":     state.cooldown_ok,
        "cooldown_left":   state.cooldown_left,
        "signal":          state.signal,
        "bars_in_buffer":  state.bars_in_buffer,
        "warmup_warning":  state.warmup_warning,
        "bar_count_total": _bar_count,
        "signal_count":    _signal_count,
    }

    # Auto-log to parity tracker on every bar close
    tracker.log_bar(state)

    if state.signal:
        sr = engine.build_signal_result(state)
        if sr:
            _signal_count += 1
            _last_signal_time = time.time()
            _last_state["signal_count"] = _signal_count
            _append_signal(sr)
            log.info(
                f"[V5] SIGNAL #{_signal_count}: {sr.signal} "
                f"entry={sr.entry_price:,.1f} sl={sr.sl:,.1f} tp2={sr.tp2:,.1f} "
                f"sl_dist={sr.sl_dist:.1f}"
            )
            log.info("[V5] NOTE: PAPER MODE — no order placed")

# ── CandleFeed ────────────────────────────────────────────────────────────────
feed = CandleFeed(
    symbol          = SYMBOL,
    buffer_size     = 300,
    on_candle_close = on_candle_close,
    logger          = logging.getLogger("candle_feed"),
)

# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(title="Vol Surge v5 — WebSocket Signal Engine (Paper)")

@app.on_event("startup")
async def startup():
    _init_csv()
    log.info("=" * 65)
    log.info("  Vol Surge v5 | PAPER MODE | WebSocket Signal Engine")
    log.info(f"  Symbol   : {SYMBOL}")
    log.info(f"  Config   : lookback={cfg.lookback} burst_mult={cfg.burst_mult}"
             f" sl_mult={cfg.sl_mult} tp2_r={cfg.tp2_r} cooldown={cfg.cooldown}")
    log.info(f"  Safety   : safety_factor={cfg.safety_factor} "
             f"(body must be >{cfg.safety_factor}× burst_threshold to fire)")
    log.info(f"  EMA filt : {'ON' if cfg.use_ema_filter else 'OFF (default)'}")
    log.info(f"  Session  : {'ON' if cfg.use_session else 'OFF (default)'}")
    log.info(f"  Data dir : {DATA_DIR}")
    log.info(f"  Signals  : {SIGNALS_CSV}")
    log.info("=" * 65)
    log.info("  Phase 2 — SIGNAL DETECTION ONLY — no orders placed")
    log.info("=" * 65)

    asyncio.create_task(feed.start())
    asyncio.create_task(_monitor_loop())
    asyncio.create_task(_daily_report_task())


async def _daily_report_task():
    """Fire a daily parity summary every 24 hours."""
    await asyncio.sleep(3600)   # first report after 1 hour of data
    while True:
        try:
            summary = tracker.daily_summary()
            log.info("[REPORT] Daily parity summary:")
            log.info(f"[REPORT]   bars_logged={summary['bars_logged']} "
                     f"with_pine={summary['bars_with_pine']} auto={summary['bars_auto']}")
            log.info(f"[REPORT]   GREEN={summary['green']} YELLOW={summary['yellow']} RED={summary['red']}")
            log.info(f"[REPORT]   parity_rate={summary['parity_rate_pct']}% "
                     f"confidence={summary['confidence_score']} streak={summary['streak']}")
            log.info(f"[REPORT]   pass_achieved={summary['pass_achieved']}")
            # Save to file
            import json as _json
            rpt_dir = DATA_DIR / "daily_reports"
            rpt_dir.mkdir(exist_ok=True)
            rpt_path = rpt_dir / f"{summary['date']}.json"
            rpt_path.write_text(_json.dumps(summary, indent=2), encoding="utf-8")
        except Exception as e:
            log.error(f"[REPORT] daily_report_task error: {e}")
        await asyncio.sleep(86400)


async def _monitor_loop():
    """Background task: log warnings every 60s on feed health issues."""
    await asyncio.sleep(90)   # give feed time to warm up before first check
    while True:
        warns = _active_warnings()
        for w in warns:
            log.warning(f"[MONITOR] {w}")
        if not warns:
            log.debug("[MONITOR] feed healthy")
        await asyncio.sleep(60)


@app.get("/health")
@app.get("/")
async def health():
    hs   = _health_status()
    m    = _metrics()
    return JSONResponse({
        "status":         hs.lower(),
        "health":         hs,
        "bot":            "Vol Surge v5 — WebSocket Signal Engine",
        "mode":           "PAPER",
        "phase":          "2 — signal detection only, no execution",
        "ws_connected":   feed.connected,
        "feed_ready":     feed.is_ready,
        "buffer_size":    len(feed.buffer),
        "mark_price":     feed.mark_price,
        "bar_count":      _bar_count,
        "signal_count":   _signal_count,
        "reconnect_count": feed.reconnect_count,
        "last_frame_age_s": m["last_frame_age_s"],
        "last_bar_age_s": m["last_bar_age_s"],
        "warnings":       _active_warnings(),
        "started_at":     _started_at,
        "timestamp":      datetime.now(tz=timezone.utc).isoformat(),
    })

@app.get("/status")
async def status():
    return JSONResponse({
        "bot":            "Vol Surge v5",
        "ws_connected":   feed.connected,
        "feed_ready":     feed.is_ready,
        "buffer_size":    len(feed.buffer),
        "mark_price":     feed.mark_price,
        "last_bar":       _last_state,
        "signal_count":   _signal_count,
        "bar_count":      _bar_count,
        "safety_factor":  cfg.safety_factor,
        "timestamp":      datetime.now(tz=timezone.utc).isoformat(),
    })

@app.get("/signals")
async def signals():
    """Return recent signals from CSV."""
    try:
        with open(SIGNALS_CSV, "r") as f:
            rows = list(csv.DictReader(f))
        return JSONResponse({"count": len(rows), "signals": rows[-20:]})
    except Exception:
        return JSONResponse({"count": 0, "signals": []})

@app.get("/indicators")
async def indicators():
    """Return current indicator state — use for manual TradingView parity check."""
    return JSONResponse({
        "note": "Compare these values with TradingView status table on the same bar",
        "last_bar": _last_state,
        "config": {
            "lookback":       cfg.lookback,
            "burst_mult":     cfg.burst_mult,
            "safety_factor":  cfg.safety_factor,
            "sl_mult":        cfg.sl_mult,
            "tp2_r":          cfg.tp2_r,
            "cooldown":       cfg.cooldown,
            "ema_length":     cfg.ema_length,
            "use_ema_filter": cfg.use_ema_filter,
            "use_session":    cfg.use_session,
        }
    })

# ── Parity endpoints ──────────────────────────────────────────────────────────

class PineSubmit(BaseModel):
    ts_bar:               int
    pine_signal:          str            # "BUY", "SELL", or "NONE"
    pine_chop_avg_tr:     Optional[float] = None
    pine_burst_threshold: Optional[float] = None
    pine_atr5:            Optional[float] = None
    pine_sl_dist:         Optional[float] = None


@app.get("/parity/status")
async def parity_status():
    """PARITY PASS condition and gate checklist."""
    ps = tracker.pass_status()
    return JSONResponse(ps)


@app.get("/parity/log")
async def parity_log(n: int = 30):
    """Return most recent n parity rows (Python side auto-logged; Pine side when submitted)."""
    rows = tracker.recent_rows(n)
    return JSONResponse({"count": len(rows), "rows": rows})


@app.post("/parity/pine-webhook")
async def pine_webhook(request: Request):
    """
    Receives Pine parity telemetry automatically on every bar close.
    Pine script calls alert(json_string, alert.freq_once_per_bar_close).
    TradingView sends it here via webhook.

    Auth: ?token=YOUR_PARITY_SECRET  (set PARITY_SECRET env var)
    """
    token = request.query_params.get("token", "")
    if token != PARITY_SECRET:
        log.warning(f"[WEBHOOK] Rejected: bad token from {request.client}")
        raise HTTPException(status_code=401, detail="Invalid parity token")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    row = tracker.receive_pine(body)
    ps  = tracker.pass_status()

    if row:
        return JSONResponse({
            "ok":         True,
            "ts":         row.ts_bar,
            "severity":   row.severity,
            "signal":     f"py={row.py_signal} pine={row.pine_signal}",
            "confidence": tracker.confidence_score,
            "streak":     ps["streak"],
        })
    else:
        # Python bar not yet closed — buffered
        return JSONResponse({"ok": True, "status": "buffered", "ts": body.get("ts")})


@app.get("/parity/anomalies")
async def parity_anomalies():
    """Recent YELLOW and RED events only — the things that need attention."""
    return JSONResponse({
        "count":    len(tracker.anomaly_feed),
        "anomalies": tracker.anomaly_feed,
    })


@app.get("/parity/report")
async def parity_report():
    """On-demand daily summary. Also written to data-v5/daily_reports/ every 24h."""
    return JSONResponse(tracker.daily_summary())


@app.post("/parity/submit")
async def parity_submit(body: PineSubmit):
    """
    Submit Pine values for a specific bar.

    Example curl:
      curl -X POST http://localhost:5002/parity/submit \\
        -H "Content-Type: application/json" \\
        -d '{"ts_bar":1778607600,"pine_signal":"BUY","pine_chop_avg_tr":41.2,
             "pine_burst_threshold":82.4,"pine_atr5":58.9,"pine_sl_dist":44.2}'
    """
    row = tracker.submit_pine(
        ts                   = body.ts_bar,
        pine_signal          = body.pine_signal,
        pine_chop_avg_tr     = body.pine_chop_avg_tr or 0.0,
        pine_burst_threshold = body.pine_burst_threshold or 0.0,
        pine_atr5            = body.pine_atr5 or 0.0,
        pine_sl_dist         = body.pine_sl_dist or 0.0,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"ts_bar={body.ts_bar} not in parity log")
    return JSONResponse({"ok": True, "row": row.to_csv_row(), "pass_status": tracker.pass_status()})


def _build_anomaly_html(feed: list) -> str:
    if not feed:
        return '<div style="background:#1e293b;border-radius:6px;padding:12px 16px;color:#22c55e;font-size:13px;margin-bottom:14px">No anomalies. All bars GREEN.</div>'
    rows = ""
    for a in feed[:10]:
        sev = a.get("severity","")
        sc  = "#ef4444" if sev == "RED" else "#f59e0b"
        ps  = a.get("pine_status","")
        exp = a.get("explanation","")
        rows += (
            f'<tr>'
            f'<td style="color:#94a3b8">{a.get("candle_time_utc","")}</td>'
            f'<td><span style="color:{sc};font-weight:bold">{sev}</span></td>'
            f'<td>{ps}</td>'
            f'<td style="color:#22c55e">{a.get("py_signal","")}</td>'
            f'<td style="color:#f59e0b">{a.get("pine_signal","")}</td>'
            f'<td style="color:#94a3b8;font-size:11px">{exp[:120]}</td>'
            f'</tr>\n'
        )
    return (
        '<table style="margin-bottom:14px"><thead><tr>'
        '<th>Candle</th><th>Severity</th><th>Source</th><th>Py Signal</th><th>Pine Signal</th><th>Explanation</th>'
        f'</tr></thead><tbody>{rows}</tbody></table>'
    )


@app.get("/parity/dashboard", response_class=HTMLResponse)
async def parity_dashboard():
    """Full observability dashboard — parity gates, live metrics, feed health, row table."""
    import json as _json

    ps   = tracker.pass_status()
    rows = tracker.recent_rows(30)
    m    = _metrics()
    hs   = _health_status()
    warns = _active_warnings()

    # ── Badge colours ─────────────────────────────────────────────────────────
    pass_color   = "#22c55e" if ps["pass_achieved"] else "#f59e0b"
    pass_label   = "PARITY PASS" if ps["pass_achieved"] else "VALIDATING..."
    health_color = {"HEALTHY": "#22c55e", "DEGRADED": "#f59e0b", "DISCONNECTED": "#ef4444"}[hs]
    conf         = tracker.confidence_score
    conf_color   = "#22c55e" if conf >= 85 else "#f59e0b" if conf >= 60 else "#ef4444"
    auto_bars    = ps.get("auto_bars", 0)
    pine_badge   = ("AUTO" if auto_bars > 0 else "MANUAL") if ps["total_submitted"] > 0 else "WAITING"
    pine_badge_c = "#22c55e" if pine_badge == "AUTO" else "#f59e0b" if pine_badge == "MANUAL" else "#475569"

    # ── Warning banner ────────────────────────────────────────────────────────
    warn_html = ""
    if warns:
        items = "".join(f"<li>{w}</li>" for w in warns)
        warn_html = (
            f'<div style="background:#7f1d1d;border:1px solid #ef4444;border-radius:6px;'
            f'padding:10px 16px;margin-bottom:16px;font-size:13px">'
            f'<b style="color:#fca5a5">FEED WARNINGS</b><ul style="margin:4px 0 0 0;padding-left:18px;color:#fca5a5">'
            f'{items}</ul></div>'
        )

    # ── Gate list ─────────────────────────────────────────────────────────────
    gates_html = "".join(
        f'<li style="color:{"#22c55e" if "[OK]" in g else "#ef4444"}">{g}</li>'
        for g in ps["gate_summary"]
    )

    # ── Latest bar JSON for copy button ───────────────────────────────────────
    latest_json = _json.dumps(_last_state, indent=2) if _last_state else "{}"
    latest_json_escaped = latest_json.replace("\\", "\\\\").replace("`", "\\`")

    # ── Latest bar summary line ───────────────────────────────────────────────
    if _last_state:
        lb = _last_state
        bar_time = datetime.fromtimestamp(lb["ts"], tz=timezone.utc).strftime("%H:%M UTC") if lb.get("ts") else "—"
        sig_raw  = lb.get("signal") or "NONE"
        sig_col  = {"BUY": "#22c55e", "SELL": "#ef4444"}.get(sig_raw, "#94a3b8")
        latest_bar_line = (
            f'<span style="color:#94a3b8">{bar_time}</span> &nbsp;|&nbsp; '
            f'Signal: <b style="color:{sig_col}">{sig_raw}</b> &nbsp;|&nbsp; '
            f'thresh={lb.get("burst_threshold","—")} &nbsp;'
            f'atr5={lb.get("atr5","—")} &nbsp;'
            f'sl_dist={lb.get("sl_dist","—")} &nbsp;'
            f'cooldown={lb.get("cooldown_left","—")}'
        )
    else:
        latest_bar_line = "Waiting for first bar close..."

    # ── Table rows ────────────────────────────────────────────────────────────
    from parity_tracker import TOL_ATR, TOL_SL_DIST

    def sig_color(s):
        return {"BUY": "#22c55e", "SELL": "#ef4444", "NONE": "#64748b", "": "#334155"}.get(s, "#64748b")

    def mismatch_reasons(r):
        reasons = []
        pm = r.get("parity_match", "")
        if str(pm).lower() != "true" and pm != "":
            sm = r.get("signal_match", "")
            if str(sm).lower() == "false":
                reasons.append("SIGNAL_MISMATCH")
            ad = r.get("atr5_diff", "")
            if ad != "" and ad is not None:
                try:
                    if float(ad) > TOL_ATR:
                        reasons.append("ATR_DIFF")
                except (ValueError, TypeError):
                    pass
            td = r.get("threshold_diff", "")
            if td != "" and td is not None:
                try:
                    if float(td) > TOL_ATR:
                        reasons.append("THRESHOLD_DIFF")
                except (ValueError, TypeError):
                    pass
            sd = r.get("sl_dist_diff", "")
            if sd != "" and sd is not None:
                try:
                    if float(sd) > TOL_SL_DIST:
                        reasons.append("SL_DIST_DIFF")
                except (ValueError, TypeError):
                    pass
        return reasons

    rows_html = ""
    for r in rows:
        pine_sig = r.get("pine_signal", "")
        sev      = r.get("severity", "")
        ps_src   = r.get("pine_status", "")
        sev_color = {"GREEN": "#22c55e", "YELLOW": "#f59e0b", "RED": "#ef4444"}.get(sev, "#475569")
        row_bg   = "background:#3b0f0f;" if sev == "RED" else "background:#2d2208;" if sev == "YELLOW" else ""
        expl     = r.get("mismatch_explanation","")

        if not sev:
            sev_td = '<td style="color:#334155">—</td>'
        else:
            sev_td = f'<td style="color:{sev_color};font-weight:bold" title="{expl}">{sev}</td>'

        src_color = {"AUTO": "#22c55e", "MANUAL": "#f59e0b", "MISSING": "#ef4444"}.get(ps_src, "#475569")
        src_td = f'<td style="color:{src_color};font-size:11px">{ps_src or "—"}</td>'

        rows_html += (
            f'<tr style="{row_bg}">'
            f'<td>{r["candle_time_utc"]}</td>'
            f'<td style="color:{sig_color(r["py_signal"])};font-weight:bold">{r["py_signal"]}</td>'
            f'<td>{r["py_chop_avg_tr"]}</td>'
            f'<td>{r["py_burst_threshold"]}</td>'
            f'<td>{r["py_atr5"]}</td>'
            f'<td>{r["py_sl_dist"]}</td>'
            f'<td style="color:{sig_color(pine_sig)};font-weight:bold">{pine_sig or "—"}</td>'
            f'<td>{r.get("pine_chop_avg_tr") or "—"}</td>'
            f'<td>{r.get("pine_burst_threshold") or "—"}</td>'
            f'<td>{r.get("pine_atr5") or "—"}</td>'
            f'<td>{r.get("atr5_diff") or "—"}</td>'
            f'<td>{r.get("threshold_diff") or "—"}</td>'
            + sev_td + src_td
            + '</tr>\n'
        )

    submit_ts = rows[0]["ts_bar"] if rows else ""

    html = f"""<!DOCTYPE html>
<html>
<head>
  <title>Vol Surge v5 — Parity Dashboard</title>
  <meta http-equiv="refresh" content="30">
  <style>
    *    {{ box-sizing:border-box; }}
    body {{ background:#0f172a; color:#e2e8f0; font-family:sans-serif; padding:20px; margin:0; }}
    h1   {{ color:#f8fafc; margin:0 0 6px 0; font-size:1.3em; }}
    h2   {{ color:#94a3b8; font-size:0.85em; font-weight:600; margin:18px 0 6px 0;
            text-transform:uppercase; letter-spacing:.06em; }}
    .badges  {{ display:flex; gap:10px; align-items:center; margin-bottom:4px; flex-wrap:wrap; }}
    .badge   {{ display:inline-block; padding:5px 16px; border-radius:6px;
                color:#0f172a; font-weight:bold; font-size:0.95em; }}
    .meta    {{ color:#475569; font-size:11px; margin-bottom:14px; }}
    .gates   {{ margin:0 0 14px 0; padding-left:20px; font-size:13px; }}
    .grid    {{ display:flex; gap:12px; margin-bottom:14px; flex-wrap:wrap; }}
    .card    {{ background:#1e293b; padding:10px 14px; border-radius:6px; min-width:110px; }}
    .card .n {{ font-size:1.5em; font-weight:bold; color:#f8fafc; }}
    .card .l {{ font-size:0.7em; color:#64748b; margin-top:1px; }}
    .section {{ background:#1e293b; border-radius:8px; padding:14px 16px; margin-bottom:14px; }}
    .latest  {{ font-family:monospace; font-size:13px; color:#cbd5e1; }}
    input    {{ background:#0f172a; color:#e2e8f0; border:1px solid #334155;
                padding:4px 8px; border-radius:4px; margin:2px 4px; width:110px; font-size:12px; }}
    select   {{ background:#0f172a; color:#e2e8f0; border:1px solid #334155;
                padding:4px 8px; border-radius:4px; font-size:12px; }}
    button   {{ background:#3b82f6; color:#fff; border:none; padding:5px 14px;
                border-radius:4px; cursor:pointer; font-size:12px; }}
    button.copy {{ background:#475569; }}
    button:hover {{ opacity:0.85; }}
    table  {{ border-collapse:collapse; width:100%; font-size:11.5px; font-family:monospace; }}
    th     {{ background:#0f172a; padding:5px 8px; text-align:left; color:#64748b;
              font-size:10px; text-transform:uppercase; letter-spacing:.05em; border-bottom:1px solid #1e293b; }}
    td     {{ padding:4px 8px; border-bottom:1px solid #1e293b; }}
    tr:hover td {{ background:#243048; }}
    #cpymsg {{ font-size:12px; color:#22c55e; margin-left:10px; }}
    #submitmsg {{ font-size:12px; margin-left:10px; }}
  </style>
</head>
<body>
  <h1>Vol Surge v5 &mdash; Parity Dashboard</h1>

  <div class="badges">
    <span class="badge" style="background:{pass_color}">{pass_label}</span>
    <span class="badge" style="background:{health_color}">{hs}</span>
    <span class="badge" style="background:{conf_color}">CONFIDENCE: {conf}</span>
    <span class="badge" style="background:{pine_badge_c}">PINE: {pine_badge}</span>
  </div>
  <div class="meta">Auto-refreshes 30s &nbsp;|&nbsp; {datetime.now(tz=timezone.utc).strftime("%H:%M:%S UTC")} &nbsp;|&nbsp; PAPER MODE — no execution</div>

  {warn_html}

  <!-- ── Latest bar ─────────────────────────────────────────────────────── -->
  <div class="section">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
      <span class="latest">{latest_bar_line}</span>
      <button class="copy" onclick="copyJson()">Copy latest bar JSON</button>
      <span id="cpymsg"></span>
    </div>
  </div>

  <!-- ── Parity gates ───────────────────────────────────────────────────── -->
  <h2>Parity Gates</h2>
  <div class="grid">
    <div class="card"><div class="n">{ps["streak"]}<span style="color:#475569;font-size:.6em">/{ps["streak_required"]}</span></div><div class="l">Streak</div></div>
    <div class="card"><div class="n">{ps["matched_signals"]}<span style="color:#475569;font-size:.6em">/{ps["signals_required"]}</span></div><div class="l">Signals matched</div></div>
    <div class="card"><div class="n" style="color:{'#ef4444' if ps['false_positives'] else '#22c55e'}">{ps["false_positives"]}</div><div class="l">False positives</div></div>
    <div class="card"><div class="n" style="color:{'#ef4444' if ps['false_negatives'] else '#22c55e'}">{ps["false_negatives"]}</div><div class="l">False negatives</div></div>
    <div class="card"><div class="n">{ps["total_submitted"]}<span style="color:#475569;font-size:.6em">/{ps["total_bars_logged"]}</span></div><div class="l">Bars submitted</div></div>
  </div>
  <ul class="gates">{gates_html}</ul>

  <!-- ── Live metrics ───────────────────────────────────────────────────── -->
  <h2>Live Metrics</h2>
  <div class="grid">
    <div class="card"><div class="n" style="font-size:1.1em">{m["uptime"]}</div><div class="l">Uptime</div></div>
    <div class="card"><div class="n">{m["bars_processed"]}</div><div class="l">Bars processed</div></div>
    <div class="card"><div class="n">{m["bars_per_hour"]}</div><div class="l">Bars / hour</div></div>
    <div class="card"><div class="n" style="color:{'#ef4444' if m['reconnect_count'] > 0 else '#22c55e'}">{m["reconnect_count"]}</div><div class="l">Reconnects</div></div>
    <div class="card"><div class="n" style="font-size:1em;color:{'#ef4444' if (m['last_frame_age_s'] or 0) > 60 else '#22c55e'}">{m["last_frame_age_s"] if m["last_frame_age_s"] is not None else "—"}s</div><div class="l">Last WS frame</div></div>
    <div class="card"><div class="n" style="font-size:1em;color:{'#ef4444' if (m['last_bar_age_s'] or 0) > 420 else '#f8fafc'}">{m["last_bar_age_s"] if m["last_bar_age_s"] is not None else "—"}s</div><div class="l">Last bar age</div></div>
    <div class="card" style="min-width:140px"><div class="n" style="font-size:0.85em">{m["last_signal"]}</div><div class="l">Last signal</div></div>
  </div>

  <!-- ── Anomaly feed ──────────────────────────────────────────────────── -->
  <h2>Anomaly Feed <span style="color:#475569;font-weight:400;font-size:0.85em">(RED = signal mismatch &bull; YELLOW = data-feed divergence &bull; empty is good)</span></h2>
  {_build_anomaly_html(tracker.anomaly_feed)}

  <!-- ── Submit Pine values ─────────────────────────────────────────────── -->
  <h2>Manual Submit <span style="color:#475569;font-weight:400;font-size:0.85em">(fallback — not needed when Pine AUTO is active)</span></h2>
  <div class="section" style="font-size:13px">
    <form id="sf" onsubmit="submitPine(event)">
      <label>ts_bar <input id="ts" type="number" value="{submit_ts}" style="width:110px"></label>
      <label>signal <select id="sig"><option>NONE</option><option>BUY</option><option>SELL</option></select></label>
      <label>chop_avg_tr <input id="cat" type="number" step="0.01" placeholder="41.2"></label>
      <label>burst_thresh <input id="bt" type="number" step="0.01" placeholder="82.4"></label>
      <label>atr5 <input id="a5" type="number" step="0.01" placeholder="58.9"></label>
      <label>sl_dist <input id="sl" type="number" step="0.01" placeholder="44.2"></label>
      <button type="submit">Submit</button>
      <span id="submitmsg"></span>
    </form>
    <p style="color:#475569;font-size:11px;margin:6px 0 0 0">ts_bar pre-fills with most recent bar. Edit to submit an older bar.</p>
  </div>

  <!-- ── Severity legend ─────────────────────────────────────────────── -->
  <div style="background:#1e293b;border-radius:8px;padding:10px 16px;margin-bottom:14px;font-size:12px;display:flex;gap:20px;flex-wrap:wrap">
    <span><b style="color:#22c55e">GREEN</b> &mdash; signal match + diffs within tolerance</span>
    <span><b style="color:#f59e0b">YELLOW</b> &mdash; signal match + data-feed divergence (expected, structural)</span>
    <span><b style="color:#ef4444">RED</b> &mdash; actual signal mismatch (BUY vs NONE, SELL vs BUY, etc.)</span>
  </div>

  <!-- ── Parity table ───────────────────────────────────────────────────── -->
  <h2>Bar Log (last 30)</h2>
  <table>
    <thead>
      <tr>
        <th>Candle (UTC)</th>
        <th>Py Sig</th><th>Py Chop</th><th>Py Thresh</th><th>Py ATR5</th><th>Py SL</th>
        <th>Pine Sig</th><th>Pine Chop</th><th>Pine Thresh</th><th>Pine ATR5</th>
        <th>ATR Diff</th><th>Thr Diff</th><th>Severity</th><th>Source</th>
      </tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>

  <script>
    const LATEST_JSON = `{latest_json_escaped}`;

    function copyJson() {{
      navigator.clipboard.writeText(LATEST_JSON).then(() => {{
        const el = document.getElementById('cpymsg');
        el.textContent = 'Copied!';
        setTimeout(() => el.textContent = '', 2000);
      }});
    }}

    async function submitPine(e) {{
      e.preventDefault();
      const body = {{
        ts_bar:               parseInt(document.getElementById('ts').value),
        pine_signal:          document.getElementById('sig').value,
        pine_chop_avg_tr:     parseFloat(document.getElementById('cat').value) || null,
        pine_burst_threshold: parseFloat(document.getElementById('bt').value) || null,
        pine_atr5:            parseFloat(document.getElementById('a5').value) || null,
        pine_sl_dist:         parseFloat(document.getElementById('sl').value) || null,
      }};
      const resp = await fetch('/parity/submit', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify(body),
      }});
      const data = await resp.json();
      const msg = document.getElementById('submitmsg');
      if (resp.ok) {{
        msg.style.color = data.row.parity_match === 'True' || data.row.parity_match === true ? '#22c55e' : '#ef4444';
        msg.textContent = 'Submitted — match=' + data.row.parity_match;
        setTimeout(() => location.reload(), 1200);
      }} else {{
        msg.style.color = '#ef4444';
        msg.textContent = data.detail || 'Error';
      }}
    }}
  </script>
</body>
</html>"""
    return HTMLResponse(content=html)

# ── Standalone entry point ────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("volsurge_v5:app", host="0.0.0.0", port=5002, workers=1)
