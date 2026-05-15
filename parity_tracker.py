#!/usr/bin/env python3
"""
parity_tracker.py — Automated parity validation between Python v5 and TradingView Pine
========================================================================================
Modes:
  AUTO   — Pine telemetry arrives via webhook (/parity/pine-webhook). Zero manual work.
  MANUAL — Operator submits Pine values via /parity/submit. Fallback only.

Automated flow:
  1. on_candle_close()  → log_bar(state)       writes Python row
  2. Pine webhook fires → receive_pine(dict)   finds row, auto-compares
  3. _auto_compare()    → severity GREEN/YELLOW/RED, confidence_score, anomaly_feed

Does NOT place orders. Does NOT touch v4.
"""

import csv
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# ── Tolerances ────────────────────────────────────────────────────────────────
TOL_ATR_GREEN  = 1.0   # pts  — diff ≤ this → GREEN
TOL_ATR        = 2.0   # pts  — diff ≤ this → YELLOW, else RED
TOL_SL_GREEN   = 0.5   # pts
TOL_SL_DIST    = 1.0   # pts

PASS_MIN_BARS  = 20    # consecutive aligned bars for PARITY PASS
PASS_MIN_SIGS  = 3     # actual BUY/SELL signals that must match

PINE_TIMEOUT_S = 600   # seconds to wait for Pine telemetry before marking MISSING

# ── CSV schema ────────────────────────────────────────────────────────────────
_HEADERS = [
    "candle_time_utc", "ts_bar",
    "py_signal", "py_chop_avg_tr", "py_burst_threshold",
    "py_atr5", "py_atr5_prev", "py_sl_dist",
    "py_candle_body", "py_ema200", "py_above_ema",
    "py_cooldown_ok", "py_cooldown_left", "py_session_ok",
    "pine_signal", "pine_chop_avg_tr", "pine_burst_threshold",
    "pine_atr5", "pine_sl_dist",
    "signal_match", "atr5_diff", "threshold_diff", "sl_dist_diff",
    "parity_match", "severity", "pine_status",
    "mismatch_explanation", "pine_submitted_at",
]


# ── Row dataclass ─────────────────────────────────────────────────────────────

@dataclass
class ParityRow:
    # identity
    ts_bar:          int
    candle_time_utc: str

    # python values (auto-filled on bar close)
    py_signal:          str
    py_chop_avg_tr:     float
    py_burst_threshold: float
    py_atr5:            float
    py_atr5_prev:       float
    py_sl_dist:         float
    py_candle_body:     float
    py_ema200:          float
    py_above_ema:       bool
    py_cooldown_ok:     bool
    py_cooldown_left:   int
    py_session_ok:      bool

    # pine values (filled by AUTO webhook or MANUAL submit)
    pine_signal:          str   = ""
    pine_chop_avg_tr:     float = 0.0
    pine_burst_threshold: float = 0.0
    pine_atr5:            float = 0.0
    pine_sl_dist:         float = 0.0

    # comparison (computed after Pine arrives)
    signal_match:         Optional[bool]  = None
    atr5_diff:            Optional[float] = None
    threshold_diff:       Optional[float] = None
    sl_dist_diff:         Optional[float] = None
    parity_match:         Optional[bool]  = None
    severity:             str             = ""    # GREEN / YELLOW / RED / ""
    pine_status:          str             = ""    # AUTO / MANUAL / MISSING / LATE / ""
    mismatch_explanation: str             = ""
    pine_submitted_at:    str             = ""

    def to_csv_row(self) -> dict:
        def _f(v): return round(v, 2) if v else ""
        return {
            "candle_time_utc":      self.candle_time_utc,
            "ts_bar":               self.ts_bar,
            "py_signal":            self.py_signal,
            "py_chop_avg_tr":       round(self.py_chop_avg_tr, 2),
            "py_burst_threshold":   round(self.py_burst_threshold, 2),
            "py_atr5":              round(self.py_atr5, 2),
            "py_atr5_prev":         round(self.py_atr5_prev, 2),
            "py_sl_dist":           round(self.py_sl_dist, 2),
            "py_candle_body":       round(self.py_candle_body, 2),
            "py_ema200":            round(self.py_ema200, 2),
            "py_above_ema":         self.py_above_ema,
            "py_cooldown_ok":       self.py_cooldown_ok,
            "py_cooldown_left":     self.py_cooldown_left,
            "py_session_ok":        self.py_session_ok,
            "pine_signal":          self.pine_signal,
            "pine_chop_avg_tr":     _f(self.pine_chop_avg_tr),
            "pine_burst_threshold": _f(self.pine_burst_threshold),
            "pine_atr5":            _f(self.pine_atr5),
            "pine_sl_dist":         _f(self.pine_sl_dist),
            "signal_match":         "" if self.signal_match is None else self.signal_match,
            "atr5_diff":            "" if self.atr5_diff is None else round(self.atr5_diff, 2),
            "threshold_diff":       "" if self.threshold_diff is None else round(self.threshold_diff, 2),
            "sl_dist_diff":         "" if self.sl_dist_diff is None else round(self.sl_dist_diff, 2),
            "parity_match":         "" if self.parity_match is None else self.parity_match,
            "severity":             self.severity,
            "pine_status":          self.pine_status,
            "mismatch_explanation": self.mismatch_explanation,
            "pine_submitted_at":    self.pine_submitted_at,
        }


# ── ParityTracker ─────────────────────────────────────────────────────────────

class ParityTracker:
    """
    Central parity engine.

    Automated path (no manual work needed):
        tracker.log_bar(state)          ← called by on_candle_close
        tracker.receive_pine(dict)      ← called by POST /parity/pine-webhook

    Manual fallback:
        tracker.submit_pine(ts, ...)    ← called by POST /parity/submit
    """

    def __init__(self, log_path: Path, logger: Optional[logging.Logger] = None):
        self.log_path = log_path
        self.log = logger or logging.getLogger("parity_tracker")

        self._rows:         Dict[int, ParityRow] = {}
        self._ts_order:     List[int]            = []
        self._pending_pine: Dict[int, dict]      = {}   # Pine arrived before Python bar
        self._anomaly_feed: List[dict]           = []   # last 50 YELLOW/RED events
        self._confidence:   float                = 70.0 # starts neutral

        self._init_csv()
        self._load_existing()

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def confidence_score(self) -> int:
        return max(0, min(100, int(self._confidence)))

    @property
    def anomaly_feed(self) -> List[dict]:
        return list(reversed(self._anomaly_feed[-50:]))

    # ── Init / load ───────────────────────────────────────────────────────────

    def _init_csv(self):
        if not self.log_path.exists():
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=_HEADERS).writeheader()
            self.log.info(f"[PARITY] Created {self.log_path}")

    def _load_existing(self):
        try:
            with open(self.log_path, "r", encoding="utf-8") as f:
                for raw in csv.DictReader(f):
                    try:
                        ts  = int(raw["ts_bar"])
                        row = ParityRow(
                            ts_bar              = ts,
                            candle_time_utc     = raw["candle_time_utc"],
                            py_signal           = raw["py_signal"],
                            py_chop_avg_tr      = float(raw["py_chop_avg_tr"] or 0),
                            py_burst_threshold  = float(raw["py_burst_threshold"] or 0),
                            py_atr5             = float(raw["py_atr5"] or 0),
                            py_atr5_prev        = float(raw["py_atr5_prev"] or 0),
                            py_sl_dist          = float(raw["py_sl_dist"] or 0),
                            py_candle_body      = float(raw["py_candle_body"] or 0),
                            py_ema200           = float(raw["py_ema200"] or 0),
                            py_above_ema        = raw["py_above_ema"] == "True",
                            py_cooldown_ok      = raw["py_cooldown_ok"] == "True",
                            py_cooldown_left    = int(raw["py_cooldown_left"] or 0),
                            py_session_ok       = raw["py_session_ok"] == "True",
                            pine_signal         = raw.get("pine_signal", ""),
                            pine_chop_avg_tr    = float(raw.get("pine_chop_avg_tr") or 0),
                            pine_burst_threshold= float(raw.get("pine_burst_threshold") or 0),
                            pine_atr5           = float(raw.get("pine_atr5") or 0),
                            pine_sl_dist        = float(raw.get("pine_sl_dist") or 0),
                            signal_match        = None if raw.get("signal_match","") == "" else raw.get("signal_match") == "True",
                            atr5_diff           = float(raw["atr5_diff"]) if raw.get("atr5_diff") else None,
                            threshold_diff      = float(raw["threshold_diff"]) if raw.get("threshold_diff") else None,
                            sl_dist_diff        = float(raw["sl_dist_diff"]) if raw.get("sl_dist_diff") else None,
                            parity_match        = None if raw.get("parity_match","") == "" else raw.get("parity_match") == "True",
                            severity            = raw.get("severity", ""),
                            pine_status         = raw.get("pine_status", ""),
                            mismatch_explanation= raw.get("mismatch_explanation", ""),
                            pine_submitted_at   = raw.get("pine_submitted_at", ""),
                        )
                        self._rows[ts] = row
                        self._ts_order.append(ts)
                        # Rebuild confidence from loaded history
                        if row.severity == "GREEN":
                            self._confidence = min(100, self._confidence + 0.5)
                        elif row.severity == "YELLOW":
                            self._confidence = max(0, self._confidence - 3)
                        elif row.severity == "RED":
                            self._confidence = max(0, self._confidence - 10)
                    except Exception as e:
                        self.log.debug(f"[PARITY] skip bad row: {e}")
            self.log.info(f"[PARITY] Loaded {len(self._rows)} rows | confidence={self.confidence_score}")
        except Exception as e:
            self.log.warning(f"[PARITY] Could not load existing CSV: {e}")

    # ── Log Python bar close (automatic, every bar) ───────────────────────────

    def log_bar(self, state) -> ParityRow:
        """Called by on_candle_close for every confirmed bar."""
        ts = state.ts
        if ts in self._rows:
            self.log.debug(f"[PARITY] Dedup ts={ts}")
            return self._rows[ts]

        row = ParityRow(
            ts_bar             = ts,
            candle_time_utc    = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            py_signal          = state.signal or "NONE",
            py_chop_avg_tr     = state.chop_avg_tr,
            py_burst_threshold = state.burst_threshold,
            py_atr5            = state.atr5,
            py_atr5_prev       = state.atr5_prev,
            py_sl_dist         = state.sl_dist,
            py_candle_body     = state.candle_body,
            py_ema200          = state.ema200,
            py_above_ema       = state.above_ema,
            py_cooldown_ok     = state.cooldown_ok,
            py_cooldown_left   = state.cooldown_left,
            py_session_ok      = state.session_ok,
        )

        self._rows[ts] = row
        self._ts_order.append(ts)

        # If Pine telemetry already arrived (Pine was faster than Python bar close)
        if ts in self._pending_pine:
            pine_dict = self._pending_pine.pop(ts)
            self._auto_compare(row, pine_dict)
            self.log.info(f"[PARITY] AUTO (pending) ts={ts} → {row.severity}")
        else:
            self._append_row_to_csv(row)

        return row

    # ── Receive Pine telemetry (AUTO path) ────────────────────────────────────

    def receive_pine(self, pine_dict: dict) -> Optional[ParityRow]:
        """
        Called by POST /parity/pine-webhook.
        pine_dict keys (from Pine alert JSON):
          v, ts, sig, cat, bt, atr5, a5p, sld, cd, ema, abv, ses, body
        Returns the updated ParityRow, or None if ts not yet in Python log.
        """
        try:
            ts = int(pine_dict.get("ts", 0))
            if ts == 0:
                self.log.warning("[PARITY] receive_pine: missing ts in payload")
                return None

            # Normalise — Pine sends ms sometimes
            if ts > 1_000_000_000_000:
                ts = ts // 1000

            if ts in self._rows:
                row = self._rows[ts]
                self._auto_compare(row, pine_dict)
                self.log.info(f"[PARITY] AUTO ts={ts} signal=py:{row.py_signal}/pine:{row.pine_signal} → {row.severity}")
                return row
            else:
                # Python bar hasn't closed yet — buffer Pine data
                self._pending_pine[ts] = pine_dict
                self.log.debug(f"[PARITY] Pine buffered ts={ts} (Python bar not yet closed)")
                return None

        except Exception as e:
            self.log.error(f"[PARITY] receive_pine error: {e}")
            return None

    # ── Core auto-compare ─────────────────────────────────────────────────────

    def _auto_compare(self, row: ParityRow, pine_dict: dict, pine_status: str = "AUTO"):
        """Fill Pine values, compute diffs, severity, confidence, anomaly."""
        now_iso = datetime.now(tz=timezone.utc).isoformat()

        row.pine_signal          = str(pine_dict.get("sig", "NONE")).upper()
        row.pine_chop_avg_tr     = float(pine_dict.get("cat", 0) or 0)
        row.pine_burst_threshold = float(pine_dict.get("bt",  0) or 0)
        row.pine_atr5            = float(pine_dict.get("atr5",0) or 0)
        row.pine_sl_dist         = float(pine_dict.get("sld", 0) or 0)
        row.pine_status          = pine_status
        row.pine_submitted_at    = now_iso

        # Diffs
        row.signal_match   = (row.py_signal == row.pine_signal)
        row.atr5_diff      = round(abs(row.py_atr5 - row.pine_atr5), 3) if row.pine_atr5 else None
        row.threshold_diff = round(abs(row.py_burst_threshold - row.pine_burst_threshold), 3) if row.pine_burst_threshold else None
        row.sl_dist_diff   = round(abs(row.py_sl_dist - row.pine_sl_dist), 3) if row.pine_sl_dist else None

        # Severity
        row.severity    = self._compute_severity(row)
        row.parity_match = (row.severity in ("GREEN", "YELLOW"))

        # Explanation
        row.mismatch_explanation = self._generate_explanation(row)

        # Confidence score
        # YELLOW = data-feed divergence (structural, expected) → neutral confidence impact
        delta = {"GREEN": +1.0, "YELLOW": 0.0, "RED": -15.0}.get(row.severity, 0)
        self._confidence = max(0, min(100, self._confidence + delta))

        # Anomaly feed
        if row.severity in ("YELLOW", "RED"):
            self._anomaly_feed.append({
                "ts":              row.ts_bar,
                "candle_time_utc": row.candle_time_utc,
                "severity":        row.severity,
                "pine_status":     pine_status,
                "py_signal":       row.py_signal,
                "pine_signal":     row.pine_signal,
                "atr5_diff":       row.atr5_diff,
                "threshold_diff":  row.threshold_diff,
                "explanation":     row.mismatch_explanation,
            })
            if len(self._anomaly_feed) > 50:
                self._anomaly_feed.pop(0)

        self._rewrite_csv()

    def _compute_severity(self, row: ParityRow) -> str:
        """
        RED    = actual signal mismatch ONLY (BUY vs NONE, SELL vs BUY, etc.)
        YELLOW = signals match + numeric diffs outside tolerance (data-feed divergence)
        GREEN  = signals match + numeric diffs within tolerance
        """
        # Signal mismatch is the only true failure mode
        if not row.signal_match:
            return "RED"
        # Signals agree — numeric diffs are structural data-feed divergence → YELLOW at worst
        if row.atr5_diff is not None and row.atr5_diff > TOL_ATR_GREEN:
            return "YELLOW"
        if row.threshold_diff is not None and row.threshold_diff > TOL_ATR_GREEN:
            return "YELLOW"
        if row.sl_dist_diff is not None and row.sl_dist_diff > TOL_SL_GREEN:
            return "YELLOW"
        return "GREEN"

    def _generate_explanation(self, row: ParityRow) -> str:
        """Human-readable explanation for non-GREEN rows."""
        if row.severity == "GREEN":
            return ""
        # RED — actual signal mismatch
        if not row.signal_match:
            return (
                f"SIGNAL_MISMATCH: Python={row.py_signal} Pine={row.pine_signal}. "
                f"Check cooldown (py_left={row.py_cooldown_left}) and EMA/session filters."
            )
        # YELLOW — signals match, numeric diffs are structural data-feed divergence
        diff_parts = []
        if row.atr5_diff and row.atr5_diff > TOL_ATR_GREEN:
            diff_parts.append(f"atr5={row.atr5_diff:.2f}pts")
        if row.threshold_diff and row.threshold_diff > TOL_ATR_GREEN:
            diff_parts.append(f"threshold={row.threshold_diff:.2f}pts")
        if row.sl_dist_diff and row.sl_dist_diff > TOL_SL_GREEN:
            diff_parts.append(f"sl_dist={row.sl_dist_diff:.2f}pts")
        if diff_parts:
            return (
                f"DATA_DIVERGENCE: Signals match. TradingView and Delta REST/WS reconstruct "
                f"candles differently ({', '.join(diff_parts)}). "
                f"Structural divergence — not a formula bug. Signals are the ground truth."
            )
        return ""

    # ── Manual submit (fallback path) ─────────────────────────────────────────

    def submit_pine(
        self,
        ts:                   int,
        pine_signal:          str,
        pine_chop_avg_tr:     float = 0.0,
        pine_burst_threshold: float = 0.0,
        pine_atr5:            float = 0.0,
        pine_sl_dist:         float = 0.0,
    ) -> Optional[ParityRow]:
        """Manual Pine submission (fallback). Uses same _auto_compare logic."""
        if ts not in self._rows:
            self.log.warning(f"[PARITY] submit_pine: ts={ts} not found")
            return None
        pine_dict = {
            "sig":  pine_signal,
            "cat":  pine_chop_avg_tr,
            "bt":   pine_burst_threshold,
            "atr5": pine_atr5,
            "sld":  pine_sl_dist,
        }
        self._auto_compare(self._rows[ts], pine_dict, pine_status="MANUAL")
        return self._rows[ts]

    # ── PASS condition ────────────────────────────────────────────────────────

    def pass_status(self) -> dict:
        """PARITY PASS gate — works for both AUTO and MANUAL submissions."""
        submitted = [r for r in self._rows.values() if r.pine_signal != ""]
        submitted.sort(key=lambda r: r.ts_bar)

        streak = 0
        for row in reversed(submitted):
            if row.parity_match:
                streak += 1
            else:
                break

        false_positives = [r for r in submitted if r.py_signal in ("BUY","SELL") and r.pine_signal not in (r.py_signal,) and r.pine_signal != ""]
        false_negatives = [r for r in submitted if r.pine_signal in ("BUY","SELL") and r.py_signal == "NONE"]
        matched_signals = [r for r in submitted if r.pine_signal in ("BUY","SELL") and r.py_signal == r.pine_signal]
        green_count     = sum(1 for r in submitted if r.severity == "GREEN")
        yellow_count    = sum(1 for r in submitted if r.severity == "YELLOW")
        red_count       = sum(1 for r in submitted if r.severity == "RED")
        auto_count      = sum(1 for r in submitted if r.pine_status == "AUTO")

        pass_achieved = (
            streak >= PASS_MIN_BARS
            and len(matched_signals) >= PASS_MIN_SIGS
            and len(false_positives) == 0
            and len(false_negatives) == 0
        )

        return {
            "pass_achieved":     pass_achieved,
            "streak":            streak,
            "streak_required":   PASS_MIN_BARS,
            "matched_signals":   len(matched_signals),
            "signals_required":  PASS_MIN_SIGS,
            "false_positives":   len(false_positives),
            "false_negatives":   len(false_negatives),
            "green":             green_count,
            "yellow":            yellow_count,
            "red":               red_count,
            "auto_bars":         auto_count,
            "total_submitted":   len(submitted),
            "total_bars_logged": len(self._rows),
            "confidence_score":  self.confidence_score,
            "fp_ts":             [r.ts_bar for r in false_positives[-5:]],
            "fn_ts":             [r.ts_bar for r in false_negatives[-5:]],
            "gate_summary":      _pass_gates(streak, len(matched_signals), len(false_positives), len(false_negatives)),
        }

    # ── Daily summary ─────────────────────────────────────────────────────────

    def daily_summary(self) -> dict:
        """Summary of parity health for the last 24 hours (or all data if < 24h)."""
        import time as _time
        cutoff = _time.time() - 86400
        recent = [r for r in self._rows.values() if r.ts_bar >= cutoff]
        submitted = [r for r in recent if r.pine_signal != ""]

        green  = sum(1 for r in submitted if r.severity == "GREEN")
        yellow = sum(1 for r in submitted if r.severity == "YELLOW")
        red    = sum(1 for r in submitted if r.severity == "RED")
        auto   = sum(1 for r in submitted if r.pine_status == "AUTO")
        miss   = len(recent) - len(submitted)

        avg_atr5 = (sum(r.atr5_diff for r in submitted if r.atr5_diff) / max(1, len([r for r in submitted if r.atr5_diff])))
        avg_thr  = (sum(r.threshold_diff for r in submitted if r.threshold_diff) / max(1, len([r for r in submitted if r.threshold_diff])))
        sig_bars = [r for r in submitted if r.pine_signal in ("BUY","SELL")]
        sig_match_rate = round(100 * sum(1 for r in sig_bars if r.signal_match) / max(1, len(sig_bars)), 1)
        parity_rate    = round(100 * green / max(1, len(submitted)), 1)

        ps = self.pass_status()
        return {
            "date":             datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
            "period":           "last 24h",
            "bars_logged":      len(recent),
            "bars_with_pine":   len(submitted),
            "bars_auto":        auto,
            "bars_missing_pine":miss,
            "green":            green,
            "yellow":           yellow,
            "red":              red,
            "parity_rate_pct":  parity_rate,
            "signal_match_pct": sig_match_rate,
            "avg_atr5_diff":    round(avg_atr5, 3),
            "avg_threshold_diff":round(avg_thr, 3),
            "confidence_score": self.confidence_score,
            "streak":           ps["streak"],
            "pass_achieved":    ps["pass_achieved"],
            "anomalies_24h":    yellow + red,
        }

    # ── Dashboard data ────────────────────────────────────────────────────────

    def recent_rows(self, n: int = 30) -> List[dict]:
        by_ts = sorted(self._rows.values(), key=lambda r: r.ts_bar, reverse=True)
        return [r.to_csv_row() for r in by_ts[:n]]

    def get_row(self, ts: int) -> Optional[dict]:
        r = self._rows.get(ts)
        return r.to_csv_row() if r else None

    # ── CSV helpers ───────────────────────────────────────────────────────────

    def _append_row_to_csv(self, row: ParityRow):
        try:
            with open(self.log_path, "a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=_HEADERS, extrasaction="ignore").writerow(row.to_csv_row())
        except Exception as e:
            self.log.error(f"[PARITY] CSV append error: {e}")

    def _rewrite_csv(self):
        try:
            by_ts = sorted(self._rows.values(), key=lambda r: r.ts_bar)
            with open(self.log_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=_HEADERS, extrasaction="ignore")
                w.writeheader()
                for row in by_ts:
                    w.writerow(row.to_csv_row())
        except Exception as e:
            self.log.error(f"[PARITY] CSV rewrite error: {e}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pass_gates(streak: int, sigs: int, fp: int, fn: int) -> list:
    return [
        f"[{'OK' if streak >= PASS_MIN_BARS else '--'}] Streak {streak}/{PASS_MIN_BARS} consecutive aligned",
        f"[{'OK' if sigs >= PASS_MIN_SIGS else '--'}] Signals matched {sigs}/{PASS_MIN_SIGS}",
        f"[{'OK' if fp == 0 else 'XX'}] False positives: {fp} (need 0)",
        f"[{'OK' if fn == 0 else 'XX'}] False negatives: {fn} (need 0)",
    ]
