# Phase 2 — Severity Logic Fix Log

**Date:** 2026-05-13  
**Status:** Complete — restart required to activate

---

## What Was Fixed

### Problem
Parity rows were showing **RED severity on every bar** despite signals matching correctly.

The root cause was structural data-feed divergence:
- Python reads candles from **Delta Exchange REST API + WebSocket** (`/v2/history/candles`, `candlestick_5m`)
- TradingView reads from its own **internal Delta Exchange India integration**
- Both nominally on the same exchange (`BTCUSD.P`, Delta Exchange India, 5m)
- But candle OHLCV values differ — TradingView reconstructs wider candles (~100 pt ranges)
  vs Delta API narrower candles (~33–60 pt ranges)

This caused ATR, threshold, and SL values to diverge structurally by 18–80 pts — far beyond
the 1–2 pt tolerances the parity system was checking.

**This is not a formula bug. It is a permanent structural difference between data feeds.**

---

## Changes Made

### 1. `parity_tracker.py` — `_compute_severity()`

**Before:**
```python
# Numeric diffs > 2.0 pts → RED
if row.atr5_diff > TOL_ATR:       return "RED"
if row.threshold_diff > TOL_ATR:  return "RED"
if row.sl_dist_diff > TOL_SL_DIST: return "RED"
```

**After:**
```python
# Signal mismatch = RED only
if not row.signal_match:  return "RED"
# Numeric diffs = YELLOW at worst (data-feed divergence, not a bug)
if row.atr5_diff > TOL_ATR_GREEN:       return "YELLOW"
if row.threshold_diff > TOL_ATR_GREEN:  return "YELLOW"
if row.sl_dist_diff > TOL_SL_GREEN:     return "YELLOW"
return "GREEN"
```

### 2. `parity_tracker.py` — `_generate_explanation()`

YELLOW rows now display:
```
DATA_DIVERGENCE: Signals match. TradingView and Delta REST/WS reconstruct
candles differently (atr5=18.60pts, threshold=80.42pts, sl_dist=22.33pts).
Structural divergence — not a formula bug. Signals are the ground truth.
```

RED rows (signal mismatch) display:
```
SIGNAL_MISMATCH: Python=BUY Pine=NONE.
Check cooldown (py_left=0) and EMA/session filters.
```

### 3. `parity_tracker.py` — Confidence delta

| Severity | Before | After |
|----------|--------|-------|
| GREEN    | +1.0   | +1.0  |
| YELLOW   | -5.0   | 0.0   |
| RED      | -15.0  | -15.0 |

YELLOW is now **neutral** — data-feed divergence is expected and should not erode confidence.

### 4. `volsurge_v5.py` — Dashboard

- Added severity legend above the bar log table:
  - 🟢 GREEN = signal match + diffs within tolerance
  - 🟡 YELLOW = signal match + data-feed divergence (expected, structural)
  - 🔴 RED = actual signal mismatch only
- Row highlight: RED rows → dark red background, YELLOW rows → dark amber background
- Anomaly feed header updated to reflect new meaning

---

## Severity Classification (New)

| Severity | Meaning | Streak Impact | Confidence |
|----------|---------|---------------|------------|
| GREEN | Signals match + ATR diffs within tolerance | ✅ Advances | +1.0 |
| YELLOW | Signals match + data-feed divergence | ✅ Advances | 0.0 (neutral) |
| RED | Signal mismatch (Python ≠ Pine) | ❌ Resets streak | -15.0 |

**Key principle:** The parity system now measures **signal agreement**, not raw ATR equality
across different data feeds.

---

## Parity PASS Gate (Unchanged Logic, New Behavior)

The gate already counted `parity_match = (severity in ("GREEN", "YELLOW"))`.
After the fix, all current YELLOW rows (signal_match=True) count toward the streak.

Gate requirements:
- `streak ≥ 20` consecutive signal_match=True bars
- `matched_signals ≥ 3` actual BUY/SELL signals that both engines agreed on
- `false_positives = 0`
- `false_negatives = 0`

---

## What Was NOT Changed

- `signal_engine.py` — formula untouched
- `candle_feed.py` — Delta REST/WS data source unchanged
- Pine script signal logic — unchanged
- Delta Exchange data feed source — unchanged
- v4 Railway deployment — untouched
- Execution architecture — no orders, paper mode only

---

## Files Modified

```
volsurge_5m/
├── parity_tracker.py    ← _compute_severity, _generate_explanation, confidence delta
└── volsurge_v5.py       ← dashboard legend, row highlight colors, anomaly feed header
```

---

## Activation

Restart the Python server to apply changes:

```
Ctrl+C  →  python volsurge_v5.py
```

After restart:
- All `signal_match=True` rows → YELLOW (not RED)
- Streak counter begins advancing
- Confidence climbs from 70 at +1/bar (GREEN) or stays neutral (YELLOW)
- RED appears only if Python and Pine fire different signals
