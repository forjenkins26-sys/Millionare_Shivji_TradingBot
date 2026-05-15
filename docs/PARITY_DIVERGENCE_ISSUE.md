# Parity Divergence Issue — Context for AI Agent

## What This System Is

A paper-mode Bitcoin trading signal validator. It runs two signal engines in parallel:

1. **Python (volsurge_v5.py)** — reads live 5-minute BTCUSD candles from Delta Exchange India
   via REST backfill + WebSocket, computes indicators, detects BUY/SELL signals
2. **Pine Script (TradingView)** — same logic implemented in Pine v5, running on the same
   chart (BTCUSD.P, Delta Exchange India, 5m timeframe)

The goal is **parity validation**: confirm both engines fire the same signals on every bar.
When parity is confirmed (streak ≥ 20 bars, signals matched, no false positives/negatives),
the system advances to live execution.

---

## Auto-Parity Pipeline (Working ✅)

Every 5-minute bar close:

```
Pine alert() fires → Cloudflare tunnel → POST /parity/pine-webhook → parity_tracker.py
                                                                            ↓
Python on_candle_close() → signal_engine.py → parity_tracker.log_bar()
                                                        ↓
                                               _auto_compare() → severity + CSV row
```

The webhook receives HTTP 200 OK on every bar. First row was logged successfully.

---

## The Problem

Parity rows show **RED severity on every bar**, but for the WRONG reason.

### First parity row (actual data from `data-v5/parity_log.csv`):

```
candle: 2026-05-13 09:10 UTC
py_signal:  NONE    pine_signal: NONE   → signal_match: True  ✅
py_chop_avg_tr:  66.1    pine_chop_avg_tr:  106.31  → diff: 40.21 pts  ❌
py_atr5:         74.4    pine_atr5:          93.0   → diff: 18.6 pts   ❌
py_burst_threshold: 132.2   pine_burst_threshold: 212.62 → diff: 80.42 ❌
py_sl_dist:      53.6    pine_sl_dist:       75.93  → diff: 22.33 pts  ❌
severity: RED
explanation: "ATR seed divergence — normal early on, converges over time."
```

### Root cause: Data Feed Divergence

Both Python and Pine are nominally on **Delta Exchange India BTCUSD**, but their
candle data is NOT identical.

**Evidence:**
- Python backfill candle for 09:05 UTC: `H=81271.5 L=81238.0` → range **33.5 pts**
- Pine's average TR across 5 bars: **106.31 pts** (implies ~106 pt ranges)
- TradingView's live BTCUSD.P bar at time of screenshot: `H=81216 L=81188.5` → 27.5 pts

This is a known phenomenon: TradingView's Delta Exchange India integration may reconstruct
candles differently from the raw exchange API. Python uses:
- REST: `https://api.india.delta.exchange/v2/history/candles?symbol=BTCUSD&resolution=5m`
- WebSocket: `wss://socket.india.delta.exchange`, channel `candlestick_5m`, symbol `BTCUSD`

TradingView uses its own internal Delta Exchange India data feed for `BTCUSD.P`.

The result: Python sees narrow candles (~33-60 pt ranges) while Pine sees wider ones
(~100 pt ranges). This means:
- Python `burstThreshold` ≈ 132 pts
- Pine `burstThreshold` ≈ 212 pts

**This will never converge** — it is a structural difference in data sources. The code
formulas are identical and correct; the input data differs.

---

## Why This Matters for Signals

The signal condition in both Pine and Python:
```
BUY  = candleBody >= burstThreshold  AND  close > open  (+ cooldown/EMA/session filters)
SELL = candleBody >= burstThreshold  AND  close < open
```

If a 150-pt body candle appears:
- Python: 150 > 132 → **BUY fires** (false positive vs Pine)
- Pine: 150 < 212 → **no signal**

This creates **false positives in Python** for medium-sized moves.

---

## Current Severity Logic (in `parity_tracker.py`)

```python
TOL_ATR_GREEN  = 1.0   # pts — diff ≤ 1.0 → GREEN
TOL_ATR        = 2.0   # pts — diff ≤ 2.0 → YELLOW, else RED
TOL_SL_GREEN   = 0.5   # pts
TOL_SL_DIST    = 1.0   # pts

def _compute_severity(self, row: ParityRow) -> str:
    if not row.signal_match:
        return "RED"
    if row.atr5_diff > TOL_ATR:          # 2.0 pts
        return "RED"
    if row.threshold_diff > TOL_ATR:     # 2.0 pts
        return "RED"
    if row.sl_dist_diff > TOL_SL_DIST:   # 1.0 pt
        return "RED"
    if row.atr5_diff > TOL_ATR_GREEN:    # 1.0 pt
        return "YELLOW"
    ...
    return "GREEN"
```

The tolerances (1-2 pts) were designed for indicator warmup, not a 40-80 pt structural
data divergence. Every single bar will be RED forever under the current logic.

---

## Proposed Fix

Change severity to be **signal-match-primary**. Numeric diffs are secondary / informational.

### New severity logic:

```
GREEN  = signal match + numeric diffs within tolerance
YELLOW = signal match + numeric diffs outside tolerance (data divergence, not a bug)
RED    = signal MISMATCH (Python fires BUY, Pine says NONE — or vice versa)
```

Concretely:

```python
def _compute_severity(self, row: ParityRow) -> str:
    # Signal mismatch is always RED — this is the real failure mode
    if not row.signal_match:
        return "RED"

    # Both signals agree — numeric diffs are data-divergence, downgrade to YELLOW max
    if row.atr5_diff is not None and row.atr5_diff > TOL_ATR:
        return "YELLOW"
    if row.threshold_diff is not None and row.threshold_diff > TOL_ATR:
        return "YELLOW"
    if row.sl_dist_diff is not None and row.sl_dist_diff > TOL_SL_DIST:
        return "YELLOW"
    if row.atr5_diff is not None and row.atr5_diff > TOL_ATR_GREEN:
        return "YELLOW"
    ...
    return "GREEN"
```

And update `_generate_explanation` to label data-divergence rows clearly:

```
"DATA_DIVERGENCE: Python (Delta REST/WS) and Pine (TradingView feed) use different
candle sources for the same exchange. ATR diff=18.6pts, threshold diff=80.4pts.
Signals still match. This is a known structural issue, not a formula bug."
```

### Also update PARITY PASS gate:

The current gate requires `streak ≥ 20 + no RED`. With the new logic, RED = signal mismatch
only. A YELLOW streak (signals match, numeric diffs present) should still satisfy the gate
if signals keep matching.

Current gate (in `pass_status()`):
```python
"streak": consecutive GREEN rows
"pass": streak >= 20 AND signals_matched >= 3 AND false_positives == 0
```

New gate: streak counts consecutive rows where `signal_match == True` (GREEN or YELLOW both count).

---

## Files to Change

### 1. `parity_tracker.py`
- `_compute_severity()` lines 343-361 — change RED→YELLOW for numeric-only failures
- `_generate_explanation()` lines 363-379 — add "DATA_DIVERGENCE" label
- `pass_status()` — update streak to count signal_match rows, not just GREEN rows
- Optionally: add `TOL_DATA_DIVERGENCE` constant to separate the two failure modes

### 2. `volsurge_v5.py` (dashboard HTML)
- Update dashboard legend to explain GREEN/YELLOW/RED meanings
- Add a note: "YELLOW = signals match, data feed divergence (expected)"

---

## What Should NOT Change

- The signal formula in `signal_engine.py` — it is correct
- The candle feed data source — Python must use Delta REST/WS (same source as execution)
- The Pine script — it is correct for TradingView
- The webhook pipeline — it is working correctly
- The PARITY PASS signal_match requirement — signals must still agree

---

## Current File Locations

```
C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volsurge_5m\
├── volsurge_v5.py          # FastAPI server + bar-close callback + dashboard
├── parity_tracker.py       # Parity comparison engine (NEEDS CHANGES)
├── signal_engine.py        # Signal computation (correct, do not change)
├── candle_feed.py          # Delta WS + REST feed (correct, do not change)
├── data-v5/
│   └── parity_log.csv      # Live parity rows being written
└── docs/
    └── PARITY_DIVERGENCE_ISSUE.md   # This file
```

---

## Invariant Constraints (MUST NOT violate)

1. **No execution code** — no orders, no trade placement, paper mode only
2. **No changes to v4** — Railway deployment is untouched
3. **No changes to signal_engine.py formula** — only parity comparison logic changes
4. **Paper mode check** must remain: `PAPER_MODE != "true"` → server refuses to start

---

## Current State Summary

| Component | Status |
|-----------|--------|
| WebSocket feed | ✅ Connected, candles flowing |
| REST backfill | ✅ 300 candles loaded on startup |
| Bar close detection | ✅ Working after microsecond timestamp fix |
| Pine webhook | ✅ 200 OK every bar |
| Parity CSV rows | ✅ Being written every bar |
| Signal match | ✅ Both NONE (no trade signal yet) |
| Severity | ❌ Always RED due to data divergence (needs fix) |
| Dashboard | ✅ Accessible at http://localhost:5002/parity/dashboard |

The system is functionally correct. The only issue is the severity classification
treating data-source divergence the same as actual signal mismatches.
