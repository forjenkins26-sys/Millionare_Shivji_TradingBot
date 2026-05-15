# Vol Surge v5 — Parity Validation Workflow

**Purpose:** Confirm Python signal engine produces identical outputs to TradingView Pine before any execution wiring begins.

**PARITY PASS requires ALL 4 gates:**
1. 20+ consecutive bars aligned (Python + Pine agree on signal direction)
2. 3+ actual BUY/SELL signals matched (not just NONE bars)
3. Zero false positives (Python fires, Pine doesn't)
4. Zero false negatives (Pine fires, Python doesn't)

---

## TradingView Setup

1. Open your Pine indicator on BTCUSD 5-minute chart.
2. Add a status table (or use the existing one) showing these fields per bar:
   - `chopAvgTR` (average of TR[1..5])
   - `burstThreshold` (chopAvgTR × burstMult)
   - `atr5` (current bar ATR5)
   - `vsSLDist` (atr5[1] × slMult)
   - Signal diamond (blue = BUY, red = SELL)
3. Keep this tab open alongside the parity dashboard.

---

## Every 5-Minute Bar Close — Operator Checklist

### Step 1: Wait for bar close

At every :00, :05, :10... mark, the 5-minute bar closes.
Python logs it within ~500ms. TradingView also updates.

### Step 2: Read Python values from the dashboard

Open: `http://localhost:5002/parity/dashboard`

The top line shows the latest closed bar:
```
Latest closed: 17:45 UTC | Signal: NONE | thresh=82.4 | atr5=58.9 | sl_dist=44.2
```

Or read from the JSON endpoint:
```bash
curl http://localhost:5002/indicators
```

Note these Python values:
- `py_signal` — BUY / SELL / NONE
- `py_burst_threshold` — the burstThreshold for this bar
- `py_chop_avg_tr` — average of previous 5 bars' TR
- `py_atr5` — ATR5 at this bar
- `py_sl_dist` — sl distance (atr5_prev × 0.75)

### Step 3: Read TradingView values

On the same bar (same candle timestamp), read from Pine status table:
- `chopAvgTR`
- `burstThreshold`
- `atr5`
- `vsSLDist`
- Did a diamond appear? If yes: BUY or SELL?

### Step 4: Compare — acceptable tolerances

| Field           | Tolerance | Why                                     |
|---|---|---|
| chop_avg_tr     | ±2.0 pts  | Float rounding across 5 TR bars         |
| burst_threshold | ±2.0 pts  | Derived from chop_avg_tr × 2.0          |
| atr5            | ±2.0 pts  | RMA seeded differently (backfill vs chart) |
| sl_dist         | ±1.0 pts  | Derived from atr5_prev × 0.75           |
| Signal          | exact      | Must match exactly: BUY/SELL/NONE       |

If all values are within tolerance AND signal matches → bar passes.

### Step 5: Submit Pine values (takes 30 seconds)

Use the inline form on the dashboard, or curl:

```bash
curl -X POST http://localhost:5002/parity/submit \
  -H "Content-Type: application/json" \
  -d '{
    "ts_bar": 1778607600,
    "pine_signal": "NONE",
    "pine_chop_avg_tr": 41.2,
    "pine_burst_threshold": 82.4,
    "pine_atr5": 58.9,
    "pine_sl_dist": 44.2
  }'
```

**How to get ts_bar**: check `candle_time_utc` in the dashboard table — the ts_bar is the Unix timestamp shown in the `ts_bar` column. Or copy it from the submit form which pre-fills the most recent bar.

The dashboard auto-refreshes every 30s and updates the gate counters.

### Step 6: Monitor the PARITY PASS gates

Dashboard badge turns **green** when all 4 gates pass.

Check `/parity/status` for gate details:
```json
{
  "pass_achieved": false,
  "streak": 7,
  "streak_required": 20,
  "matched_signals": 1,
  "signals_required": 3,
  "false_positives": 0,
  "false_negatives": 0,
  "gate_summary": [
    "[--] Streak 7/20 consecutive aligned",
    "[--] Signals matched 1/3",
    "[OK] False positives: 0 (need 0)",
    "[OK] False negatives: 0 (need 0)"
  ]
}
```

---

## Signal Investigation Guide

### Python fires, TradingView doesn't (False Positive)

Possible causes:
1. **Cooldown mismatch** — check `py_cooldown_left` in the row. Pine may have a different cooldown counter.
2. **chopAvgTR discrepancy** — if Python's threshold is lower, a bar that barely misses in Pine could pass in Python.
3. **EMA filter** — Pine may have EMA filter ON while Python has it OFF. Check `use_ema_filter` in config.
4. **Bar close timing** — Python emits on new-start detection. Verify Python's bar timestamps match Pine's.

Investigation steps:
```bash
curl http://localhost:5002/parity/log?n=5
# Compare py_chop_avg_tr vs pine_chop_avg_tr for the preceding 5 bars
```

### TradingView fires, Python doesn't (False Negative)

Possible causes:
1. **ATR warmup** — check `warmup_warning` in the row. If buffer has <250 bars, ATR may be inaccurate.
2. **Burst threshold higher in Python** — TR series divergence. Check bars before the signal.
3. **Session filter** — if `use_session=True` in Python but not in Pine (or vice versa).
4. **EMA direction** — if Python `above_ema=False` and EMA filter is ON.

---

## Tolerance Explanation

**Why atr5 diverges:**
Pine seeds EMA/RMA from the very first bar on the chart (potentially 6+ months ago).
Python seeds from the first bar in the 300-candle backfill (≈25 hours ago).
After 300+ bars, Wilder's RMA is 99%+ converged — difference is typically <1 ATR point.
If atr5_diff > 5 pts consistently, the backfill may be insufficient.

**Why chopAvgTR diverges:**
chopAvgTR is the mean of 5 previous True Ranges.
True Range depends on previous close (gap-filling). If a candle timestamp boundary is slightly different between Python and Pine, TRs may differ by the gap amount.
Typical divergence: <1 pt. If >3 pts, investigate timestamp alignment.

---

## Daily Workflow Summary

| Time      | Action                                              | Time cost |
|---|---|---|
| Every 5m  | Glance at dashboard top bar — signal match obvious  | 10 sec    |
| Every 5m  | Enter Pine values in submit form                    | 30 sec    |
| Any time  | Check `/parity/status` for gate progress            | 5 sec     |
| On FAIL   | Compare full row in `/parity/log` — find diverging field | 2 min |
| On PASS   | Screenshot `/parity/status`, proceed to Phase 3 planning | — |

---

## Phase 3 Gate

Once the dashboard shows **PARITY PASS** (green badge) AND the log confirms:
- ≥20 consecutive aligned
- ≥3 actual signals matched (blue/red diamonds on Pine matched Python)
- 0 false positives
- 0 false negatives

→ Proceed to Phase 3: wire execution into volsurge_v5.py (order placement).

Until then: **no execution code, no order placement, v4 on Railway unchanged.**

---

*Generated 2026-05-12 — Phase 2 parity validation*
