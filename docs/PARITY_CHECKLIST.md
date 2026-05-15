# Vol Surge v5 — Pine Parity Validation Checklist

## How to use this checklist

1. Run `python volsurge_v5.py` locally (or check `/indicators` endpoint on Railway)
2. Open TradingView with the Pine script on BTCUSD 5m
3. After each bar closes, compare the Python log with Pine's status table

---

## Checklist — Per-Bar Comparison

For each bar, find the bar on TradingView and compare:

| Field | Pine source | Python log field | Notes |
|---|---|---|---|
| `chop_avg_tr` | Status table "Chop avg TR" | `chop_avg_tr` | Average of previous 5 TR values |
| `burst_threshold` | Status table "Burst needs X pts body" | `burst_threshold` | = chop_avg_tr × 2.0 |
| `candle_body` | Status table "Current body" | `candle_body` | = abs(close - open) of current bar |
| `is_burst_bull` | Yellow bg on bar = long burst | `is_burst_bull` | `True` when body ≥ threshold AND close > open |
| `is_burst_bear` | Orange bg on bar = short burst | `is_burst_bear` | `True` when body ≥ threshold AND close < open |
| `atr5` | Not shown directly | `atr5` | Wilder's RMA(TR, 5) |
| `atr5[1]` | Not shown directly | `atr5_prev` | Previous bar's ATR5 — used for SL distance |
| `sl_dist` | Alert payload `sl_dist` field | `sl_dist` | = atr5[1] × 0.75 |
| `ema200` | Status table "3m EMA" (above/below) | `ema200` | Compare direction (above/below), not exact value* |
| `signal` | Diamond shape on chart | `signal` | "BUY" / "SELL" / "" |

*EMA200 seed difference: Python seeds from 300-bar backfill start. Pine seeds from chart history
start (potentially years of data). Values converge after ~500 bars. At 300 bars, expect < 0.1%
difference. If use_ema_filter is OFF (default), this does NOT affect signal generation.

---

## Known Differences (expected, not bugs)

### EMA200 seed discrepancy
- **Pine**: EMA200 starts from the very first bar of BTCUSD chart history
- **Python**: EMA200 seeds from the oldest bar in the 300-bar backfill window
- **Effect**: Small difference (~0.05–0.1%) that diminishes over time
- **Impact**: Zero when `use_ema_filter=False` (default). Only matters if EMA filter enabled.
- **Fix**: Increase backfill to 500+ bars, or just accept the warmup period

### ATR5 seed discrepancy
- Same seeding issue as EMA200 but converges much faster (period=5, alpha=0.2)
- After 50 bars the difference is negligible (< 0.01%)

### Candle timestamp
- Pine uses `time` (bar open) in IST display, UTC internally
- Python uses Unix seconds (UTC). Convert with: `datetime.fromtimestamp(ts, tz=timezone.utc)`

---

## Step-by-Step Parity Test

### 1. Run Python signal engine locally
```bash
cd C:\Users\ANAND SONI\OneDrive\Desktop\TradingBots\volsurge_5m
pip install -r requirements_v5.txt
python volsurge_v5.py
```

Wait for warmup: look for `[FEED] Backfill complete — 300 candles loaded`.

### 2. Open TradingView
- Open BTCUSD on 5m timeframe
- Load "Vol Surge v5 — Chop→Spike [Standalone]" indicator
- Enable "Show status table" in settings

### 3. After each bar closes, compare

**Python log output (every bar)**:
```
[ENGINE] ── Bar #42 · 2024-05-12 17:25 UTC (22:55 IST) ──────────────────
  close          :     80,123.4
  candle body    :         87.3 pts
  chop_avg_tr    :         41.2 pts  (avg TR of 5 bars before)
  burst_threshold:         82.4 pts  (chop × 2.0)
  burst          : BULL ✓
  atr5[1]        :         58.7 pts  (prev bar — Pine atr5[1])
  sl_dist        :         44.0 pts  (= atr5[1] × 0.75)
  ema200         :     79,850.2
  session        : OK ✓  (filter OFF)
  cooldown       : READY
  ─────────────────────────────────────────────────────
  SIGNAL         : BUY
```

**TradingView status table** (top-right):
- Chop avg TR: `41.2 pts` ← should match Python
- Burst needs:  `82.4 pts body` ← should match Python
- Current body: `87.3 pts ✓` ← should match Python

**On the chart**:
- Yellow diamond shape on the burst bar = BUY signal in Pine

---

## Test Matrix

After running for 5+ bars, verify:

| Test | Expected | How to verify |
|---|---|---|
| Burst threshold matches Pine table | ± 0.1 pts | Status table "Burst needs X pts body" |
| Non-burst bar has signal="" | Correct | No diamond on chart for that bar |
| Burst bar has signal="BUY" or "SELL" | Matches diamond on chart | Compare chart vs Python log |
| sl_dist matches alert payload | ± 0.1 pts | Check TradingView alert JSON sl_dist field |
| Cooldown bars produce no signal | 3 bars blocked after signal | Compare Python log vs chart |

---

## Green / Red criteria for Phase 2 completion

✅ **PASS** if:
- `chop_avg_tr` matches TradingView within ± 1 pt for 10+ consecutive bars
- `burst_threshold` matches TradingView within ± 1 pt
- Every BUY/SELL signal in Python log has a corresponding diamond on TradingView chart
- No Python signal fires on a bar where TradingView shows no diamond
- `sl_dist` matches TradingView alert payload `sl_dist` within ± 0.5 pts

❌ **FAIL** (investigate before proceeding to Phase 3) if:
- `chop_avg_tr` differs by > 5 pts consistently (may indicate TR indexing bug)
- A Python signal fires with no corresponding TradingView diamond
- A TradingView diamond appears but Python produces no signal
- `sl_dist` differs by > 2 pts from Pine's `sl_dist` in the alert payload

---

## Phase 3 gate criteria

Before adding execution to v5:
- [ ] ≥ 20 consecutive bar closes logged without errors
- [ ] ≥ 3 signals detected by Python that match Pine (verified manually)
- [ ] EMA200 direction (above/below) matches Pine status table for all 20 bars
- [ ] No false positives (Python signals with no Pine diamond) observed
- [ ] `sl_dist` within ± 1 pt of Pine alert payload for all signals
