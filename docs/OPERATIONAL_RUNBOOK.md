# Vol Surge v5 — Operational Runbook
## 24–48 Hour Validation Phase Before Phase 3

**Purpose:** Prove real-world stability and Pine parity before any execution code is connected.  
**Rule:** No architecture changes. No execution. No v4 Railway changes. Observe and record only.

---

## Part 1 — Every-5-Minute Operator Routine

Each 5-minute bar closes at :00, :05, :10... UTC. The full routine takes under 2 minutes.

### Step 1 — Confirm bar arrived (10 seconds)

Check the console or dashboard top line:
```
------------------------------------------------
BAR CLOSED | 17:45 UTC
SIGNAL         : NONE
...
------------------------------------------------
```

**Expected:** Bar within 5–15 seconds of close time.  
**Ignore:** Up to 20 seconds delay — normal WS propagation.  
**Flag if:** No bar for >90 seconds after expected close time.

---

### Step 2 — Read Python values (20 seconds)

From the console block or `http://localhost:5002/indicators`:

Note these four numbers:
- `chop_avg_tr`
- `burst_threshold`
- `atr5`
- `sl_dist`
- `signal` (BUY / SELL / NONE)

---

### Step 3 — Read TradingView Pine values (30 seconds)

On the same bar (same candle open time), read from the Pine status table:
- `chopAvgTR`
- `burstThreshold`
- `atr5`
- `vsSLDist`
- Did a signal diamond appear? Blue = BUY, Red = SELL.

**Which bar to read in TradingView:**  
The bar that just closed. Its open time matches `ts_bar` in Python.  
Convert: `datetime.fromtimestamp(ts_bar, utc)` or check `candle_time_utc` column on dashboard.

---

### Step 4 — Compare (30 seconds)

**Acceptable differences — do NOT flag these:**

| Field | Acceptable diff | Reason |
|---|---|---|
| `chop_avg_tr` | ±2.0 pts | Float rounding across 5 TR bars |
| `burst_threshold` | ±2.0 pts | Derived from chop_avg_tr |
| `atr5` | ±2.0 pts | RMA seed divergence (300 bars vs chart history) |
| `sl_dist` | ±1.0 pts | Derived from atr5_prev |
| Signal direction | exact match required | BUY/SELL/NONE must agree |

**Ignore completely:**
- `ema200` value differences — EMA200 will converge slowly and is filtered OFF by default. Not relevant until Phase 3.
- `bars_in_buffer` — always 300 once warm, not a parity field.
- Minor mark_price differences — tick timing, not bar logic.

---

### Step 5 — Submit Pine values (30 seconds)

Use the dashboard submit form at `http://localhost:5002/parity/dashboard`.

Pre-filled fields: `ts_bar` (latest bar), `pine_signal` (default NONE).  
Fill in Pine numeric values. Click Submit.

The dashboard updates `parity_match` and the streak counter immediately.

**When to skip submission:**
- You were away from the screen for this bar. Skip it — do not guess.
- TradingView was loading / chart was refreshing. Skip it.
- Partial bars (first bar after script reload on TV). Skip it.

Skipped bars do not break the streak — they are simply not submitted.

---

### Step 6 — Check health badge (5 seconds)

Glance at the two badges:

```
[ VALIDATING... ]   [ HEALTHY ]
```

If either badge is amber or red, pause and investigate before the next bar.  
See Part 2 for investigation checklists.

---

### What to record in notes (not in the system)

Keep a simple running note alongside the CSV for anything unusual:
```
17:45 UTC — atr5 diff 1.8 pts (within tolerance). Streak 5.
17:50 UTC — skipped, TV chart reloaded mid-bar.
17:55 UTC — BUY matched! Streak 6. Signal #1 matched.
```

---

## Part 2 — Issue Investigation Checklists

### WS FREEZE / DISCONNECTED badge

Symptoms: `DISCONNECTED` badge, `last_frame_age_s` growing, no bar-close console blocks.

```
[ ] Check dashboard: health badge = DISCONNECTED?
[ ] Check /health: ws_connected=false?
[ ] Check console/log for: [FEED] WS disconnected
[ ] Wait 60s — auto-reconnect should fire
[ ] After reconnect: check buffer_size=300 and feed_ready=true
[ ] Check gap fill: [FEED] Gap fill complete — N bars added
[ ] If reconnect takes >5 min: restart the server manually
```

**What to log:** Note UTC time of disconnect, UTC time of reconnect, bars missed.  
**Mark CRITICAL if:** Reconnect fails after 5 minutes, or buffer drops below 250 after reconnect.

---

### DUPLICATE CANDLES

Symptoms: Same `candle_time_utc` appears twice in parity_log, `[FEED] Dedup skip ts=...` in console.

```
[ ] Check for "Dedup skip" in log
[ ] Check parity_log.csv — is the same candle_time_utc present twice?
[ ] If dedup fired but no duplicate row in CSV → working as designed (dedup prevented it)
[ ] If duplicate row exists in CSV → parity_tracker dedup missed it
```

**Expected behaviour:** Dedup guard in `_emit_closed()` prevents duplicates. An occasional dedup log line is NORMAL — it means the guard worked.  
**Mark WARNING if:** Duplicate rows appear in parity_log.csv despite dedup guard.

---

### STALE BAR (>7 MINUTES SINCE LAST CLOSE)

Symptoms: `DEGRADED` badge, `[MONITOR] No bar received for N.N min` in log.

```
[ ] Is market open? Delta BTC/USD trades 24/7 — no exchange downtime expected.
[ ] Check WS connected: dashboard health badge.
[ ] Check last_frame_age_s in metrics: is WS still receiving anything?
[ ] If WS connected but no bar: Delta may have paused candlestick feed (rare)
[ ] If last_frame_age_s > 120s: WS is frozen — trigger reconnect by restarting server
[ ] If last_frame_age_s < 30s but no bar: candle detection logic issue
```

**Mark WARNING if:** >10 minutes, WS connected, frames arriving but no bar emitted.  
**Mark CRITICAL if:** >20 minutes with WS connected and frames arriving.

---

### DELAYED BAR CLOSE

Symptom: Bar-close console block arrives significantly after the expected :00/:05 mark.

```
[ ] Note actual delay in seconds (check console timestamp vs bar close time)
[ ] Delay 0–20s: NORMAL — WS propagation + tick boundary detection
[ ] Delay 20–60s: ACCEPTABLE — occasional under load
[ ] Delay >60s: Investigate. Check if 'forming' candle had stale ts.
[ ] Check if two bars fired back-to-back (catch-up after delay)
```

**Root cause of delays:** Python detects bar close when the NEXT bar's first tick arrives.  
If the market is quiet at the boundary, the first tick of the new bar may be delayed.  
This is structural — not a bug.

---

### COOLDOWN MISMATCH

Symptom: Python shows `cooldown_left > 0` but TradingView fired a signal, or vice versa.

```
[ ] Check: when did Python last fire a signal? (parity_log, last BUY/SELL row)
[ ] Count bars since that signal in both Python and Pine
[ ] Default cooldown = 3 bars. Signal bar itself counts as bar 0.
[ ] Bar N+1: cooldown_left=2, Bar N+2: cooldown_left=1, Bar N+3: cooldown_left=0 → eligible
[ ] If Pine fires on bar N+2 but Python won't: Pine may use a different cooldown default
[ ] Check Pine input: vsCoooldown parameter value
```

**Mark WARNING if:** Cooldown count differs by more than 1 bar consistently.  
**Resolution:** Verify `SignalConfig.cooldown` matches the Pine input parameter value exactly.

---

### TIMESTAMP DRIFT

Symptom: Python's `candle_time_utc` for a bar does not match TradingView's bar open time for the visually same bar.

```
[ ] Convert Python ts_bar (Unix seconds) to UTC manually: datetime.utcfromtimestamp(ts_bar)
[ ] Compare to TradingView bar open time (shown in bottom bar on hover)
[ ] Expected: exact match to the second (both are Unix-aligned 5-minute boundaries)
[ ] Off by 300s (5 min): Python may be tracking the PREVIOUS bar, not current
[ ] Off by 19800s (5h30m): IST/UTC confusion — check if TV chart is set to IST
```

**Mark WARNING if:** Timestamps are consistently off by the same delta.  
**Resolution:** Ensure TradingView chart timezone is UTC, not IST or local.

---

### TRADINGVIEW vs PYTHON SIGNAL DIVERGENCE

Symptom: `parity_match=False` with `SIGNAL_MISMATCH` in dashboard.

```
[ ] Identify which bar: ts_bar, candle_time_utc
[ ] Which direction: Python says BUY, Pine says NONE? Or Python NONE, Pine BUY?
[ ] Check py_burst_threshold vs pine_burst_threshold for this bar
[ ] If threshold_diff > 2.0: numeric divergence caused signal divergence — ATR issue
[ ] If threshold_diff < 2.0 but signals differ: check cooldown state
[ ] Check py_candle_body in parity_log — was the body right at the threshold boundary?
    (within 1-2 pts of threshold = edge case, small numeric diff can flip it)
[ ] Check py_cooldown_left: was Python in cooldown when Pine wasn't?
[ ] Check session filter: use_session=False in Python — confirm same in Pine
[ ] Check EMA filter: use_ema_filter=False in Python — confirm same in Pine
```

**Mark WARNING if:** Signal divergence with threshold_diff < 0.5 pts and no cooldown explanation.  
**Mark CRITICAL if:** 3+ signal mismatches in a row with no numeric explanation.

---

## Part 3 — Severity Classification

### INFO — Expected, no action required

| Event | Example |
|---|---|
| Bar closed normally | `BAR CLOSED \| 17:45 UTC` in console |
| Dedup guard fired | `[FEED] Dedup skip ts=1778607600` |
| Gap fill after reconnect | `[FEED] Gap fill complete — 2 bars added` |
| Parity match within tolerance | Dashboard: `PASS`, atr5_diff = 1.3 |
| Cooldown active | `cooldown_left: 2` — working as designed |
| Buffer warms up | `buffer_size: 300, feed_ready: true` at startup |
| Heartbeat log | `[FEED] heartbeat ->` |

### WARNING — Investigate but do not stop validation

| Event | Example | Action |
|---|---|---|
| Single parity mismatch | `THRESHOLD_DIFF` in one row | Note it, look for pattern across 5 bars |
| Reconnect occurred | `reconnect_count: 1` | Verify gap fill succeeded, buffer=300 |
| Bar delayed 30–90s | Console block arrives late | Note UTC, check if single occurrence |
| Stale bar 7–15 min | `[MONITOR] No bar received for 9.2 min` | Check WS health, mark bar as skipped |
| `buffer_size < 300` after reconnect | `/health` shows 285 | Wait for next bars to fill up |
| Numeric diff near tolerance | `atr5_diff: 1.9` (limit is 2.0) | Continue but watch the next 5 bars |
| Single false positive | Python BUY, Pine NONE | Investigate cooldown + threshold |

### CRITICAL — Stop and resolve before continuing parity count

| Event | Example | Action |
|---|---|---|
| WS disconnected >5 min | `DISCONNECTED` badge, no reconnect | Restart server |
| Buffer stays <250 after reconnect | `feed_ready: false` persists | Restart server, verify backfill |
| 3+ consecutive signal mismatches | Three `SIGNAL_MISMATCH` rows in a row | Stop submitting. Find root cause. |
| Duplicate rows in parity_log.csv | Same candle_time_utc appears twice | Fix dedup, clear duplicates manually |
| Bars arriving out of order | `buffer[-1]` is older than `buffer[-2]` | Restart server, investigate |
| Timestamp drift | Python bars consistently 300s off Pine | Fix chart timezone, restart |
| No signals in 500+ bars | Engine running, markets active, zero signals | Check burst_mult config vs Pine |

---

## Part 4 — Phase Gate Criteria

### "Phase 2 Stable" — Engine is healthy, continue running

All of these must be true:

```
[ ] Server running continuously ≥ 4 hours without manual restart
[ ] reconnect_count ≤ 2 per 24h period
[ ] No CRITICAL events in log
[ ] buffer_size = 300 at all times (check /health)
[ ] Bar-close console blocks arriving within 30s of expected close time
[ ] No duplicate rows in parity_log.csv
[ ] Zero WARNING-level events that are unexplained
```

### "Ready for Phase 3 Planning" — Can begin designing execution wiring

All of these must be true:

```
[ ] Phase 2 Stable conditions met
[ ] 20+ consecutive parity submissions with parity_match=True
[ ] 3+ actual BUY/SELL signals matched (not just NONE bars)
[ ] Zero false positives across entire parity_log
[ ] Zero false negatives across entire parity_log
[ ] All numeric diffs within tolerance for 20+ bars:
      chop_avg_tr:     ±2.0 pts
      burst_threshold: ±2.0 pts
      atr5:            ±2.0 pts
      sl_dist:         ±1.0 pts
[ ] Dashboard shows green PARITY PASS badge
[ ] Screenshot of /parity/status saved to docs/
```

### "Safe to Enable Paper Execution" — Orders placed in paper/sim mode only

All Phase 3 Planning criteria plus:

```
[ ] Minimum 48 hours continuous runtime
[ ] Minimum 6 matched signals (BUY + SELL both seen at least once each)
[ ] Reconnect count ≤ 3 per 24h in the most recent 48h
[ ] No CRITICAL events in the most recent 24h
[ ] All Phase 2 gate criteria still holding
[ ] Paper execution code reviewed line-by-line before enabling
[ ] A manual "kill switch" tested and confirmed working
[ ] Execution disabled by default — requires explicit env var to enable
```

### "Safe to Test Live Execution" — Real money, test size only

All Paper Execution criteria plus:

```
[ ] Paper execution ran for ≥ 5 complete trades (entry + exit) without errors
[ ] All paper trades matched expected SL/TP levels within 0.5%
[ ] No order placement errors in paper mode log
[ ] Position size set to minimum allowed (1 contract)
[ ] Manual stop tested: server killed mid-trade, position confirmed closed
[ ] Delta API credentials tested for order write access
[ ] Risk per trade confirmed ≤ 0.5% of account for first live trades
[ ] Another human has reviewed the execution logic (or you have reviewed cold)
```

---

## Part 5 — Operational Recommendations

### Minimum runtime before Phase 3

| Gate | Minimum hours | Why |
|---|---|---|
| Phase 2 Stable | 4h | Enough for reconnect test + warmup |
| Ready for Phase 3 Planning | 24h | Covers London + NY + Asian sessions |
| Safe to enable paper execution | 48h | Two full trading days, catches session edge cases |
| Safe for live test | 7 days paper execution | Statistically meaningful signal sample |

**Recommendation:** Do not rush. Each extra day of parity validation eliminates one more category of hidden divergence. There is zero cost to waiting.

---

### Minimum matched signals

| Gate | Minimum | Notes |
|---|---|---|
| Parity PASS | 3 signals | Both BUY and SELL should appear at least once each |
| Paper execution | 6 signals | Enough to see SL hit, TP hit, and time-exit cases |
| Live test | 10 signals in paper | Confidence in entry + exit handling |

**Note on signal frequency:** Vol Surge fires on burst bars only. On quiet days, 0–2 signals in 24h is normal. On volatile days, 3–6 signals. Do not increase `burst_mult` or lower cooldown to generate more signals for testing — this changes the system you are validating.

---

### Acceptable reconnect frequency

| Frequency | Classification | Action |
|---|---|---|
| 0–1 per 24h | NORMAL | No action |
| 2–3 per 24h | ACCEPTABLE | Monitor. Note UTC times. Check if same hour each day. |
| 4–6 per 24h | WARNING | Investigate network stability. Check Delta WS status page. |
| >6 per 24h | CRITICAL | Resolve before Phase 3. May indicate VPS/network issue. |

**After every reconnect:** Verify `/health` shows `buffer_size=300` and `feed_ready=true` before resuming parity submission.

---

### Acceptable latency ranges

| Metric | Green | Amber | Red |
|---|---|---|---|
| Bar-close delay | 0–20s | 20–60s | >60s |
| Last WS frame age | 0–30s | 30–60s | >60s |
| Last bar age | 0–7 min | 7–15 min | >15 min |
| Gap fill after reconnect | <5s | 5–30s | >30s |

**Bar-close delay note:** Python detects close when the NEXT bar's first tick arrives. On a quiet market, this can be 15–30 seconds after the :00 boundary. This is normal and does not affect signal accuracy — signals fire on the closed bar's data, not on tick timing.

---

### Acceptable parity tolerances

| Field | Green | Amber | Red |
|---|---|---|---|
| `chop_avg_tr` diff | <1.0 pts | 1.0–2.0 pts | >2.0 pts |
| `burst_threshold` diff | <1.0 pts | 1.0–2.0 pts | >2.0 pts |
| `atr5` diff | <1.0 pts | 1.0–2.0 pts | >2.0 pts |
| `sl_dist` diff | <0.5 pts | 0.5–1.0 pts | >1.0 pts |
| Signal match | exact | — | any mismatch |

**Why amber is not a failure:** Amber diffs can occur due to:
- ATR seed divergence (Python seeded from 300-bar backfill vs Pine's full history)
- Floating-point accumulation across Wilder's RMA

If all diffs are consistently <0.5 pts, the engines are in near-perfect sync. If diffs are consistently 1.5–1.9 pts (near limit), that's a signal to investigate seed divergence but is not a failure.

---

## Part 6 — Production Readiness Checklist

Complete this checklist in full before writing a single line of execution code.

### Feed reliability

```
[ ] Server ran ≥ 48 hours without manual restart
[ ] Total reconnect count ≤ 6 over the 48h period
[ ] buffer_size = 300 confirmed stable (no unexpected drops)
[ ] No duplicate candles in parity_log.csv (zero dedup guard surprises)
[ ] Gap fill verified to work correctly after at least one observed reconnect
[ ] bar-close timing: zero bars with >90s delay in final 24h
```

### Parity quality

```
[ ] Dashboard shows PARITY PASS badge (green)
[ ] Streak ≥ 20 consecutive aligned bars
[ ] Matched signals ≥ 3 (ideally ≥ 6, both BUY and SELL seen)
[ ] False positives = 0 across entire parity_log
[ ] False negatives = 0 across entire parity_log
[ ] All atr5_diff values in parity_log ≤ 2.0 pts
[ ] All threshold_diff values in parity_log ≤ 2.0 pts
[ ] At least one high-volatility bar included (burst that DID NOT fire — verify threshold held)
[ ] At least one confirmed signal bar included (burst that DID fire — verify match)
```

### Signal logic sanity

```
[ ] Cooldown counter matches Pine for 20+ bars after a signal
[ ] SL distance (sl_dist) within ±1 pt of Pine's vsSLDist on signal bars
[ ] Entry price = close of signal bar (confirmed in parity_log)
[ ] TP2 = entry ± (sl_dist × tp2_r) — verify manually on at least one signal
[ ] EMA filter: confirmed OFF in both Python (use_ema_filter=False) and Pine
[ ] Session filter: confirmed OFF in both Python (use_session=False) and Pine
```

### Infrastructure

```
[ ] Log file (logs-v5/volsurge_v5.log) exists and is being written
[ ] parity_log.csv and signals.csv exist in data-v5/
[ ] Server survives a kill + restart without data loss (CSV persists)
[ ] All endpoints respond: /health /status /indicators /signals /parity/dashboard
[ ] /health shows health=HEALTHY (not DEGRADED or DISCONNECTED)
[ ] No uncaught exceptions in the last 24h of logs
[ ] Monitor loop running: [MONITOR] entries visible in log every ~60s
```

### Documentation and review

```
[ ] PHASE2_BUILD_LOG.md reviewed — architecture understood
[ ] PHASE2_PARITY_LOG.md reviewed — parity system understood
[ ] PHASE2_OBSERVABILITY_LOG.md reviewed — monitoring understood
[ ] This runbook reviewed — operational discipline understood
[ ] parity_log.csv opened and reviewed manually — at least 30 rows examined
[ ] signals.csv opened and reviewed — all signals look structurally correct
[ ] Screenshot of PARITY PASS dashboard saved to docs/parity_pass_proof.png
[ ] Screenshot of /parity/status JSON saved to docs/parity_status_proof.json
```

### Pre-execution freeze (do this last)

```
[ ] v4 Railway deployment confirmed still running (check Railway dashboard)
[ ] v4 webhook confirmed still receiving TradingView alerts (check v4 /health)
[ ] v5 is LOCAL ONLY — not deployed to Railway
[ ] PAPER_MODE=true confirmed in v5 environment
[ ] No Delta API execution credentials loaded in v5 environment
[ ] Git status clean — all Phase 2 work committed
[ ] A written note exists confirming the date/time Phase 2 was declared complete
```

---

## Quick Reference Card

### Every 5 minutes

1. See bar-close block in console → note signal
2. Read same 4 values from TradingView
3. Compare — all within tolerance?
4. Submit via dashboard form
5. Glance at health badge

### When something looks wrong

- Single outlier → note it, continue
- Two consecutive mismatches → investigate before next submission
- Three mismatches or any CRITICAL → stop, diagnose, fix, restart streak

### Pass criteria (summary)

```
20+ streak + 3+ signals + 0 FP + 0 FN = PARITY PASS = Phase 3 ready
```

---

*Generated 2026-05-12 — Operational discipline guide for 24–48h validation phase*
