# Vol Surge v5 — Automated Parity Validation Architecture
## Design Document (Pre-Implementation)

**Date:** 2026-05-12  
**Status:** Design only — no implementation yet  
**Scope:** Full automation of Pine ↔ Python parity, zero manual candle comparison  
**Constraint:** No execution. No v4 changes. Parity + observability only.

---

## 1. Architecture Evaluation

### Options Considered

| Option | Approach | Verdict |
|---|---|---|
| A — TradingView webhook parity channel | Pine fires `alert()` webhook on every bar close | **Primary path — recommended** |
| B — Pine `alert()` JSON parity snapshots | Design of the JSON payload Pine constructs | **Combined with A** |
| C — Screenshot / OCR | Headless browser screenshots TradingView | Rejected — fragile, breaks on TV layout changes |
| D — Browser automation (Selenium / Playwright) | Reads TradingView DOM | Rejected — TV uses canvas rendering, not accessible DOM |
| E — Lightweight Pine telemetry export | Pine writes to status table, script reads it | Rejected — still requires browser automation to extract |
| F — Hybrid (webhook primary + manual fallback) | A+B as primary, manual `/parity/submit` as backup | **Recommended** |

### Why Option F is the right answer

TradingView exposes exactly one reliable, documented mechanism for exporting data out of Pine: the `alert()` function with webhook delivery. Everything else (OCR, browser automation, DOM scraping) is brittle and breaks whenever TradingView updates their UI.

The hybrid approach (F) adds resilience: if a Pine alert is delayed, dropped, or missed due to TradingView infrastructure issues, the manual submit path remains available as a fallback. Once Pine webhooks are confirmed stable over 48h, the manual path becomes emergency-only.

---

## 2. Full Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     TradingView (Pine)                          │
│                                                                 │
│  barstate.isconfirmed                                           │
│       │                                                         │
│       ▼                                                         │
│  parity_json = build_json(chopAvgTR, burstThreshold, atr5 ...)  │
│       │                                                         │
│       ▼                                                         │
│  alert(parity_json, alert.freq_once_per_bar_close)              │
│       │                                                         │
│  TradingView alert delivery (~1–10s delay)                      │
└───────┼─────────────────────────────────────────────────────────┘
        │  HTTPS POST  (shared secret in header)
        ▼
┌─────────────────────────────────────────────────────────────────┐
│            v5 Python Server (Railway or ngrok)                  │
│                                                                 │
│  POST /parity/pine-webhook                                      │
│       │                                                         │
│       ▼                                                         │
│  Validate secret → Parse Pine JSON → Normalise ts_bar           │
│       │                                                         │
│       ├── Python row already exists for this ts_bar?            │
│       │      YES → auto_compare() → severity → parity_log.csv  │
│       │      NO  → buffer Pine snapshot (Python not closed yet) │
│       │                                                         │
│  on_candle_close (WebSocket bar close)                          │
│       │                                                         │
│       ├── Python row written to parity_log                      │
│       ├── Buffered Pine snapshot found? → auto_compare()        │
│       └── No Pine snapshot yet? → row waits (Pine arrives soon) │
│                                                                 │
│  auto_compare()                                                 │
│       │                                                         │
│       ├── signal_match, atr5_diff, threshold_diff, sl_dist_diff │
│       ├── severity: GREEN / YELLOW / RED                        │
│       ├── confidence_score update                               │
│       ├── mismatch_explanation generation                       │
│       ├── anomaly_feed append (RED/YELLOW events only)          │
│       └── Telegram alert if RED                                 │
│                                                                 │
│  daily_report_task (runs at 00:00 UTC)                          │
│       └── /parity/report → summary JSON + Telegram             │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
  /parity/dashboard — fully automated, no manual input needed
  parity_log.csv   — all rows auto-filled, no operator submissions
```

---

## 3. Risk Analysis

### Risk 1 — Pine alert timing vs Python bar close timing

**The problem:**  
Pine fires its alert at `barstate.isconfirmed`. Python detects bar close when the NEXT bar's first tick arrives (structural delay of 0–30s). TradingView then delivers the webhook (additional 1–10s delay). The two events arrive at the server out of order about 20–40% of the time on a quiet market.

**Mitigation:**  
Match Pine and Python snapshots by `ts_bar` (the candle's Unix timestamp), never by arrival time. Use a `_pending_pine` buffer on the server: if Pine's webhook arrives before the Python bar is written, hold it for up to 10 minutes. When Python's bar closes, check the buffer first. After 10 minutes, log `PINE_ORPHAN` and discard.

**Residual risk:** Low. Timestamp matching is deterministic.

---

### Risk 2 — Pine alert delivery reliability

**The problem:**  
TradingView can delay or drop alerts under heavy load. TV's SLA for webhook delivery is "best effort." On a free/Pro account, alerts may fire late. On Pro+/Premium, delivery is much more reliable.

**Mitigation:**  
After each bar, the Python side checks: did a Pine snapshot arrive for this ts_bar within 10 minutes? If not, the row is marked `pine_status=MISSING` in the CSV. Missing rows do not break the streak counter — they are excluded from alignment calculations. A `PINE_MISSING` anomaly is logged if >3 consecutive bars are missing.

**Residual risk:** Medium. If Pine alerts fail frequently, automation degrades to the manual path. Resolved by deploying on a plan with reliable webhook delivery (TV Pro+ or Premium).

---

### Risk 3 — Public URL requirement

**The problem:**  
TradingView webhooks require a publicly reachable HTTPS URL. v5 currently runs locally.

**Options in order of preference:**

| Option | Effort | Reliability | Cost |
|---|---|---|---|
| Deploy v5 to Railway (new service) | Low — already use Railway | High | Railway Pro ~$5/mo |
| Cloudflare Tunnel (`cloudflared`) | Low — one command | High | Free |
| ngrok | Very low | Medium (tunnels reset) | Free (with restart limits) |
| Dynamic DNS + port forward | Medium | Medium | Free |

**Recommended:** Cloudflare Tunnel for local testing (zero config, stable URL), Railway deployment for production (already the deployment platform).

**Residual risk:** Low once one of the above is set up.

---

### Risk 4 — Pine JSON construction errors

**The problem:**  
Pine has no native JSON serialiser. The JSON string must be manually concatenated using `str.tostring()`. A single typo (missing comma, wrong bracket) produces a malformed payload.

**Mitigation:**  
The Python webhook receiver validates JSON parsing and schema. Malformed payloads are logged as `PINE_MALFORMED` and discarded — they do not crash the server. The Pine string is tested offline before deployment.

**Residual risk:** Low once the Pine string is validated.

---

### Risk 5 — TradingView alert rate limits

**The problem:**  
BTCUSD 5m = ~288 bars/day. TradingView alert limits:
- Free: 1 alert max total
- Pro: 20 alerts
- Pro+: unlimited bars per alert
- Premium: unlimited

**Mitigation:**  
This requires Pro+ or Premium for automated operation. On Pro+, a single alert condition fires on every confirmed bar with `alert.freq_once_per_bar_close` — this counts as one alert slot, unlimited firings per day.

**Residual risk:** Blocking on free/Pro plans. Non-issue on Pro+/Premium.

---

### Risk 6 — Shared secret visibility in Pine

**The problem:**  
The webhook authentication token must be embedded in the Pine alert payload or TradingView alert webhook URL. TradingView stores this in alert settings (visible to anyone with account access).

**Mitigation:**  
Use a simple bearer token in the `X-Parity-Token` header (set in the TradingView alert webhook URL as a query param). Rotate the token if the script is published or account is compromised. This is low risk for a personal, non-published script.

**Residual risk:** Acceptable for a personal bot.

---

### Risk 7 — Mismatched Pine script version

**The problem:**  
If the Pine script on TradingView is updated (inputs changed, logic modified) without updating `SignalConfig` in Python, the auto-comparison will produce systematic mismatches.

**Mitigation:**  
The Pine telemetry payload includes a `version` field (`"v":"5.2"` or similar). If the Python server sees an unexpected version, it logs `PINE_VERSION_MISMATCH` and suspends auto-comparison until the mismatch is resolved.

**Residual risk:** Low with version field in place.

---

## 4. Implementation Plan

### Phase A — Server-side automation (no Pine changes, no public URL yet)

All work stays in v5 locally. Manual `/parity/submit` still used. This phase makes the server ready to receive automated Pine data.

**Files to create/modify:**

| File | Change |
|---|---|
| `parity_tracker.py` | Add `auto_compare()`, severity classification, confidence score, anomaly feed, `_pending_pine` buffer, `daily_summary()` |
| `volsurge_v5.py` | Add `POST /parity/pine-webhook`, daily report task, `/parity/report`, `/parity/anomalies`, updated dashboard |

**Deliverables:**
- `auto_compare()` works correctly when called with a Pine dict
- Severity classification (GREEN / YELLOW / RED) working
- Confidence score updating per bar
- Anomaly feed populated on YELLOW/RED events
- Dashboard shows automated parity feed + anomaly section
- Tested via manual curl calls simulating Pine webhook

**Completion criteria:** All existing 57 tests pass. New unit tests for `auto_compare()` pass. Dashboard renders all new sections.

---

### Phase B — Pine alert modification

Modify the Pine script to emit parity telemetry on every confirmed bar.

**Files to modify:**
- The Pine script on TradingView (add `alert()` call)

**Deliverables:**
- Pine emits JSON parity snapshot on every `barstate.isconfirmed`
- JSON payload schema matches what Python expects
- Alert configured in TradingView to hit a test URL (ngrok or Cloudflare Tunnel)
- First 10 bars verified: Pine payload arrives, Python auto-matches

**Completion criteria:** 10 consecutive auto-matched bars in parity_log.csv with `pine_status=AUTO`.

---

### Phase C — Public deployment + Telegram alerts

**Files to modify:**
- Railway: create new v5 service (or promote local to Railway)
- `volsurge_v5.py`: add Telegram alert on RED conditions

**Deliverables:**
- v5 publicly accessible via Railway URL or Cloudflare Tunnel
- TradingView alert webhook URL updated to production URL
- Telegram message fires on first RED anomaly
- Daily summary report fires at 00:00 UTC to Telegram

**Completion criteria:** End-to-end test — force a RED condition manually, Telegram alert received within 60s.

---

### Phase D — Migration from manual to automated

**Process:**
1. Run both paths in parallel for 48h: Pine webhooks auto-fill, operator also manually submits for spot-checks
2. Compare auto vs manual rows: do they agree?
3. After 48h with zero discrepancy: disable manual-only workflow, keep manual submit as emergency fallback
4. Update OPERATIONAL_RUNBOOK to reflect automated workflow

**Completion criteria:** 48h of autonomous operation, zero discrepancies between auto and manual, operator reduced to reviewing anomaly feed only (not every candle).

---

## 5. Required Pine Modifications

### What to add to the Pine script

```pine
// ── Automated parity telemetry ──────────────────────────────────────────────
// Add near bottom of script, after all indicator calculations

// Signal string
var string _sig = "NONE"
if buySignal
    _sig := "BUY"
else if sellSignal
    _sig := "SELL"
else
    _sig := "NONE"

// Build JSON parity payload
if barstate.isconfirmed
    string _json =
        '{"v":"1"'                                                         +
        ',"ts":'     + str.tostring(math.round(time / 1000))              +
        ',"sig":"'   + _sig                                                + '"' +
        ',"cat":'    + str.tostring(chopAvgTR,      "#.##")               +
        ',"bt":'     + str.tostring(burstThreshold, "#.##")               +
        ',"atr5":'   + str.tostring(atr5,           "#.##")               +
        ',"a5p":'    + str.tostring(atr5[1],        "#.##")               +
        ',"sld":'    + str.tostring(vsSLDist,       "#.##")               +
        ',"cd":'     + str.tostring(cooldownLeft,   "#")                  +
        ',"ema":'    + str.tostring(ema200,         "#.##")               +
        ',"abv":'    + (close > ema200 ? "true" : "false")                +
        ',"ses":'    + (sessionOK ? "true" : "false")                     +
        ',"body":'   + str.tostring(math.abs(close - open), "#.##")       +
        '}'
    alert(_json, alert.freq_once_per_bar_close)
```

### TradingView alert configuration

```
Alert name:    Vol Surge v5 Parity Telemetry
Condition:     [your indicator] — any alert() call
Webhook URL:   https://your-v5-url.railway.app/parity/pine-webhook?token=YOUR_SECRET
Message:       (leave empty — payload comes from alert() call above)
Expiry:        Open-ended
Frequency:     Once per bar close (set automatically by alert.freq_once_per_bar_close)
```

**Important:** The `?token=YOUR_SECRET` in the URL is how Python authenticates the request. TradingView sends the full URL including query params to the webhook endpoint.

### Pine variable name mapping

| Pine variable | JSON key | Notes |
|---|---|---|
| `time / 1000` | `ts` | Unix seconds. Pine `time` is milliseconds. |
| signal condition | `sig` | Construct string from buySignal / sellSignal bools |
| `chopAvgTR` | `cat` | Average of TR[1..lookback] |
| `burstThreshold` | `bt` | chopAvgTR × burstMult |
| `atr5` | `atr5` | Current bar ATR5 |
| `atr5[1]` | `a5p` | Previous bar ATR5 |
| `vsSLDist` | `sld` | atr5[1] × slMult |
| `cooldownLeft` | `cd` | Bars remaining in cooldown |
| `ema200` | `ema` | EMA200 value |
| `close > ema200` | `abv` | Boolean |
| `sessionOK` | `ses` | Boolean |
| `math.abs(close - open)` | `body` | Candle body size |
| `"1"` (literal) | `v` | Payload schema version |

---

## 6. Required v5 Modifications

### 6.1 `parity_tracker.py` additions

**New method: `receive_pine(pine_dict) → Optional[ParityRow]`**

```
Input:  parsed Pine JSON dict
Logic:
  1. Extract ts_bar from pine_dict["ts"]
  2. If Python row exists for ts_bar: call _auto_compare(row, pine_dict), return row
  3. If Python row does NOT exist: store in _pending_pine[ts_bar], return None
     (Python bar will claim it when it closes)
```

**New method: `_auto_compare(row, pine_dict)`**

```
Fills Pine fields on the row.
Computes signal_match, atr5_diff, threshold_diff, sl_dist_diff.
Computes severity (GREEN/YELLOW/RED).
Computes confidence_score update.
Generates mismatch_explanation string.
Appends to anomaly_feed if YELLOW or RED.
Sets pine_status = "AUTO".
Rewrites CSV.
```

**New method: `daily_summary() → dict`**

```
Returns:
  bars_logged, bars_with_pine, bars_missing_pine,
  green_count, yellow_count, red_count,
  parity_rate (green / with_pine),
  signal_match_rate,
  avg_atr5_diff, avg_threshold_diff,
  confidence_score (current),
  streak (current),
  anomaly_count,
  date
```

**New field: `confidence_score` (0–100)**

```
Starts at 70 (neutral — no data).
+1 per consecutive GREEN bar (capped: max +30 total bonus)
-5 per YELLOW event
-15 per RED event
-8 per PINE_MISSING bar (data gap)
Never goes below 0 or above 100.
```

**New field: `anomaly_feed` (list of dicts, last 50)**

```
Each entry: { ts, candle_time_utc, severity, reasons, py_signal, pine_signal, diffs }
Only populated for YELLOW and RED events.
Used by /parity/anomalies endpoint and dashboard anomaly panel.
```

---

### 6.2 `volsurge_v5.py` additions

**New endpoint: `POST /parity/pine-webhook`**

```
1. Extract token from query param ?token=...
2. Compare to PARITY_SECRET env var. 401 if mismatch.
3. Parse request body as JSON.
4. Validate schema: must have "ts", "sig", "cat", "bt", "atr5", "sld".
5. Call tracker.receive_pine(pine_dict).
6. If RED severity result: trigger Telegram alert (async, non-blocking).
7. Return {"ok": true, "status": result_status}.
```

**New endpoint: `GET /parity/report`**

```
Returns tracker.daily_summary() as JSON.
Also available as: /parity/report?format=text for plain text (Telegram-friendly).
```

**New endpoint: `GET /parity/anomalies`**

```
Returns tracker.anomaly_feed (last 50 events), newest first.
Filtered by: ?severity=RED, ?severity=YELLOW, ?since=ts
```

**Updated `on_candle_close`:**

```
After tracker.log_bar(state):
  if ts_bar in tracker._pending_pine:
      pine_dict = tracker._pending_pine.pop(ts_bar)
      tracker._auto_compare(row, pine_dict)
```

**Background task: `_daily_report_task`**

```
Fires every 24h at 00:00 UTC.
Calls tracker.daily_summary().
Sends to Telegram if TELEGRAM_TOKEN env var is set.
Writes to data-v5/daily_reports/YYYY-MM-DD.json.
```

---

## 7. Example Telemetry Payloads

### Pine → Python (webhook body)

```json
{
  "v":    "1",
  "ts":   1778607600,
  "sig":  "BUY",
  "cat":  41.23,
  "bt":   82.46,
  "atr5": 58.91,
  "a5p":  57.12,
  "sld":  42.84,
  "cd":   0,
  "ema":  80215.40,
  "abv":  true,
  "ses":  true,
  "body": 87.30
}
```

### Python auto-comparison result (parity_log row after auto-compare)

```json
{
  "candle_time_utc":      "2026-05-12 17:40 UTC",
  "ts_bar":               1778607600,
  "py_signal":            "BUY",
  "py_chop_avg_tr":       41.10,
  "py_burst_threshold":   82.20,
  "py_atr5":              58.88,
  "py_atr5_prev":         57.09,
  "py_sl_dist":           42.82,
  "pine_signal":          "BUY",
  "pine_chop_avg_tr":     41.23,
  "pine_burst_threshold": 82.46,
  "pine_atr5":            58.91,
  "pine_sl_dist":         42.84,
  "signal_match":         true,
  "atr5_diff":            0.03,
  "threshold_diff":       0.26,
  "sl_dist_diff":         0.02,
  "parity_match":         true,
  "severity":             "GREEN",
  "pine_status":          "AUTO",
  "confidence_delta":     "+1",
  "pine_submitted_at":    "2026-05-12T17:41:23Z"
}
```

### RED anomaly (signal mismatch)

```json
{
  "ts":              1778609100,
  "candle_time_utc": "2026-05-12 18:05 UTC",
  "severity":        "RED",
  "reasons":         ["SIGNAL_MISMATCH"],
  "py_signal":       "BUY",
  "pine_signal":     "NONE",
  "explanation":     "Python fired BUY but Pine did not. threshold_diff=0.31 (within tol). Possible cooldown mismatch: py_cooldown_left=0, pine cooldown unknown. Check Pine cooldown input vs SignalConfig.cooldown=3.",
  "diffs": {
    "atr5_diff":       0.31,
    "threshold_diff":  0.31,
    "sl_dist_diff":    0.09
  }
}
```

### Daily summary report (00:00 UTC)

```json
{
  "date":              "2026-05-12",
  "bars_logged":       288,
  "bars_with_pine":    284,
  "bars_missing_pine": 4,
  "green":             279,
  "yellow":            4,
  "red":               1,
  "parity_rate":       98.2,
  "signal_match_rate": 100.0,
  "avg_atr5_diff":     0.42,
  "avg_threshold_diff":0.61,
  "confidence_score":  87,
  "streak":            48,
  "signals_detected":  3,
  "signals_matched":   3,
  "false_positives":   0,
  "false_negatives":   0,
  "anomalies":         1
}
```

---

## 8. Severity Classification (Automated)

### GREEN — no action

```
signal_match = True
AND atr5_diff ≤ 1.0
AND threshold_diff ≤ 1.0
AND sl_dist_diff ≤ 0.5
```

All values well within tolerance. Engine in sync.

### YELLOW — log, no alert

```
signal_match = True
AND (1.0 < atr5_diff ≤ 2.0
  OR 1.0 < threshold_diff ≤ 2.0
  OR 0.5 < sl_dist_diff ≤ 1.0)
```

Signals agree but numeric values drifting toward tolerance limit. Watch for trend.

Also YELLOW if:
- `pine_status = MISSING` (Pine alert did not arrive within 10 min)
- `pine_status = LATE` (Pine alert arrived 5–10 min after bar close)

### RED — Telegram alert immediately

```
signal_match = False                     (SIGNAL_MISMATCH)
OR atr5_diff > 2.0                       (ATR_DRIFT)
OR threshold_diff > 2.0                  (THRESHOLD_DRIFT)
OR sl_dist_diff > 1.0                    (SL_DIST_DRIFT)
OR 3+ consecutive YELLOW (trending)      (DRIFT_TREND)
OR 3+ consecutive PINE_MISSING           (FEED_GAP)
OR pine_version != expected              (VERSION_MISMATCH)
```

A RED event does NOT automatically reset the streak. The streak is based on `parity_match` (signal match + numeric within tolerance), which a YELLOW does not break. Only a signal mismatch or numeric violation breaks the streak.

### Alert suppression

Do not re-alert for the same RED condition type if it fired within the last 30 minutes. One Telegram message per RED event class per 30 min window.

---

## 9. Confidence Score

Tracks overall parity health as a single number (0–100).

```
Starting value:  70  (neutral — waiting for data)

Per bar:
  GREEN:          +1  (max bonus capped at +30 from baseline)
  YELLOW:         -5
  RED:            -15
  PINE_MISSING:   -3

Floor: 0    Ceiling: 100
```

**Interpretation:**

| Score | Meaning |
|---|---|
| 90–100 | Excellent parity. High confidence in signal engine. |
| 75–89 | Good parity. Minor drift, within tolerance. |
| 60–74 | Acceptable. Some YELLOW events. Monitor. |
| 45–59 | Concerning. Multiple YELLOW or one RED. Investigate. |
| <45 | Poor parity. Do not proceed to Phase 3. |

**Phase 3 gate:** Confidence score ≥ 85 required, sustained over 24h.

---

## 10. Updated Dashboard Design

The dashboard at `/parity/dashboard` gains three new sections:

### Section 1: Automation status bar (top)

```
[ PARITY PASS ]  [ HEALTHY ]  [ CONFIDENCE: 87 ]  [ PINE: AUTO ]
Auto-parity running — last Pine telemetry: 3m 12s ago
```

Four badges:
1. Parity pass (green/amber)
2. Feed health (green/amber/red)
3. Confidence score (color-coded by range)
4. Pine telemetry status: `AUTO` (green) / `MANUAL` (amber) / `MISSING` (red)

### Section 2: Live metrics (unchanged, existing)

### Section 3: Anomaly feed (new)

A rolling list of the last 10 YELLOW/RED events:

```
┌────────────────────────────────────────────────────────────┐
│ ANOMALY FEED                                               │
├───────────┬──────────┬────────────────────────────────────┤
│ 18:05 UTC │ RED      │ SIGNAL_MISMATCH: py=BUY pine=NONE  │
│ 16:20 UTC │ YELLOW   │ ATR_DIFF: 1.82 pts (limit 2.0)    │
│ 14:55 UTC │ YELLOW   │ PINE_MISSING                       │
└───────────┴──────────┴────────────────────────────────────┘
```

No entry = "No anomalies in last 50 bars" (this is the target state).

### Section 4: Parity table (enhanced)

Same as before, plus:
- New column: `Severity` (colored GREEN/YELLOW/RED dot)
- New column: `Pine Status` (AUTO / MANUAL / MISSING / LATE)
- `parity_match=True` rows in deep green tint (not just text color)
- Rows with `pine_status=MISSING` in grey tint (not red — data gap, not failure)

### Section 5: Manual submit form (unchanged, demoted to bottom)

Kept as emergency fallback. Label updated: "Manual override (use only if Pine webhook missed this bar)."

---

## 11. Migration Plan: Manual → Automated

### Week 1: Parallel operation

- Pine webhook active, auto-filling parity_log
- Operator also manually submits for ~10 bars/day as spot-check
- Compare: `pine_status=AUTO` rows vs `pine_status=MANUAL` rows — do they agree?
- Expected: 100% agreement (same underlying values)

### Week 2: Reduce manual to spot-checks only

- Manual submission reduced to:
  - Any RED anomaly bar (to verify auto-comparison is correct)
  - Once per session (London open, NY open) as sanity check
  - Any bar where Pine telemetry arrived LATE or MISSING

### Week 3: Full automation

- Manual submissions: emergency fallback only
- Human reviews: anomaly feed only (not the full table)
- Operator routine changes from "every 5 min" to "once per session":
  1. Open dashboard
  2. Check confidence score and anomaly feed
  3. If anomaly feed is empty → done (30 seconds)
  4. If anomaly feed has RED → investigate the specific bars

### Signs that automation is working correctly

```
[ ] parity_log has zero MANUAL rows for 24h+
[ ] Pine telemetry arriving for ≥ 95% of bars (≤ 5% MISSING)
[ ] Confidence score stable ≥ 85 for 24h
[ ] Anomaly feed empty or only YELLOW for 24h
[ ] Daily report generates correctly at 00:00 UTC
[ ] Telegram receives daily report
```

---

## 12. What This Does NOT Change

This entire automation layer is observability infrastructure:

- No execution code
- No order placement
- No SL/TP management
- No Delta API write operations
- v4 Railway deployment: completely unchanged
- `SignalConfig`: completely unchanged
- `SignalEngine`: completely unchanged
- `CandleFeed`: completely unchanged
- All 57 existing tests: pass without modification

The Pine script gains one `alert()` call at the bottom. The signal logic is not modified.

---

## 13. Implementation Sequence Checklist

### Before writing any code

```
[ ] Confirm TradingView account plan supports unlimited webhook firings
    (requires Pro+ or Premium — check at tradingview.com/pricing)
[ ] Identify deployment path for public URL (Railway new service or Cloudflare Tunnel)
[ ] Set PARITY_SECRET env var (generate a random 32-char token)
[ ] Read existing parity_tracker.py to confirm auto_compare integration points
[ ] Read existing volsurge_v5.py to confirm webhook endpoint placement
```

### Phase A implementation order

```
[ ] 1. Add severity + confidence_score + anomaly_feed to parity_tracker.py
[ ] 2. Add receive_pine() + _auto_compare() to parity_tracker.py
[ ] 3. Add pending_pine buffer to parity_tracker.py
[ ] 4. Add daily_summary() to parity_tracker.py
[ ] 5. Write unit tests for auto_compare() (GREEN, YELLOW, RED cases)
[ ] 6. All 57 existing tests still pass
[ ] 7. Add POST /parity/pine-webhook to volsurge_v5.py
[ ] 8. Add GET /parity/anomalies to volsurge_v5.py
[ ] 9. Add GET /parity/report to volsurge_v5.py
[ ] 10. Add _daily_report_task to volsurge_v5.py startup
[ ] 11. Update on_candle_close to check _pending_pine buffer
[ ] 12. Update dashboard HTML with new sections
[ ] 13. Test Phase A end-to-end: curl POST /parity/pine-webhook with sample JSON
[ ] 14. Verify parity_log.csv auto-fills correctly
[ ] 15. Verify anomaly feed populates on RED test payload
```

### Phase B (Pine + public URL)

```
[ ] 16. Set up Cloudflare Tunnel or Railway deployment
[ ] 17. Add alert() call to Pine script (do NOT change any signal logic)
[ ] 18. Configure TradingView alert with webhook URL
[ ] 19. Verify first 5 auto-matched bars in parity_log.csv
[ ] 20. Verify Pine timestamps match Python timestamps exactly
```

### Phase C (Telegram + daily report)

```
[ ] 21. Add Telegram RED alert to /parity/pine-webhook handler
[ ] 22. Test: force a RED payload, confirm Telegram message
[ ] 23. Verify daily report fires at 00:00 UTC
[ ] 24. Verify daily_reports/ directory populated
```

---

*Generated 2026-05-12 — Full automation architecture design. Implementation begins after design review.*
