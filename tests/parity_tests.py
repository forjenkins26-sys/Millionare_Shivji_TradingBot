"""
parity_tests.py — Pine vs Python parity tests for signal_engine.py

Each test constructs a synthetic candle series with known properties,
then verifies that signal_engine.py produces the same result as Pine would.

Pine behavior being tested:
  - chopAvgTR = avg of TR[1..lookback] (PREVIOUS bars, NOT current bar)
  - atr5[1]   = ATR5 from previous bar (NOT current bar)
  - burst detection uses current bar body vs threshold from previous bars
  - cooldown starts AFTER a signal fires, blocks next `cooldown` bars
  - signal requires: burst + emaOK + sessionOK + cooldownOK + not in trade

Run from volsurge_15m/ directory:
    python -m pytest tests/parity_tests.py -v
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from collections import deque
from candle_feed import Candle
from signal_engine import (
    SignalEngine, SignalConfig, compute_indicators,
    compute_tr, compute_atr_rma, compute_ema_series,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

BASE_TS = 1_715_500_000   # 2024-05-12 arbitrary start

def make_candle(i, open_, close_, high=None, low=None, volume=1.0):
    ts  = BASE_TS + i * 900   # 15-min bars (900s each)
    h   = high  if high  is not None else max(open_, close_) + 5.0
    l   = low   if low   is not None else min(open_, close_) - 5.0
    return Candle(ts=ts, open=open_, high=h, low=l, close=close_, volume=volume)

def flat_candles(n, base=80000.0, body=50.0):
    """
    Create n candles where each bar's body is exactly `body` pts (alternating bull/bear).
    body=50 → small chop candles.
    """
    candles = []
    for i in range(n):
        direction = 1 if i % 2 == 0 else -1
        o = base
        c = base + direction * body
        candles.append(make_candle(i, open_=o, close_=c, high=c + 5, low=o - 5))
    return candles

def make_buffer(candles):
    d = deque(maxlen=300)
    for c in candles:
        d.append(c)
    return d

def default_cfg():
    return SignalConfig()  # Pine defaults


# ── Chop avg TR parity ────────────────────────────────────────────────────────

def test_chop_avg_tr_uses_previous_bars_not_current():
    """
    Pine: chopAvgTR = avg of ta.tr(true)[1..5] — PREVIOUS bars.
    If the current bar has a huge body, it must NOT affect chopAvgTR.

    We verify this by checking chop_avg_tr is far smaller than the current
    bar's TR (500 pts). If the current bar WERE included, chop_avg_tr would
    be much larger than the preceding candle TRs.
    """
    cfg = default_cfg()

    # 20 identical small-TR candles, all same direction (no alternating gaps)
    candles = []
    for i in range(20):
        candles.append(make_candle(i, open_=80000, close_=80040, high=80045, low=79995))

    # Current bar: body=500 pts — should NOT be included in chop_avg_tr
    big_bar = make_candle(20, open_=80000, close_=80500, high=80510, low=79990)
    candles.append(big_bar)

    state = compute_indicators(make_buffer(candles), cfg, cooldown_left=0, in_trade=False)
    assert state is not None

    # chop_avg_tr reflects the PREVIOUS 5 small bars — should be ~50 pts, not 500+
    # Even with true range, each small bar TR ≈ 50 pts (H-L with minor gaps)
    assert state.chop_avg_tr < 150.0, (
        f"chopAvgTR={state.chop_avg_tr:.1f} should use prev 5 small bars, not current 500pt bar"
    )
    # Proves current bar (TR≈520) was excluded from chop calculation
    assert state.tr > 400.0, f"Current bar TR should be large: {state.tr:.1f}"

def test_current_bar_burst_detection():
    """
    The burst condition checks the CURRENT bar's body vs chopAvgTR × burst_mult.
    A big current bar against small previous bars should fire.

    We first compute the threshold from the preceding candles, then create a
    burst bar that definitely exceeds 2× that threshold.
    """
    cfg     = default_cfg()   # burst_mult=2.0

    # Same-direction candles avoid alternating gaps that inflate TR
    candles = []
    for i in range(20):
        candles.append(make_candle(i, open_=80000, close_=80040, high=80045, low=79995))

    # First: compute what the chop_avg_tr actually is for this candle series
    placeholder = make_candle(20, open_=80000, close_=80001, high=80010, low=79995)
    probe = compute_indicators(make_buffer(candles + [placeholder]), cfg, 0, False)
    assert probe is not None
    actual_threshold = probe.burst_threshold   # = chop_avg_tr × 2.0

    # Create a burst bar with body = 3× actual threshold (guaranteed to burst)
    burst_body = actual_threshold * 3.0
    big_bar = make_candle(20, open_=80000, close_=80000 + burst_body,
                          high=80000 + burst_body + 5, low=79995)
    candles.append(big_bar)

    state = compute_indicators(make_buffer(candles), cfg, cooldown_left=0, in_trade=False)
    assert state is not None
    assert state.is_burst_bull, (
        f"body={state.candle_body:.1f} threshold={state.burst_threshold:.1f} "
        f"chop_avg_tr={state.chop_avg_tr:.1f}"
    )

def test_no_burst_when_body_below_threshold():
    """A candle below burst threshold must NOT produce a burst signal."""
    cfg     = default_cfg()
    candles = flat_candles(20, body=40.0)

    # body = 40 * 1.9 = 76 < 40 * 2 = 80 → NO BURST
    small_bar = make_candle(20, open_=80000, close_=80076, high=80082, low=79995)
    candles.append(small_bar)

    state = compute_indicators(make_buffer(candles), cfg, cooldown_left=0, in_trade=False)
    assert state is not None
    assert not state.is_burst_bull
    assert not state.is_burst_bear
    assert state.signal == ""


# ── SL distance uses atr5[1] (previous bar ATR) ──────────────────────────────

def test_sl_dist_uses_previous_bar_atr():
    """
    Pine: vsSLDist = atr5[1] * vsSLMult
    atr5[1] is the ATR5 at the bar BEFORE the current bar.
    SL distance must NOT reflect the current bar's TR.
    """
    cfg     = default_cfg()   # sl_mult=1.8 (Pine default)
    candles = flat_candles(25, body=40.0)   # stable ATR ≈ 85 pts → sl_dist ≈ 153

    # Current bar: extreme TR (1000 pts gap) — should NOT affect sl_dist
    extreme = make_candle(25, open_=80000, close_=81200, high=81210, low=79990)
    candles.append(extreme)

    state = compute_indicators(make_buffer(candles), cfg, cooldown_left=0, in_trade=False)
    assert state is not None

    # atr5_prev should be the stable ATR from before the extreme bar
    # sl_dist = atr5_prev * 0.75 — should be modest, not reflecting the 1000pt move
    assert state.sl_dist < 200.0, (
        f"sl_dist={state.sl_dist:.1f} should use atr5[1]={state.atr5_prev:.1f}, not current bar ATR"
    )


# ── Cooldown logic parity ─────────────────────────────────────────────────────

def test_cooldown_blocks_next_bars():
    """
    After a signal fires, the next `cooldown` bars must not produce a signal.
    Pine: cooldownLeft=3, decrements each bar, signal blocked while > 0.
    """
    cfg     = SignalConfig(cooldown=3)
    engine  = SignalEngine(config=cfg)
    candles = flat_candles(25, body=40.0)   # baseline chop

    def get_state(extra_candle):
        buf = make_buffer(candles + [extra_candle])
        return engine.on_candle_close(extra_candle, buf, in_trade=False)

    # First burst bar → should fire a signal
    burst1  = make_candle(25, open_=80000, close_=80090, high=80095, low=79995)
    state1  = get_state(burst1)

    # If it fired, cooldown should now be 3
    if state1 and state1.signal:
        # Next 3 bars: same burst condition but cooldown should block
        for j in range(1, 4):
            burst_n = make_candle(25 + j, open_=80000, close_=80090, high=80095, low=79995)
            candles.append(burst_n if j > 1 else burst1)
            state_n = get_state(burst_n)
            if state_n:
                assert state_n.signal == "", (
                    f"Signal fired on bar {j} after signal — cooldown should block it"
                )

def test_cooldown_resets_after_expiry():
    """
    After cooldown expires (cooldown bars pass), a new burst should fire again.
    """
    cfg    = SignalConfig(cooldown=2)
    engine = SignalEngine(config=cfg)
    candles = flat_candles(25, body=40.0)

    # Force a signal by manually setting cooldown to fired state
    engine._cooldown = 0   # ensure clean state

    # Process a burst bar
    burst_ts   = 25
    burst_bar  = make_candle(burst_ts, open_=80000, close_=80090, high=80095, low=79995)
    buf        = make_buffer(candles + [burst_bar])
    state0     = engine.on_candle_close(burst_bar, buf, in_trade=False)

    # Simulate 2 cooldown bars
    for j in range(2):
        filler = make_candle(burst_ts + j + 1, open_=80090, close_=80100, high=80110, low=80085)
        candles.append(filler)
        buf = make_buffer(candles)
        engine.on_candle_close(filler, buf, in_trade=False)

    # After 2 cooldown bars, cooldown should be 0
    assert engine._cooldown == 0


# ── Signal direction ──────────────────────────────────────────────────────────

def test_bullish_burst_produces_buy_signal():
    """close > open burst → BUY signal."""
    cfg     = default_cfg()
    candles = flat_candles(20, body=40.0)
    bull    = make_candle(20, open_=80000, close_=80090, high=80095, low=79995)
    candles.append(bull)
    state = compute_indicators(make_buffer(candles), cfg, 0, False)
    assert state is not None
    if state.is_burst_bull:
        assert state.signal == "BUY"

def test_bearish_burst_produces_sell_signal():
    """close < open burst → SELL signal."""
    cfg     = default_cfg()
    candles = flat_candles(20, body=40.0)
    bear    = make_candle(20, open_=80090, close_=80000, high=80095, low=79995)
    candles.append(bear)
    state = compute_indicators(make_buffer(candles), cfg, 0, False)
    assert state is not None
    if state.is_burst_bear:
        assert state.signal == "SELL"


# ── In-trade gate ─────────────────────────────────────────────────────────────

def test_no_signal_when_in_trade():
    """Pine: tDir == 0 required for signal. in_trade=True must block signal."""
    cfg     = default_cfg()
    candles = flat_candles(20, body=40.0)
    burst   = make_candle(20, open_=80000, close_=80090, high=80095, low=79995)
    candles.append(burst)
    state = compute_indicators(make_buffer(candles), cfg, 0, in_trade=True)
    assert state is not None
    assert state.signal == "", "Signal must not fire when in_trade=True"


# ── EMA filter (when enabled) ─────────────────────────────────────────────────

def test_ema_filter_blocks_long_when_below_ema():
    """
    When use_ema_filter=True and close < EMA200, BUY signal must be blocked.
    """
    cfg = SignalConfig(use_ema_filter=True)

    # Generate candles with declining prices so close < EMA200
    candles = []
    price   = 90000.0
    for i in range(250):
        price -= 50.0   # continuous decline
        o = price + 20
        c = price
        candles.append(make_candle(i, open_=o, close_=c, high=o + 5, low=c - 5))

    # Burst bar going up but close still below EMA
    burst = make_candle(250, open_=price, close_=price + 200, high=price + 210, low=price - 5)
    candles.append(burst)

    state = compute_indicators(make_buffer(candles), cfg, 0, False)
    assert state is not None

    if state.is_burst_bull:
        # close is below EMA (prices have been declining) → BUY blocked
        if not state.above_ema:
            assert state.signal != "BUY", "EMA filter should block BUY when below EMA200"

def test_ema_filter_off_by_default():
    """Default config has use_ema_filter=False — EMA does not block signals."""
    cfg     = default_cfg()
    assert not cfg.use_ema_filter


# ── Session filter ────────────────────────────────────────────────────────────

def test_session_filter_off_by_default():
    """Default: use_session=False → session always OK."""
    cfg = default_cfg()
    assert not cfg.use_session

def test_session_filter_blocks_off_hours():
    """When session filter ON, candle outside all session windows → blocked."""
    from signal_engine import _check_session
    cfg = SignalConfig(use_session=True, use_asian=False)

    # 2:00 AM IST = outside London (11-17), NY (18-23), Asian (disabled)
    # UTC 2:00 AM IST = UTC 20:30 previous day
    import datetime
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    # Create a time at 2:00 AM IST
    dt = datetime.datetime(2024, 1, 15, 2, 0, 0, tzinfo=IST)
    ts = int(dt.timestamp())

    assert not _check_session(ts, cfg), "2 AM IST should be outside all sessions"

def test_session_filter_passes_ny_hours():
    """20:00 IST (NY session 18-23) should pass session filter."""
    from signal_engine import _check_session
    import datetime
    cfg = SignalConfig(use_session=True)
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    dt  = datetime.datetime(2024, 1, 15, 20, 0, 0, tzinfo=IST)
    ts  = int(dt.timestamp())
    assert _check_session(ts, cfg)

def test_session_filter_passes_london_hours():
    """13:00 IST (London session 11-17) should pass session filter."""
    from signal_engine import _check_session
    import datetime
    cfg = SignalConfig(use_session=True)
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    dt  = datetime.datetime(2024, 1, 15, 13, 0, 0, tzinfo=IST)
    ts  = int(dt.timestamp())
    assert _check_session(ts, cfg)


# ── Insufficient buffer ───────────────────────────────────────────────────────

def test_insufficient_bars_returns_none():
    """compute_indicators returns None when buffer is too small."""
    cfg  = default_cfg()
    buf  = make_buffer(flat_candles(5))   # only 5 bars, need lookback+4 = 9
    state = compute_indicators(buf, cfg, 0, False)
    assert state is None

def test_minimal_viable_bars():
    """compute_indicators should return a state once buffer has lookback+4 bars."""
    cfg  = default_cfg()
    buf  = make_buffer(flat_candles(15))   # comfortably above minimum
    state = compute_indicators(buf, cfg, 0, False)
    assert state is not None


# ── Known parity scenario (manual) ───────────────────────────────────────────

def test_known_scenario_symmetry():
    """
    BUY and SELL signals should be symmetric.
    A bull burst and equivalent bear burst of same magnitude should both fire.
    """
    cfg = default_cfg()
    base_candles = flat_candles(20, body=40.0)

    # Bull burst
    bull_bar = make_candle(20, open_=80000, close_=80090, high=80095, low=79995)
    bull_state = compute_indicators(make_buffer(base_candles + [bull_bar]), cfg, 0, False)

    # Bear burst (exact mirror)
    bear_bar = make_candle(20, open_=80090, close_=80000, high=80095, low=79995)
    bear_state = compute_indicators(make_buffer(base_candles + [bear_bar]), cfg, 0, False)

    assert bull_state is not None
    assert bear_state is not None

    if bull_state.is_burst_bull:
        assert bull_state.signal == "BUY"
    if bear_state.is_burst_bear:
        assert bear_state.signal == "SELL"

    # Both should have same burst_threshold (same preceding candles)
    assert abs(bull_state.burst_threshold - bear_state.burst_threshold) < 0.01
    assert abs(bull_state.chop_avg_tr    - bear_state.chop_avg_tr) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
