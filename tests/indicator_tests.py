"""
indicator_tests.py — Unit tests for signal_engine math functions.

Tests each function in isolation with known expected values.
Run from volsurge_5m/ directory:
    python -m pytest tests/indicator_tests.py -v
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from signal_engine import compute_tr, compute_atr_rma, compute_ema_series

# ── compute_tr ────────────────────────────────────────────────────────────────

def test_tr_no_gap():
    """Standard bar, no gap from previous close."""
    # high=105, low=95, prev_close=100 → H-L=10, |H-pC|=5, |L-pC|=5 → TR=10
    assert compute_tr(105, 95, 100) == 10.0

def test_tr_gap_up():
    """Gap up: prev_close far below low → |high-prev_close| dominates."""
    # high=110, low=105, prev_close=80 → H-L=5, |H-pC|=30, |L-pC|=25 → TR=30
    assert compute_tr(110, 105, 80) == 30.0

def test_tr_gap_down():
    """Gap down: prev_close far above high → |low-prev_close| dominates."""
    # high=95, low=90, prev_close=120 → H-L=5, |H-pC|=25, |L-pC|=30 → TR=30
    assert compute_tr(95, 90, 120) == 30.0

def test_tr_exact_open_prev_close():
    """No gap — open equals prev_close."""
    # high=102, low=98, prev_close=100 → H-L=4, |H-pC|=2, |L-pC|=2 → TR=4
    assert compute_tr(102, 98, 100) == 4.0


# ── compute_atr_rma ───────────────────────────────────────────────────────────

def test_atr_rma_insufficient_bars():
    """Returns zeros when fewer bars than period."""
    trs = [10.0, 12.0, 8.0]
    result = compute_atr_rma(trs, period=5)
    assert result == [0.0, 0.0, 0.0]

def test_atr_rma_exact_seed():
    """
    First valid ATR value (index period-1) should equal SMA of first period TRs.
    period=5, TRs=[10,10,10,10,10,...] → seed=10.0, all subsequent=10.0.
    """
    trs    = [10.0] * 10
    result = compute_atr_rma(trs, period=5)
    # index 0..3 = 0.0, index 4..9 = 10.0
    assert result[:4] == [0.0, 0.0, 0.0, 0.0]
    for v in result[4:]:
        assert abs(v - 10.0) < 1e-9

def test_atr_rma_known_values():
    """
    Manual calculation for period=3 with TRs=[6,8,10,12]:
      seed = (6+8+10)/3 = 8.0  → result[2] = 8.0
      result[3] = (1/3)*12 + (2/3)*8 = 4 + 5.333 = 9.333...
    """
    trs    = [6.0, 8.0, 10.0, 12.0]
    result = compute_atr_rma(trs, period=3)
    assert result[0] == 0.0
    assert result[1] == 0.0
    assert abs(result[2] - 8.0) < 1e-9
    expected_r3 = (1/3) * 12.0 + (2/3) * 8.0
    assert abs(result[3] - expected_r3) < 1e-9

def test_atr_rma_wilder_alpha():
    """
    Wilder RMA for period=5 should converge if all TRs are same value.
    After seeding, each step: alpha*v + (1-alpha)*prev = 0.2*v + 0.8*prev
    If v == prev, result stays constant.
    """
    trs    = [50.0] * 20
    result = compute_atr_rma(trs, period=5)
    for v in result[4:]:
        assert abs(v - 50.0) < 1e-9

def test_atr_rma_prev_value():
    """
    ATR at [-2] should equal ATR at current bar minus one step of RMA.
    This mirrors Pine's atr5[1] reference.
    """
    trs    = [10.0] * 20
    result = compute_atr_rma(trs, period=5)
    # Both atr[-1] and atr[-2] should be 10.0 (converged to constant input)
    assert abs(result[-1] - 10.0) < 1e-9
    assert abs(result[-2] - 10.0) < 1e-9


# ── compute_ema_series ────────────────────────────────────────────────────────

def test_ema_single_bar():
    result = compute_ema_series([100.0], length=200)
    assert result == [100.0]

def test_ema_constant_series_converges():
    """EMA of a constant series should converge to that constant."""
    prices = [100.0] * 500
    result = compute_ema_series(prices, length=200)
    assert abs(result[-1] - 100.0) < 0.1

def test_ema_alpha():
    """
    For length=3: alpha=2/4=0.5
    prices=[10,20,30]:
      ema[0]=10
      ema[1] = 0.5*20 + 0.5*10 = 15
      ema[2] = 0.5*30 + 0.5*15 = 22.5
    """
    result = compute_ema_series([10.0, 20.0, 30.0], length=3)
    assert abs(result[0] - 10.0)  < 1e-9
    assert abs(result[1] - 15.0)  < 1e-9
    assert abs(result[2] - 22.5)  < 1e-9

def test_ema_seed_from_first_price():
    """EMA series starts from first price, not zero."""
    prices = [80000.0] + [80000.0] * 10
    result = compute_ema_series(prices, length=200)
    assert result[0] == 80000.0

def test_ema_uptrend_above_price():
    """EMA200 with slow alpha — lags price significantly on fast move."""
    base   = [50000.0] * 300
    jump   = [100000.0] * 10   # sudden spike
    prices = base + jump
    result = compute_ema_series(prices, length=200)
    # After 10 bars at 100k, EMA should still be well below 100k (strong lag)
    assert result[-1] < 60000.0

def test_ema_length_200_bar_count():
    """EMA series length always equals input length."""
    prices = list(range(1, 101))
    result = compute_ema_series(prices, length=200)
    assert len(result) == len(prices)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
