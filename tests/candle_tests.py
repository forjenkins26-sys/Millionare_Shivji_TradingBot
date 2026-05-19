"""
candle_tests.py — Unit tests for CandleFeed buffer and candle handling.

Tests buffer behavior, dedup, candle parsing, and WebSocket message handling
without requiring a live connection.

Run from volsurge_5m/ directory:
    python -m pytest tests/candle_tests.py -v
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from collections import deque
from candle_feed import Candle, CandleFeed


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_candle(ts, close, open_=None, high=None, low=None, volume=1.0):
    o = open_ if open_ is not None else close - 10
    return Candle(
        ts=ts, open=o, high=high or close + 5,
        low=low or close - 15, close=close, volume=volume
    )

def make_feed(buffer_size=300):
    return CandleFeed(buffer_size=buffer_size)


# ── Candle dataclass ──────────────────────────────────────────────────────────

def test_candle_repr():
    c = make_candle(1715500000, close=80000.0)
    r = repr(c)
    assert "80000" in r
    assert "UTC" in r

def test_candle_to_dict():
    c = make_candle(1715500000, close=80050.0)
    d = c.to_dict()
    assert d["ts"]    == 1715500000
    assert d["close"] == 80050.0
    assert "open" in d and "high" in d and "low" in d


# ── Ring buffer ───────────────────────────────────────────────────────────────

def test_buffer_max_size():
    """Ring buffer should not exceed maxlen."""
    feed = make_feed(buffer_size=10)
    for i in range(20):
        feed._emit_closed(make_candle(1000 + i * 300, close=80000.0 + i))
    assert len(feed.buffer) == 10

def test_buffer_oldest_drops():
    """When full, oldest candle is dropped."""
    feed = make_feed(buffer_size=5)
    for i in range(6):
        feed._emit_closed(make_candle(1000 + i * 300, close=80000.0 + i))
    # ts=1000 (first) should be gone; ts=1300 (second) should be oldest
    assert feed.buffer[0].ts == 1300

def test_buffer_newest_last():
    """Most recent closed candle should be last in buffer."""
    feed = make_feed(buffer_size=300)
    for i in range(5):
        feed._emit_closed(make_candle(1000 + i * 300, close=80000.0 + i))
    assert feed.buffer[-1].ts == 1000 + 4 * 300
    assert feed.buffer[-1].close == 80004.0


# ── Dedup guard ───────────────────────────────────────────────────────────────

def test_dedup_same_ts():
    """Emitting the same ts twice should only add one entry."""
    feed  = make_feed()
    c1    = make_candle(1715500000, close=80000.0)
    c2    = make_candle(1715500000, close=80050.0)   # same ts, different close
    feed._emit_closed(c1)
    feed._emit_closed(c2)
    assert len(feed.buffer) == 1
    assert feed.buffer[-1].close == 80000.0   # first one kept

def test_dedup_older_ts():
    """Emitting an older ts after a newer one should be ignored."""
    feed = make_feed()
    feed._emit_closed(make_candle(1715500600, close=80100.0))
    feed._emit_closed(make_candle(1715500000, close=80000.0))   # older
    assert len(feed.buffer) == 1
    assert feed.buffer[-1].ts == 1715500600


# ── Callback ─────────────────────────────────────────────────────────────────

def test_callback_fires_on_close():
    """on_candle_close callback should be called once per closed candle."""
    received = []
    feed = CandleFeed(on_candle_close=lambda c, b: received.append(c.ts))
    for i in range(3):
        feed._emit_closed(make_candle(1000 + i * 300, close=80000.0))
    assert received == [1000, 1300, 1600]

def test_callback_receives_correct_buffer():
    """Callback buffer should contain all candles up to and including current."""
    sizes = []
    feed = CandleFeed(
        buffer_size=300,
        on_candle_close=lambda c, b: sizes.append(len(b))
    )
    for i in range(5):
        feed._emit_closed(make_candle(1000 + i * 300, close=80000.0))
    assert sizes == [1, 2, 3, 4, 5]

def test_callback_exception_does_not_crash_feed():
    """A callback that raises should not prevent buffer update."""
    def bad_callback(c, b):
        raise ValueError("intentional error")

    feed = CandleFeed(on_candle_close=bad_callback)
    # Should not raise
    feed._emit_closed(make_candle(1000, close=80000.0))
    assert len(feed.buffer) == 1   # buffer updated despite callback crash


# ── REST candle parsing ───────────────────────────────────────────────────────

def test_parse_rest_candle_standard():
    feed  = make_feed()
    raw   = {"time": 1715500000, "open": "80000", "high": "80100", "low": "79900", "close": "80050", "volume": "1.5"}
    c     = feed._parse_rest_candle(raw)
    assert c is not None
    assert c.ts    == 1715500000
    assert c.open  == 80000.0
    assert c.close == 80050.0
    assert c.volume == 1.5

def test_parse_rest_candle_ms_timestamp():
    """Delta may return timestamps in milliseconds — should convert to seconds."""
    feed = make_feed()
    raw  = {"time": 1715500000000, "open": "80000", "high": "80100", "low": "79900", "close": "80050", "volume": "1"}
    c    = feed._parse_rest_candle(raw)
    assert c is not None
    assert c.ts == 1715500000

def test_parse_rest_candle_start_field():
    """Some Delta endpoints use 'start' instead of 'time'."""
    feed = make_feed()
    raw  = {"start": 1715500000, "open": "79900", "high": "80200", "low": "79800", "close": "80100", "volume": "0.5"}
    c    = feed._parse_rest_candle(raw)
    assert c is not None
    assert c.ts == 1715500000

def test_parse_rest_candle_missing_ts():
    feed = make_feed()
    c    = feed._parse_rest_candle({"open": "80000", "close": "80100"})
    assert c is None

def test_parse_rest_candle_invalid_float():
    feed = make_feed()
    c    = feed._parse_rest_candle({"time": 1715500000, "open": "not_a_number", "close": "80000"})
    assert c is None   # should not raise, just return None


# ── WebSocket candle handling ─────────────────────────────────────────────────

def test_ws_candle_new_bar_closes_previous():
    """
    When a WS message arrives with a different start time,
    the previous forming candle should be closed and emitted.
    """
    closed = []
    feed = CandleFeed(on_candle_close=lambda c, b: closed.append(c.ts))
    feed._warmed_up = True

    # First bar forming
    feed._handle_candle({"type": "candlestick_5m", "data": {
        "start": 1000, "open": "80000", "high": "80100", "low": "79900", "close": "80050", "volume": "1"
    }})
    assert len(closed) == 0   # still forming

    # Second bar arrives → first bar is now closed
    feed._handle_candle({"type": "candlestick_5m", "data": {
        "start": 1300, "open": "80050", "high": "80200", "low": "80000", "close": "80150", "volume": "1"
    }})
    assert len(closed) == 1
    assert closed[0] == 1000

def test_ws_candle_updates_same_bar():
    """Multiple messages for same start time should update high/low/close."""
    feed = make_feed()
    feed._warmed_up = True

    feed._handle_candle({"data": {"start": 1000, "open": "80000", "high": "80100", "low": "79900", "close": "80050", "volume": "1"}})
    feed._handle_candle({"data": {"start": 1000, "open": "80000", "high": "80200", "low": "79800", "close": "80150", "volume": "2"}})

    assert feed._forming is not None
    assert feed._forming["high"]  == 80200.0
    assert feed._forming["low"]   == 79800.0
    assert feed._forming["close"] == 80150.0

def test_ws_candle_explicit_closed_true():
    """Explicit 'closed': true should emit immediately without waiting for next bar."""
    closed = []
    feed = CandleFeed(on_candle_close=lambda c, b: closed.append(c))
    feed._warmed_up = True

    feed._handle_candle({"data": {
        "start": 1000, "open": "80000", "high": "80100", "low": "79900", "close": "80050",
        "volume": "1", "closed": True
    }})
    assert len(closed) == 1
    assert closed[0].ts == 1000
    assert feed._forming is None   # forming cleared after explicit close

def test_ws_candle_ms_timestamp():
    """WS messages with millisecond timestamps should be normalized to seconds."""
    closed = []
    feed = CandleFeed(on_candle_close=lambda c, b: closed.append(c))
    feed._warmed_up = True

    # Forming bar with ms timestamps
    feed._handle_candle({"data": {"start": 1715500000000, "open": "80000", "high": "80100", "low": "79900", "close": "80050", "volume": "1"}})
    # New bar (ms) → closes previous
    feed._handle_candle({"data": {"start": 1715500300000, "open": "80050", "high": "80200", "low": "80000", "close": "80150", "volume": "1"}})

    assert len(closed) == 1
    assert closed[0].ts == 1715500000   # converted to seconds

def test_mark_price_update():
    feed = make_feed()
    feed._handle_mark_price({"data": {"mark_price": "80123.5"}})
    assert feed.mark_price == 80123.5

def test_mark_price_fallback_field():
    feed = make_feed()
    feed._handle_mark_price({"data": {"price": "79999.0"}})
    assert feed.mark_price == 79999.0


# ── Feed is_ready ─────────────────────────────────────────────────────────────

def test_is_ready_false_before_warmup():
    feed = make_feed()
    assert feed.is_ready is False

def test_is_ready_false_insufficient_bars():
    feed = make_feed()
    feed._warmed_up = True
    for i in range(100):
        feed._emit_closed(make_candle(1000 + i * 300, close=80000.0))
    assert feed.is_ready is False   # need >= 250

def test_is_ready_true_after_250_bars():
    feed = make_feed()
    feed._warmed_up = True
    for i in range(250):
        feed._emit_closed(make_candle(1000 + i * 300, close=80000.0))
    assert feed.is_ready is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
