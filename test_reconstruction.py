"""
Test script for trade outcome reconstruction.

Validates the reconstruction logic with sample data.
"""
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Set encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Add dashboard to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'pm_dashboard'))

from pm_dashboard.analytics import reconstruct_trade_outcome, get_pip_size


def create_test_bars(start_time, num_bars=100, timeframe_minutes=60):
    """Create synthetic OHLC bars for testing."""
    times = []
    opens = []
    highs = []
    lows = []
    closes = []

    current_time = start_time
    price = 1.1000  # Start price

    for i in range(num_bars):
        times.append(current_time)

        # Random walk
        change = (i % 3 - 1) * 0.0010  # +0.0010, 0, or -0.0010
        price += change

        open_price = price
        close_price = price + 0.0005
        high_price = max(open_price, close_price) + 0.0002
        low_price = min(open_price, close_price) - 0.0002

        opens.append(open_price)
        highs.append(high_price)
        lows.append(low_price)
        closes.append(close_price)

        current_time += timedelta(minutes=timeframe_minutes)

    df = pd.DataFrame({
        'Open': opens,
        'High': highs,
        'Low': lows,
        'Close': closes,
        'Volume': [1000] * num_bars
    }, index=times)

    return df


def test_long_tp_hit():
    """Test LONG trade that hits TP."""
    print("\n=== Test 1: LONG trade hits TP ===")

    entry_time = datetime(2026, 2, 1, 10, 0, 0)
    entry_price = 1.1000
    sl = 1.0950  # 50 pips below
    tp = 1.1100  # 100 pips above

    trade_entry = {
        '_parsed_timestamp': entry_time,
        'symbol': 'EURUSD',
        'entry_price': entry_price,
        'sl': sl,
        'tp': tp,
        'direction': 'LONG'
    }

    # Create bars where price rises to TP
    bars = create_test_bars(entry_time + timedelta(hours=1), num_bars=50)

    # Manually set a bar that hits TP
    bars.loc[bars.index[10], 'High'] = tp + 0.0010

    result = reconstruct_trade_outcome(trade_entry, bars)

    print(f"Close Reason: {result['close_reason']}")
    print(f"Exit Price: {result['exit_price']}")
    print(f"PnL (pips): {result['pnl_pips']:.1f}")
    print(f"Duration (min): {result['duration_minutes']}")

    assert result['close_reason'] == 'TP_HIT', f"Expected TP_HIT, got {result['close_reason']}"
    assert result['pnl_pips'] > 90, f"Expected ~100 pips, got {result['pnl_pips']:.1f}"
    print("✓ Test passed!")


def test_long_sl_hit():
    """Test LONG trade that hits SL."""
    print("\n=== Test 2: LONG trade hits SL ===")

    entry_time = datetime(2026, 2, 1, 10, 0, 0)
    entry_price = 1.1000
    sl = 1.0950  # 50 pips below
    tp = 1.1100  # 100 pips above

    trade_entry = {
        '_parsed_timestamp': entry_time,
        'symbol': 'EURUSD',
        'entry_price': entry_price,
        'sl': sl,
        'tp': tp,
        'direction': 'LONG'
    }

    # Create bars where price falls to SL
    bars = create_test_bars(entry_time + timedelta(hours=1), num_bars=50)

    # Manually set a bar that hits SL
    bars.loc[bars.index[5], 'Low'] = sl - 0.0010

    result = reconstruct_trade_outcome(trade_entry, bars)

    print(f"Close Reason: {result['close_reason']}")
    print(f"Exit Price: {result['exit_price']}")
    print(f"PnL (pips): {result['pnl_pips']:.1f}")
    print(f"Duration (min): {result['duration_minutes']}")

    assert result['close_reason'] == 'SL_HIT', f"Expected SL_HIT, got {result['close_reason']}"
    assert result['pnl_pips'] < -40, f"Expected ~-50 pips, got {result['pnl_pips']:.1f}"
    print("✓ Test passed!")


def test_short_tp_hit():
    """Test SHORT trade that hits TP."""
    print("\n=== Test 3: SHORT trade hits TP ===")

    entry_time = datetime(2026, 2, 1, 10, 0, 0)
    entry_price = 1.1000
    sl = 1.1050  # 50 pips above
    tp = 1.0900  # 100 pips below

    trade_entry = {
        '_parsed_timestamp': entry_time,
        'symbol': 'EURUSD',
        'entry_price': entry_price,
        'sl': sl,
        'tp': tp,
        'direction': 'SHORT'
    }

    # Create bars where price falls to TP
    bars = create_test_bars(entry_time + timedelta(hours=1), num_bars=50)

    # Manually set a bar that hits TP
    bars.loc[bars.index[15], 'Low'] = tp - 0.0010

    result = reconstruct_trade_outcome(trade_entry, bars)

    print(f"Close Reason: {result['close_reason']}")
    print(f"Exit Price: {result['exit_price']}")
    print(f"PnL (pips): {result['pnl_pips']:.1f}")
    print(f"Duration (min): {result['duration_minutes']}")

    assert result['close_reason'] == 'TP_HIT', f"Expected TP_HIT, got {result['close_reason']}"
    assert result['pnl_pips'] > 90, f"Expected ~100 pips, got {result['pnl_pips']:.1f}"
    print("✓ Test passed!")


def test_short_sl_hit():
    """Test SHORT trade that hits SL."""
    print("\n=== Test 4: SHORT trade hits SL ===")

    entry_time = datetime(2026, 2, 1, 10, 0, 0)
    entry_price = 1.1000
    sl = 1.1050  # 50 pips above
    tp = 1.0900  # 100 pips below

    trade_entry = {
        '_parsed_timestamp': entry_time,
        'symbol': 'EURUSD',
        'entry_price': entry_price,
        'sl': sl,
        'tp': tp,
        'direction': 'SHORT'
    }

    # Create bars where price rises to SL
    bars = create_test_bars(entry_time + timedelta(hours=1), num_bars=50)

    # Manually set a bar that hits SL
    bars.loc[bars.index[8], 'High'] = sl + 0.0010

    result = reconstruct_trade_outcome(trade_entry, bars)

    print(f"Close Reason: {result['close_reason']}")
    print(f"Exit Price: {result['exit_price']}")
    print(f"PnL (pips): {result['pnl_pips']:.1f}")
    print(f"Duration (min): {result['duration_minutes']}")

    assert result['close_reason'] == 'SL_HIT', f"Expected SL_HIT, got {result['close_reason']}"
    assert result['pnl_pips'] < -40, f"Expected ~-50 pips, got {result['pnl_pips']:.1f}"
    print("✓ Test passed!")


def test_timeout():
    """Test trade that times out (neither SL nor TP hit)."""
    print("\n=== Test 5: Trade timeout (no SL/TP hit) ===")

    entry_time = datetime(2026, 2, 1, 10, 0, 0)
    entry_price = 1.1000
    sl = 1.0900  # 100 pips below
    tp = 1.1200  # 200 pips above

    trade_entry = {
        '_parsed_timestamp': entry_time,
        'symbol': 'EURUSD',
        'entry_price': entry_price,
        'sl': sl,
        'tp': tp,
        'direction': 'LONG'
    }

    # Create bars that stay within range
    bars = create_test_bars(entry_time + timedelta(hours=1), num_bars=20)

    # Ensure no bar hits SL or TP (keep price around entry)
    for idx in bars.index:
        bars.loc[idx, 'High'] = min(bars.loc[idx, 'High'], entry_price + 0.0050)
        bars.loc[idx, 'Low'] = max(bars.loc[idx, 'Low'], entry_price - 0.0050)

    result = reconstruct_trade_outcome(trade_entry, bars, timeout_bars=20)

    print(f"Close Reason: {result['close_reason']}")
    print(f"Exit Price: {result['exit_price']}")
    print(f"PnL (pips): {result['pnl_pips']:.1f}")

    assert result['close_reason'] == 'TIMEOUT', f"Expected TIMEOUT, got {result['close_reason']}"
    assert result['pnl_pips'] == 0, f"Expected 0 pips, got {result['pnl_pips']:.1f}"
    print("✓ Test passed!")


def test_pip_sizes():
    """Test pip size calculation for different symbols."""
    print("\n=== Test 6: Pip size calculation ===")

    test_cases = [
        ('EURUSD', 0.0001),
        ('GBPUSD', 0.0001),
        ('USDJPY', 0.01),
        ('EURJPY', 0.01),
        ('XAUUSD', 0.1),
        ('BTCUSD', 1.0),
    ]

    for symbol, expected_pip in test_cases:
        actual_pip = get_pip_size(symbol)
        print(f"{symbol}: {actual_pip} (expected: {expected_pip})")
        assert actual_pip == expected_pip, f"Pip size mismatch for {symbol}"

    print("✓ All pip sizes correct!")


def main():
    """Run all tests."""
    print("=" * 60)
    print("Trade Outcome Reconstruction Tests")
    print("=" * 60)

    try:
        test_pip_sizes()
        test_long_tp_hit()
        test_long_sl_hit()
        test_short_tp_hit()
        test_short_sl_hit()
        test_timeout()

        print("\n" + "=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)

    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
