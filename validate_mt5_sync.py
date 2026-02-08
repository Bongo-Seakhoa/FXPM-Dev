"""
Validation script for MT5 spec synchronization.

This script demonstrates the MT5 spec sync functionality and shows how
instrument specs are updated with live broker values during LiveTrader initialization.
"""

from dataclasses import dataclass
from pm_core import InstrumentSpec, sync_instrument_spec_from_mt5


@dataclass
class MockMT5SymbolInfo:
    """Mock MT5 symbol info for testing."""
    trade_tick_value: float
    trade_tick_size: float
    volume_step: float
    volume_min: float
    volume_max: float
    spread: int
    point: float
    digits: int
    trade_contract_size: float
    trade_stops_level: int
    swap_long: float
    swap_short: float


def validate_eurusd_sync():
    """Validate EURUSD spec synchronization."""
    print("\n" + "="*80)
    print("TEST 1: EURUSD Sync (Major Pair)")
    print("="*80)

    # Create spec with config.json values
    spec = InstrumentSpec(
        symbol="EURUSD",
        pip_position=4,
        pip_value=10.0,
        spread_avg=1.0,  # Config value
        min_lot=0.01,
        max_lot=100.0,
        tick_value=0.0,  # Will be set from MT5
        tick_size=0.0,   # Will be set from MT5
    )

    print("\nBEFORE sync (config.json values):")
    print(f"  tick_value: {spec.tick_value:.4f}")
    print(f"  tick_size: {spec.tick_size:.6f}")
    print(f"  spread_avg: {spec.spread_avg:.1f} pips")
    print(f"  volume_step: {spec.volume_step}")
    print(f"  max_lot: {spec.max_lot}")

    # Mock MT5 data (realistic broker values)
    mt5_info = MockMT5SymbolInfo(
        trade_tick_value=1.0,
        trade_tick_size=0.00001,
        volume_step=0.01,
        volume_min=0.01,
        volume_max=50.0,  # Broker limit
        spread=15,  # 15 points = 1.5 pips
        point=0.00001,
        digits=5,
        trade_contract_size=100000.0,
        trade_stops_level=10,
        swap_long=-6.5,
        swap_short=1.2,
    )

    # Sync from MT5
    sync_instrument_spec_from_mt5(spec, mt5_info)

    print("\nAFTER sync (MT5 broker values):")
    print(f"  tick_value: {spec.tick_value:.4f}")
    print(f"  tick_size: {spec.tick_size:.6f}")
    print(f"  spread_avg: {spec.spread_avg:.1f} pips")
    print(f"  volume_step: {spec.volume_step}")
    print(f"  max_lot: {spec.max_lot}")
    print(f"  contract_size: {spec.contract_size}")
    print(f"  stops_level: {spec.stops_level}")
    print(f"  swap_long: {spec.swap_long:.1f}")
    print(f"  swap_short: {spec.swap_short:.1f}")

    # Validate critical changes
    assert spec.tick_value == 1.0, "tick_value not synced"
    assert spec.max_lot == 50.0, "max_lot not synced"
    assert abs(spec.spread_avg - 1.5) < 0.01, "spread not converted correctly"

    print("\n[PASS] VALIDATION PASSED: Major pair synced correctly")


def validate_audnzd_sync():
    """Validate AUDNZD spec synchronization (cross pair with variable tick_value)."""
    print("\n" + "="*80)
    print("TEST 2: AUDNZD Sync (Cross Pair - Critical for Accurate Position Sizing)")
    print("="*80)

    # Create spec with config.json values (likely stale)
    spec = InstrumentSpec(
        symbol="AUDNZD",
        pip_position=4,
        pip_value=6.0,  # Config estimate
        spread_avg=2.5,
        min_lot=0.01,
        max_lot=100.0,
        tick_value=0.0,  # CRITICAL: Must get from MT5
    )

    print("\nBEFORE sync (config.json values - potentially INACCURATE):")
    print(f"  tick_value: {spec.tick_value:.4f} (ZERO - will cause position sizing errors!)")
    print(f"  pip_value: {spec.pip_value:.4f} (config estimate)")
    print(f"  spread_avg: {spec.spread_avg:.1f} pips")

    # Mock MT5 data with REAL broker tick_value
    # For cross pairs, tick_value varies based on exchange rates
    mt5_info = MockMT5SymbolInfo(
        trade_tick_value=0.6523,  # Real USD value per tick (varies daily)
        trade_tick_size=0.00001,
        volume_step=0.01,
        volume_min=0.01,
        volume_max=100.0,
        spread=25,  # 25 points = 2.5 pips
        point=0.00001,
        digits=5,
        trade_contract_size=100000.0,
        trade_stops_level=10,
        swap_long=-2.0,
        swap_short=-1.5,
    )

    # Sync from MT5
    sync_instrument_spec_from_mt5(spec, mt5_info)

    print("\nAFTER sync (MT5 broker values - ACCURATE):")
    print(f"  tick_value: {spec.tick_value:.4f} (from MT5 - NOW ACCURATE!)")
    print(f"  tick_size: {spec.tick_size:.6f}")
    print(f"  spread_avg: {spec.spread_avg:.1f} pips")
    print(f"  volume_step: {spec.volume_step}")
    print(f"  contract_size: {spec.contract_size}")

    # Calculate position sizing impact
    risk_amount = 100.0  # $100 risk
    stop_distance_pips = 50.0  # 50 pip stop

    # Using config pip_value (WRONG)
    wrong_volume = risk_amount / (stop_distance_pips * 6.0)
    print(f"\n[SIZING] Position Sizing Impact:")
    print(f"  Risk: ${risk_amount}, Stop: {stop_distance_pips} pips")
    print(f"  Using config pip_value={6.0:.4f}: {wrong_volume:.2f} lots (WRONG)")

    # Using MT5 tick_value (CORRECT)
    correct_pip_value = spec.tick_value / (spec.tick_size / spec.pip_size)
    correct_volume = risk_amount / (stop_distance_pips * correct_pip_value)
    print(f"  Using MT5 tick_value={spec.tick_value:.4f}: {correct_volume:.2f} lots (CORRECT)")

    sizing_error_pct = abs(wrong_volume - correct_volume) / correct_volume * 100
    print(f"  Position sizing error without sync: {sizing_error_pct:.1f}%")

    # Validate
    assert spec.tick_value == 0.6523, "tick_value not synced"
    assert abs(spec.spread_avg - 2.5) < 0.01, "spread not converted correctly"

    print("\n[PASS] VALIDATION PASSED: Cross pair synced correctly")
    print("  -> Position sizing now uses accurate broker tick_value")


def validate_graceful_fallback():
    """Validate graceful fallback when MT5 info not available."""
    print("\n" + "="*80)
    print("TEST 3: Graceful Fallback (Symbol Not Available on MT5)")
    print("="*80)

    # Create spec with config values
    spec = InstrumentSpec(
        symbol="BTCUSD",
        pip_position=2,
        pip_value=10.0,
        spread_avg=50.0,
        min_lot=0.01,
        max_lot=10.0,
    )

    print("\nBEFORE sync (config.json values):")
    print(f"  spread_avg: {spec.spread_avg:.1f} pips")
    print(f"  max_lot: {spec.max_lot}")

    # Sync with None (symbol not on broker)
    result = sync_instrument_spec_from_mt5(spec, None)

    print("\nAFTER sync with None (unchanged - using config fallback):")
    print(f"  spread_avg: {spec.spread_avg:.1f} pips")
    print(f"  max_lot: {spec.max_lot}")

    # Validate spec unchanged
    assert spec.spread_avg == 50.0, "Spec should remain unchanged"
    assert spec.max_lot == 10.0, "Spec should remain unchanged"
    assert result is spec, "Should return same spec object"

    print("\n[PASS] VALIDATION PASSED: Graceful fallback when MT5 info not available")


def validate_volume_step_rounding():
    """Validate that volume_step synchronization affects position sizing."""
    print("\n" + "="*80)
    print("TEST 4: Volume Step Impact on Position Rounding")
    print("="*80)

    # Create spec with config values
    spec = InstrumentSpec(
        symbol="XAUUSD",
        pip_position=2,
        pip_value=10.0,
        spread_avg=5.0,
        min_lot=0.01,
        max_lot=100.0,
        volume_step=0.01,  # Config default
    )

    print("\nBEFORE sync:")
    print(f"  volume_step: {spec.volume_step}")

    # Mock MT5 with different volume_step
    mt5_info = MockMT5SymbolInfo(
        trade_tick_value=1.0,
        trade_tick_size=0.01,
        volume_step=0.1,  # Broker requires 0.1 lot increments
        volume_min=0.1,
        volume_max=50.0,
        spread=50,
        point=0.01,
        digits=2,
        trade_contract_size=100.0,
        trade_stops_level=100,
        swap_long=-10.0,
        swap_short=-8.0,
    )

    # Sync from MT5
    sync_instrument_spec_from_mt5(spec, mt5_info)

    print("\nAFTER sync:")
    print(f"  volume_step: {spec.volume_step}")
    print(f"  min_lot: {spec.min_lot}")

    # Test volume rounding
    test_volumes = [0.15, 0.25, 0.37, 0.42]
    print("\n[ROUNDING] Volume Rounding Tests:")
    for vol in test_volumes:
        rounded = spec.round_volume(vol)
        print(f"  {vol:.2f} lots -> {rounded:.2f} lots (step={spec.volume_step})")

    # Validate
    assert spec.volume_step == 0.1, "volume_step not synced"
    assert spec.min_lot == 0.1, "min_lot not synced"
    assert spec.round_volume(0.15) == 0.1, "Volume rounding incorrect"
    assert spec.round_volume(0.25) == 0.2, "Volume rounding incorrect"

    print("\n[PASS] VALIDATION PASSED: Volume step synced and rounding works correctly")


def main():
    """Run all validations."""
    print("\n" + "="*80)
    print("MT5 SPEC SYNCHRONIZATION VALIDATION")
    print("="*80)
    print("\nThis validates that InstrumentSpec synchronization from MT5 works correctly")
    print("and ensures position sizing uses accurate broker values instead of stale config.")

    try:
        validate_eurusd_sync()
        validate_audnzd_sync()
        validate_graceful_fallback()
        validate_volume_step_rounding()

        print("\n" + "="*80)
        print("[PASS] ALL VALIDATIONS PASSED")
        print("="*80)
        print("\nMT5 spec synchronization is working correctly:")
        print("  [+] Major pairs sync tick_value, spread, volume constraints")
        print("  [+] Cross pairs get accurate tick_value (critical for position sizing)")
        print("  [+] Graceful fallback when symbol not available on MT5")
        print("  [+] Volume step synchronization ensures proper lot rounding")
        print("\n[IMPACT] Position sizing now uses real-time broker values")
        print("  -> Eliminates cross-pair position sizing errors")
        print("  -> Prevents over-leverage from incorrect tick_value")
        print("  -> Ensures volume rounding matches broker constraints")
        print("  -> Keeps spread calculations accurate for live trading\n")

    except AssertionError as e:
        print(f"\n[FAIL] VALIDATION FAILED: {e}\n")
        raise


if __name__ == "__main__":
    main()
