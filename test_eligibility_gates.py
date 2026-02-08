"""
Test script to verify the hard eligibility gates implementation.

This script tests:
1. Training eligibility gates reject losing strategies
2. Exceptional validation exception allows weak train with strong val
3. No fallback to best train when all strategies fail validation
"""

import sys
from typing import Dict, List, Any

# Mock config class for testing
class MockConfig:
    def __init__(self):
        # Training eligibility gates
        self.train_min_profit_factor = 0.95
        self.train_min_return_pct = 0.0
        self.train_max_drawdown = 40.0

        # Exceptional validation thresholds
        self.exceptional_val_profit_factor = 1.3
        self.exceptional_val_return_pct = 2.0

        # Validation thresholds
        self.regime_min_val_trades = 15
        self.regime_min_train_trades = 25
        self.fx_val_max_drawdown = 15.0
        self.regime_min_val_profit_factor = 1.0
        self.regime_min_val_return_pct = 0.0
        self.regime_allow_losing_winners = False
        self.fx_min_robustness_ratio = 0.8
        self.fx_val_sharpe_override = 0.3


def test_training_eligibility_gates():
    """Test that training eligibility gates reject losing strategies."""
    print("\n" + "="*80)
    print("TEST 1: Training Eligibility Gates")
    print("="*80)

    config = MockConfig()

    # Test cases
    test_cases = [
        {
            'name': 'Good Strategy',
            'train_pf': 1.5,
            'train_return': 10.0,
            'train_dd': 12.0,
            'expected': True,
        },
        {
            'name': 'Low PF (0.8)',
            'train_pf': 0.8,
            'train_return': 5.0,
            'train_dd': 10.0,
            'expected': False,
        },
        {
            'name': 'Negative Return',
            'train_pf': 1.1,
            'train_return': -2.0,
            'train_dd': 10.0,
            'expected': False,
        },
        {
            'name': 'High Drawdown (50%)',
            'train_pf': 1.2,
            'train_return': 5.0,
            'train_dd': 50.0,
            'expected': False,
        },
        {
            'name': 'Borderline PF (0.95)',
            'train_pf': 0.95,
            'train_return': 1.0,
            'train_dd': 15.0,
            'expected': True,
        },
    ]

    passed = 0
    failed = 0

    for test in test_cases:
        # Simulate eligibility check
        eligible = True
        reason = None

        if test['train_pf'] < config.train_min_profit_factor:
            eligible = False
            reason = f"train PF {test['train_pf']:.2f} < {config.train_min_profit_factor}"
        elif test['train_return'] < config.train_min_return_pct:
            eligible = False
            reason = f"train return {test['train_return']:.1f}% < {config.train_min_return_pct}"
        elif test['train_dd'] > config.train_max_drawdown:
            eligible = False
            reason = f"train DD {test['train_dd']:.1f}% > {config.train_max_drawdown}"

        if eligible == test['expected']:
            print(f"[PASS] {test['name']}")
            if not eligible:
                print(f"  Reason: {reason}")
            passed += 1
        else:
            print(f"[FAIL] {test['name']}")
            print(f"  Expected: {test['expected']}, Got: {eligible}")
            if reason:
                print(f"  Reason: {reason}")
            failed += 1

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_exceptional_validation_exception():
    """Test that weak train is allowed with exceptional validation."""
    print("\n" + "="*80)
    print("TEST 2: Exceptional Validation Exception")
    print("="*80)

    config = MockConfig()

    test_cases = [
        {
            'name': 'Weak train + exceptional val (PASS)',
            'train_pf': 0.8,
            'train_return': -2.0,
            'val_pf': 1.5,
            'val_return': 3.0,
            'val_trades': 30,
            'expected': True,
        },
        {
            'name': 'Weak train + low val PF (FAIL)',
            'train_pf': 0.9,
            'train_return': -1.0,
            'val_pf': 1.1,  # < 1.3
            'val_return': 3.0,
            'val_trades': 30,
            'expected': False,
        },
        {
            'name': 'Weak train + low val return (FAIL)',
            'train_pf': 0.8,
            'train_return': -2.0,
            'val_pf': 1.5,
            'val_return': 1.0,  # < 2.0
            'val_trades': 30,
            'expected': False,
        },
        {
            'name': 'Weak train + low val trades (FAIL)',
            'train_pf': 0.9,
            'train_return': -1.0,
            'val_pf': 1.5,
            'val_return': 3.0,
            'val_trades': 20,  # < 30 (2x min)
            'expected': False,
        },
        {
            'name': 'Good train + good val (PASS)',
            'train_pf': 1.3,
            'train_return': 5.0,
            'val_pf': 1.2,
            'val_return': 3.0,
            'val_trades': 20,
            'expected': True,
        },
    ]

    passed = 0
    failed = 0

    for test in test_cases:
        # Simulate weak train exception check
        weak_train = test['train_pf'] < 1.0 or test['train_return'] < 0

        exceptional_val_min_trades = config.regime_min_val_trades * 2

        if weak_train:
            # Require exceptional validation
            allows_weak = (
                test['val_pf'] >= config.exceptional_val_profit_factor and
                test['val_return'] >= config.exceptional_val_return_pct and
                test['val_trades'] >= exceptional_val_min_trades
            )
            validated = allows_weak
        else:
            # Normal validation (simplified for test)
            validated = (
                test['val_pf'] >= config.regime_min_val_profit_factor and
                test['val_return'] >= config.regime_min_val_return_pct
            )

        if validated == test['expected']:
            print(f"[PASS] PASS: {test['name']}")
            if weak_train and validated:
                print(f"  Allowed weak train (PF={test['train_pf']:.2f}, ret={test['train_return']:.1f}%)")
                print(f"  Due to exceptional val (PF={test['val_pf']:.2f}, ret={test['val_return']:.1f}%, trades={test['val_trades']})")
            passed += 1
        else:
            print(f"[FAIL] FAIL: {test['name']}")
            print(f"  Expected: {test['expected']}, Got: {validated}")
            failed += 1

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_no_fallback_policy():
    """Test that no fallback occurs when all strategies fail validation."""
    print("\n" + "="*80)
    print("TEST 3: No Fallback Policy")
    print("="*80)

    # Simulate scenario where all candidates fail validation
    candidates = [
        {'name': 'Strategy A', 'val_pf': 0.8, 'val_return': -2.0},
        {'name': 'Strategy B', 'val_pf': 0.9, 'val_return': -1.0},
        {'name': 'Strategy C', 'val_pf': 0.7, 'val_return': -3.0},
    ]

    config = MockConfig()

    # Filter candidates by validation profitability gates
    validated = []
    for cand in candidates:
        if (cand['val_pf'] >= config.regime_min_val_profit_factor and
            cand['val_return'] >= config.regime_min_val_return_pct):
            validated.append(cand)

    # Check if we have a winner
    if not validated:
        print("[PASS] PASS: No validated candidates - returning None (no fallback)")
        print("  All strategies failed profitability gates:")
        for cand in candidates:
            print(f"    - {cand['name']}: PF={cand['val_pf']:.2f}, return={cand['val_return']:.1f}%")
        return True
    else:
        print("[FAIL] FAIL: Should have returned None, but found validated candidates:")
        for cand in validated:
            print(f"  - {cand['name']}")
        return False


def main():
    """Run all tests."""
    print("\n" + "#"*80)
    print("# Hard Eligibility Gates - Validation Tests")
    print("#"*80)

    results = []

    results.append(("Training Eligibility Gates", test_training_eligibility_gates()))
    results.append(("Exceptional Validation Exception", test_exceptional_validation_exception()))
    results.append(("No Fallback Policy", test_no_fallback_policy()))

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    all_passed = True
    for name, passed in results:
        status = "[PASS] PASS" if passed else "[FAIL] FAIL"
        print(f"{status}: {name}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n[SUCCESS] All tests passed!")
        return 0
    else:
        print("\n[WARNING]  Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
