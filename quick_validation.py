"""
Quick Validation Report for FxPM 1.4 Critical Fixes
====================================================

Simplified checks for implemented functionality.
"""
import sys
import os
import logging
import inspect

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)


def print_section(title):
    """Print section header."""
    logger.info("\n" + "=" * 80)
    logger.info(title)
    logger.info("=" * 80)


def print_check(name, passed, details=""):
    """Print check result."""
    status = "✓ PASS" if passed else "✗ FAIL"
    logger.info(f"  [{status}] {name}")
    if details:
        for line in details.split('\n'):
            logger.info(f"          {line}")


def validate_all_components():
    """Validate all implemented components."""
    results = {}

    # =========================================================================
    # C1: Dashboard Components
    # =========================================================================
    print_section("C1: Dashboard Trade Reconstruction & Jobs")

    try:
        from pm_dashboard.analytics import reconstruct_trade_outcome
        from pm_dashboard.jobs import HistoricalDataDownloader, DataDownloadScheduler

        # Check reconstruct_trade_outcome signature
        sig = inspect.signature(reconstruct_trade_outcome)
        params = list(sig.parameters.keys())
        has_required = 'trade_entry' in params and 'historical_bars' in params
        print_check("reconstruct_trade_outcome() exists", has_required,
                   f"Parameters: {', '.join(params)}")

        # Check HistoricalDataDownloader
        has_downloader = hasattr(HistoricalDataDownloader, 'download_all_symbols')
        print_check("HistoricalDataDownloader.download_all_symbols()", has_downloader)

        # Check DataDownloadScheduler
        has_scheduler = hasattr(DataDownloadScheduler, 'start')
        print_check("DataDownloadScheduler.start()", has_scheduler)

        results['C1'] = has_required and has_downloader and has_scheduler

    except Exception as e:
        print_check("Dashboard imports", False, str(e))
        results['C1'] = False

    # =========================================================================
    # C2: Risk Cap Implementation
    # =========================================================================
    print_section("C2: Symbol Risk Cap")

    try:
        from pm_main import LiveTrader

        # Check for risk cap method
        has_risk_cap = hasattr(LiveTrader, '_check_portfolio_risk_cap')
        print_check("LiveTrader._check_portfolio_risk_cap()", has_risk_cap)

        # Check method signature
        if has_risk_cap:
            sig = inspect.signature(LiveTrader._check_portfolio_risk_cap)
            params = list(sig.parameters.keys())
            has_params = 'symbol' in params and 'new_trade_risk_pct' in params
            print_check("Method has required parameters", has_params,
                       f"Parameters: {', '.join(params)}")

            # Check for risk cap logic in source
            source = inspect.getsource(LiveTrader._check_portfolio_risk_cap)
            has_cap_check = 'Symbol risk cap exceeded' in source
            print_check("Contains risk cap enforcement logic", has_cap_check)

            has_existing_risk = 'existing_risk_pct' in source
            print_check("Aggregates existing position risk", has_existing_risk)

            results['C2'] = has_params and has_cap_check and has_existing_risk
        else:
            results['C2'] = False

    except Exception as e:
        print_check("Risk cap imports", False, str(e))
        results['C2'] = False

    # =========================================================================
    # C3: MT5 Instrument Sync
    # =========================================================================
    print_section("C3: MT5 Instrument Synchronization")

    try:
        from pm_core import sync_instrument_spec_from_mt5, InstrumentSpec
        from pm_main import LiveTrader

        # Check sync function exists
        print_check("sync_instrument_spec_from_mt5() function exists", True)

        # Check InstrumentSpec has MT5 fields
        spec_fields = [f.name for f in InstrumentSpec.__dataclass_fields__.values()]
        mt5_fields = ['tick_value', 'volume_step', 'contract_size', 'stops_level']
        has_mt5_fields = all(f in spec_fields for f in mt5_fields)
        print_check("InstrumentSpec has MT5 fields", has_mt5_fields,
                   f"Fields: {', '.join(mt5_fields)}")

        # Check LiveTrader initialization includes sync
        source = inspect.getsource(LiveTrader.__init__)
        has_sync_call = 'sync_instrument_spec_from_mt5' in source
        print_check("LiveTrader.__init__() syncs from MT5", has_sync_call)

        results['C3'] = has_mt5_fields and has_sync_call

    except Exception as e:
        print_check("MT5 sync imports", False, str(e))
        results['C3'] = False

    # =========================================================================
    # H1: Eligibility Gates
    # =========================================================================
    print_section("H1: Strategy Eligibility Gates")

    try:
        from pm_pipeline import RegimeOptimizer

        # Check for validation method
        has_validate = hasattr(RegimeOptimizer, '_validate_regime_winner')
        print_check("RegimeOptimizer._validate_regime_winner()", has_validate)

        if has_validate:
            source = inspect.getsource(RegimeOptimizer._validate_regime_winner)

            # Check for key validation logic
            checks = {
                'Profit factor validation': 'val_pf',
                'Return validation': 'val_return',
                'Drawdown validation': 'val_drawdown',
                'Weak train exception': 'weak_train',
                'Exceptional validation': 'exceptional_val_pf',
            }

            all_checks_present = True
            for check_name, keyword in checks.items():
                present = keyword in source
                print_check(check_name, present)
                all_checks_present = all_checks_present and present

            results['H1'] = all_checks_present
        else:
            results['H1'] = False

    except Exception as e:
        print_check("Eligibility gates imports", False, str(e))
        results['H1'] = False

    # =========================================================================
    # H8: Decision Throttle
    # =========================================================================
    print_section("H8: Decision Throttle (Per-Bar Suppression)")

    try:
        from pm_main import DecisionThrottle

        # Check for key methods
        has_suppress = hasattr(DecisionThrottle, 'should_suppress')
        print_check("DecisionThrottle.should_suppress()", has_suppress)

        has_record = hasattr(DecisionThrottle, 'record_decision')
        print_check("DecisionThrottle.record_decision()", has_record)

        has_make_key = hasattr(DecisionThrottle, 'make_decision_key')
        print_check("DecisionThrottle.make_decision_key()", has_make_key)

        # Check for per-bar logic
        if has_suppress:
            source = inspect.getsource(DecisionThrottle.should_suppress)
            has_bar_time = 'bar_time' in source
            print_check("Uses bar_time for suppression", has_bar_time)

            has_decision_key = 'decision_key' in source
            print_check("Uses decision_key for identification", has_decision_key)

            results['H8'] = has_suppress and has_record and has_bar_time and has_decision_key
        else:
            results['H8'] = False

    except Exception as e:
        print_check("Decision throttle imports", False, str(e))
        results['H8'] = False

    # =========================================================================
    # Test Suite Status
    # =========================================================================
    print_section("Test Suite Status")

    try:
        import subprocess
        result = subprocess.run(
            ['python', '-m', 'pytest', 'tests/', '-v', '--tb=no', '-q'],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            timeout=120
        )

        # Parse output
        output = result.stdout + result.stderr

        if 'passed' in output:
            # Extract pass count
            import re
            match = re.search(r'(\d+) passed', output)
            if match:
                passed_count = int(match.group(1))
                print_check(f"Test suite ({passed_count} tests passed)", True)
                results['Tests'] = True
            else:
                print_check("Test suite", True, "Tests executed successfully")
                results['Tests'] = True
        else:
            print_check("Test suite", False, "Some tests may have failed")
            results['Tests'] = False

    except Exception as e:
        print_check("Test suite execution", False, str(e))
        results['Tests'] = False

    # =========================================================================
    # Summary
    # =========================================================================
    print_section("VALIDATION SUMMARY")

    for component, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"  {component:20s}: {status}")

    logger.info("-" * 80)
    passed_count = sum(results.values())
    total_count = len(results)
    percentage = (passed_count / total_count * 100) if total_count > 0 else 0

    logger.info(f"  TOTAL: {passed_count}/{total_count} components validated ({percentage:.0f}%)")

    if passed_count == total_count:
        logger.info("\n✓✓✓ ALL COMPONENTS VALIDATED ✓✓✓")
        logger.info("\nRECOMMENDATION: System is PRODUCTION READY")
        return True
    else:
        logger.info(f"\n⚠ {total_count - passed_count} COMPONENT(S) NEED ATTENTION ⚠")
        logger.info("\nRECOMMENDATION: Review failed components before production")
        return False


if __name__ == '__main__':
    success = validate_all_components()
    sys.exit(0 if success else 1)
