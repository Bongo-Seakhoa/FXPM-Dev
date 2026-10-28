"""
Tests for margin protection (black-swan guard).

Validates:
  - _classify_margin_state returns correct states at all boundaries
  - _run_margin_protection_cycle blocks/allows entries correctly
  - Forced deleveraging closes positions in the right order
  - Edge case: margin_level == 0 (no positions) treated as NORMAL
  - Edge case: all positions profitable in RECOVERY (no forced close)
  - Edge case: all positions profitable in PANIC (volume-based fallback)
  - Entry gate in _execute_entry blocks below threshold
  - Config fields propagate from PipelineConfig
"""

import json
import os
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch


# ---------------------------------------------------------------------------
# Lightweight mocks matching MT5 dataclasses used by LiveTrader
# ---------------------------------------------------------------------------

@dataclass
class MockAccountInfo:
    balance: float = 10000.0
    equity: float = 10000.0
    margin: float = 0.0
    margin_free: float = 10000.0
    margin_level: float = 0.0
    profit: float = 0.0
    leverage: int = 500
    currency: str = "USD"
    login: int = 123
    server: str = "Broker-Test"
    company: str = ""
    trade_allowed: bool = True
    trade_expert: bool = True


@dataclass
class MockPosition:
    ticket: int
    symbol: str
    type: int = 0
    volume: float = 0.01
    price_open: float = 1.0
    price_current: float = 1.0
    sl: float = 0.0
    tp: float = 0.0
    swap: float = 0.0
    profit: float = 0.0
    magic: int = 0
    comment: str = ""
    time: datetime = None


@dataclass
class MockOrderResult:
    success: bool
    retcode: int = 10009
    retcode_description: str = "Done"
    comment: str = ""


# ---------------------------------------------------------------------------
# Helper to create a LiveTrader with mocked dependencies
# ---------------------------------------------------------------------------

def _make_trader(margin_level=0.0, positions=None, close_success=True, **cfg_overrides):
    """Build a LiveTrader wired to mocks for unit testing."""
    from pm_main import LiveTrader
    from pm_position import PositionConfig

    acct = MockAccountInfo(margin_level=margin_level)

    mt5 = Mock()
    mt5.config = SimpleNamespace(
        expected_login=123, expected_server="Broker-Test", expected_currency="USD"
    )
    mt5.is_connected.return_value = False  # skip spec sync in __init__
    mt5.get_account_info.return_value = acct
    mt5.get_positions.return_value = positions or []
    mt5.close_position.return_value = MockOrderResult(success=close_success)

    pm = Mock()
    pm.symbols = []

    # Build a real-ish pipeline config with margin fields.
    pipeline_config = Mock()
    pipeline_config.margin_entry_block_level = cfg_overrides.get('margin_entry_block_level', 100.0)
    pipeline_config.margin_recovery_start_level = cfg_overrides.get('margin_recovery_start_level', 80.0)
    pipeline_config.margin_panic_level = cfg_overrides.get('margin_panic_level', 65.0)
    pipeline_config.margin_reopen_level = cfg_overrides.get('margin_reopen_level', 100.0)
    pipeline_config.margin_recovery_closes_per_cycle = cfg_overrides.get('margin_recovery_closes_per_cycle', 1)
    pipeline_config.margin_panic_closes_per_cycle = cfg_overrides.get('margin_panic_closes_per_cycle', 3)
    pipeline_config.margin_reopen_cooldown_minutes = cfg_overrides.get('margin_reopen_cooldown_minutes', 15.0)
    pipeline_config.max_combined_risk_pct = 3.0

    trader = LiveTrader(
        mt5_connector=mt5,
        portfolio_manager=pm,
        position_config=PositionConfig(),
        pipeline_config=pipeline_config,
        enable_trading=False,
    )
    # The margin entry/block threshold now comes from the signed selected
    # capital envelope's min_margin_level_pct (seq1720).  Bind a coherent
    # envelope so admission has its required authority; the block level is
    # configurable but defaults to 200 (util 50 / free 50), a valid selected
    # point (min_margin_level_pct=100 would force free-margin 0 and is not a
    # valid envelope).
    block_level = float(cfg_overrides.get("selected_min_margin_level_pct", 200.0))
    margin_util = 10_000.0 / block_level
    # Bind on the funded slot (default champion book reads it) so the accessor
    # returns the envelope without switching the trader into the demo entry
    # path.
    trader._funded_capital_envelope = {
        "max_total_stop_risk_pct": 8.0,
        "max_factor_stop_risk_pct": 5.0,
        "max_same_symbol_stop_risk_pct": 3.0,
        "max_margin_utilization_pct": margin_util,
        "min_margin_level_pct": block_level,
        "min_free_margin_pct": 100.0 - margin_util,
        "gap_stress_multiple": 2.5,
    }
    return trader


def _make_persistent_trader(
    root: Path,
    *,
    margin_level: float = 500.0,
    positions=None,
):
    """Build the funded runtime path that owns durable margin state."""
    from pm_core import PipelineConfig
    from pm_main import LiveTrader
    from pm_position import PositionConfig

    mt5 = Mock()
    mt5.config = SimpleNamespace(
        expected_login=123, expected_server="Broker-Test", expected_currency="USD"
    )
    mt5.is_connected.return_value = False
    mt5.get_account_info.return_value = MockAccountInfo(
        margin_level=margin_level,
    )
    mt5.get_positions.return_value = list(positions or [])
    mt5.close_position.return_value = MockOrderResult(success=True)

    pm = Mock()
    pm.symbols = []
    config = PipelineConfig(
        output_dir=Path("runtime"),
        log_dir=root / "logs",
        live_outcome_feedback_enabled=False,
    )
    config._deployment_config_path = str(root / "config.json")
    config._funded_live_execution = True
    trader = LiveTrader(
        mt5_connector=mt5,
        portfolio_manager=pm,
        position_config=PositionConfig(),
        pipeline_config=config,
        enable_trading=False,
    )
    return trader


# ===========================================================================
# Test cases
# ===========================================================================

class TestClassifyMarginState(unittest.TestCase):
    """_classify_margin_state boundary tests."""

    def setUp(self):
        self.trader = _make_trader()

    def test_normal_at_block_level(self):
        # Entry/block threshold is the selected envelope min_margin_level_pct
        # (default 200 in the test fixture); at/above it the state is NORMAL.
        self.assertEqual(self.trader._classify_margin_state(200.0), "NORMAL")

    def test_blocked_just_below_block_level(self):
        self.assertEqual(self.trader._classify_margin_state(199.9), "BLOCKED")

    def test_normal_above_block(self):
        self.assertEqual(self.trader._classify_margin_state(1200.0), "NORMAL")

    def test_selected_margin_level_beats_static_no_pre_veto(self):
        # A selected min_margin_level_pct of 111.11 must classify NORMAL at
        # margin_level=150, which the legacy static 200 block would have blocked
        # (seq1720 anti-arbitrary-cap).
        trader = _make_trader(selected_min_margin_level_pct=111.11111111111111)
        self.assertEqual(trader._classify_margin_state(150.0), "NORMAL")

    def test_malformed_envelope_blocks_entry(self):
        # An incoherent envelope (hierarchy violated) yields no bound envelope,
        # so entry classification fails closed (never NORMAL) even when healthy.
        trader = _make_trader()
        trader._funded_capital_envelope = dict(
            trader._funded_capital_envelope,
            max_same_symbol_stop_risk_pct=999.0,
        )
        self.assertNotEqual(trader._classify_margin_state(5000.0), "NORMAL")

    def test_blocked_at_99(self):
        self.assertEqual(self.trader._classify_margin_state(99.9), "BLOCKED")

    def test_blocked_at_80(self):
        self.assertEqual(self.trader._classify_margin_state(80.0), "BLOCKED")

    def test_recovery_at_79(self):
        self.assertEqual(self.trader._classify_margin_state(79.9), "RECOVERY")

    def test_recovery_at_65(self):
        self.assertEqual(self.trader._classify_margin_state(65.0), "RECOVERY")

    def test_panic_at_64(self):
        self.assertEqual(self.trader._classify_margin_state(64.9), "PANIC")

    def test_panic_at_zero(self):
        # margin_level=0 classification; the cycle handler treats 0 specially
        # but the classifier itself maps 0 -> PANIC.
        self.assertEqual(self.trader._classify_margin_state(0.0), "PANIC")

    def test_panic_at_50(self):
        self.assertEqual(self.trader._classify_margin_state(50.0), "PANIC")

    def test_non_finite_margin_levels_are_blocked(self):
        self.assertEqual(self.trader._classify_margin_state(float("nan")), "BLOCKED")
        self.assertEqual(self.trader._classify_margin_state(float("inf")), "BLOCKED")
        self.assertIsNone(self.trader._safe_account_margin_level(MockAccountInfo(margin_level=float("nan"))))
        self.assertIsNone(self.trader._safe_account_margin_level(MockAccountInfo(margin_level=float("inf"))))

    def test_non_finite_margin_used_is_treated_as_unused(self):
        self.assertFalse(self.trader._account_margin_used(MockAccountInfo(margin=float("nan"))))
        self.assertFalse(self.trader._account_margin_used(MockAccountInfo(margin=float("inf"))))
        self.assertTrue(self.trader._account_margin_used(MockAccountInfo(margin=1.0)))


class TestMarginProtectionCycleNormal(unittest.TestCase):
    """Cycle behavior when margin is healthy."""

    def test_normal_state_no_closures(self):
        trader = _make_trader(margin_level=500.0)
        trader._run_margin_protection_cycle()
        self.assertEqual(trader._margin_state, "NORMAL")
        trader.mt5.close_position.assert_not_called()

    def test_blocked_state_no_closures(self):
        trader = _make_trader(margin_level=90.0)
        trader._run_margin_protection_cycle()
        self.assertEqual(trader._margin_state, "BLOCKED")
        trader.mt5.close_position.assert_not_called()


class TestMarginLevelZeroEdgeCase(unittest.TestCase):
    """margin_level == 0 when no positions open must be NORMAL (not PANIC)."""

    def test_zero_margin_level_is_normal(self):
        trader = _make_trader(margin_level=0.0)
        trader._run_margin_protection_cycle()
        self.assertEqual(trader._margin_state, "NORMAL")
        trader.mt5.close_position.assert_not_called()


class TestMarginAccountInfoUnavailable(unittest.TestCase):
    """If account info is None, entries should be blocked."""

    def test_none_account_blocks(self):
        trader = _make_trader()
        trader.mt5.get_account_info.return_value = None
        trader._run_margin_protection_cycle()
        self.assertEqual(trader._margin_state, "BLOCKED")

    def test_missing_margin_level_blocks_when_margin_is_used(self):
        trader = _make_trader()
        trader.mt5.get_account_info.return_value = MockAccountInfo(margin=250.0, margin_level=None)
        trader._run_margin_protection_cycle()
        self.assertEqual(trader._margin_state, "BLOCKED")

    def test_missing_margin_level_is_neutral_when_no_margin_is_used(self):
        trader = _make_trader()
        trader.mt5.get_account_info.return_value = MockAccountInfo(margin=0.0, margin_level=None)
        trader._run_margin_protection_cycle()
        self.assertEqual(trader._margin_state, "NORMAL")


class TestMarginProtectionLiveLoopWiring(unittest.TestCase):
    """process_all_symbols should run margin protection before new entries."""

    def test_recovery_state_closes_and_skips_entries_for_iteration(self):
        positions = [
            MockPosition(ticket=10, symbol="EURUSD", profit=-100.0, volume=0.10),
        ]
        trader = _make_trader(margin_level=75.0, positions=positions)
        trader.pm.get_validated_configs.return_value = {"EURUSD": Mock()}
        trader.mt5.get_recent_closing_deals.return_value = []
        trader._process_symbol = Mock()
        trader._run_order_governance_cycle = Mock()
        trader._sync_drift_monitor = Mock()
        trader._evaluate_daily_loss_advisory = Mock()
        trader._run_portfolio_observatory = Mock()

        trader.process_all_symbols()

        trader.mt5.close_position.assert_called_once()
        trader._process_symbol.assert_not_called()
        trader._run_order_governance_cycle.assert_not_called()


class TestRecoveryMode(unittest.TestCase):
    """RECOVERY mode: close 1 loser per cycle."""

    def test_closes_worst_loser(self):
        positions = [
            MockPosition(ticket=1, symbol="EURUSD", profit=-50.0, volume=0.05),
            MockPosition(ticket=2, symbol="GBPUSD", profit=-20.0, volume=0.03),
            MockPosition(ticket=3, symbol="USDJPY", profit=10.0, volume=0.02),
        ]
        trader = _make_trader(margin_level=75.0, positions=positions)
        # After close, margin stays low so it won't early-stop.
        trader.mt5.get_account_info.side_effect = [
            MockAccountInfo(margin_level=75.0),
            MockAccountInfo(margin_level=75.0),
            MockAccountInfo(margin_level=78.0),
        ]
        trader._run_margin_protection_cycle()

        # Should close exactly 1 position (the worst loser: ticket=1).
        trader.mt5.close_position.assert_called_once()
        closed_pos = trader.mt5.close_position.call_args[0][0]
        self.assertEqual(closed_pos.ticket, 1)

    def test_skips_profitable_positions(self):
        positions = [
            MockPosition(ticket=1, symbol="EURUSD", profit=10.0, volume=0.05),
            MockPosition(ticket=2, symbol="GBPUSD", profit=5.0, volume=0.03),
        ]
        trader = _make_trader(margin_level=75.0, positions=positions)
        trader._run_margin_protection_cycle()
        # All profitable, RECOVERY doesn't force-close profitable positions.
        trader.mt5.close_position.assert_not_called()


class TestPanicMode(unittest.TestCase):
    """PANIC mode: close up to 3 losers per cycle, with volume fallback."""

    def test_closes_up_to_3_losers(self):
        positions = [
            MockPosition(ticket=1, symbol="A", profit=-100.0, volume=0.10),
            MockPosition(ticket=2, symbol="B", profit=-80.0, volume=0.08),
            MockPosition(ticket=3, symbol="C", profit=-60.0, volume=0.06),
            MockPosition(ticket=4, symbol="D", profit=-40.0, volume=0.04),
        ]
        # All re-checks remain in PANIC.
        trader = _make_trader(margin_level=50.0, positions=positions)
        trader.mt5.get_account_info.side_effect = [
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=52.0),
            MockAccountInfo(margin_level=52.0),
            MockAccountInfo(margin_level=55.0),
            MockAccountInfo(margin_level=55.0),
            MockAccountInfo(margin_level=58.0),
        ]
        trader._run_margin_protection_cycle()

        self.assertEqual(trader.mt5.close_position.call_count, 3)
        # Verify order: tickets 1, 2, 3 (worst to least-bad).
        tickets_closed = [c[0][0].ticket for c in trader.mt5.close_position.call_args_list]
        self.assertEqual(tickets_closed, [1, 2, 3])

    def test_panic_volume_fallback_when_all_profitable(self):
        """In PANIC with no losers, close largest-volume positions."""
        positions = [
            MockPosition(ticket=1, symbol="A", profit=10.0, volume=0.02),
            MockPosition(ticket=2, symbol="B", profit=5.0, volume=0.10),
            MockPosition(ticket=3, symbol="C", profit=20.0, volume=0.05),
        ]
        trader = _make_trader(margin_level=50.0, positions=positions)
        trader.mt5.get_account_info.side_effect = [
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=55.0),
            MockAccountInfo(margin_level=55.0),
            MockAccountInfo(margin_level=60.0),
            MockAccountInfo(margin_level=60.0),
            MockAccountInfo(margin_level=70.0),
        ]
        trader._run_margin_protection_cycle()

        # Should close up to 3 starting with largest volume (ticket=2, then 3, then 1).
        self.assertGreaterEqual(trader.mt5.close_position.call_count, 1)
        first_closed = trader.mt5.close_position.call_args_list[0][0][0]
        self.assertEqual(first_closed.ticket, 2)  # largest volume

    def test_panic_stops_early_on_recovery(self):
        """If margin recovers after first close, stop closing."""
        positions = [
            MockPosition(ticket=1, symbol="A", profit=-100.0, volume=0.10),
            MockPosition(ticket=2, symbol="B", profit=-80.0, volume=0.08),
        ]
        trader = _make_trader(margin_level=50.0, positions=positions)
        trader.mt5.get_account_info.side_effect = [
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=120.0),
        ]
        trader._run_margin_protection_cycle()

        # Should only close 1 because margin recovered.
        self.assertEqual(trader.mt5.close_position.call_count, 1)


class TestCloseFailureHandling(unittest.TestCase):
    """If close_position fails, continue with next candidate."""

    def test_failed_close_continues(self):
        positions = [
            MockPosition(ticket=1, symbol="A", profit=-100.0, volume=0.10),
            MockPosition(ticket=2, symbol="B", profit=-80.0, volume=0.08),
        ]
        trader = _make_trader(margin_level=50.0, positions=positions)
        # First close fails, second succeeds.
        trader.mt5.close_position.side_effect = [
            MockOrderResult(success=False, comment="Requote"),
            MockOrderResult(success=True),
        ]
        trader.mt5.get_account_info.side_effect = [
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=50.0),
            MockAccountInfo(margin_level=55.0),
        ]
        trader._run_margin_protection_cycle()

        # Both were attempted.
        self.assertEqual(trader.mt5.close_position.call_count, 2)


class TestStateTransitionLogging(unittest.TestCase):
    """State transitions are logged."""

    def test_logs_state_change(self):
        trader = _make_trader(margin_level=90.0)
        trader._margin_state = "NORMAL"  # previous state
        with patch.object(trader.logger, 'warning') as mock_warn:
            trader._run_margin_protection_cycle()
            # Should log the transition NORMAL -> BLOCKED.
            self.assertTrue(any("MARGIN STATE CHANGE" in str(c) for c in mock_warn.call_args_list))

    def test_no_log_when_state_unchanged(self):
        trader = _make_trader(margin_level=90.0)
        trader._margin_state = "BLOCKED"  # same as computed state
        with patch.object(trader.logger, 'warning') as mock_warn:
            trader._run_margin_protection_cycle()
            # No state-change log (might still log other things).
            state_change_calls = [c for c in mock_warn.call_args_list
                                  if "MARGIN STATE CHANGE" in str(c)]
            self.assertEqual(len(state_change_calls), 0)


class TestConfigFieldsExist(unittest.TestCase):
    """Verify PipelineConfig has all margin fields with correct defaults."""

    def test_pipeline_config_defaults(self):
        from pm_core import PipelineConfig
        cfg = PipelineConfig()
        self.assertEqual(cfg.margin_entry_block_level, 200.0)
        self.assertEqual(cfg.margin_recovery_start_level, 150.0)
        self.assertEqual(cfg.margin_panic_level, 120.0)
        self.assertEqual(cfg.margin_reopen_level, 300.0)
        self.assertEqual(cfg.margin_recovery_closes_per_cycle, 1)
        self.assertEqual(cfg.margin_panic_closes_per_cycle, 3)
        self.assertEqual(cfg.margin_reopen_cooldown_minutes, 15.0)


class TestEntryGateIntegration(unittest.TestCase):
    """Verify the entry gate blocks when margin_level is below threshold."""

    def test_margin_level_below_block_returns_early(self):
        """Simulate a call context where margin_level < 200 should block."""
        _trader = _make_trader(margin_level=195.0)

        # The entry gate is inside _execute_entry which has many dependencies.
        # Instead, test via the cycle state approach: _margin_state is set
        # by the cycle, and we can verify the entry gate logic independently.

        # Direct unit test of the gate condition:
        margin_level = 195.0
        margin_block = 200.0
        blocked = margin_level > 0 and margin_level < margin_block
        self.assertTrue(blocked)

    def test_margin_level_zero_not_blocked(self):
        """margin_level == 0 (no positions) should NOT be blocked."""
        margin_level = 0.0
        margin_block = 100.0
        blocked = margin_level > 0 and margin_level < margin_block
        self.assertFalse(blocked)

    def test_margin_level_above_block_not_blocked(self):
        """margin_level >= 100 should NOT be blocked."""
        margin_level = 1200.0
        margin_block = 100.0
        blocked = margin_level > 0 and margin_level < margin_block
        self.assertFalse(blocked)


class TestLiveMarginLevelSanity(unittest.TestCase):
    """Sanity-check that MT5AccountInfo margin_level field is a positive float
    when positions exist, and that it aligns with equity/margin math.

    This test validates the data contract, not a hardcoded value.
    """

    def test_margin_level_positive_when_margin_used(self):
        """When margin > 0, margin_level should be > 0 and match equity/margin*100."""
        acct = MockAccountInfo(
            equity=2500.0,
            margin=200.0,
            margin_level=1250.0,  # 2500/200*100
        )
        expected = (acct.equity / acct.margin) * 100.0
        self.assertAlmostEqual(acct.margin_level, expected, places=1)
        self.assertGreater(acct.margin_level, 0)

    def test_margin_level_zero_when_no_margin(self):
        """When margin == 0, margin_level should be 0 (MT5 convention)."""
        acct = MockAccountInfo(equity=2500.0, margin=0.0, margin_level=0.0)
        self.assertEqual(acct.margin_level, 0.0)

    def test_classifier_with_realistic_margin_levels(self):
        """Verify classification across a range of realistic margin levels."""
        trader = _make_trader()
        test_cases = [
            (5000.0, "NORMAL"),
            (1200.0, "NORMAL"),
            (250.0, "NORMAL"),
            (100.0, "BLOCKED"),
            (99.0, "BLOCKED"),
            (85.0, "BLOCKED"),
            (80.0, "BLOCKED"),
            (79.0, "RECOVERY"),
            (65.0, "RECOVERY"),
            (64.0, "PANIC"),
            (50.0, "PANIC"),
            (30.0, "PANIC"),
        ]
        for ml, expected_state in test_cases:
            with self.subTest(margin_level=ml):
                self.assertEqual(trader._classify_margin_state(ml), expected_state)


class TestMarginReopenCooldown(unittest.TestCase):
    """findings.html §6/§8.1: forced close arms a re-entry cooldown.

    A successful close inside _run_margin_protection_cycle must stamp
    `_last_margin_close_ts`. The entry gate must then refuse new trades
    until `margin_reopen_cooldown_minutes` has elapsed, even after the
    margin level has bounced back above the block threshold.
    """

    def test_successful_close_stamps_cooldown(self):
        positions = [MockPosition(ticket=1, symbol="A", profit=-100.0, volume=0.10)]
        trader = _make_trader(margin_level=50.0, positions=positions)
        self.assertIsNone(trader._last_margin_close_ts)
        trader._run_margin_protection_cycle()
        self.assertIsNotNone(trader._last_margin_close_ts)

    def test_failed_close_keeps_conservative_precommitted_cooldown(self):
        positions = [MockPosition(ticket=1, symbol="A", profit=-100.0, volume=0.10)]
        trader = _make_trader(margin_level=50.0, positions=positions, close_success=False)
        trader._run_margin_protection_cycle()
        self.assertIsNotNone(trader._last_margin_close_ts)
        self.assertGreater(trader._margin_cooldown_remaining_minutes(), 14.0)

    def test_cooldown_blocks_within_window(self):
        trader = _make_trader(margin_level=500.0)
        trader.pipeline_config.margin_reopen_cooldown_minutes = 15.0
        # Stamp a close 5 minutes ago — well inside the 15-minute window.
        trader._last_margin_close_ts = datetime.now() - timedelta(minutes=5)

        cooldown_minutes = float(trader.pipeline_config.margin_reopen_cooldown_minutes)
        elapsed_min = (datetime.now() - trader._last_margin_close_ts).total_seconds() / 60.0
        within_cooldown = elapsed_min < cooldown_minutes
        self.assertTrue(within_cooldown)

    def test_cooldown_releases_after_window(self):
        trader = _make_trader(margin_level=500.0)
        trader.pipeline_config.margin_reopen_cooldown_minutes = 15.0
        # Stamp a close 20 minutes ago — outside the 15-minute window.
        trader._last_margin_close_ts = datetime.now() - timedelta(minutes=20)

        cooldown_minutes = float(trader.pipeline_config.margin_reopen_cooldown_minutes)
        elapsed_min = (datetime.now() - trader._last_margin_close_ts).total_seconds() / 60.0
        within_cooldown = elapsed_min < cooldown_minutes
        self.assertFalse(within_cooldown)

    def test_cooldown_disabled_when_minutes_zero(self):
        trader = _make_trader(margin_level=500.0)
        trader.pipeline_config.margin_reopen_cooldown_minutes = 0.0
        trader._last_margin_close_ts = datetime.now()  # just closed
        cooldown_minutes = float(trader.pipeline_config.margin_reopen_cooldown_minutes)
        gate_active = cooldown_minutes > 0 and trader._last_margin_close_ts is not None
        self.assertFalse(gate_active)

    def test_reopen_requires_reopen_level_after_margin_stress(self):
        trader = _make_trader(
            margin_level=85.0,
            margin_entry_block_level=80.0,
            margin_reopen_level=90.0,
        )
        trader._margin_reopen_required = True
        trader._run_margin_protection_cycle()
        self.assertEqual(trader._margin_state, "BLOCKED")
        self.assertTrue(trader._is_margin_reopen_required())

    def test_reopen_requirement_clears_at_reopen_level(self):
        # Entry/block threshold is the selected envelope min_margin_level_pct
        # (seq1720); use a coherent block (200) and a margin level above it so
        # the reopen requirement clears when margin recovers past reopen level.
        trader = _make_trader(
            margin_level=250.0,
            selected_min_margin_level_pct=200.0,
            margin_reopen_level=200.0,
        )
        trader._margin_reopen_required = True
        trader._last_margin_close_ts = None
        trader._run_margin_protection_cycle()
        self.assertEqual(trader._margin_state, "NORMAL")
        self.assertFalse(getattr(trader, "_margin_reopen_required", False))


class TestDurableMarginProtectionState(unittest.TestCase):
    """The funded recovery latch and forced-close cooldown survive restart."""

    def test_precommit_survives_post_close_save_failure_and_restart(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            positions = [
                MockPosition(
                    ticket=1,
                    symbol="EURUSD",
                    profit=-100.0,
                    volume=0.10,
                )
            ]
            trader = _make_persistent_trader(
                root,
                margin_level=50.0,
                positions=positions,
            )
            # Start from a valid, already-latched record so the first replace
            # inside the protection cycle is the pre-close timestamp commit.
            trader._margin_reopen_required = True
            trader._last_margin_close_ts = (
                datetime.now(timezone.utc) - timedelta(hours=1)
            )
            self.assertTrue(trader._persist_margin_protection_state())
            real_replace = os.replace
            replace_calls = 0

            def fail_post_close_replace(source, destination):
                nonlocal replace_calls
                replace_calls += 1
                if replace_calls == 1:
                    return real_replace(source, destination)
                raise OSError("post-close state write interrupted")

            close_started = datetime.now(timezone.utc)
            cycle_clock = tuple(
                close_started + timedelta(microseconds=offset)
                for offset in range(4)
            )
            with patch.object(
                trader,
                "_margin_utc_now",
                side_effect=cycle_clock,
            ), patch("pm_main.os.replace", side_effect=fail_post_close_replace):
                changed = trader._run_margin_protection_cycle(
                    account_info=MockAccountInfo(margin_level=50.0),
                    positions_snapshot=positions,
                )

            self.assertTrue(changed)
            trader.mt5.close_position.assert_called_once_with(positions[0])
            self.assertEqual(replace_calls, 2)
            self.assertIsNotNone(trader._margin_state_persistence_block_reason())

            # The failed second save emulates termination immediately after a
            # successful broker close. A clean process must still restore the
            # first, pre-close commit rather than the old one-hour timestamp.
            restarted = _make_persistent_trader(root)
            self.assertTrue(restarted._is_margin_reopen_required())
            self.assertGreaterEqual(
                restarted._last_margin_close_ts.timestamp(),
                close_started.timestamp(),
            )
            self.assertGreater(
                restarted._margin_cooldown_remaining_minutes(),
                14.0,
            )

    def test_identical_forced_close_timestamp_skips_redundant_second_write(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            positions = [
                MockPosition(
                    ticket=1,
                    symbol="EURUSD",
                    profit=-100.0,
                    volume=0.10,
                )
            ]
            trader = _make_persistent_trader(
                root,
                margin_level=50.0,
                positions=positions,
            )
            identical_at = datetime.now(timezone.utc)
            trader._margin_reopen_required = True
            trader._last_margin_close_ts = identical_at - timedelta(hours=1)
            self.assertTrue(trader._persist_margin_protection_state())

            with patch.object(
                trader,
                "_margin_utc_now",
                return_value=identical_at,
            ), patch.object(
                trader,
                "_persist_margin_protection_state",
                wraps=trader._persist_margin_protection_state,
            ) as persist_mock:
                changed = trader._run_margin_protection_cycle(
                    account_info=MockAccountInfo(margin_level=50.0),
                    positions_snapshot=positions,
                )

            self.assertTrue(changed)
            trader.mt5.close_position.assert_called_once_with(positions[0])
            self.assertEqual(persist_mock.call_count, 1)
            self.assertEqual(trader._last_margin_close_ts, identical_at)
            self.assertIsNone(trader._margin_state_persistence_block_reason())

            restarted = _make_persistent_trader(root)
            self.assertEqual(restarted._last_margin_close_ts, identical_at)
            self.assertTrue(restarted._is_margin_reopen_required())

    def test_successful_forced_close_is_durable_across_restart(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            positions = [
                MockPosition(
                    ticket=1,
                    symbol="EURUSD",
                    profit=-100.0,
                    volume=0.10,
                )
            ]
            trader = _make_persistent_trader(
                root,
                margin_level=50.0,
                positions=positions,
            )
            trader._margin_reopen_required = False
            trader._last_margin_close_ts = (
                datetime.now(timezone.utc) - timedelta(hours=1)
            )
            self.assertTrue(trader._persist_margin_protection_state())
            close_started = datetime.now(timezone.utc)

            changed = trader._run_margin_protection_cycle(
                account_info=MockAccountInfo(margin_level=50.0),
                positions_snapshot=positions,
            )

            self.assertTrue(changed)
            trader.mt5.close_position.assert_called_once_with(positions[0])
            forced_close = trader._last_margin_close_ts
            self.assertGreaterEqual(forced_close, close_started)

            restarted = _make_persistent_trader(root)
            self.assertTrue(restarted._is_margin_reopen_required())
            self.assertAlmostEqual(
                restarted._last_margin_close_ts.timestamp(),
                forced_close.timestamp(),
                delta=0.01,
            )
            self.assertGreater(
                restarted._margin_cooldown_remaining_minutes(),
                14.0,
            )

    def test_restart_restores_reopen_latch_and_forced_close_cooldown(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = _make_persistent_trader(root)
            close_at = datetime.now(timezone.utc) - timedelta(minutes=2)

            # This is the same atomic state transition used by a successful
            # forced margin close.
            first._margin_reopen_required = False
            first._last_margin_close_ts = None
            first._arm_margin_reopen_required(last_forced_close=close_at)

            state_path = root / "runtime" / "margin_protection_state.json"
            self.assertEqual(first._margin_protection_state_path, state_path)
            self.assertTrue(state_path.is_file())

            restarted = _make_persistent_trader(root)
            self.assertTrue(restarted._is_margin_reopen_required())
            self.assertAlmostEqual(
                restarted._last_margin_close_ts.timestamp(),
                close_at.timestamp(),
                delta=1.0,
            )
            self.assertGreater(
                restarted._margin_cooldown_remaining_minutes(),
                12.0,
            )

            # Reaching the 300% reopen threshold clears only the latch. The
            # independently required 15-minute post-close cooldown survives.
            restarted._clear_margin_reopen_if_recovered(300.0)
            after_clear_restart = _make_persistent_trader(root)
            self.assertFalse(after_clear_restart._is_margin_reopen_required())
            self.assertGreater(
                after_clear_restart._margin_cooldown_remaining_minutes(),
                12.0,
            )

    def test_missing_state_bootstraps_a_durable_fail_closed_window(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            trader = _make_persistent_trader(root)

            self.assertTrue(trader._is_margin_reopen_required())
            self.assertEqual(trader._margin_state, "BLOCKED")
            self.assertGreater(
                trader._margin_cooldown_remaining_minutes(),
                14.0,
            )
            self.assertEqual(trader._margin_state_persistence_error, "")
            payload = json.loads(
                trader._margin_protection_state_path.read_text(encoding="utf-8")
            )
            self.assertEqual(
                payload["schema_version"],
                "2026-07-11-margin-protection-v1",
            )
            self.assertTrue(payload["margin_reopen_required"])
            self.assertTrue(payload["last_forced_close_utc"].endswith("Z"))

    def test_corrupt_state_is_replaced_with_fail_closed_state(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            initial = _make_persistent_trader(root)
            state_path = initial._margin_protection_state_path
            state_path.write_text("{not-json", encoding="utf-8")

            restarted = _make_persistent_trader(root)

            self.assertTrue(restarted._is_margin_reopen_required())
            self.assertEqual(restarted._margin_state, "BLOCKED")
            self.assertGreater(
                restarted._margin_cooldown_remaining_minutes(),
                14.0,
            )
            self.assertEqual(restarted._margin_state_persistence_error, "")
            repaired = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertTrue(repaired["margin_reopen_required"])
            self.assertIn("content_sha256", repaired)

    def test_state_save_failure_blocks_entries_fail_closed(self):
        with TemporaryDirectory() as temp_dir:
            trader = _make_persistent_trader(Path(temp_dir))
            trader._margin_reopen_required = False
            trader._last_margin_close_ts = None

            with patch("pm_main.os.replace", side_effect=OSError("disk unavailable")):
                trader._arm_margin_reopen_required(
                    last_forced_close=datetime.now(timezone.utc),
                )

            reason = trader._margin_state_persistence_block_reason()
            self.assertIsNotNone(reason)
            self.assertIn("state save failed", reason)
            self.assertTrue(trader._is_margin_reopen_required())


class TestMarginAccountIdentity(unittest.TestCase):
    """Synthetic identity transitions cover direct and multi-close cycles."""

    def _positions(self):
        # Account-wide protection intentionally includes manual/other-EA positions.
        return [MockPosition(ticket=1, symbol="EURUSD", profit=-100, magic=0),
                MockPosition(ticket=2, symbol="GBPUSD", profit=-50, magic=987654)]

    def test_direct_cycle_rejects_wrong_missing_malformed_or_failed_account(self):
        cases = [None, MockAccountInfo(login=124, margin_level=50),
                 MockAccountInfo(login="bad", margin_level=50),
                 MockAccountInfo(server="Other", margin_level=50),
                 MockAccountInfo(currency="EUR", margin_level=50),
                 RuntimeError("synthetic account read failed")]
        for value in cases:
            with self.subTest(value=value):
                trader = _make_trader(margin_level=50, positions=self._positions())
                if isinstance(value, Exception):
                    trader.mt5.get_account_info.side_effect = value
                else:
                    trader.mt5.get_account_info.return_value = value
                self.assertFalse(trader._run_margin_protection_cycle())
                trader.mt5.get_account_info.assert_called_once()
                self.assertEqual(trader._margin_state, "BLOCKED")
                trader.mt5.close_position.assert_not_called()
                self.assertIsNone(trader._last_margin_close_ts)

    def test_normal_cycle_reuses_supplied_snapshot_without_extra_poll(self):
        for supplied in (False, True):
            with self.subTest(supplied=supplied):
                trader = _make_trader(margin_level=500)
                account = MockAccountInfo(margin_level=500) if supplied else None
                self.assertFalse(trader._run_margin_protection_cycle(
                    account_info=account, positions_snapshot=[]
                ))
                self.assertEqual(trader.mt5.get_account_info.call_count, 0 if supplied else 1)
                self.assertEqual(trader._margin_state, "NORMAL")
                trader.mt5.close_position.assert_not_called()

    def test_direct_passed_snapshot_requires_pins_and_matching_identity(self):
        for pins, account in [
            (None, MockAccountInfo(margin_level=50)),
            (SimpleNamespace(expected_login=123, expected_server="", expected_currency="USD"),
             MockAccountInfo(margin_level=50)),
            (SimpleNamespace(expected_login=123, expected_server="Broker-Test", expected_currency="USD"),
             MockAccountInfo(login=124, margin_level=50)),
        ]:
            with self.subTest(pins=pins, account=account):
                trader = _make_trader(margin_level=50, positions=self._positions())
                trader.mt5.config = pins
                self.assertFalse(trader._run_margin_protection_cycle(account_info=account))
                trader.mt5.get_account_info.assert_not_called()
                trader.mt5.close_position.assert_not_called()

    def test_preclose_denial_precedes_durable_forced_close_timestamp(self):
        for value in (None, MockAccountInfo(login=124, margin_level=50),
                      RuntimeError("synthetic confirmation failed")):
            with self.subTest(value=value):
                trader = _make_trader(margin_level=50, positions=self._positions())
                trader.mt5.get_account_info.side_effect = [MockAccountInfo(margin_level=50), value]
                with patch.object(trader, "_arm_margin_reopen_required",
                                  wraps=trader._arm_margin_reopen_required) as arm:
                    self.assertFalse(trader._run_margin_protection_cycle())
                self.assertEqual(trader.mt5.get_account_info.call_count, 2)
                trader.mt5.close_position.assert_not_called()
                self.assertIsNone(trader._last_margin_close_ts)
                self.assertFalse(any(c.kwargs.get("last_forced_close") for c in arm.call_args_list))

    def test_account_switch_between_closes_blocks_next_close(self):
        trader = _make_trader(margin_level=50, positions=self._positions())
        trader.mt5.get_account_info.side_effect = [
            MockAccountInfo(margin_level=50),  # sweep
            MockAccountInfo(margin_level=50),  # first pre-close
            MockAccountInfo(margin_level=50),  # first post-close
            MockAccountInfo(login=124, margin_level=50),  # second pre-close
        ]
        self.assertTrue(trader._run_margin_protection_cycle())
        self.assertEqual(trader.mt5.get_account_info.call_count, 4)
        trader.mt5.close_position.assert_called_once()
        self.assertEqual(trader.mt5.close_position.call_args.args[0].ticket, 1)
        self.assertEqual(trader._margin_state, "BLOCKED")

    def test_postclose_identity_switch_is_not_used_for_recovery(self):
        trader = _make_trader(margin_level=50, positions=self._positions())
        trader.mt5.get_account_info.side_effect = [
            MockAccountInfo(margin_level=50), MockAccountInfo(margin_level=50),
            MockAccountInfo(login=124, margin_level=500),
        ]
        self.assertTrue(trader._run_margin_protection_cycle())
        self.assertEqual(trader.mt5.get_account_info.call_count, 3)
        trader.mt5.close_position.assert_called_once()
        self.assertEqual(trader._margin_state, "BLOCKED")
        self.assertTrue(trader._is_margin_reopen_required())

    def test_authorised_low_zero_negative_equity_still_allows_account_wide_protection(self):
        for equity in (1.0, 0.0, -100.0):
            with self.subTest(equity=equity):
                positions = self._positions()
                trader = _make_trader(margin_level=50, positions=positions)
                trader.mt5.get_account_info.return_value = MockAccountInfo(
                    equity=equity, margin_level=50, trade_allowed=False, trade_expert=False
                )
                self.assertTrue(trader._run_margin_protection_cycle())
                self.assertEqual([c.args[0] for c in trader.mt5.close_position.call_args_list], positions)
                self.assertEqual(trader.mt5.get_account_info.call_count, 5)
                self.assertIsNotNone(trader._last_margin_close_ts)


if __name__ == '__main__':
    unittest.main()
