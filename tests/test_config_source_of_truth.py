import unittest
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from functools import lru_cache

import pandas as pd

from pm_core import DataLoader, PipelineConfig, RETIRED_SCORING_CONFIG_KEYS
from pm_main import (
    ACTIONABLE_SCHEMA_VERSION,
    FUNDED_FIXED_PIPELINE_VALUES,
    FUNDED_PIPELINE_CONFIG_KEYS,
    FUNDED_POSITION_CONFIG_KEYS,
    FUNDED_RAW_PIPELINE_POLICY_KEYS,
    FXPortfolioManagerApp,
    LiveTrader,
    _apply_funded_pipeline_contract,
    _apply_funded_position_cost_contract,
    _filter_dataclass_kwargs,
    _is_research_discovery_only_config,
    _normalize_storage_section_for_pipeline,
    _resolve_config_relative_path,
    _validate_funded_config_schema,
)
from pm_demo_readiness import AUTHORIZED_DEMO_ACCOUNT_IDENTITY
from pm_position import PositionConfig, TradeTagEncoder
from pm_live_outcomes import LiveOutcomeError
from pm_spread_profile import load_spread_profile


@lru_cache(maxsize=1)
def _risk_fixture_spread_profile():
    return load_spread_profile()


def _execute_with_frozen_spread_fixture(trader, **kwargs):
    """Synthetic broker inputs exercise real spread checks before risk assertions."""
    symbol = kwargs["symbol"]
    point = {"XAUUSD": 0.01, "GBPUSD": 0.00001}[symbol]
    candidate = dict(kwargs["best_candidate"])
    timeframe = candidate["timeframe"]
    identity_sha = "0be6d0831806c1a62ab723193a87a362772a14d8bf4ccbe2d3a8774962eefe09"
    profile = _risk_fixture_spread_profile()
    tier = profile.resolve(symbol, timeframe, "P75", point=point,
                           source_identity_sha256=identity_sha)
    cell = candidate.get("regime_config")
    if cell is None:
        cell = SimpleNamespace(artifact_meta={})
    cell.artifact_meta = {**cell.artifact_meta, "validated_spread_tier": tier.descriptor()}
    candidate["regime_config"] = cell
    kwargs["best_candidate"] = candidate
    kwargs["spec"].point = point
    trader.mt5.get_symbol_info.return_value.point = point
    features = kwargs["features"].copy()
    if not isinstance(features.index, pd.DatetimeIndex):
        features.index = pd.date_range(
            "2020-01-01", periods=len(features),
            freq=f"{DataLoader.TIMEFRAME_MINUTES[timeframe]}min", tz="UTC",
        )
    kwargs["features"] = features
    target_open = features.index[-1]
    trader.mt5.get_bars.return_value = pd.DataFrame(
        {"Spread": [1, 1, 1000]},
        index=pd.date_range(target_open - pd.Timedelta(minutes=10), periods=3, freq="5min"),
    )
    if symbol == "GBPUSD":
        # Preserve the executed side; both quote checks see five spread points.
        quote = trader.mt5.get_symbol_tick.return_value
        trader.mt5.get_symbol_tick.return_value = (
            SimpleNamespace(ask=quote.ask, bid=quote.ask - 5 * point)
            if kwargs["signal"] == 1
            else SimpleNamespace(bid=quote.bid, ask=quote.bid + 5 * point)
        )
    trader._frozen_spread_profile = profile
    with patch("tools.acquire_native_history.snapshot_read_only_runtime_metadata",
               return_value={"synthetic_risk_fixture_metadata": True}), patch(
        "pm_native_request_contract.canonical_source_identity",
        return_value={"identity_sha256": identity_sha},
    ):
        return trader._execute_entry(**kwargs)


class ConfigSourceOfTruthTests(unittest.TestCase):
    def test_runtime_authority_paths_are_rooted_at_selected_config(self):
        config_path = Path("C:/approved/deployment/config.live.json")

        self.assertEqual(
            _resolve_config_relative_path("broker_specs.json", config_path),
            Path("C:/approved/deployment/broker_specs.json"),
        )
        self.assertEqual(
            _resolve_config_relative_path("data", config_path),
            Path("C:/approved/deployment/data"),
        )

    def test_runtime_manual_kill_switch_uses_selected_config_directory(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            deployment = Path(tmp) / "approved"
            config_path = deployment / "config.live.json"
            flag = deployment / "runtime" / "entries.flag"
            flag.parent.mkdir(parents=True)
            config_path.write_text("{}", encoding="utf-8")
            flag.write_text("blocked", encoding="utf-8")

            trader = LiveTrader.__new__(LiveTrader)
            trader.pipeline_config = SimpleNamespace(
                manual_new_entries_enabled=True,
                manual_kill_switch_path="runtime/entries.flag",
                _deployment_config_path=str(config_path),
            )

            reason = trader._manual_new_entries_block_reason()
            self.assertIsNotNone(reason)
            self.assertIn(str(flag.resolve()), reason)

    def _resolve_configs(self, config_data):
        pipeline_config = PipelineConfig(
            **_filter_dataclass_kwargs(PipelineConfig, config_data.get("pipeline", {}))
        )
        position_config = PositionConfig(
            **_filter_dataclass_kwargs(PositionConfig, config_data.get("position", {}))
        )

        # Mirror the backward-compatible handoff in pm_main.main().
        if "risk_per_trade_pct" not in (config_data.get("position") or {}) and hasattr(
            pipeline_config, "risk_per_trade_pct"
        ):
            position_config.risk_per_trade_pct = pipeline_config.risk_per_trade_pct

        return pipeline_config, position_config

    def test_position_risk_per_trade_pct_overrides_pipeline_value(self):
        _, position_config = self._resolve_configs(
            {
                "pipeline": {"risk_per_trade_pct": 1.0},
                "position": {"risk_per_trade_pct": 0.5},
            }
        )

        self.assertAlmostEqual(position_config.risk_per_trade_pct, 0.5, places=6)

    def test_pipeline_risk_per_trade_pct_backfills_only_when_position_value_missing(self):
        _, position_config = self._resolve_configs(
            {
                "pipeline": {"risk_per_trade_pct": 0.75},
                "position": {},
            }
        )

        self.assertAlmostEqual(position_config.risk_per_trade_pct, 0.75, places=6)

    def test_position_max_risk_pct_comes_from_position_section(self):
        _, position_config = self._resolve_configs(
            {
                "pipeline": {"max_combined_risk_pct": 2.0},
                "position": {"max_risk_pct": 1.0},
            }
        )

        self.assertAlmostEqual(position_config.max_risk_pct, 1.0, places=6)

    def test_fixed_funded_safety_defaults_survive_omitted_json_switches(self):
        """Removing fixed switches from JSON must not weaken resolved policy."""
        config_path = Path(__file__).resolve().parents[1] / "config.json"
        pipeline_payload = dict(
            json.loads(config_path.read_text(encoding="utf-8"))["pipeline"]
        )
        fixed_switches = {
            "use_spread",
            "scoring_use_trade_frequency_bonus",
            "local_governance_tournament_enabled",
            "local_governance_live_mode",
            "risk_management_optimization_enabled",
            "walk_forward_audit_enabled",
            "walk_forward_promotion_policy",
            "walk_forward_live_blocking_enabled",
        }
        for key in fixed_switches:
            pipeline_payload.pop(key, None)

        resolved = PipelineConfig(
            **_filter_dataclass_kwargs(PipelineConfig, pipeline_payload)
        )

        self.assertTrue(resolved.use_spread)
        live_position_defaults = PositionConfig()
        self.assertTrue(live_position_defaults.use_spread)
        self.assertTrue(live_position_defaults.use_slippage)
        self.assertEqual(live_position_defaults.slippage_pips, 0.5)
        self.assertFalse(live_position_defaults.auto_widen_sl)
        self.assertFalse(live_position_defaults.allow_min_lot_risk_clamp)
        self.assertTrue(
            all(
                value == 1.0
                for value in live_position_defaults.regime_tp_multipliers.values()
            )
        )
        self.assertFalse(resolved.scoring_use_trade_frequency_bonus)
        self.assertFalse(resolved.local_governance_tournament_enabled)
        self.assertEqual(resolved.local_governance_live_mode, "off")
        self.assertFalse(resolved.risk_management_optimization_enabled)
        self.assertTrue(resolved.walk_forward_audit_enabled)
        self.assertEqual(resolved.walk_forward_promotion_policy, "promotion_gate")
        self.assertTrue(resolved.walk_forward_live_blocking_enabled)

    def test_funded_live_slippage_buffer_is_independent_of_pipeline_cost_model(self):
        pipeline = SimpleNamespace(
            use_spread=True,
            use_slippage=False,
            slippage_pips=9.0,
        )
        position = PositionConfig()

        _apply_funded_position_cost_contract(
            {"_meta": {"scope": "funded_live_v1"}},
            pipeline,
            position,
        )

        self.assertTrue(position.use_spread)
        self.assertTrue(position.use_slippage)
        self.assertEqual(position.slippage_pips, 0.5)

        research_position = PositionConfig(
            use_spread=False,
            use_slippage=False,
            slippage_pips=9.0,
        )
        _apply_funded_position_cost_contract(
            {"_meta": {"scope": "research_discovery_only_not_live_trading"}},
            pipeline,
            research_position,
        )
        self.assertFalse(research_position.use_spread)
        self.assertFalse(research_position.use_slippage)
        self.assertEqual(research_position.slippage_pips, 9.0)

    def test_nested_storage_section_accepts_natural_keys(self):
        normalized = _normalize_storage_section_for_pipeline(
            {
                "observe_only": False,
                "live_sync_bars": 123,
                "state_filename": "custom_state.json",
            }
        )

        self.assertFalse(normalized["storage_observe_only"])
        self.assertEqual(normalized["storage_live_sync_bars"], 123)
        self.assertEqual(normalized["state_filename"], "custom_state.json")

    def test_shipped_funded_profile_matches_positive_schema(self):
        config_path = Path(__file__).resolve().parents[1] / "config.json"
        config_data = json.loads(config_path.read_text(encoding="utf-8"))

        _validate_funded_config_schema(config_data)

        self.assertEqual(
            set(config_data["pipeline"]),
            FUNDED_PIPELINE_CONFIG_KEYS | FUNDED_RAW_PIPELINE_POLICY_KEYS,
        )
        self.assertEqual(set(config_data["position"]), FUNDED_POSITION_CONFIG_KEYS)
        self.assertEqual(len(FUNDED_POSITION_CONFIG_KEYS), 6)
        self.assertTrue(
            FUNDED_PIPELINE_CONFIG_KEYS.issubset(PipelineConfig.__dataclass_fields__)
        )
        self.assertLess(len(FUNDED_PIPELINE_CONFIG_KEYS), len(PipelineConfig.__dataclass_fields__))

    def test_funded_schema_admits_only_named_raw_capital_policy_surface(self):
        self.assertEqual(
            FUNDED_RAW_PIPELINE_POLICY_KEYS,
            {"production_capital_authority"},
        )
        self.assertTrue(
            FUNDED_RAW_PIPELINE_POLICY_KEYS.isdisjoint(
                PipelineConfig.__dataclass_fields__
            )
        )
        _validate_funded_config_schema(
            {
                "_meta": {"scope": "funded_live_v1"},
                "pipeline": {
                    # Full content verification belongs to the strict Capital
                    # authority loader; this gate only proves the raw object is
                    # not silently dropped as an unknown dataclass field.
                    "production_capital_authority": {},
                },
            }
        )
        with self.assertRaisesRegex(
            ValueError,
            "unsupported funded-live pipeline",
        ):
            _validate_funded_config_schema(
                {
                    "_meta": {"scope": "funded_live_v1"},
                    "pipeline": {"production_capital_authority_typo": {}},
                }
            )

    def test_funded_schema_rejects_fixed_and_research_only_pipeline_knobs(self):
        for key, value in (
            ("research_mode", True),
            ("max_bars", 1234),
            ("use_spread", False),
            ("scoring_use_trade_frequency_bonus", True),
            ("walk_forward_audit_mode", "off"),
            ("local_governance_tournament_enabled", True),
            ("regime_validation_top_k", 1),
            ("false_discovery_scope_power", 1.0),
            ("false_discovery_symbol_count", 100),
            ("walk_forward_do_not_displace_latest_surface", False),
            ("walk_forward_audit_enabled", False),
            ("walk_forward_promotion_policy", "observe_only"),
            ("walk_forward_live_blocking_enabled", False),
            ("tuning_validation_bars", 10),
            ("final_lockbox_bars", 10),
            ("split_embargo_bars", 0),
            ("oos_kfold_splits", 3),
            ("oos_embargo_bars", 2),
            ("oos_aggregate_policy", "all_folds"),
            ("max_param_combos", 10),
            ("min_trades", 1),
            ("fx_opt_min_trades", 1),
            ("fx_val_min_trades", 1),
            ("fx_selection_top_k", 1),
            ("fx_opt_top_k", 1),
            ("oos_min_trades", 1),
            ("promote_small_risk_multiplier", 0.5),
        ):
            payload = {
                "_meta": {"scope": "funded_live_v1"},
                "pipeline": {key: value},
            }
            with self.subTest(key=key), self.assertRaisesRegex(
                ValueError, "unsupported funded-live pipeline"
            ):
                _validate_funded_config_schema(payload)

        # The remaining research fields stay constructor-compatible outside
        # the funded profile. Retired scoring controls are tested separately.
        research = PipelineConfig(
            research_mode=True,
            production_split_mode="bleeding_edge",
            max_bars=1234,
            use_spread=False,
            scoring_use_trade_frequency_bonus=True,
            tuning_validation_bars=123,
            final_lockbox_bars=456,
            split_embargo_bars=7,
            oos_validation_mode="purged_kfold",
            oos_kfold_splits=3,
            oos_embargo_bars=2,
            oos_aggregate_policy="all_folds",
        )
        self.assertTrue(research.research_mode)
        self.assertEqual(research.max_bars, 1234)
        self.assertFalse(research.use_spread)
        self.assertEqual(research.tuning_validation_bars, 123)
        self.assertEqual(research.final_lockbox_bars, 456)
        self.assertEqual(research.split_embargo_bars, 7)
        self.assertEqual(research.oos_kfold_splits, 3)
        self.assertEqual(research.oos_embargo_bars, 2)
        self.assertEqual(research.oos_aggregate_policy, "all_folds")

    def test_retired_pipeline_settings_are_rejected_and_oos_remains_fixed(self):
        self.assertEqual(
            FUNDED_FIXED_PIPELINE_VALUES,
            {"oos_validation_mode": "fixed_holdout"},
        )
        self.assertEqual(
            RETIRED_SCORING_CONFIG_KEYS,
            {
                "execution_spread_spike_mult",
                "legacy_confidence_bonus_cap",
                "oos_evidence_execution_cost_max_candidates_per_tf",
                "oos_evidence_execution_stress_mode",
                "oos_evidence_include_execution_cost_survival",
                "production_completion_marker_path",
                "production_expected_promote_cells",
                "production_promotion_cell_safety_path",
                "production_promotion_review_path",
                "regime_scoring_model",
                "slippage_pips",
                "specialist_spokes_v2_allow_promote_small_default",
                "specialist_spokes_v2_cost_frequency_strength",
                "specialist_spokes_v2_default_fallback_enabled",
                "specialist_spokes_v2_max_validation_sharpe",
                "threadspark_historical_chunk_count",
                "threadspark_historical_cost_scope",
                "threadspark_selection_surface",
                "threadspark_use_historical_cost_evidence",
                "use_commission",
                "use_slippage",
            },
        )
        self.assertTrue(
            RETIRED_SCORING_CONFIG_KEYS.isdisjoint(
                PipelineConfig.__dataclass_fields__
            )
        )
        self.assertTrue(
            RETIRED_SCORING_CONFIG_KEYS.isdisjoint(FUNDED_PIPELINE_CONFIG_KEYS)
        )
        for key in RETIRED_SCORING_CONFIG_KEYS:
            payload = {
                "_meta": {"scope": "funded_live_v1"},
                "pipeline": {key: "retired"},
            }
            with self.subTest(key=key), self.assertRaisesRegex(
                ValueError,
                "unsupported funded-live pipeline",
            ):
                _validate_funded_config_schema(payload)
            with self.subTest(key=key), self.assertRaisesRegex(
                ValueError,
                "retired pipeline config keys",
            ):
                _filter_dataclass_kwargs(PipelineConfig, {key: "retired"})

        with self.assertRaisesRegex(ValueError, "is fixed to"):
            _validate_funded_config_schema(
                {
                    "_meta": {"scope": "funded_live_v1"},
                    "pipeline": {"oos_validation_mode": "purged_kfold"},
                }
            )

        funded = PipelineConfig(
            research_mode=True,
            oos_validation_mode="purged_kfold",
        )
        _apply_funded_pipeline_contract(
            {"_meta": {"scope": "funded_live_v1"}},
            funded,
        )
        self.assertEqual(funded.oos_validation_mode, "fixed_holdout")
        research = PipelineConfig(
            research_mode=True,
            oos_validation_mode="purged_kfold",
        )
        self.assertEqual(research.oos_validation_mode, "purged_kfold")

    def test_funded_schema_rejects_dormant_position_management_branches(self):
        for key, value in (
            ("use_trailing_stop", True),
            ("use_breakeven_stop", True),
            ("max_trade_duration_bars", 10),
            ("use_slippage", False),
            ("auto_widen_sl", True),
            ("allow_min_lot_risk_clamp", True),
            ("regime_tp_multipliers", {"TREND": 1.5}),
        ):
            payload = {
                "_meta": {"scope": "funded_live_v1"},
                "position": {key: value},
            }
            with self.subTest(key=key), self.assertRaisesRegex(
                ValueError, "unsupported funded-live position"
            ):
                _validate_funded_config_schema(payload)

    def test_funded_storage_section_cannot_override_pipeline_policy(self):
        payload = {
            "_meta": {"scope": "funded_live_v1"},
            "storage": {"risk_per_trade_pct": 25.0},
        }
        with self.assertRaisesRegex(ValueError, "unknown funded-live storage"):
            _validate_funded_config_schema(payload)

        _validate_funded_config_schema(
            {
                "_meta": {"scope": "funded_live_v1"},
                "storage": {"observe_only": True, "live_sync_bars": 500},
            }
        )

    def test_app_defaults_to_pipeline_winner_ledger_path(self):
        pipeline_config = PipelineConfig(winner_ledger_path="custom_pm_configs.json")
        app = FXPortfolioManagerApp(
            symbols=["EURUSD"],
            pipeline_config=pipeline_config,
            position_config=SimpleNamespace(),
            mt5_config=SimpleNamespace(),
        )

        self.assertEqual(app.config_file, "custom_pm_configs.json")

    def test_research_discovery_only_config_marker_is_detected(self):
        self.assertTrue(
            _is_research_discovery_only_config(
                {"_meta": {"scope": "research_discovery_only_not_live_trading"}}
            )
        )
        self.assertFalse(_is_research_discovery_only_config({"_meta": {"scope": "live"}}))
        self.assertFalse(_is_research_discovery_only_config({}))


class LiveRiskIntentTests(unittest.TestCase):
    def _make_trader(self, position_config):
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = MagicMock()
        trader._decision_throttle = MagicMock()
        trader._actionable_log = MagicMock()
        trader._last_order_times = {}
        trader.position_config = position_config
        trader.pipeline_config = SimpleNamespace(
            min_trade_risk_pct=0.01,
            d1_secondary_risk_multiplier=1.0,
            secondary_trade_max_risk_pct=0.5,
            max_combined_risk_pct=2.0,
            margin_entry_block_level=100.0,
            target_annual_vol=0.10,
            live_min_account_equity=0.0,
            live_min_account_equity_currency="",
            production_require_promotion_gate=False,
        )
        trader.position_calc = MagicMock()
        trader.position_calc.calculate_stop_prices.return_value = (1.1900, 1.2200)
        trader.enable_trading = False
        trader.logger = MagicMock()
        trader._enhancement_seams = SimpleNamespace(
            risk_scalar_stack=SimpleNamespace(overlays=[]),
            execution_quality_overlay=SimpleNamespace(enabled=False),
        )
        trader._equity_peak = 0.0
        # These tests exercise logic after the production Capital V8 admission
        # gate.  Bind a coherent test-only envelope so each case reaches the
        # branch it is intended to verify; production deliberately remains
        # fail-closed when no signed/replay-selected envelope is available.
        trader._funded_capital_envelope = {
            "max_total_stop_risk_pct": 10.0,
            "max_factor_stop_risk_pct": 8.0,
            "max_same_symbol_stop_risk_pct": 5.0,
            "max_margin_utilization_pct": 50.0,
            "min_margin_level_pct": 200.0,
            "min_free_margin_pct": 50.0,
            "gap_stress_multiple": 1.5,
        }
        trader._current_refit_demo_capital_envelope = dict(
            trader._funded_capital_envelope
        )
        trader._check_symbol_combined_risk_cap = MagicMock(
            return_value=(True, "Symbol risk OK for XAUUSD: 0.10% / 2.00% (0 open positions, adding 1 new)")
        )
        trader._log_trade = MagicMock()

        trader.mt5.get_symbol_info.return_value = SimpleNamespace(
            visible=True,
            trade_mode=2,
            volume_min=0.01,
            volume_max=100.0,
            trade_stops_level=0,
            point=0.01,
            trade_tick_size=0.0,
            trade_tick_value=0.0,
        )
        trader.mt5.get_symbol_tick.return_value = SimpleNamespace(ask=1.2000, bid=1.1998)
        trader.mt5.normalize_volume.side_effect = lambda volume, _info: volume

        def _calc_loss_amount(_order_type, _symbol, volume, _entry_price, _stop_price):
            if abs(volume - 1.0) < 1e-9:
                return 1000.0
            if abs(volume - 0.01) < 1e-9:
                return 10.0
            return 1000.0 * float(volume)

        trader.mt5.calc_loss_amount.side_effect = _calc_loss_amount
        trader.mt5.calc_margin_required.return_value = None
        trader.mt5.config = SimpleNamespace(
            expected_login=123,
            expected_server="Broker-Live",
            expected_currency="USD",
        )
        trader.mt5.get_account_info.return_value = SimpleNamespace(
            login=123,
            server="Broker-Live",
            currency="USD",
            balance=10000.0,
            equity=10000.0,
            margin_level=0.0,
            margin_free=10000.0,
            trade_allowed=True,
            trade_expert=True,
        )
        return trader

    def _execute_standard_market_entry(self, trader, regime_config=None):
        features = pd.DataFrame(
            {
                "Open": [1.0, 1.0],
                "High": [1.0, 1.0],
                "Low": [1.0, 1.0],
                "Close": [1.0, 1.0],
            }
        )
        best_candidate = {
            "timeframe": "M15",
            "regime": "CHOP",
            "strategy_name": "MomentumBurstStrategy",
            "signal": 1,
        }
        if regime_config is not None:
            best_candidate["regime_config"] = regime_config
        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(
                pip_size=0.01,
                pip_value=1.0,
                spread_avg=3.0,
                point=0.01,
            ),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="pre-send-integration",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate=best_candidate,
            trade_intent=SimpleNamespace(
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
            ),
            positions_snapshot=[],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin_level=0.0,
                margin_free=10000.0,
            ),
        )

    @staticmethod
    def _enable_durable_feedback(trader):
        ledger = MagicMock()
        ledger.reserve_pre_send_intent.return_value = {
            "reservation_id": "a" * 32
        }
        trader._live_outcome_feedback_initialized = True
        trader._live_outcome_feedback_error = ""
        trader._live_outcome_ledger = ledger
        trader.pipeline_config.live_outcome_feedback_enabled = True
        return ledger

    def test_execute_entry_blocks_subminimum_risk_target_without_promotion(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.05,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )

        strategy = MagicMock()
        strategy.calculate_stops.return_value = (10.0, 20.0)
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="risk-intent",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_MIN_LOT_EXCEEDS_CAP")
        self.assertAlmostEqual(actionable_payload["target_risk_pct"], 0.05, places=6)
        self.assertIsNone(actionable_payload["actual_risk_pct"])
        self.assertAlmostEqual(
            actionable_payload["context"]["requested_volume"],
            0.005,
            places=9,
        )
        normalized_request = trader.mt5.normalize_volume.call_args.args[0]
        self.assertAlmostEqual(normalized_request, 0.005, places=9)
        self.assertLess(normalized_request, 0.01)
        requested_loss_volumes = [
            call.args[2] for call in trader.mt5.calc_loss_amount.call_args_list
        ]
        self.assertEqual(requested_loss_volumes, [1.0])
        trader.mt5.send_market_order.assert_not_called()

    def test_additional_specialist_ignores_legacy_d1_risk_derates(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.05,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.pipeline_config.d1_secondary_risk_multiplier = 0.10
        trader.pipeline_config.secondary_trade_max_risk_pct = 0.01
        existing = SimpleNamespace(
            symbol="XAUUSD",
            magic=TradeTagEncoder.encode_magic("XAUUSD", "H4", "TREND"),
            ticket=42,
            comment=TradeTagEncoder.encode_comment(
                "XAUUSD",
                "H4",
                "MomentumBurstStrategy",
                "LONG",
                risk_pct=0.01,
            ),
        )
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.get_positions.return_value = [existing]
        features = pd.DataFrame(
            {
                "Open": [1.0, 1.0],
                "High": [1.0, 1.0],
                "Low": [1.0, 1.0],
                "Close": [1.0, 1.0],
            }
        )

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(
                pip_size=0.01,
                pip_value=1.0,
                spread_avg=3.0,
                point=0.01,
            ),
            magic=TradeTagEncoder.encode_magic("XAUUSD", "M15", "CHOP"),
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="additional-specialist-risk-parity",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            is_secondary_trade=True,
            positions_snapshot=[existing],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin_level=0.0,
                margin_free=10000.0,
                margin_mode=2,
                hedge_allowed=True,
            ),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_MIN_LOT_EXCEEDS_CAP")
        self.assertAlmostEqual(actionable_payload["target_risk_pct"], 0.05, places=6)
        trader.mt5.send_market_order.assert_not_called()

    def test_execute_entry_uses_fresh_symbol_positions_for_combined_risk(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.10,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.pipeline_config.allow_d1_plus_lower_tf = True
        trader.mt5.get_position_by_symbol_magic.return_value = None
        fresh_positions = [
            SimpleNamespace(
                symbol="XAUUSD",
                magic=TradeTagEncoder.encode_magic("XAUUSD", "D1", "TREND"),
                ticket=42,
                comment=TradeTagEncoder.encode_comment(
                    "XAUUSD", "D1", "MomentumBurstStrategy", "LONG", risk_pct=0.05
                ),
            )
        ]
        trader.mt5.get_positions.return_value = fresh_positions

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="fresh-risk-snapshot",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin_level=0.0,
                margin_mode=2,
                hedge_allowed=True,
            ),
        )

        self.assertGreaterEqual(trader._check_symbol_combined_risk_cap.call_count, 1)
        self.assertTrue(
            any(
                call.kwargs.get("positions_snapshot")
                and call.kwargs["positions_snapshot"][0] is fresh_positions[0]
                for call in trader._check_symbol_combined_risk_cap.call_args_list
            )
        )
    def test_execute_entry_blocks_fresh_exact_specialist_cell(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.10,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.pipeline_config.allow_d1_plus_lower_tf = True
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.get_positions.return_value = [
            SimpleNamespace(
                symbol="XAUUSD",
                magic=TradeTagEncoder.encode_magic("XAUUSD", "D1", "TREND"),
                ticket=42,
                comment=TradeTagEncoder.encode_comment(
                    "XAUUSD", "D1", "MomentumBurstStrategy", "LONG", risk_pct=0.05
                ),
            )
        ]
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=TradeTagEncoder.encode_magic("XAUUSD", "D1", "TREND"),
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="fresh-position-pairing",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "D1",
                "regime": "TREND",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin_level=0.0,
                margin_mode=2,
                hedge_allowed=True,
            ),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_POSITION_EXISTS")

    def test_execute_entry_blocks_cross_horizon_pair_on_netting_account(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.10,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.pipeline_config.allow_d1_plus_lower_tf = True
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.get_positions.return_value = [
            SimpleNamespace(
                symbol="XAUUSD",
                magic=TradeTagEncoder.encode_magic("XAUUSD", "D1", "TREND"),
                ticket=42,
                comment=TradeTagEncoder.encode_comment(
                    "XAUUSD", "D1", "MomentumBurstStrategy", "LONG", risk_pct=0.05
                ),
            )
        ]
        features = pd.DataFrame(
            {"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]}
        )

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="netting-pair-block",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin_level=0.0,
                margin_mode=0,
                hedge_allowed=False,
            ),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_ACCOUNT_POSITION_MODE")
        trader.mt5.send_market_order.assert_not_called()

    def test_execute_entry_backup_hard_cap_does_not_override_target_envelope(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.05,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )

        strategy = MagicMock()
        strategy.calculate_stops.return_value = (10.0, 20.0)
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="risk-intent-backup",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin_level=0.0,
                margin_mode=2,
                hedge_allowed=True,
            ),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_MIN_LOT_EXCEEDS_CAP")
        self.assertIsNone(actionable_payload["actual_risk_pct"])
        trader.mt5.send_market_order.assert_not_called()

    def test_execute_entry_blocks_before_submit_when_margin_required_exceeds_free_margin(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.calc_margin_required.return_value = 150.0
        trader.mt5.send_market_order = MagicMock()

        strategy = MagicMock()
        strategy.calculate_stops.return_value = (10.0, 20.0)
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="risk-intent-margin",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=100.0),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_MARGIN_REQUIRED")

    def test_execute_entry_blocks_nonfinite_margin_level_when_margin_is_used(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.send_market_order = MagicMock()

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="nonfinite-margin-level",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin=25.0,
                margin_level=float("nan"),
                margin_free=10000.0,
            ),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_MARGIN_UNAVAILABLE")

    def test_execute_entry_blocks_when_required_margin_known_but_free_margin_nonfinite(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.calc_margin_required.return_value = 150.0
        trader.mt5.send_market_order = MagicMock()

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="nonfinite-free-margin",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=float("nan")),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_MARGIN_UNAVAILABLE")

    def test_execute_entry_blocks_nonfinite_margin_required_value(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.calc_margin_required.return_value = float("inf")
        trader.mt5.send_market_order = MagicMock()

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="nonfinite-required-margin",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=10000.0),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_MARGIN_UNAVAILABLE")

    def test_execute_entry_uses_gross_stop_risk_and_records_fill_slippage(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.get_symbol_tick.side_effect = [
            SimpleNamespace(ask=1.2000, bid=1.1998),
            SimpleNamespace(ask=1.2050, bid=1.2048),
        ]
        trader.mt5.send_market_order.return_value = SimpleNamespace(
            success=True,
            volume=0.05,
            price=1.2052,
            retcode=10009,
            retcode_description="Done",
        )

        strategy = MagicMock()
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(
                pip_size=0.01,
                pip_value=1.0,
                spread_avg=3.0,
                point=0.01,
                commission_per_lot=7.0,
            ),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="prechecked-price",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin_level=0.0,
                margin_free=10000.0,
            ),
        )

        kwargs = trader.mt5.send_market_order.call_args.kwargs
        self.assertEqual(kwargs["price"], 1.2050)
        self.assertAlmostEqual(kwargs["volume"], 50.0 / 1000.0, places=9)
        self.assertGreater(kwargs["volume"], 50.0 / (1000.0 + 7.0))
        self.assertEqual(kwargs["deviation"], 30)
        self.assertAlmostEqual(kwargs["sl"], 1.1950, places=6)
        self.assertAlmostEqual(kwargs["tp"], 1.2250, places=6)
        self.assertIs(kwargs["symbol_info"], trader.mt5.get_symbol_info.return_value)
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "EXECUTED")
        self.assertEqual(actionable_payload["actionable_schema_version"], ACTIONABLE_SCHEMA_VERSION)
        self.assertAlmostEqual(actionable_payload["expected_entry_price"], 1.2050, places=6)
        self.assertAlmostEqual(actionable_payload["filled_entry_price"], 1.2052, places=6)
        self.assertAlmostEqual(actionable_payload["entry_slippage_pips"], 0.02, places=6)
        self.assertAlmostEqual(
            actionable_payload["entry_slippage_r"],
            0.02 / 1.02,
            places=6,
        )
        self.assertEqual(actionable_payload["entry_slippage_direction"], "adverse")

    def test_pre_send_reservation_precedes_send_and_fill_binding(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        ledger = self._enable_durable_feedback(trader)
        events = []

        def _reserve(**_kwargs):
            events.append("reserve")
            return {"reservation_id": "a" * 32}

        def _send(**_kwargs):
            events.append("send")
            return SimpleNamespace(
                success=True,
                volume=0.05,
                price=1.2050,
                retcode=10009,
                retcode_description="Done",
                deal=701,
                order=702,
            )

        def _bind(*_args, **_kwargs):
            events.append("bind")
            return {}

        ledger.reserve_pre_send_intent.side_effect = _reserve
        ledger.bind_pre_send_intent.side_effect = _bind
        trader.mt5.send_market_order.side_effect = _send

        self._execute_standard_market_entry(trader)

        self.assertEqual(events, ["reserve", "send", "bind"])
        reserve_kwargs = ledger.reserve_pre_send_intent.call_args.kwargs
        self.assertGreater(reserve_kwargs["risk_cash"], 0.0)
        self.assertEqual(reserve_kwargs["management_capsule_id"], "")
        self.assertEqual(reserve_kwargs["management_generation_id"], "")
        bind_args = ledger.bind_pre_send_intent.call_args
        self.assertEqual(bind_args.args, ("a" * 32,))
        self.assertEqual(bind_args.kwargs["opening_deal_ticket"], 701)
        self.assertEqual(bind_args.kwargs["opening_order_ticket"], 702)

    def test_send_exception_retains_ambiguous_reservation(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        ledger = self._enable_durable_feedback(trader)
        trader.mt5.send_market_order.side_effect = OSError("transport lost")
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.get_positions.return_value = []

        self._execute_standard_market_entry(trader)

        ledger.retain_ambiguous_pre_send_intent.assert_called_once()
        self.assertEqual(
            ledger.retain_ambiguous_pre_send_intent.call_args.args,
            ("a" * 32,),
        )
        ledger.cancel_pre_send_intent.assert_not_called()
        ledger.bind_pre_send_intent.assert_not_called()

    def test_definitive_broker_rejection_cancels_reservation(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        ledger = self._enable_durable_feedback(trader)
        trader.mt5.send_market_order.return_value = SimpleNamespace(
            success=False,
            retcode=10014,
            retcode_description="Invalid volume",
            error_message="",
            ambiguous=False,
        )

        self._execute_standard_market_entry(trader)

        ledger.cancel_pre_send_intent.assert_called_once_with(
            "a" * 32,
            definitive_no_fill=True,
            reason="broker rejected order with retcode 10014",
        )
        ledger.retain_ambiguous_pre_send_intent.assert_not_called()
        ledger.bind_pre_send_intent.assert_not_called()

    def test_ambiguous_fill_reconciliation_binds_reservation(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        ledger = self._enable_durable_feedback(trader)
        trader.mt5.send_market_order.return_value = SimpleNamespace(
            success=False,
            retcode=10012,
            retcode_description="Request canceled by timeout",
            error_message="",
            ambiguous=True,
            order=0,
        )
        trader.mt5.get_position_by_symbol_magic.side_effect = [
            None,
            SimpleNamespace(
                symbol="XAUUSD",
                magic=12345,
                ticket=777,
                identifier=778,
                volume=0.05,
                price_open=1.2050,
            ),
        ]

        self._execute_standard_market_entry(trader)

        ledger.bind_pre_send_intent.assert_called_once()
        self.assertEqual(
            ledger.bind_pre_send_intent.call_args.kwargs["position_id"], 778
        )
        ledger.cancel_pre_send_intent.assert_not_called()
        ledger.retain_ambiguous_pre_send_intent.assert_not_called()

    def test_demo_management_authority_binding_is_exact_and_one_way(self):
        trader = LiveTrader.__new__(LiveTrader)
        trader._is_current_refit_demo = True
        trader._demo_management_capsule_id = ""
        trader._demo_management_generation_id = ""

        trader.bind_demo_management_authority("b" * 64, "generation-7")
        trader.bind_demo_management_authority("b" * 64, "generation-7")

        self.assertEqual(trader._demo_management_capsule_id, "b" * 64)
        self.assertEqual(trader._demo_management_generation_id, "generation-7")
        with self.assertRaises(LiveOutcomeError):
            trader.bind_demo_management_authority("c" * 64, "generation-8")
        invalid = LiveTrader.__new__(LiveTrader)
        invalid._is_current_refit_demo = True
        with self.assertRaises(ValueError):
            invalid.bind_demo_management_authority("not-a-hash", "generation-7")

    def test_demo_reservation_carries_exact_management_authority(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader._is_current_refit_demo = True
        trader._demo_management_capsule_id = ""
        trader._demo_management_generation_id = ""
        ledger = self._enable_durable_feedback(trader)
        account = trader.mt5.get_account_info.return_value
        account.trade_mode = 0
        account.margin_mode = 2
        account.hedge_allowed = True
        # Pre-send identity enforcement requires the exact authorized Demo
        # account before the management-authority gate can be reached.
        account.login = AUTHORIZED_DEMO_ACCOUNT_IDENTITY.login
        account.server = AUTHORIZED_DEMO_ACCOUNT_IDENTITY.server
        account.currency = AUTHORIZED_DEMO_ACCOUNT_IDENTITY.currency
        trader._promotion_authorization_block_reason = MagicMock(return_value=None)
        trader._current_refit_demo_position_block_reason = MagicMock(
            return_value=None
        )
        trader._get_management_configs = MagicMock(return_value={})
        trader.mt5.send_market_order.return_value = SimpleNamespace(
            success=True,
            volume=0.05,
            price=1.2050,
            retcode=10009,
            retcode_description="Done",
            deal=701,
            order=702,
        )
        regime_config = SimpleNamespace(
            artifact_meta={
                "current_refit_binding": {"current_refit_cell_id": "d" * 64}
            }
        )

        self._execute_standard_market_entry(trader, regime_config=regime_config)

        ledger.reserve_pre_send_intent.assert_not_called()
        trader.mt5.send_market_order.assert_not_called()
        self.assertEqual(
            trader._actionable_log.record.call_args[0][1]["action"],
            "BLOCKED_DEMO_MANAGEMENT_AUTHORITY",
        )

        trader.bind_demo_management_authority("b" * 64, "generation-7")
        self._execute_standard_market_entry(trader, regime_config=regime_config)

        reserve_kwargs = ledger.reserve_pre_send_intent.call_args.kwargs
        self.assertEqual(reserve_kwargs["management_capsule_id"], "b" * 64)
        self.assertEqual(
            reserve_kwargs["management_generation_id"], "generation-7"
        )
        self.assertEqual(
            reserve_kwargs["book_role"], "PROCESS_VALIDATED_CURRENT_REFIT"
        )

    def test_execute_entry_reconciles_ambiguous_timeout_to_open_position(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.get_positions.return_value = []
        reconciled_position = SimpleNamespace(
            symbol="XAUUSD",
            magic=12345,
            ticket=777,
            volume=0.05,
            price_open=1.2050,
        )
        trader.mt5.get_position_by_symbol_magic.side_effect = [None, reconciled_position]
        trader.mt5.send_market_order.return_value = SimpleNamespace(
            success=False,
            retcode=10012,
            retcode_description="Request canceled by timeout",
            error_message="",
        )

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="ambiguous-reconciled",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=10000.0),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "EXECUTION_RECONCILED_AFTER_AMBIGUOUS_SEND")
        trader._decision_throttle.record_decision.assert_called()
        self.assertEqual(
            trader._decision_throttle.record_decision.call_args.kwargs["action"],
            "EXECUTION_RECONCILED_AFTER_AMBIGUOUS_SEND",
        )
        trader._log_trade.assert_called()
        self.assertIn("XAUUSD", trader._last_order_times)

    def test_execute_entry_reconciles_explicit_ambiguous_order_result(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        reconciled_position = SimpleNamespace(
            symbol="XAUUSD",
            magic=12345,
            ticket=888,
            volume=0.05,
            price_open=1.2050,
        )
        trader.mt5.get_position_by_symbol_magic.side_effect = [None, reconciled_position]
        trader.mt5.get_positions.return_value = []
        trader.mt5.send_market_order.return_value = SimpleNamespace(
            success=False,
            retcode=0,
            retcode_description="ok",
            error_message="",
            ambiguous=True,
        )

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="ambiguous-flag-reconciled",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=10000.0),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "EXECUTION_RECONCILED_AFTER_AMBIGUOUS_SEND")

    def test_execute_entry_blocks_after_unreconciled_ambiguous_timeout(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.pipeline_config.ambiguous_order_cooldown_seconds = 90
        trader.mt5.get_positions.return_value = []
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.send_market_order.return_value = SimpleNamespace(
            success=False,
            retcode=10012,
            retcode_description="Request canceled by timeout",
            error_message="",
        )

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="ambiguous-unreconciled",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=10000.0),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_AMBIGUOUS_ORDER_UNRECONCILED")
        self.assertIn("XAUUSD", trader._ambiguous_order_block_until)
        self.assertEqual(
            trader._decision_throttle.record_decision.call_args.kwargs["cooldown_seconds"],
            90,
        )

    def test_execute_entry_manual_kill_switch_blocks_new_entries(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.pipeline_config.manual_new_entries_enabled = False

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="manual-block",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=10000.0),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_MANUAL_KILL_SWITCH")
        trader.mt5.send_market_order.assert_not_called()

    def test_execute_entry_records_hidden_symbol_skip(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(visible=False, trade_mode=2)

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=None,
            spec=SimpleNamespace(),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="hidden-symbol",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
        )

        self.assertEqual(
            trader._decision_throttle.record_decision.call_args.kwargs["action"],
            "SKIPPED_SYMBOL_NOT_VISIBLE",
        )
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_SYMBOL_NOT_VISIBLE")

    def test_execute_entry_records_disabled_symbol_skip(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(visible=True, trade_mode=0)

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=None,
            spec=SimpleNamespace(),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="disabled-symbol",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
        )

        self.assertEqual(
            trader._decision_throttle.record_decision.call_args.kwargs["action"],
            "SKIPPED_SYMBOL_TRADE_DISABLED",
        )
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_SYMBOL_TRADE_DISABLED")

    def test_execute_entry_secondary_snapshot_unavailable_records_cleanly(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.get_positions.return_value = None
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.send_market_order = MagicMock()

        strategy = MagicMock()
        strategy.calculate_stops.return_value = (10.0, 20.0)
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        _execute_with_frozen_spread_fixture(trader,
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="secondary-snapshot-missing",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
                "selection_score": 0.25,
                "quality_score": 0.40,
                "freshness": 1.0,
                "regime_strength": 0.62,
            },
            is_secondary_trade=True,
            position_context={"open_positions_total": 1},
            secondary_reason="d1_plus_lower",
            positions_snapshot=None,
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_POSITION_SNAPSHOT_UNAVAILABLE")
        self.assertIsNone(actionable_payload["volume"])
        self.assertIsNone(actionable_payload["actual_risk_pct"])
        self.assertEqual(actionable_payload["secondary_reason"], "d1_plus_lower")
        self.assertEqual(actionable_payload["position_context"]["open_positions_total"], 1)

        throttle_context = trader._decision_throttle.record_decision.call_args.kwargs["context"]
        self.assertEqual(throttle_context["secondary_reason"], "d1_plus_lower")
        self.assertEqual(throttle_context["position_context"]["open_positions_total"], 1)


if __name__ == "__main__":
    unittest.main()
