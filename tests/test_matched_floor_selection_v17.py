"""Prospective invented contract fixtures; no financial results or run acceptance.

These tests have not been executed during SOURCE251 preparation.
"""
import copy
import unittest

import pm_winner_selection as winner
import pm_book_multiplicity as book


def candidate(identity, neff, cardinality=None, active=None):
    return {"process_candidate_id": identity,
        "lane_statistics": {"P75": {"completed_trade_hac_effective_count": neff}},
        "selection_complexity": {"finite_domain_cardinality": cardinality,
            "active_free_parameter_count": active}}


class MatchedFloorTieContract(unittest.TestCase):
    def test_neff_precedes_complexity_and_id(self):
        low = candidate("a", 9, 1, 0)
        high = candidate("z", 10, 1000, 8)
        self.assertIs(winner._v17_statistical_tie([low, high]), high)

    def test_finite_domain_is_compared_only_with_complete_remaining_pool(self):
        left = candidate("z", 10, 2, 3)
        right = candidate("a", 10, 4, 0)
        self.assertIs(winner._v17_statistical_tie([right, left]), left)

    def test_one_unknown_cardinality_uses_whole_pool_active_count(self):
        left = candidate("z", 10, 1, 3)
        right = candidate("a", 10, None, 1)
        self.assertIs(winner._v17_statistical_tie([left, right]), right)

    def test_one_unknown_active_count_uses_whole_pool_id(self):
        left = candidate("a", 10, 1, 7)
        right = candidate("z", 10, None, None)
        self.assertIs(winner._v17_statistical_tie([right, left]), left)

    def test_lower_neff_missing_complexity_does_not_contaminate_remaining_pool(self):
        low = candidate("0", 9, None, None)
        left = candidate("z", 10, 2, 3)
        right = candidate("a", 10, 4, 0)
        self.assertIs(winner._v17_statistical_tie([low, right, left]), left)

    def test_permutation_cannot_change_choice(self):
        a = candidate("a", 10, 2, 1)
        z = candidate("z", 10, 2, 1)
        self.assertIs(winner._v17_statistical_tie([z, a]), a)
        self.assertIs(winner._v17_statistical_tie([a, z]), a)


class MatchedFloorFrozenComplexity(unittest.TestCase):
    def test_unavailable_complexity_cannot_smuggle_parameter_key_count(self):
        row = {"ordinal": 0, "process_candidate_id": "a" * 40, "strategy_id": "fixture_strategy",
            "trial_definition_sha256": "b" * 64, "refit_recipe_sha256": "c" * 64,
            "source_strategy_domain_sha256": None, "parameter_domain": None,
            "parameter_domain_sha256": None, "finite_domain_cardinality": None,
            "active_free_parameter_count": 8}
        with self.assertRaises(ValueError):
            winner.validate_selection_complexity_roster([row], expected_count=1)
        row["active_free_parameter_count"] = None
        self.assertEqual(winner.validate_selection_complexity_roster([row], expected_count=1), [row])

    def test_trial_recipe_splice_is_rejected(self):
        row = {"ordinal": 0, "process_candidate_id": "a" * 40, "strategy_id": "fixture_strategy",
            "trial_definition_sha256": "b" * 64, "refit_recipe_sha256": "c" * 64,
            "source_strategy_domain_sha256": None, "parameter_domain": None,
            "parameter_domain_sha256": None, "finite_domain_cardinality": None,
            "active_free_parameter_count": None}
        attempt = {key: row[key] for key in ("ordinal", "process_candidate_id", "strategy_id",
            "trial_definition_sha256", "refit_recipe_sha256")}
        attempt["refit_recipe_sha256"] = "d" * 64
        with self.assertRaises(ValueError):
            winner.validate_selection_complexity_roster([row], attempts=[attempt], expected_count=1)

    def test_generation_is_explicit_and_preserves_transport(self):
        self.assertEqual(winner.V16_GENERATION.selection_version, "fxpm.winner-selection.v16")
        self.assertEqual(winner.V17_GENERATION.selection_version, "fxpm.winner-selection.v17")
        self.assertIs(winner.V17_GENERATION.transport, winner.V16_GENERATION.transport)
        self.assertIs(book.V17_GENERATION.winner_generation, winner.V17_GENERATION)
        self.assertNotEqual(book.V17_GENERATION.raw_schema, book.V16_GENERATION.raw_schema)
        self.assertNotEqual(winner.V17_GENERATION.policy_sha256, winner.V16_GENERATION.policy_sha256)

    def test_full_panel_selection_controls_are_retained(self):
        changed = {key for key in winner.V17_SELECTION_POLICY
            if winner.V17_SELECTION_POLICY[key] != winner.CURRENT_SELECTION_POLICY.get(key)}
        self.assertEqual(changed, {"policy_version", "selection_addendum_contract_sha256",
            "candidate_admissibility_gate_codes", "winner_rule"})
        self.assertEqual(winner.V17_SELECTION_POLICY["candidate_admissibility_gate_codes"][:-2],
            winner.CURRENT_SELECTION_POLICY["candidate_admissibility_gate_codes"])


class MatchedFloorSourceComparison(unittest.TestCase):
    """Invented narrow source-comparator fixtures; no owner or financial authority."""
    def fixture(self):
        import types
        from unittest import mock
        import numpy as np
        import pandas as pd
        import pm_authoritative_search_execution as execution
        frame = pd.DataFrame({"Open": [10., 11.], "High": [12., 13.],
            "Low": [9., 10.], "Close": [11., 12.]},
            index=pd.date_range("2020-01-01", periods=2, freq="h", tz="UTC"))
        spec = execution.InstrumentSpec("INVENTED", 2, point=0.01, tick_size=0.01,
            tick_value=0.01, pip_value=1.0)
        config = execution.PipelineConfig(swap_model_enabled=False)
        arrays = {"P75": (np.array([0.1, 0.1]), np.array([0.2, 0.2])),
            "P95": (np.array([0.3, 0.3]), np.array([0.4, 0.4]))}
        original_pricing = execution.Backtester
        class PricingOnly(original_pricing):
            def _build_half_spread_arrays(self, source, actual_spec, *, spread_tier, timeframe):
                return (*arrays[spread_tier], {"invented_tier": spread_tier})
        geometry = {"frame_start_utc": frame.index[0].isoformat(),
            "end_exclusive_utc": (frame.index[-1] + pd.Timedelta(hours=1)).isoformat(),
            "frame_rows": len(frame), "frame_sha256": execution._frame_sha256(frame)}
        facts = {"instrument_spec": vars(spec), "half_spread_arrays": {},
            "spread_model_metadata": {tier: {"invented_tier": tier} for tier in arrays},
            "floor_status": "AVAILABLE_MATCHED_BASE_VERIFIED",
            "trades": [{"base_fill_witness": {"entry_bar": 0,
                "entry_mid_open": 10., "entry_time_utc": frame.index[0].isoformat(),
                "held_bar_inputs": [{"bar": i, "open_utc": stamp.isoformat(),
                    "ohlc": {name: float(frame.iloc[i][name]) for name in ("Open", "High", "Low", "Close")}}
                    for i, stamp in enumerate(frame.index)]}}]}
        # This source-comparison fixture uses real disabled-swap input reconstruction.
        # The invented captured trade grants no issuer or economic selection authority.
        row = facts["trades"][0]
        row.update(entry_price=10.1, recorded_stop_price=9.1, executed_volume=1.0,
            base_trade_identity={"signal_bar": 0, "entry_bar": 0, "exit_bar": 1,
                "direction": "LONG", "exit_reason": execution.pm_core.TradeStatus.CLOSED_EOD.value,
                "position_size": 1.0})
        row["base_fill_witness"]["observed_exit_price"] = 12.0
        row["executed_price_swap_capture"] = {"schema_version": "fxpm.backtest-exact-executed-trade.v1",
            "ordinal": 0, "backend": "PYTHON", "signal_bar": 0, "entry_bar": 0, "exit_bar": 1,
            "direction": "LONG", "entry_price": 10.1, "exit_price": 12.0, "initial_stop_price": 9.1,
            "executed_volume": 1.0, "exit_reason": execution.pm_core.TradeStatus.CLOSED_EOD.value,
            "swap_dollars_exact": 0.0, "swap_rollover_units": 0,
            "swap_source_kind": "DISABLED_OR_PENDING_LEGACY", "swap_cash_steps": []}
        facts["exact_swap_model_enabled"] = False
        facts["exact_swap_accrual_contract"] = original_pricing(config)._build_causal_swap_inputs(
            frame, spec, np.array([10.1, np.nan]), timeframe="INVENTED")[-1]
        for tier, (entry, intrabar) in arrays.items():
            facts["half_spread_arrays"][tier] = {
                "entry_half_spread": execution._route_a_encode_spread_array(entry, "entry_half_spread"),
                "intrabar_half_spread": execution._route_a_encode_spread_array(intrabar, "intrabar_half_spread")}
        def check(value):
            with mock.patch.object(execution, "Backtester", PricingOnly):
                return execution._verify_route_a_matched_fact_source(value, geometry,
                    source_frame=frame, spec=spec, tiers={tier: tier for tier in arrays},
                    config=config, timeframe="INVENTED")
        return facts, check

    def test_actual_source_comparison_accepts_identical_retained_inputs(self):
        facts, check = self.fixture()
        self.assertTrue(check(facts))

    def test_claimed_geometry_hash_does_not_allow_held_ohlc_drift(self):
        facts, check = self.fixture()
        facts["trades"][0]["base_fill_witness"]["held_bar_inputs"][1]["ohlc"]["High"] += 1
        with self.assertRaisesRegex(RuntimeError, "MATCHED_FLOOR_HELD_BAR_SOURCE_CHANGED"):
            check(facts)

    def test_full_array_source_drift_is_rejected_before_selected_half_use(self):
        facts, check = self.fixture()
        facts["half_spread_arrays"]["P95"]["entry_half_spread"] = copy.deepcopy(
            facts["half_spread_arrays"]["P75"]["entry_half_spread"])
        with self.assertRaisesRegex(RuntimeError, "MATCHED_FLOOR_SPREAD_ARRAY_SOURCE_CHANGED"):
            check(facts)


class MatchedFloorPackedProof(unittest.TestCase):
    """Invented codec/packing fixtures only; fake sink grants no production origin."""
    def fixture(self):
        import types
        from unittest import mock
        import numpy as np
        import pandas as pd
        import pm_authoritative_search_execution as execution
        import pm_winner_raw_evidence as raw
        frame = pd.DataFrame({"Open": [10., 11.], "High": [12., 13.],
            "Low": [9., 10.], "Close": [11., 12.]},
            index=pd.date_range("2020-01-01", periods=2, freq="h", tz="UTC"))
        geometry = {"role": "final_lockbox", "receipt_sha256": "a" * 64,
            "frame_sha256": execution._frame_sha256(frame), "frame_start_utc": frame.index[0].isoformat(),
            "end_exclusive_utc": (frame.index[-1] + pd.Timedelta(hours=1)).isoformat()}
        entry = execution._route_a_encode_spread_array(np.array([0.1, 0.1]), "entry_half_spread")
        intrabar = execution._route_a_encode_spread_array(np.array([0.2, 0.2]), "intrabar_half_spread")
        held = [{"bar": i, "open_utc": stamp.isoformat(),
            "ohlc": {name: float(frame.iloc[i][name]) for name in ("Open", "High", "Low", "Close")},
            "p75_intrabar_half_price": 0.2} for i, stamp in enumerate(frame.index)]
        witnesses = [{"fold_ordinal": 0, "partition_role": "final_lockbox",
            "partition_geometry_receipt_sha256": geometry["receipt_sha256"],
            "partition_frame_sha256": geometry["frame_sha256"],
            "facts": {"half_spread_arrays": {tier: copy.deepcopy({"entry_half_spread": entry,
                "intrabar_half_spread": intrabar}) for tier in ("P75", "P95")},
                "trades": [{"base_fill_witness": {"held_bar_inputs": held,
                    "held_bar_inputs_sha256": raw._sha_json(held)}}]}}]
        frozen = types.SimpleNamespace(artifact_namespace="declared-slot/invented", timeframe="M30",
            regime=book.REGIME_ORDER[0], precommitment={"fixture": "INVENTED_PACKING_ONLY"})
        attempt = {"ordinal": 0, "attempt_id": "c" * 40}
        bodies, entries = {}, {}
        def put(sink, artifact_id, artifact_type, body):
            digest = raw._sha_json(body)
            item = {"artifact_id": artifact_id, "artifact_type": artifact_type,
                "descriptor": {"sha256": digest}}
            bodies[artifact_id] = copy.deepcopy(body);entries[artifact_id] = item
            return item
        with mock.patch.object(raw, "_put_ascii_json", side_effect=put):
            packed, _ = raw._pack_matched_floor_witnesses(sink=object(), frozen=frozen,
                attempt=attempt, witnesses=witnesses, array_entries={})
        def expand(source):
            return raw._expand_matched_floor_witnesses(packed, frozen=frozen, attempt=attempt,
                raw_folds=[{"provenance": {"anchor_horizon_decision": {"partition_geometry": [geometry]}}}],
                source_frame=source, read=lambda artifact_id: copy.deepcopy(bodies[artifact_id]), entries=entries)
        return witnesses, packed, bodies, frame, expand

    def test_identical_immutable_arrays_share_two_blobs_and_expand_exact_original_hash(self):
        original, packed, blobs, frame, expand = self.fixture()
        self.assertEqual(len(blobs), 2)
        self.assertEqual(expand(frame), original)
        held = packed["rows"][0]["facts"]["trades"][0]["base_fill_witness"]["held_bar_inputs"]
        self.assertIsInstance(held, dict)
        self.assertNotIn("ohlc", held)

    def test_source_bar_change_cannot_be_hidden_by_reusing_old_geometry_label(self):
        _, _, _, frame, expand = self.fixture()
        frame.iloc[1, frame.columns.get_loc("High")] += 1
        with self.assertRaisesRegex(ValueError, "MATCHED_FLOOR_ORIGINAL_SOURCE_BAR_CONTENT_CHANGED"):
            expand(frame)


class MatchedFloorExactDecimalAdmission(unittest.TestCase):
    def test_equal_decimal_means_pass_and_adjacent_e12_below_fails(self):
        """INVENTED canonical R fixture: binary cancellation must not reject equality."""
        base, costs = [0.3, 0.2, -0.4], [0.03, 0.03, 0.04]
        self.assertLess(winner._mean(base), winner._mean(costs))
        self.assertFalse(winner._v17_below_matched_stress_economic_floor(base, costs))
        self.assertTrue(winner._v17_below_matched_stress_economic_floor(
            [0.3, 0.2, -0.400000000001], costs))
