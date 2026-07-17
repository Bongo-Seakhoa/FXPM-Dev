from __future__ import annotations

import itertools
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from tools.calibrate_capital_envelope import (
    MAX_OUTPUT_PATH_CHARS,
    MAX_REPORT_BYTES,
    MAX_SENSITIVITY_PATHS,
    _bounded_existing_bytes,
    _prepare_output_path,
    _simulate_scenario,
    build_report,
    deterministic_stress_table,
    derive_capital_envelope,
    exact_binary_trade_math,
    main,
    margin_level_from_utilization,
)


class CapitalEnvelopeCalibrationTests(unittest.TestCase):
    def test_offline_tool_withholds_every_numeric_recommendation(self) -> None:
        envelope = derive_capital_envelope()

        self.assertEqual(
            envelope.recommendation_status,
            "WITHHELD_NO_FRESH_AUTHENTICATED_FRONTIER",
        )
        self.assertIsNone(envelope.total_initial_stop_risk_ceiling_pct)
        self.assertIsNone(envelope.factor_group_initial_stop_risk_ceiling_pct)
        self.assertIsNone(envelope.same_symbol_initial_stop_risk_ceiling_pct)
        self.assertIsNone(envelope.margin_utilization_ceiling_pct)
        self.assertIsNone(envelope.gross_notional_to_equity_ceiling_pct)
        self.assertIsNone(envelope.abs_net_notional_to_equity_ceiling_pct)
        self.assertIn(
            "external_pareto_member_authorization", envelope.evidence_required
        )

    def test_margin_utilization_is_not_stop_risk(self) -> None:
        self.assertAlmostEqual(margin_level_from_utilization(50.0), 200.0)
        self.assertAlmostEqual(margin_level_from_utilization(100.0 / 3.0), 300.0)
        envelope = derive_capital_envelope()
        self.assertIsNone(envelope.margin_utilization_ceiling_pct)
        self.assertIsNone(envelope.total_initial_stop_risk_ceiling_pct)

    def test_thin_edge_math_is_not_misrepresented(self) -> None:
        result = exact_binary_trade_math(0.35, 2.0)
        self.assertAlmostEqual(result["gross_expectancy_r"], 0.05)
        self.assertAlmostEqual(result["break_even_win_probability"], 1.0 / 3.0)
        self.assertAlmostEqual(result["binary_kelly_fraction"], 0.025)

    def test_invalid_per_trade_risk_rejected(self) -> None:
        with self.assertRaises(ValueError):
            derive_capital_envelope(per_trade_risk_pct=0.0)

    def test_sensitivity_report_cannot_masquerade_as_selection(self) -> None:
        report = build_report(paths=100, batches=1, seed=7)
        self.assertEqual(
            report["selection_rule"],
            "NONE_RECOMMENDATION_WITHHELD_PENDING_AUTHENTICATED_PARETO_FRONTIER",
        )
        self.assertFalse(report["profitability_guarantee"])
        self.assertIsNone(
            report["capital_envelope"]["total_initial_stop_risk_ceiling_pct"]
        )
        self.assertIn(
            "factor_cap_pct_sensitivity_assumption",
            report["simulation_contract"],
        )

    def test_sensitivity_arrays_and_work_are_explicitly_bounded(self) -> None:
        with self.assertRaisesRegex(ValueError, "paths"):
            build_report(paths=MAX_SENSITIVITY_PATHS + 1, batches=1, seed=7)
        with self.assertRaisesRegex(ValueError, "cap roster"):
            deterministic_stress_table(itertools.repeat(3.0), (1.0,))

    def test_work_bound_counts_caps_factors_and_scenarios_before_allocation(self) -> None:
        with self.assertRaisesRegex(ValueError, "cap/scenario/factor work"):
            _simulate_scenario(
                caps=(90.0,),
                paths=100_000,
                batches=10,
                per_trade_risk_pct=0.25,
                factor_cap_pct=2.0,
                win_probability=0.35,
                payoff_r=2.0,
                within_factor_correlation=0.25,
                factor_shock_probability=0.001,
                global_shock_probability=0.001,
                shock_loss_multiple=1.25,
                seed=7,
            )

    def test_cap_to_trade_ratio_is_bounded_before_sizes_list(self) -> None:
        with self.assertRaisesRegex(ValueError, "ratio"):
            _simulate_scenario(
                caps=(90.0,),
                paths=100,
                batches=1,
                per_trade_risk_pct=0.000001,
                factor_cap_pct=2.0,
                win_probability=0.35,
                payoff_r=2.0,
                within_factor_correlation=0.25,
                factor_shock_probability=0.001,
                global_shock_probability=0.001,
                shock_loss_multiple=1.25,
                seed=7,
            )

    def test_existing_output_comparison_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "oversized.json"
            with path.open("wb") as handle:
                handle.seek(MAX_REPORT_BYTES)
                handle.write(b"x")
            with self.assertRaisesRegex(ValueError, "byte bound"):
                _bounded_existing_bytes(path)

    def test_output_path_rejects_symlink_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            target = root / "target"
            target.mkdir()
            link = root / "link"
            try:
                os.symlink(target, link, target_is_directory=True)
            except OSError:
                self.skipTest("symlink creation is unavailable")
            with self.assertRaisesRegex(ValueError, "link|reparse"):
                _prepare_output_path(str(link / "report.json"))

    def test_cli_rejects_oversized_output_path_before_simulation(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "calibrate_capital_envelope.py",
                "--output",
                "x" * (MAX_OUTPUT_PATH_CHARS + 1),
            ],
        ):
            with self.assertRaises(SystemExit) as raised:
                main()
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
