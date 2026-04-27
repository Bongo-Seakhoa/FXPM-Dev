"""Tests for findings.html §C6: regime coverage one-liner per (symbol, TF)
emitted from pipeline.run_for_symbol after the per-TF winner summary."""

import logging
import unittest

import pandas as pd

from pm_pipeline import OptimizationPipeline as PortfolioPipeline  # noqa: N813


class RegimeCoverageReportTests(unittest.TestCase):
    @staticmethod
    def _df(regimes):
        return pd.DataFrame({'REGIME': regimes})

    def test_all_four_buckets_logged_in_order(self):
        train = {'H1': self._df(['TREND'] * 50 + ['RANGE'] * 30 + ['BREAKOUT'] * 10 + ['CHOP'] * 10)}
        val = {'H1': self._df(['TREND'] * 25 + ['CHOP'] * 25)}
        with self.assertLogs("pm_pipeline", level="INFO") as cm:
            PortfolioPipeline._log_regime_coverage("EURUSD", train, val)
        coverage_lines = [m for m in cm.output if "regime coverage" in m]
        self.assertEqual(len(coverage_lines), 1)
        line = coverage_lines[0]
        # Combined (150 rows): TREND 75/150=50%, RANGE 30/150=20%, BREAKOUT 10/150≈6.7%, CHOP 35/150≈23.3%.
        self.assertIn("TREND 50.0%", line)
        self.assertIn("RANGE 20.0%", line)
        self.assertIn("BREAKOUT 6.7%", line)
        self.assertIn("CHOP 23.3%", line)
        self.assertLess(line.index("TREND"), line.index("RANGE"))
        self.assertLess(line.index("RANGE"), line.index("BREAKOUT"))
        self.assertLess(line.index("BREAKOUT"), line.index("CHOP"))

    def test_emits_one_line_per_timeframe(self):
        train = {
            'H1': self._df(['TREND'] * 100),
            'H4': self._df(['CHOP'] * 100),
        }
        val = {'H1': self._df(['TREND'] * 10), 'H4': self._df(['CHOP'] * 10)}
        with self.assertLogs("pm_pipeline", level="INFO") as cm:
            PortfolioPipeline._log_regime_coverage("EURUSD", train, val)
        coverage_lines = [m for m in cm.output if "regime coverage" in m]
        self.assertEqual(len(coverage_lines), 2)

    def test_missing_regime_column_silent(self):
        train = {'H1': pd.DataFrame({'OTHER': [1, 2, 3]})}
        val = {'H1': pd.DataFrame({'OTHER': [4, 5, 6]})}
        # No INFO at all → assertLogs context would fail with no logs; use a logger and check.
        logger = logging.getLogger("pm_pipeline")
        with self.assertLogs(logger, level="DEBUG") as cm:
            logger.debug("trigger")  # ensure context has at least one record
            PortfolioPipeline._log_regime_coverage("EURUSD", train, val)
        self.assertEqual([m for m in cm.output if "regime coverage" in m], [])

    def test_empty_after_dropna_silent(self):
        train = {'H1': pd.DataFrame({'REGIME': [None, None]})}
        val = {'H1': pd.DataFrame({'REGIME': [None]})}
        logger = logging.getLogger("pm_pipeline")
        with self.assertLogs(logger, level="DEBUG") as cm:
            logger.debug("trigger")
            PortfolioPipeline._log_regime_coverage("EURUSD", train, val)
        self.assertEqual([m for m in cm.output if "regime coverage" in m], [])

    def test_uses_only_train_when_val_missing(self):
        train = {'H1': self._df(['TREND'] * 80 + ['RANGE'] * 20)}
        val = {}
        with self.assertLogs("pm_pipeline", level="INFO") as cm:
            PortfolioPipeline._log_regime_coverage("EURUSD", train, val)
        coverage_lines = [m for m in cm.output if "regime coverage" in m]
        self.assertEqual(len(coverage_lines), 1)
        self.assertIn("TREND 80.0%", coverage_lines[0])
        self.assertIn("RANGE 20.0%", coverage_lines[0])


if __name__ == "__main__":
    unittest.main()
