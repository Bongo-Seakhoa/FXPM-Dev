"""Invented Grid rows retained under explicit FXPM_DERIVED_QA_ROOT."""

import os
from pathlib import Path
import unittest
from unittest import mock
import uuid

import pm_end_grid as grid


class EndGridAtomicBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        expected = Path(__file__).resolve().parents[1] / "pm_end_grid.py"
        if Path(grid.__file__).resolve() != expected:
            raise AssertionError("Grid must be imported from the checkout under test")

    def setUp(self):
        value = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if not value:
            self.skipTest("explicit retained FXPM_DERIVED_QA_ROOT is required")
        parent = Path(value)
        self.assertTrue(parent.is_absolute() and parent.is_dir())
        grid._refuse_reparse(parent)
        self.root = parent / ("end_grid_atomic_batch_" + uuid.uuid4().hex)
        self.root.mkdir(exist_ok=False)
        self.writer = grid.EndGridWriter(self.root)
        self.contract = grid.store_view_contract(self.root, {"fixture": "invented"})
        self.path = self.root / "XAGUSD.jsonl"
        self.first = self.row(1)
        self.writer.append(self.first)
        self.before = self.path.read_bytes()

    def row(self, day, **overrides):
        start = f"2024-06-{day:02d}T00:00:00+00:00"
        end = f"2024-06-{day + 1:02d}T00:00:00+00:00"
        return grid.build_end_grid_row(**{
            "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND",
            "cycle_start_utc": start, "cycle_end_utc": end, "information_cutoff_utc": start,
            "outcome": "NO_TRADE", "reason_code": "INVENTED_NO_TRADE",
            "source_kind": "broker_native", "source_contract_sha256": None,
            "no_trade_run_receipt_sha256": "1" * 64, "view_key": "2" * 64,
            "view_contract_sha256": self.contract,
            "source_identity": {"activation_id": "invented", "native_bars_sha256": "3" * 64},
            "cost_tier": "P75", "process_edition_sha256": "4" * 64,
            "code_sha256": {"pm_end_grid": "5" * 64},
            "produced_at_utc": "2024-07-01T00:00:00+00:00",
            **overrides,
        })

    def test_one_replacement_preserves_prefix_and_returns_input_order(self):
        second, third = self.row(2), self.row(3)
        order = (third, self.first, second)
        with mock.patch.object(grid.os, "replace", wraps=grid.os.replace) as replace:
            result = self.writer.append_many_same_symbol(iter(order))
        replace.assert_called_once()
        source, target = replace.call_args.args
        self.assertEqual(source.parent, self.path.parent)
        self.assertEqual(target, self.path)
        self.assertFalse(source.exists())
        self.assertEqual(self.path.read_bytes(), self.before + third.to_bytes() + b"\n" + second.to_bytes() + b"\n")
        self.assertEqual(result, [
            {"path": str(self.path), "row_sha256": row.row_sha256,
             "appended": appended, "rows_in_file": 3}
            for row, appended in zip(order, (True, False, True), strict=True)
        ])

        with mock.patch.object(grid.os, "replace") as replace:
            retry = self.writer.append_many_same_symbol(order)
        replace.assert_not_called()
        self.assertEqual(retry, [{**descriptor, "appended": False} for descriptor in result])

    def test_refused_batch_keeps_original_grid(self):
        second = self.row(2)
        cases = (
            ("GRID_BATCH_EMPTY", ()),
            ("GRID_BATCH_DUPLICATE_CYCLE", (second, second)),
            ("GRID_BATCH_SYMBOL_MISMATCH", (second, self.row(3, symbol="EURUSD"))),
            ("CELL_CYCLE_ALREADY_RECORDED", (second, self.row(1, reason_code="INVENTED_DIFFERENT"))),
            ("CONTRACT_NOT_RETAINED", (second, self.row(3, view_contract_sha256="6" * 64))),
        )
        for code, rows in cases:
            with self.subTest(code=code):
                with mock.patch.object(grid.os, "replace") as replace:
                    with self.assertRaisesRegex(grid.EndGridError, code):
                        self.writer.append_many_same_symbol(rows)
                replace.assert_not_called()
                self.assertEqual(self.path.read_bytes(), self.before)
        self.assertFalse((self.root / "EURUSD.jsonl").exists())

    def test_aggregate_size_refusal_preserves_original_grid(self):
        second = self.row(2)
        cap = len(self.before) + len(second.to_bytes())
        with mock.patch.object(grid, "MAX_FILE_BYTES", cap):
            with self.assertRaisesRegex(grid.EndGridError, "GRID_FILE_TOO_LARGE"):
                self.writer.append_many_same_symbol((second,))
        self.assertEqual(self.path.read_bytes(), self.before)

    def test_failed_replacement_preserves_original_and_cleans_only_owned_temporary(self):
        unrelated = self.root / "XAGUSD.jsonl.unrelated.tmp"
        unrelated.write_bytes(b"unrelated")
        with mock.patch.object(grid.os, "replace", side_effect=OSError("invented replace failure")) as replace:
            with self.assertRaisesRegex(OSError, "invented replace failure"):
                self.writer.append_many_same_symbol((self.row(2), self.row(3)))
        replace.assert_called_once()
        source, target = replace.call_args.args
        self.assertEqual(source.parent, self.path.parent)
        self.assertEqual(target, self.path)
        self.assertFalse(source.exists())
        self.assertEqual(self.path.read_bytes(), self.before)
        self.assertEqual(unrelated.read_bytes(), b"unrelated")
