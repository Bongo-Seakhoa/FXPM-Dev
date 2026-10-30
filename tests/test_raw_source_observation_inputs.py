"""Invented in-memory inputs for Raw source reuse; no market or qualification claim.

The eight owner checks retain their QA oracles and mocked native boundary.
The ninth check inspects actual producer wiring without executing its AST.
"""

from copy import deepcopy
import ast
import inspect
from pathlib import Path
from types import SimpleNamespace
import sys
import textwrap
import unittest
from unittest import mock

import pandas as pd


PROJECT = Path(__file__).resolve().parents[1]
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

import pm_winner_raw_evidence as raw


SYNTHETIC_CONTEXT_SHA = "c" * 64


def _carrier(digest="a" * 64):
    return {"data_dir": "SYNTHETIC_UNOPENED_DATA_ROOT",
            "index_descriptor": {
                "schema_version": "fxpm.authority-spool-artifact-descriptor.v1",
                "relative_path": "objects/sha256/" + digest[:2] + "/" + digest + ".canonical-json",
                "sha256": digest, "size_bytes": 1, "format": "canonical-json"},
            "index_sha256": digest, "symbol": "SYNTHETIC", "timeframe": "M5",
            "cutoff_utc": "2024-01-01T00:30:00Z", "activation_manifest_sha256": "d" * 64,
            "production_contract_required": True}


def _frame(spreads=(0.0, 2.0, float("nan"), 4.0)):
    return pd.DataFrame({"Spread": list(spreads)},
                        index=pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="5min"))


def _case():
    frozen = SimpleNamespace(symbol="SYNTHETIC", generation_context_sha256=SYNTHETIC_CONTEXT_SHA)
    sink = SimpleNamespace(context=SimpleNamespace(context_sha256=SYNTHETIC_CONTEXT_SHA))
    return frozen, sink, raw._SourceObservationInputs(frozen)


class SourceObservationInputChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(raw.__file__).resolve()
        if path != PROJECT / "pm_winner_raw_evidence.py":
            raise AssertionError("RAW_NORMAL_IMPORT_ORIGIN_MISMATCH")

    def test_same_carrier_across_receipts_has_one_acquisition_boundary_call(self):
        frozen, sink, owner = _case()
        product, fine = object(), _frame()
        carrier = _carrier()
        equivalent = dict(reversed(list(deepcopy(carrier).items())))
        with mock.patch.object(raw, "_open_source_observation_native", return_value=(product, fine)) as acquire:
            for receipt_carrier in (carrier, deepcopy(carrier), equivalent):
                actual_product, actual_frame = owner.open(receipt_carrier, frozen=frozen, sink=sink)
                self.assertIs(actual_product, product)
                self.assertIs(actual_frame, fine)
            acquire.assert_called_once_with(carrier, sink=sink, symbol=frozen.symbol)

    def test_repeated_exact_axis_memo_preserves_zero_missing_points_and_close_axis(self):
        frozen, sink, owner = _case()
        carrier, fine = _carrier(), _frame()
        target = pd.DatetimeIndex(["2024-01-01T00:05:00Z", "2024-01-01T00:10:00Z",
                                   "2024-01-01T00:15:00Z", "2024-01-01T00:17:00Z"])
        actual_align = raw.align_prior_completed_spreads
        with mock.patch.object(raw, "_open_source_observation_native", return_value=(object(), fine)):
            owner.open(carrier, frozen=frozen, sink=sink)
            with mock.patch.object(raw, "align_prior_completed_spreads", wraps=actual_align) as align:
                first = owner.align(target, fine, "M5", carrier)
                second = owner.align(target.copy(), fine, "M5", deepcopy(carrier))
                self.assertIs(first, second)
                self.assertEqual(align.call_count, 1)
        expected_points = pd.Series([0.0, 2.0, float("nan"), float("nan")],
                                    index=target, name="EntrySpreadPoints")
        expected_closes = pd.Series([target[0], target[1], target[2], pd.NaT],
                                    index=target, dtype="datetime64[ns, UTC]",
                                    name="EntrySpreadSourceCloseUTC")
        pd.testing.assert_series_equal(first[0], expected_points)
        pd.testing.assert_series_equal(first[1], expected_closes)

    def test_distinct_axes_are_bounded_at_twelve_and_evicted_axis_is_recomputed(self):
        frozen, sink, owner = _case()
        carrier, fine = _carrier(), _frame()
        targets = [pd.DatetimeIndex([pd.Timestamp("2024-01-01T00:05:00Z") + pd.Timedelta(minutes=5 * n)])
                   for n in range(13)]
        actual_align = raw.align_prior_completed_spreads
        with mock.patch.object(raw, "_open_source_observation_native", return_value=(object(), fine)):
            owner.open(carrier, frozen=frozen, sink=sink)
            with mock.patch.object(raw, "align_prior_completed_spreads", wraps=actual_align) as align:
                first = owner.align(targets[0], fine, "M5", carrier)
                for target in targets[1:]:
                    owner.align(target, fine, "M5", carrier)
                    self.assertLessEqual(len(owner._axes), 12)
                self.assertEqual(len(owner._axes), 12)
                self.assertEqual(align.call_count, 13)
                repeated = owner.align(targets[0], fine, "M5", carrier)
                self.assertEqual(align.call_count, 14)
                self.assertIsNot(first, repeated)
                pd.testing.assert_series_equal(first[0], repeated[0])
                pd.testing.assert_series_equal(first[1], repeated[1])

    def test_foreign_frozen_or_context_refuses_before_acquisition_and_release(self):
        for foreign in ("frozen", "context"):
            with self.subTest(foreign=foreign):
                frozen, sink, owner = _case()
                wrong_frozen = SimpleNamespace(**vars(frozen)) if foreign == "frozen" else frozen
                wrong_sink = (SimpleNamespace(context=SimpleNamespace(context_sha256="e" * 64))
                              if foreign == "context" else sink)
                with mock.patch.object(raw, "_open_source_observation_native") as acquire:
                    with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_INPUT_OWNER_MISMATCH"):
                        owner.open(_carrier(), frozen=wrong_frozen, sink=wrong_sink)
                    with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_INPUT_OWNER_MISMATCH"):
                        owner.finish(frozen=wrong_frozen, sink=wrong_sink)
                    acquire.assert_not_called()

    def test_copied_frame_cannot_impersonate_the_cached_source_owner(self):
        frozen, sink, owner = _case()
        carrier, fine = _carrier(), _frame()
        with mock.patch.object(raw, "_open_source_observation_native", return_value=(object(), fine)):
            owner.open(carrier, frozen=frozen, sink=sink)
        with mock.patch.object(raw, "align_prior_completed_spreads") as align:
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_INPUT_OWNER_MISMATCH"):
                owner.align(fine.index + pd.Timedelta(minutes=5), fine.copy(deep=True), "M5", carrier)
            align.assert_not_called()

    def test_distinct_carrier_is_independently_acquired_and_never_aliases_first_cache(self):
        frozen, sink, owner = _case()
        first_carrier, other_carrier = _carrier(), _carrier("b" * 64)
        first_frame, other_frame = _frame(), _frame((10.0, 20.0, 30.0, 40.0))
        first_product, other_product = object(), object()

        def acquire(carrier, **kwargs):
            self.assertEqual(kwargs, {"sink": sink, "symbol": frozen.symbol})
            return ((first_product, first_frame) if carrier == first_carrier else (other_product, other_frame))

        target = pd.DatetimeIndex(["2024-01-01T00:05:00Z"])
        actual_align = raw.align_prior_completed_spreads
        with mock.patch.object(raw, "_open_source_observation_native", side_effect=acquire) as opened:
            owner.open(first_carrier, frozen=frozen, sink=sink)
            with mock.patch.object(raw, "align_prior_completed_spreads", wraps=actual_align) as aligned:
                first = owner.align(target, first_frame, "M5", first_carrier)
                for _ in range(2):
                    product, fine = owner.open(other_carrier, frozen=frozen, sink=sink)
                    self.assertIs(product, other_product)
                    other = owner.align(target, fine, "M5", other_carrier)
                    self.assertEqual(other[0].iloc[0], 10.0)
                self.assertEqual(first[0].iloc[0], 0.0)
                self.assertEqual(aligned.call_count, 3)
                self.assertEqual(len(owner._axes), 1)
            self.assertEqual(opened.call_count, 3)
            self.assertIs(owner._entry[2], first_frame)
            owner.finish(frozen=frozen, sink=sink)
            self.assertEqual(opened.call_count, 4)
            self.assertEqual(opened.call_args.args, (first_carrier,))
            self.assertIsNone(owner._entry)
            self.assertEqual(owner._axes, {})

    def test_changed_carrier_verifier_refusal_propagates_without_cached_substitution(self):
        frozen, sink, owner = _case()
        first_carrier, other_carrier = _carrier(), _carrier("b" * 64)
        fine = _frame()
        failure = raw.WinnerRawEvidenceError("SOURCE_OBSERVATION_NATIVE_REOPEN_FAILED")
        with mock.patch.object(raw, "_open_source_observation_native", side_effect=[(object(), fine), failure]) as acquire:
            owner.open(first_carrier, frozen=frozen, sink=sink)
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_NATIVE_REOPEN_FAILED"):
                owner.open(other_carrier, frozen=frozen, sink=sink)
            self.assertEqual(acquire.call_count, 2)
            self.assertEqual(acquire.call_args.args, (other_carrier,))
            self.assertIs(owner._entry[2], fine)

    def test_finish_releases_bulk_state_before_independent_reopen_and_propagates_tamper_refusal(self):
        frozen, sink, owner = _case()
        carrier, fine = _carrier(), _frame()
        product = object()
        calls = []

        def acquire(actual_carrier, **kwargs):
            calls.append(deepcopy(actual_carrier))
            self.assertEqual(kwargs, {"sink": sink, "symbol": frozen.symbol})
            if len(calls) == 1:
                return product, fine
            self.assertIsNone(owner._entry)
            self.assertEqual(owner._axes, {})
            raise raw.WinnerRawEvidenceError("SOURCE_OBSERVATION_NATIVE_REOPEN_FAILED")

        with mock.patch.object(raw, "_open_source_observation_native", side_effect=acquire):
            owner.open(carrier, frozen=frozen, sink=sink)
            owner.align(fine.index + pd.Timedelta(minutes=5), fine, "M5", carrier)
            self.assertTrue(owner._axes)
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_NATIVE_REOPEN_FAILED"):
                owner.finish(frozen=frozen, sink=sink)
        self.assertEqual(calls, [carrier, carrier])
        self.assertIsNone(owner._entry)
        self.assertEqual(owner._axes, {})


    def test_public_producer_finishes_source_inputs_before_winner_request(self):
        producer = ast.parse(textwrap.dedent(inspect.getsource(raw._produce_winner_raw_evidence))).body[0]
        self.assertIsInstance(producer, ast.FunctionDef)

        def dotted(node):
            if isinstance(node, ast.Name):
                return node.id
            if isinstance(node, ast.Attribute):
                parent = dotted(node.value)
                return None if parent is None else parent + "." + node.attr
            return None

        def calls(name):
            return [node for node in ast.walk(producer)
                    if isinstance(node, ast.Call) and dotted(node.func) == name]

        def top_index(node):
            owners = [index for index, statement in enumerate(producer.body)
                      if any(child is node for child in ast.walk(statement))]
            self.assertEqual(len(owners), 1)
            return owners[0]

        finishes = calls("partition_cost_index.source_inputs.finish")
        self.assertEqual(len(finishes), 1)
        finish = finishes[0]
        finish_index = top_index(finish)
        statement = producer.body[finish_index]
        self.assertIsInstance(statement, ast.Expr)
        self.assertIs(statement.value, finish)
        self.assertEqual(finish.args, [])
        self.assertEqual({item.arg: dotted(item.value) for item in finish.keywords},
                         {"frozen": "frozen", "sink": "sink"})
        self.assertEqual(len(finish.keywords), 2)

        collections = calls("evidence_entries.append")
        self.assertTrue(collections)
        self.assertTrue(all(top_index(call) < finish_index for call in collections))
        ledgers = [index for index, node in enumerate(producer.body)
                   if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "evidence_ledger" for target in node.targets)]
        self.assertEqual(len(ledgers), 1)
        self.assertLess(ledgers[0], finish_index)
        ledger = producer.body[ledgers[0]].value
        self.assertIsInstance(ledger, ast.Dict)
        self.assertTrue(any(isinstance(key, ast.Constant) and key.value == "entries"
                            and dotted(value) == "evidence_entries"
                            for key, value in zip(ledger.keys, ledger.values, strict=True)))

        builders = [index for index, node in enumerate(producer.body)
                    if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == "request_builder" for target in node.targets)]
        self.assertEqual(len(builders), 1)
        self.assertLess(finish_index, builders[0])
        requests = calls("request_builder")
        self.assertEqual(len(requests), 1)
        self.assertLess(finish_index, top_index(requests[0]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
