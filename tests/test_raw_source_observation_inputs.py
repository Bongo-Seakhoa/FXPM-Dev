"""Invented in-memory inputs for Raw source reuse; no market or qualification claim.

The eight owner checks retain their QA oracles and mocked native boundary.
The ninth check inspects actual producer wiring without executing its AST.
"""

from copy import deepcopy
import ast
import json
import hashlib
import numpy as np
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



def _receipt_validation_case(offset=0, *, frozen=None, sink=None):
    """Invented native-boundary inputs; actual alignment, hashing and admission run."""
    fine = pd.DataFrame({"Spread": [0.0, 2.0, float("nan"), 4.0] * 9},
        index=pd.date_range("2024-01-01T00:00:00Z", periods=36, freq="5min"))
    target = fine.index[offset + 1:offset + 5]
    points = [fine["Spread"].iloc[offset + n] for n in range(4)]
    observations = pd.Series(points, index=target, name="EntrySpreadPoints", dtype=float)
    observations_sha = hashlib.sha256(pd.util.hash_pandas_object(
        observations, index=True).values.tobytes()).hexdigest()
    carrier = _carrier()
    carrier["cutoff_utc"] = "2024-01-01T03:00:00Z"
    source = {"bars_sha256": "1" * 64, "audit_sidecar_sha256": "2" * 64,
        "provenance_sha256": "3" * 64, "source_identity_sha256": "b" * 64}
    activation = {"manifest_sha256": carrier["activation_manifest_sha256"],
        "activation_id": "SYNTHETIC_ACTIVATION"}
    product = SimpleNamespace(index={"source": source, "activation": activation})
    pair = {"tiers": [{"tier": tier, "frozen_spread_tier": {
        "symbol": "SYNTHETIC", "timeframe": "M5", "source_timeframe": "M5",
        "source_binding": {"bars_sha256": source["bars_sha256"],
            "sidecar_sha256": source["audit_sidecar_sha256"],
            "provenance_sha256": source["provenance_sha256"]},
        "source_identity_sha256": source["source_identity_sha256"],
        "activation": {"manifest_sha256": activation["manifest_sha256"],
            "id": activation["activation_id"]}, "spread_points": cap}}
        for tier, cap in (("P75", 3), ("P95", 5))]}
    if frozen is None:
        frozen = SimpleNamespace(symbol="SYNTHETIC", timeframe="M5",
            generation_context_sha256=SYNTHETIC_CONTEXT_SHA, frozen_tier_pair_contract=pair)
    if sink is None:
        sink = SimpleNamespace(context=SimpleNamespace(context_sha256=SYNTHETIC_CONTEXT_SHA))
    def utc(stamp):
        return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = [{"target_open_utc": utc(stamp), "expected_prior_fine_close_utc": utc(stamp),
        "prior_fine_close_utc": utc(stamp), "points": None if pd.isna(point) else float(point)}
        for stamp, point in zip(target, points, strict=True)]
    body = {"schema_version": raw.SOURCE_OBSERVATIONS_SCHEMA, "symbol": frozen.symbol,
        "timeframe": frozen.timeframe, "source_timeframe": "M5", "source_product": carrier,
        "observations": rows, "entry_observations_sha256": observations_sha}
    receipt = {**body, "receipt_sha256": raw._sha_json(body)}
    signals = {"values": [0, 0, 0, 0]}
    axis = {"schema_version": "fxpm.backtest-partition-axis.v1", "timeframe": "M5",
        "start_utc": utc(target[1]), "end_utc": utc(target[-1] + pd.Timedelta(minutes=5)),
        "scored_bars": 3, "warmup_bars": 1, "event_axis_sha256": "e" * 64}
    axis["horizon_receipt_sha256"] = raw._sha_json(axis)
    lanes = {}
    for tier, cap in (("P75", 3), ("P95", 5)):
        mask = np.array([False if pd.isna(point) else point <= cap for point in points], dtype=np.uint8)
        lanes["optimizer_train_" + tier.lower()] = {
            "trade_power_partition_axis": deepcopy(axis), "trade_identity_pnl": [],
            "spread_admission": {"schema_version": raw.ADMISSION_SCHEMA,
                "entry_admission_policy": raw.ADMISSION_POLICY,
                "entry_observations_sha256": observations_sha,
                "entry_admission_mask_sha256": hashlib.sha256(mask.tobytes()).hexdigest(),
                "input_provenance_authenticated": False, "qualification_authority": False,
                "signal_opportunities": 0, "known_cap_pass_opportunities": 0,
                "known_cap_refused_opportunities": 0, "missing_refused_opportunities": 0,
                "missing_executed_legs": 0},
            "spread_model": {"entry_admission_schema": raw.ADMISSION_SCHEMA,
                "entry_admission_policy": raw.ADMISSION_POLICY,
                "entry_observations_sha256": observations_sha}}
    return dict(frozen=frozen, sink=sink, product=product, fine=fine, receipt=receipt,
        signals=signals, lanes=lanes, target=target)


def _validate_receipt_case(case, owner, *, receipt=None, expected_sha256=None, frozen=None, sink=None,
        signals=None, lanes=None):
    return raw._validated_source_observation_receipt(
        case["receipt"] if receipt is None else receipt,
        sink=case["sink"] if sink is None else sink,
        frozen=case["frozen"] if frozen is None else frozen,
        expected_sha256=case["receipt"]["receipt_sha256"] if expected_sha256 is None else expected_sha256,
        partition="optimizer_train", signals=case["signals"] if signals is None else signals,
        lanes=case["lanes"] if lanes is None else lanes, source_inputs=owner)


class SourceObservationReceiptReuseChecks(unittest.TestCase):
    def test_more_than_twelve_distinct_receipts_prepare_once_per_invocation(self):
        first = _receipt_validation_case()
        owner = raw._SourceObservationInputs(first["frozen"])
        cases = [_receipt_validation_case(n, frozen=first["frozen"], sink=first["sink"])
            for n in range(14)]
        calls = 0
        with mock.patch.object(raw, "_open_source_observation_native",
                return_value=(first["product"], first["fine"])) as opened:
            with mock.patch.object(raw, "_prepare_source_observation_receipt",
                    wraps=raw._prepare_source_observation_receipt) as prepared:
                for _ in range(3):
                    for case in cases:
                        self.assertEqual(_validate_receipt_case(case, owner), case["receipt"])
                        calls += 1
                self.assertEqual(prepared.call_count, 14)
                self.assertEqual(calls, 42)
                self.assertEqual(len(owner._receipts), 14)
                opened.assert_called_once()
                print(json.dumps({"fixture_provenance": "INVENTED_RAW_RECEIPT_REUSE",
                    "validation_calls": calls, "distinct_receipts": 14,
                    "preparations": prepared.call_count, "native_acquisitions": opened.call_count}))

    def test_cached_arrays_are_readonly_and_finish_clears_before_cold_reopen(self):
        case = _receipt_validation_case()
        owner = raw._SourceObservationInputs(case["frozen"])
        calls = []
        def acquire(carrier, **kwargs):
            calls.append(carrier)
            if len(calls) == 2:
                self.assertEqual(owner._receipts, {})
                self.assertIsNone(owner._entry)
                self.assertEqual(owner._axes, {})
                raise raw.WinnerRawEvidenceError("SOURCE_OBSERVATION_NATIVE_REOPEN_FAILED")
            return case["product"], case["fine"]
        with mock.patch.object(raw, "_open_source_observation_native", side_effect=acquire):
            with mock.patch.object(raw, "_prepare_source_observation_receipt",
                    wraps=raw._prepare_source_observation_receipt) as prepared:
                _validate_receipt_case(case, owner)
                for _, states, mask, _ in next(iter(owner._receipts.values()))[2]:
                    self.assertFalse(states.flags.writeable)
                    self.assertFalse(mask.flags.writeable)
                    with self.assertRaises(ValueError): mask[0] = False
                    with self.assertRaises(ValueError): states[0] = "INVALID"
                with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_NATIVE_REOPEN_FAILED"):
                    owner.finish(frozen=case["frozen"], sink=case["sink"])
                self.assertEqual(owner._receipts, {})
                other_owner = raw._SourceObservationInputs(case["frozen"])
                _validate_receipt_case(case, other_owner)
                self.assertEqual(prepared.call_count, 2)
        self.assertEqual(len(calls), 3)

    def test_cached_receipt_still_authenticates_actual_body_before_lookup(self):
        case = _receipt_validation_case()
        owner = raw._SourceObservationInputs(case["frozen"])
        with mock.patch.object(raw, "_open_source_observation_native",
                return_value=(case["product"], case["fine"])):
            _validate_receipt_case(case, owner)
            changed = deepcopy(case["receipt"])
            changed["observations"][0]["points"] += 1
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID"):
                _validate_receipt_case(case, owner, receipt=changed)
            changed.pop("receipt_sha256")
            changed["receipt_sha256"] = raw._sha_json(changed)
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_RECEIPT_BINDING_INVALID"):
                _validate_receipt_case(case, owner, receipt=changed)
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_VALUES_CHANGED"):
                _validate_receipt_case(case, owner, receipt=changed, expected_sha256=changed["receipt_sha256"])
            malformed = deepcopy(case["receipt"])
            malformed["extra"] = "SYNTHETIC_UNEXPECTED_FIELD"
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_RECEIPT_SCHEMA_INVALID"):
                _validate_receipt_case(case, owner, receipt=malformed)
            malformed = deepcopy(case["receipt"])
            malformed["source_product"]["timeframe"] = "INVALID"
            malformed.pop("receipt_sha256")
            malformed["receipt_sha256"] = raw._sha_json(malformed)
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_NATIVE_CARRIER_INVALID"):
                _validate_receipt_case(case, owner, receipt=malformed,
                    expected_sha256=malformed["receipt_sha256"])
        self.assertEqual(len(owner._receipts), 1)

    def test_cached_receipt_refuses_foreign_frozen_or_context_before_lookup(self):
        case = _receipt_validation_case()
        owner = raw._SourceObservationInputs(case["frozen"])
        with mock.patch.object(raw, "_open_source_observation_native",
                return_value=(case["product"], case["fine"])) as opened:
            _validate_receipt_case(case, owner)
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_INPUT_OWNER_MISMATCH"):
                _validate_receipt_case(case, owner, frozen=SimpleNamespace(**vars(case["frozen"])))
            with self.assertRaisesRegex(raw.WinnerRawEvidenceError, "SOURCE_OBSERVATION_INPUT_OWNER_MISMATCH"):
                _validate_receipt_case(case, owner,
                    sink=SimpleNamespace(context=SimpleNamespace(context_sha256="f" * 64)))
            opened.assert_called_once()
        self.assertEqual(len(owner._receipts), 1)

    def test_cache_hits_keep_axes_signals_lane_models_counts_and_trade_checks(self):
        case = _receipt_validation_case()
        owner = raw._SourceObservationInputs(case["frozen"])
        with mock.patch.object(raw, "_open_source_observation_native",
                return_value=(case["product"], case["fine"])):
            with mock.patch.object(raw, "_prepare_source_observation_receipt",
                    wraps=raw._prepare_source_observation_receipt) as prepared:
                _validate_receipt_case(case, owner)
                for mutation in ("axis_length", "axis_time", "signal_length", "signals",
                        "lane_mask", "model", "counts", "trade"):
                    with self.subTest(mutation=mutation):
                        signals, lanes = deepcopy(case["signals"]), deepcopy(case["lanes"])
                        lane = lanes["optimizer_train_p75"]
                        if mutation.startswith("axis_"):
                            axis = lane["trade_power_partition_axis"]
                            if mutation == "axis_length": axis["scored_bars"] += 1
                            else: axis["start_utc"] = "2024-01-01T00:11:00Z"
                            axis.pop("horizon_receipt_sha256")
                            axis["horizon_receipt_sha256"] = raw._sha_json(axis)
                        elif mutation == "signal_length": signals["values"].pop()
                        elif mutation == "signals": signals["values"][0] = 1
                        elif mutation == "lane_mask": lane["spread_admission"]["entry_admission_mask_sha256"] = "0" * 64
                        elif mutation == "model": lane["spread_model"]["entry_observations_sha256"] = "0" * 64
                        elif mutation == "counts": lane["spread_admission"]["signal_opportunities"] += 1
                        else:
                            lane["trade_identity_pnl"] = [{"entry_bar": 1, "exit_bar": 1,
                                "signal_bar": 0, "direction": "LONG", "exit_reason": "SYNTHETIC",
                                "entry_time": "2024-01-01T00:10:00Z", "exit_time": "2024-01-01T00:10:00Z",
                                "position_size": 1.0, "risk_amount": 1.0, "pnl_dollars": 0.0}]
                        expected = ("SOURCE_OBSERVATION_PARTITION_AXIS_SPLICE" if mutation in {
                            "axis_length", "axis_time", "signal_length"} else
                            "TIER_TRADE_ENTRY_ADMISSION_FAILED" if mutation == "trade" else
                            "TIER_ADMISSION_RECONSTRUCTION_FAILED")
                        with self.assertRaisesRegex(raw.WinnerRawEvidenceError, expected):
                            _validate_receipt_case(case, owner, signals=signals, lanes=lanes)
                signals, lanes = deepcopy(case["signals"]), deepcopy(case["lanes"])
                signals["values"][0] = 1
                for lane in lanes.values():
                    lane["spread_admission"]["signal_opportunities"] = 1
                    lane["spread_admission"]["known_cap_pass_opportunities"] = 1
                self.assertEqual(_validate_receipt_case(case, owner, signals=signals, lanes=lanes), case["receipt"])
                self.assertEqual(prepared.call_count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
