"""End-grid row tests: invented rows under a temp root, no native data, no authority.

Every hash here is a fixture value.  The tests prove the record's shape,
fail-closed validation, idempotent append, verified reads and the rebuild
identity.  They prove nothing about any trading outcome.
"""

from dataclasses import replace
import hashlib
import json
import unittest
from pathlib import Path
import tempfile
import threading

import pm_book_multiplicity as book
import pm_end_grid as grid


H = {name: pair * 32 for name, pair in zip(
    ("source", "view", "process", "config", "core", "exec", "other", "receipt"),
    ("a1", "b2", "c3", "d4", "e5", "f6", "a7", "b8"))}
T0 = "2024-06-02T00:00:00+00:00"
T1 = "2024-06-30T00:00:00+00:00"
CUTOFF = "2024-06-02T00:00:00+00:00"
CONTRACT = {"view_interval": {"start_utc": "2019-06-02T00:00:00+00:00",
                              "end_exclusive_utc": "2024-06-02T00:00:00+00:00"},
            "cutoff_utc": CUTOFF, "timeframe": "H1", "stratum": {"timeframe": "H1", "session": "fx_24x5"},
            "fit_windows": {"train": ["2019-06-02", "2023-06-02"], "tuning": ["2023-06-02", "2024-06-02"]},
            "warmup_bars": 200, "spread_identification": {"method": "spread_min_exact_prior_m5_close"}}
CONTRACT_SHA = hashlib.sha256(grid.canonical_bytes(CONTRACT)).hexdigest()


def base_fields(**overrides):
    value = dict(
        symbol="EURUSD", timeframe="H1", regime="TREND",
        cycle_start_utc=T0, cycle_end_utc=T1, information_cutoff_utc=CUTOFF,
        outcome="NO_TRADE", reason_code="NO_QUALIFIED_CANDIDATE",
        source_kind="broker_native", source_contract_sha256=H["source"],
        view_key=H["view"], view_contract_sha256=CONTRACT_SHA,
        source_identity={"activation_id": "20261028T221800Z-workshop-native-reverify-v3",
                         "native_bars_sha256": H["other"], "m5_source_sha256": H["other"]},
        job_binding_sha256=None, consumer_binding_sha256=None,
        cost_tier="P95", process_edition_sha256=H["process"],
        code_sha256={"pm_core": H["core"], "pm_authoritative_search_execution": H["exec"]},
        metrics={"candidates": 5, "tuning_trades": 31, "net_return_pct": -0.42,
                 "qualified": False, "note": None},
        produced_at_utc="2024-07-01T10:00:00+00:00",
    )
    value.update(overrides)
    # v2: a NO_TRADE row is evidenced by its own cell-run receipt; every other
    # outcome carries none unless a test sets it on purpose.
    value.setdefault("no_trade_run_receipt_sha256",
                     H["receipt"] if value["outcome"] == "NO_TRADE" else None)
    return value


def winner_fields(**overrides):
    return base_fields(outcome="WINNER", reason_code="QUALIFIED",
                       strategy_family="ema_ribbon_adx", configuration_sha256=H["config"],
                       **overrides)


def unavailable_fields(**overrides):
    value = base_fields(outcome="UNAVAILABLE", reason_code="SOURCE_TECHNICAL_ERROR",
                        cost_tier="NONE", source_contract_sha256=None, view_key=None,
                        view_contract_sha256=None, source_identity={"activation_id": "act-1"})
    value.update(overrides)
    return value


def derived_identity():
    return {"activation_id": "20261028T221800Z-workshop-native-reverify-v3",
            "m5_source_sha256": H["other"], "constructor_implementation_sha256": H["core"],
            "gap_findings_sha256": H["exec"]}


def derived_fields(**overrides):
    # Overrides reach base_fields so the receipt default follows the final outcome.
    defaults = dict(source_kind="m5_derived", source_identity=derived_identity(),
                    job_binding_sha256=H["process"], consumer_binding_sha256=H["config"])
    return base_fields(**{**defaults, **overrides})


class RowShapeTests(unittest.TestCase):
    def test_round_trip_and_stable_hash(self):
        row = grid.build_end_grid_row(**winner_fields())
        stored = json.loads(row.to_bytes().decode("ascii"))
        self.assertEqual(set(stored), set(grid._ROW_KEYS))
        self.assertEqual(stored["cell_id"], book.compute_cell_id("EURUSD", "H1", "TREND"))
        again = grid.row_from_dict(stored)
        self.assertEqual(again, row)
        self.assertEqual(again.row_sha256, stored["row_sha256"])
        self.assertEqual(grid.build_end_grid_row(**winner_fields()).row_sha256, row.row_sha256)

    def test_metrics_order_does_not_change_hash(self):
        left = grid.build_end_grid_row(**base_fields(metrics={"a": 1, "b": 2.5}))
        right = grid.build_end_grid_row(**base_fields(metrics={"b": 2.5, "a": 1}))
        self.assertEqual(left.row_sha256, right.row_sha256)

    def test_reproducibility_identity_excludes_metrics_and_time(self):
        early = grid.build_end_grid_row(**winner_fields(produced_at_utc="2024-07-01T10:00:00+00:00",
                                                         metrics={"x": 1}))
        late = grid.build_end_grid_row(**winner_fields(produced_at_utc="2024-07-02T10:00:00+00:00",
                                                        metrics={"x": 2}))
        self.assertNotEqual(early.row_sha256, late.row_sha256)
        self.assertEqual(early.reproducibility_identity(), late.reproducibility_identity())
        identity = early.reproducibility_identity()
        self.assertEqual(set(identity), set(grid._REPRODUCIBILITY_KEYS) | {"identity_sha256"})
        self.assertEqual(identity["configuration_sha256"], H["config"])

    def test_produced_at_defaults_to_canonical_now(self):
        row = grid.build_end_grid_row(**base_fields(produced_at_utc=None))
        self.assertTrue(row.produced_at_utc.endswith("+00:00"))
        grid._utc(row.produced_at_utc, "X")


class RefusalTests(unittest.TestCase):
    def refuse(self, code, **overrides):
        with self.assertRaises(grid.EndGridError) as caught:
            grid.build_end_grid_row(**base_fields(**overrides))
        self.assertEqual(str(caught.exception), code)

    def test_cell_outside_family(self):
        self.refuse("CELL_NOT_IN_FAMILY", symbol="NOTASYMBOL")
        self.refuse("CELL_NOT_IN_FAMILY", timeframe="H2")
        self.refuse("CELL_NOT_IN_FAMILY", regime="SIDEWAYS")

    def test_cycle_and_cutoff_rules(self):
        self.refuse("CYCLE_EMPTY", cycle_end_utc=T0)
        self.refuse("CUTOFF_AFTER_CYCLE_START", information_cutoff_utc="2024-06-03T00:00:00+00:00")
        self.refuse("CYCLE_START_INVALID", cycle_start_utc="2024-06-02T00:00:00Z")
        self.refuse("CYCLE_START_INVALID", cycle_start_utc="2024-06-02T00:00:00")
        self.refuse("CYCLE_START_INVALID", cycle_start_utc="2024-06-02T02:00:00+02:00")

    def test_outcome_and_selection_consistency(self):
        self.refuse("WINNER_WITHOUT_SELECTION", outcome="WINNER", reason_code="QUALIFIED")
        self.refuse("WINNER_WITHOUT_SELECTION", outcome="WINNER", reason_code="QUALIFIED",
                    strategy_family="ema_ribbon_adx")
        self.refuse("SELECTION_WITHOUT_WINNER", strategy_family="ema_ribbon_adx",
                    configuration_sha256=H["config"])
        self.refuse("WINNER_WITHOUT_COST_TIER", outcome="WINNER", reason_code="QUALIFIED",
                    strategy_family="ema_ribbon_adx", configuration_sha256=H["config"],
                    cost_tier="NONE")
        self.refuse("COST_TIER_NONE_REQUIRES_STOP_OR_UNAVAILABLE", cost_tier="NONE")
        self.refuse("OUTCOME_INVALID", outcome="MAYBE")
        self.refuse("REASON_CODE_INVALID", reason_code="lower case words")
        self.refuse("SOURCE_KIND_INVALID", source_kind="NATIVE_HTF")  # label, not a kind
        self.refuse("SOURCE_KIND_INVALID", source_kind="NATIVE")
        self.refuse("COST_TIER_INVALID", cost_tier="P50")

    def test_stop_and_unavailable_allow_tier_none(self):
        stop = grid.build_end_grid_row(**base_fields(outcome="STOP", reason_code="COST_UNAVAILABLE",
                                                     cost_tier="NONE"))
        self.assertEqual(stop.cost_tier, "NONE")
        unavailable = grid.build_end_grid_row(**unavailable_fields())
        self.assertIsNone(unavailable.view_contract_sha256)
        self.assertIsNone(unavailable.source_contract_sha256)

    def test_view_contract_is_distinct_from_the_source_contract(self):
        row = grid.build_end_grid_row(**winner_fields())
        self.assertEqual(row.source_contract_sha256, H["source"])
        self.assertEqual(row.view_contract_sha256, CONTRACT_SHA)
        self.assertEqual(row.reproducibility_identity()["view_contract_sha256"], CONTRACT_SHA)
        self.refuse("VIEW_KEY_AND_CONTRACT_MUST_PAIR", view_contract_sha256=None)
        self.refuse("VIEW_KEY_AND_CONTRACT_MUST_PAIR", view_key=None)
        self.refuse("VIEW_CONTRACT_SHA_INVALID", view_contract_sha256="abc")

    def test_honest_stops_carry_only_the_facts_that_existed(self):
        before_prefix = grid.build_end_grid_row(**base_fields(
            outcome="STOP", reason_code="SOURCE_PREFIX_UNAVAILABLE", cost_tier="NONE",
            source_contract_sha256=None, view_key=None, view_contract_sha256=None,
            source_identity={}))
        self.assertEqual(before_prefix.source_identity, {})
        self.assertEqual(grid.row_from_dict(before_prefix.to_dict()), before_prefix)
        partial = grid.build_end_grid_row(**base_fields(
            outcome="STOP", reason_code="SOURCE_PREFIX_UNAVAILABLE", cost_tier="NONE",
            source_contract_sha256=None, view_key=None, view_contract_sha256=None,
            source_identity={"activation_id": "act-1"}))
        self.assertEqual(partial.source_identity, {"activation_id": "act-1"})
        after_view = grid.build_end_grid_row(**base_fields(
            outcome="STOP", reason_code="TRADE_SOURCE_SEED_FAILED", cost_tier="NONE",
            source_contract_sha256=None))
        self.assertIsNone(after_view.source_contract_sha256)
        self.assertEqual(after_view.view_contract_sha256, CONTRACT_SHA)
        # v2: only a WINNER needs the trade contract; a NO_TRADE may predate any trade source.
        self.refuse("RESULT_WITHOUT_SOURCE_CONTRACT", outcome="WINNER", reason_code="QUALIFIED",
                    strategy_family="ema_ribbon_adx", configuration_sha256=H["config"],
                    source_contract_sha256=None)
        self.assertIsNone(grid.build_end_grid_row(**base_fields(source_contract_sha256=None)).source_contract_sha256)
        self.refuse("RESULT_WITHOUT_VIEW", view_key=None, view_contract_sha256=None)
        with self.assertRaises(grid.EndGridError) as caught:
            grid.build_end_grid_row(**unavailable_fields(source_contract_sha256=H["source"]))
        self.assertEqual(str(caught.exception), "UNAVAILABLE_ROW_WITH_SOURCE_CONTRACT")
        with self.assertRaises(grid.EndGridError) as caught:
            grid.build_end_grid_row(**unavailable_fields(view_key=H["view"],
                                                         view_contract_sha256=CONTRACT_SHA))
        self.assertEqual(str(caught.exception), "UNAVAILABLE_ROW_WITH_VIEW")
        # A STOP still refuses invented or path-like identity values.
        with self.assertRaises(grid.EndGridError) as caught:
            grid.build_end_grid_row(**base_fields(
                outcome="STOP", reason_code="SOURCE_PREFIX_UNAVAILABLE", cost_tier="NONE",
                source_contract_sha256=None, view_key=None, view_contract_sha256=None,
                source_identity={"native_bars_sha256": "nothex"}))
        self.assertEqual(str(caught.exception), "SOURCE_IDENTITY_SHA_INVALID")

    def test_identity_and_metric_validation(self):
        self.refuse("SOURCE_CONTRACT_SHA_INVALID", source_contract_sha256="abc")
        self.refuse("VIEW_KEY_INVALID", view_key=H["view"].upper())
        self.refuse("PROCESS_EDITION_SHA_INVALID", process_edition_sha256=None)
        self.refuse("CODE_IDENTITY_INVALID", code_sha256={})
        self.refuse("CODE_IDENTITY_SHA_INVALID", code_sha256={"pm_core": "nothex"})
        self.refuse("CODE_IDENTITY_NAME_INVALID", code_sha256={"bad name": H["core"]})
        self.refuse("METRIC_NOT_FINITE", metrics={"ratio": float("nan")})
        self.refuse("METRIC_NOT_FINITE", metrics={"ratio": float("inf")})
        self.refuse("METRIC_TYPE_INVALID", metrics={"text": "1.0"})
        self.refuse("METRIC_TYPE_INVALID", metrics={"nested": {"a": 1}})
        self.refuse("METRIC_NAME_INVALID", metrics={"": 1})
        self.refuse("METRICS_INVALID", metrics={("k%d" % i): i for i in range(grid.MAX_METRIC_ENTRIES + 1)})

    def test_authority_flags_are_false_and_bindings_validate(self):
        row = grid.build_end_grid_row(**winner_fields(job_binding_sha256=H["core"],
                                                      consumer_binding_sha256=H["exec"]))
        stored = row.to_dict()
        for flag in grid.AUTHORITY_FLAGS:
            self.assertIs(stored[flag], False)
        self.assertEqual(stored["job_binding_sha256"], H["core"])
        self.assertEqual(row.reproducibility_identity()["consumer_binding_sha256"], H["exec"])
        with self.assertRaises(grid.EndGridError) as caught:
            grid.row_from_dict(dict(stored, live_authority=True))
        self.assertEqual(str(caught.exception), "AUTHORITY_FLAG_SET")
        self.refuse("JOB_BINDING_SHA_INVALID", job_binding_sha256="abc")
        self.refuse("CONSUMER_BINDING_SHA_INVALID", consumer_binding_sha256=12)
        self.refuse("SOURCE_IDENTITY_INVALID", source_identity={})
        self.refuse("SOURCE_IDENTITY_NAME_INVALID", source_identity={"bad name": "x"})
        self.refuse("SOURCE_IDENTITY_VALUE_INVALID", source_identity={"activation_id": "has space"})
        self.refuse("SOURCE_IDENTITY_VALUE_INVALID", source_identity={"activation_id": H["other"] * 3})

    def test_derived_results_need_job_context_but_stops_do_not(self):
        row = grid.build_end_grid_row(**derived_fields())
        self.assertEqual(row.source_kind, "m5_derived")
        for missing in ("job_binding_sha256", "consumer_binding_sha256"):
            with self.assertRaises(grid.EndGridError) as caught:
                grid.build_end_grid_row(**derived_fields(**{missing: None}))
            self.assertEqual(str(caught.exception), "DERIVED_RESULT_WITHOUT_JOB_CONTEXT")
        stop = grid.build_end_grid_row(**derived_fields(outcome="STOP", reason_code="COST_UNAVAILABLE",
                                                        cost_tier="NONE", job_binding_sha256=None,
                                                        consumer_binding_sha256=None))
        self.assertIsNone(stop.job_binding_sha256)
        native = grid.build_end_grid_row(**base_fields())  # native rows keep their nullability
        self.assertIsNone(native.consumer_binding_sha256)

    def test_canonical_kinds_are_hashed_and_labels_are_rendered_outside(self):
        row = grid.build_end_grid_row(**derived_fields())
        self.assertNotIn("source_kind_label", row.to_dict())
        self.assertEqual(grid.source_kind_label(row), "DERIVED_FROM_M5")
        rendered = grid.render_row(row)
        self.assertEqual(rendered["source_kind"], "m5_derived")
        self.assertEqual(rendered["source_kind_label"], "DERIVED_FROM_M5")
        self.assertEqual(rendered["cost_lane_label"], "STRESS")
        self.assertEqual(grid.source_kind_label(grid.build_end_grid_row(**base_fields())), "NATIVE_HTF")
        self.assertEqual(set(grid.SOURCE_KIND_LABELS), set(grid.SOURCE_KINDS))

    def test_clock_derived_kind_can_only_stop_or_be_unavailable(self):
        clock_identity = dict(derived_identity(), clock_template_sha256=H["view"])
        stop = grid.build_end_grid_row(**derived_fields(
            source_kind="m5_derived_control_clock.v1", source_identity=clock_identity,
            outcome="STOP", reason_code="CLOCK_CAUSAL_ELIGIBILITY_UNPROVED", cost_tier="NONE",
            source_contract_sha256=None, job_binding_sha256=None, consumer_binding_sha256=None))
        self.assertEqual(grid.source_kind_label(stop), "DERIVED_FROM_M5_CLOCK")
        self.assertEqual(grid.row_from_dict(stop.to_dict()), stop)
        for outcome, reason in (("NO_TRADE", "NO_QUALIFIED_CANDIDATE"), ("WINNER", "QUALIFIED")):
            with self.assertRaises(grid.EndGridError) as caught:
                grid.build_end_grid_row(**derived_fields(
                    source_kind="m5_derived_control_clock.v1", source_identity=clock_identity,
                    outcome=outcome, reason_code=reason,
                    strategy_family="ema_ribbon_adx" if outcome == "WINNER" else None,
                    configuration_sha256=H["config"] if outcome == "WINNER" else None))
            self.assertEqual(str(caught.exception), "CLOCK_DERIVED_RESULT_NOT_PERMITTED")

    def test_source_identity_required_keys_and_path_refusal(self):
        self.refuse("SOURCE_IDENTITY_MISSING:native_bars_sha256",
                    source_identity={"activation_id": "act-1"})
        with self.assertRaises(grid.EndGridError) as caught:
            grid.build_end_grid_row(**derived_fields(source_identity={
                "activation_id": "act-1", "m5_source_sha256": H["other"]}))
        self.assertEqual(str(caught.exception), "SOURCE_IDENTITY_MISSING:constructor_implementation_sha256")
        self.refuse("SOURCE_IDENTITY_SHA_INVALID",
                    source_identity={"activation_id": "act-1", "native_bars_sha256": "nothex"})
        for path_like in ("spool/EURUSD/H1/bars.csv", "C:\\spool\\x", "x.jsonl", "a:b"):
            self.refuse("SOURCE_IDENTITY_PATH_REFUSED",
                        source_identity={"activation_id": path_like, "native_bars_sha256": H["other"]})

    def test_stored_row_tampering_is_refused(self):
        stored = grid.build_end_grid_row(**winner_fields()).to_dict()
        tampered = dict(stored, metrics={"net_return_pct": 99.0})
        with self.assertRaises(grid.EndGridError) as caught:
            grid.row_from_dict(tampered)
        self.assertEqual(str(caught.exception), "ROW_SHA_MISMATCH")
        with self.assertRaises(grid.EndGridError) as caught:
            grid.row_from_dict(dict(stored, extra=1))
        self.assertEqual(str(caught.exception), "ROW_KEYS_INVALID")
        with self.assertRaises(grid.EndGridError) as caught:
            grid.row_from_dict(dict(stored, cell_id=H["other"]))
        self.assertEqual(str(caught.exception), "ROW_CELL_ID_MISMATCH")
        with self.assertRaises(grid.EndGridError) as caught:
            grid.row_from_dict(dict(stored, schema_version="fxpm.end-grid-row.v0"))
        self.assertEqual(str(caught.exception), "ROW_SCHEMA_VERSION_INVALID")


class WriterReaderTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="endgrid-")
        self.root = Path(self._tmp.name)
        # Every fixture row with a view resolves to this retained body.
        self.contract_sha = grid.store_view_contract(self.root, CONTRACT)
        self.assertEqual(self.contract_sha, CONTRACT_SHA)

    def tearDown(self):
        self._tmp.cleanup()

    def test_contract_store_is_content_addressed_write_once_and_rebuilds(self):
        self.assertEqual(self.contract_sha, grid.store_view_contract(self.root, dict(CONTRACT)))
        self.assertEqual(grid.load_view_contract(self.root, self.contract_sha), CONTRACT)
        path = self.root / "contracts" / (self.contract_sha + ".json")
        path.write_bytes(path.read_bytes().replace(b"200", b"201"))
        with self.assertRaises(grid.EndGridError) as caught:
            grid.load_view_contract(self.root, self.contract_sha)
        self.assertTrue(str(caught.exception).startswith("CONTRACT_SHA_MISMATCH"))
        with self.assertRaises(grid.EndGridError) as caught:
            grid.store_view_contract(self.root, CONTRACT)
        self.assertEqual(str(caught.exception), "VIEW_CONTRACT_SHA_COLLISION")
        with self.assertRaises(grid.EndGridError):
            grid.store_view_contract(self.root, {})
        with self.assertRaises(grid.EndGridError):
            grid.store_view_contract(self.root, {"bad": float("nan")})
        with self.assertRaises(grid.EndGridError) as caught:
            grid.load_view_contract(self.root, H["other"])
        self.assertTrue(str(caught.exception).startswith("CONTRACT_NOT_RETAINED"))

    def test_rows_with_a_view_need_a_retained_contract_and_rebuild_after_spool_deletion(self):
        writer = grid.EndGridWriter(self.root)
        with self.assertRaises(grid.EndGridError) as caught:
            writer.append(grid.build_end_grid_row(**base_fields(view_contract_sha256=H["other"])))
        self.assertTrue(str(caught.exception).startswith("CONTRACT_NOT_RETAINED"))
        unavailable = grid.build_end_grid_row(**unavailable_fields())
        self.assertTrue(writer.append(unavailable)["appended"])  # never had a view
        before_prefix = grid.build_end_grid_row(**base_fields(
            regime="CHOP", outcome="STOP", reason_code="SOURCE_PREFIX_UNAVAILABLE", cost_tier="NONE",
            source_contract_sha256=None, view_key=None, view_contract_sha256=None, source_identity={}))
        self.assertTrue(writer.append(before_prefix)["appended"])  # no view, nothing to resolve
        after_view = grid.build_end_grid_row(**base_fields(
            regime="BREAKOUT", outcome="STOP", reason_code="TRADE_SOURCE_SEED_FAILED", cost_tier="NONE",
            source_contract_sha256=None))
        self.assertTrue(writer.append(after_view)["appended"])  # view retained, no trade contract
        row = grid.build_end_grid_row(**winner_fields(regime="RANGE"))
        writer.append(row)
        spool = self.root / "working_spool"
        spool.mkdir()
        (spool / "view.bin").write_bytes(b"transient")
        for item in spool.iterdir():
            item.unlink()
        spool.rmdir()
        rows = grid.read_end_grid(self.root, "EURUSD")
        winner = next(item for item in rows if item.outcome == "WINNER")
        rebuilt = grid.rebuild_inputs(self.root, winner)
        self.assertEqual(rebuilt["view_contract"], CONTRACT)
        self.assertEqual(rebuilt["identity"]["view_contract_sha256"], self.contract_sha)
        self.assertEqual(rebuilt["identity"]["source_contract_sha256"], H["source"])
        self.assertIsNone(grid.rebuild_inputs(self.root, unavailable)["view_contract"])
        self.assertIsNone(grid.rebuild_inputs(self.root, before_prefix)["view_contract"])
        self.assertEqual(grid.rebuild_inputs(self.root, after_view)["view_contract"], CONTRACT)
        self.assertEqual(len(rows), 4)

    def test_reader_refuses_a_reparse_point_on_the_symbol_file(self):
        writer = grid.EndGridWriter(self.root)
        writer.append(grid.build_end_grid_row(**base_fields()))
        seen = []
        original = grid._refuse_reparse

        def recording(path):
            seen.append(Path(path))
            if Path(path).name == "EURUSD.jsonl":
                raise grid.EndGridError("GRID_PATH_REPARSE_REFUSED")
            return original(path)

        grid._refuse_reparse = recording
        try:
            with self.assertRaises(grid.EndGridError) as caught:
                grid.read_end_grid(self.root, "EURUSD")
        finally:
            grid._refuse_reparse = original
        self.assertEqual(str(caught.exception), "GRID_PATH_REPARSE_REFUSED")
        self.assertIn(self.root / "EURUSD.jsonl", seen)

    def test_append_is_idempotent_and_read_verifies(self):
        writer = grid.EndGridWriter(self.root)
        row = grid.build_end_grid_row(**winner_fields())
        first = writer.append(row)
        second = writer.append(row)
        self.assertTrue(first["appended"])
        self.assertFalse(second["appended"])
        self.assertEqual(first["row_sha256"], row.row_sha256)
        self.assertEqual(Path(first["path"]).name, "EURUSD.jsonl")
        rows = grid.read_end_grid(self.root, "EURUSD")
        self.assertEqual(rows, (row,))
        self.assertEqual(grid.read_end_grid(self.root), (row,))

    def test_same_cell_cycle_with_different_row_is_refused(self):
        writer = grid.EndGridWriter(self.root)
        writer.append(grid.build_end_grid_row(**base_fields()))
        with self.assertRaises(grid.EndGridError) as caught:
            writer.append(grid.build_end_grid_row(**base_fields(metrics={"candidates": 6})))
        self.assertEqual(str(caught.exception), "CELL_CYCLE_ALREADY_RECORDED")
        later = grid.build_end_grid_row(**base_fields(
            cycle_start_utc=T1, cycle_end_utc="2024-07-28T00:00:00+00:00",
            information_cutoff_utc=T1, produced_at_utc="2024-07-29T00:00:00+00:00"))
        self.assertTrue(writer.append(later)["appended"])
        self.assertEqual(len(grid.read_end_grid(self.root, "EURUSD")), 2)

    def test_latest_by_cell_and_summary(self):
        writer = grid.EndGridWriter(self.root)
        first = grid.build_end_grid_row(**base_fields())
        later = grid.build_end_grid_row(**winner_fields(
            cycle_start_utc=T1, cycle_end_utc="2024-07-28T00:00:00+00:00",
            information_cutoff_utc=T1, produced_at_utc="2024-07-29T00:00:00+00:00"))
        other = grid.build_end_grid_row(**derived_fields(symbol="XAGUSD", timeframe="H4",
                                                         outcome="STOP", reason_code="COST_UNAVAILABLE",
                                                         cost_tier="NONE", job_binding_sha256=None,
                                                         consumer_binding_sha256=None))
        for row in (first, later, other):
            writer.append(row)
        latest = grid.latest_rows_by_cell(grid.read_end_grid(self.root))
        self.assertEqual(latest[first.cell_id], later)
        self.assertEqual(latest[other.cell_id], other)
        summary = grid.grid_summary(grid.read_end_grid(self.root))
        self.assertEqual(summary["cells_recorded"], 2)
        self.assertEqual(summary["family_cells"], 2400)
        self.assertEqual(summary["by_outcome"], {"WINNER": 1, "NO_TRADE": 0, "STOP": 1, "UNAVAILABLE": 0})

    def test_corrupted_file_fails_the_read(self):
        writer = grid.EndGridWriter(self.root)
        writer.append(grid.build_end_grid_row(**base_fields()))
        path = self.root / "EURUSD.jsonl"
        with path.open("ab") as handle:
            handle.write(b"{not json}\n")
        with self.assertRaises(grid.EndGridError) as caught:
            grid.read_end_grid(self.root, "EURUSD")
        self.assertTrue(str(caught.exception).startswith("ROW_NOT_JSON:EURUSD.jsonl:2"))

    def test_writer_revalidates_from_bytes_before_writing(self):
        writer = grid.EndGridWriter(self.root)
        row = grid.build_end_grid_row(**base_fields())
        row.metrics["poisoned"] = float("nan")  # the frozen dataclass still holds a mutable dict
        with self.assertRaises(grid.EndGridError) as caught:
            writer.append(row)
        self.assertEqual(str(caught.exception), "ROW_NOT_SERIALIZABLE")
        row.metrics["poisoned"] = "text"
        with self.assertRaises(grid.EndGridError) as caught:
            writer.append(row)
        self.assertEqual(str(caught.exception), "METRIC_TYPE_INVALID")
        flipped = replace(grid.build_end_grid_row(**base_fields()), live_authority=True)
        with self.assertRaises(grid.EndGridError) as caught:
            writer.append(flipped)
        self.assertEqual(str(caught.exception), "AUTHORITY_FLAG_SET")
        self.assertFalse((self.root / "EURUSD.jsonl").exists())
        result = writer.append(grid.build_end_grid_row(**base_fields()))
        self.assertEqual(result["rows_in_file"], 1)

    def test_concurrent_appends_are_serialized_and_idempotent(self):
        writer = grid.EndGridWriter(self.root)
        same = grid.build_end_grid_row(**base_fields())
        distinct = [grid.build_end_grid_row(**base_fields(regime=regime))
                    for regime in ("TREND", "RANGE", "CHOP", "BREAKOUT")]
        outcomes, errors = [], []

        def run(row):
            try:
                outcomes.append(writer.append(row)["appended"])
            except Exception as error:  # noqa: BLE001 - collected for the assertion
                errors.append(error)

        threads = [threading.Thread(target=run, args=(same,)) for _ in range(6)]
        threads += [threading.Thread(target=run, args=(row,)) for row in distinct]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(sum(1 for appended in outcomes if appended), 4)
        rows = grid.read_end_grid(self.root, "EURUSD")
        self.assertEqual(len(rows), 4)
        self.assertEqual({row.regime for row in rows}, {"TREND", "RANGE", "CHOP", "BREAKOUT"})
        self.assertTrue((self.root / "EURUSD.jsonl.lock").exists())

    def test_partial_tail_is_refused_until_the_line_completes(self):
        writer = grid.EndGridWriter(self.root)
        row = grid.build_end_grid_row(**base_fields())
        writer.append(row)
        path = self.root / "EURUSD.jsonl"
        with path.open("ab") as handle:
            handle.write(row.to_bytes()[:40])
        with self.assertRaises(grid.EndGridError) as caught:
            grid.read_end_grid(self.root, "EURUSD")
        self.assertEqual(str(caught.exception), "GRID_PARTIAL_TAIL:EURUSD.jsonl")
        with self.assertRaises(grid.EndGridError):
            writer.append(grid.build_end_grid_row(**base_fields(symbol="EURUSD", regime="RANGE")))

    def test_row_in_wrong_symbol_file_is_refused(self):
        row = grid.build_end_grid_row(**base_fields())
        with (self.root / "GBPUSD.jsonl").open("wb") as handle:
            handle.write(row.to_bytes() + b"\n")
        with self.assertRaises(grid.EndGridError) as caught:
            grid.read_end_grid(self.root)
        self.assertTrue(str(caught.exception).startswith("ROW_SYMBOL_FILE_MISMATCH"))

    def test_root_must_be_existing_absolute_directory(self):
        with self.assertRaises(grid.EndGridError):
            grid.EndGridWriter(self.root / "missing")
        with self.assertRaises(grid.EndGridError):
            grid.EndGridWriter("relative/path")
        with self.assertRaises(grid.EndGridError):
            grid.EndGridWriter(Path(self._tmp.name) / "EURUSD.jsonl")

    def test_writer_appends_only_the_current_schema_and_reads_mixed_files(self):
        writer = grid.EndGridWriter(self.root)
        v2_row = grid.build_end_grid_row(**base_fields())
        v1_dict = {key: value for key, value in v2_row.to_dict().items()
                   if key not in ("row_sha256", "no_trade_run_receipt_sha256")}
        v1_dict.update(schema_version=grid.END_GRID_SCHEMA_VERSION_V1, regime="RANGE",
                       cell_id=book.compute_cell_id("EURUSD", "H1", "RANGE"))
        v1_dict["row_sha256"] = hashlib.sha256(grid.canonical_bytes(v1_dict)).hexdigest()
        v1_row = grid.row_from_dict(v1_dict)
        with self.assertRaises(grid.EndGridError) as caught:
            writer.append(v1_row)
        self.assertEqual(str(caught.exception), "ROW_SCHEMA_VERSION_NOT_CURRENT")
        # A v1 line already on disk keeps reading beside new v2 lines.
        with (self.root / "EURUSD.jsonl").open("wb") as handle:
            handle.write(grid.canonical_bytes(v1_dict) + b"\n")
        self.assertTrue(writer.append(v2_row)["appended"])
        rows = grid.read_end_grid(self.root, "EURUSD")
        self.assertEqual([row.schema_version for row in rows],
                         [grid.END_GRID_SCHEMA_VERSION_V1, grid.END_GRID_SCHEMA_VERSION])
        self.assertEqual(rows[0], v1_row)
        self.assertEqual(rows[0].to_bytes(), grid.canonical_bytes(v1_dict))
        self.assertEqual(set(grid.latest_rows_by_cell(rows)), {v1_row.cell_id, v2_row.cell_id})
        self.assertEqual(grid.grid_summary(rows)["by_outcome"]["NO_TRADE"], 2)


class SchemaVersionTests(unittest.TestCase):
    """v2 adds the NO_TRADE evidence field; v1 lines keep their own keys, hash and rules."""

    def refuse(self, code, **overrides):
        with self.assertRaises(grid.EndGridError) as caught:
            grid.build_end_grid_row(**base_fields(**overrides))
        self.assertEqual(str(caught.exception), code)

    @staticmethod
    def rehash(body):
        body = {key: value for key, value in body.items() if key != "row_sha256"}
        return dict(body, row_sha256=hashlib.sha256(grid.canonical_bytes(body)).hexdigest())

    def test_no_trade_needs_its_run_receipt_and_the_trade_contract_is_optional(self):
        row = grid.build_end_grid_row(**base_fields())
        self.assertEqual(row.schema_version, grid.END_GRID_SCHEMA_VERSION)
        self.assertEqual(row.no_trade_run_receipt_sha256, H["receipt"])
        self.assertEqual(row.source_contract_sha256, H["source"])
        self.assertEqual(row.reproducibility_identity()["no_trade_run_receipt_sha256"], H["receipt"])
        unseeded = grid.build_end_grid_row(**base_fields(source_contract_sha256=None))
        self.assertIsNone(unseeded.source_contract_sha256)
        self.assertEqual(unseeded.no_trade_run_receipt_sha256, H["receipt"])
        self.assertNotEqual(unseeded.row_sha256, row.row_sha256)
        self.refuse("NO_TRADE_WITHOUT_RUN_RECEIPT", no_trade_run_receipt_sha256=None)
        self.refuse("NO_TRADE_WITHOUT_RUN_RECEIPT", no_trade_run_receipt_sha256=None,
                    source_contract_sha256=None)
        self.refuse("NO_TRADE_RUN_RECEIPT_SHA_INVALID", no_trade_run_receipt_sha256="abc")
        self.refuse("NO_TRADE_RUN_RECEIPT_SHA_INVALID", no_trade_run_receipt_sha256=H["receipt"].upper())
        self.assertEqual(set(grid._ROW_KEYS) - set(grid._ROW_KEYS_V1), {"no_trade_run_receipt_sha256"})

    def test_run_receipt_is_refused_outside_no_trade(self):
        self.refuse("WINNER_WITH_NO_TRADE_RUN_RECEIPT", outcome="WINNER", reason_code="QUALIFIED",
                    strategy_family="ema_ribbon_adx", configuration_sha256=H["config"],
                    no_trade_run_receipt_sha256=H["receipt"])
        self.refuse("NO_TRADE_RUN_RECEIPT_REQUIRES_NO_TRADE", outcome="STOP", reason_code="COST_UNAVAILABLE",
                    cost_tier="NONE", no_trade_run_receipt_sha256=H["receipt"])
        with self.assertRaises(grid.EndGridError) as caught:
            grid.build_end_grid_row(**unavailable_fields(no_trade_run_receipt_sha256=H["receipt"]))
        self.assertEqual(str(caught.exception), "NO_TRADE_RUN_RECEIPT_REQUIRES_NO_TRADE")
        winner = grid.build_end_grid_row(**winner_fields())
        self.assertIsNone(winner.no_trade_run_receipt_sha256)
        self.assertIn("no_trade_run_receipt_sha256", winner.to_dict())
        self.assertIsNone(winner.reproducibility_identity()["no_trade_run_receipt_sha256"])

    def test_v1_lines_read_under_v1_rules_and_v2_lines_under_v2(self):
        v2 = grid.build_end_grid_row(**base_fields()).to_dict()
        # Exactly what the published v1 writer wrote: v1 keys and a hash over them.
        v1 = self.rehash(dict({key: value for key, value in v2.items()
                               if key != "no_trade_run_receipt_sha256"},
                              schema_version=grid.END_GRID_SCHEMA_VERSION_V1))
        row = grid.row_from_dict(v1)
        self.assertEqual(row.schema_version, grid.END_GRID_SCHEMA_VERSION_V1)
        self.assertIsNone(row.no_trade_run_receipt_sha256)
        self.assertEqual(row.to_dict(), v1)
        self.assertEqual(row.to_bytes(), grid.canonical_bytes(v1))
        self.assertNotIn("no_trade_run_receipt_sha256", row.reproducibility_identity())
        self.assertEqual(grid.row_from_dict(json.loads(row.to_bytes().decode("ascii"))), row)
        cases = (
            (self.rehash(dict(v1, no_trade_run_receipt_sha256=H["receipt"])), "ROW_KEYS_INVALID"),
            (self.rehash(dict(v1, source_contract_sha256=None)), "RESULT_WITHOUT_SOURCE_CONTRACT"),
            (self.rehash({key: value for key, value in v2.items()
                          if key != "no_trade_run_receipt_sha256"}), "ROW_KEYS_INVALID"),
            (self.rehash(dict(v2, no_trade_run_receipt_sha256=None)), "NO_TRADE_WITHOUT_RUN_RECEIPT"),
            (self.rehash(dict(v2, schema_version=grid.END_GRID_SCHEMA_VERSION_V1)), "ROW_KEYS_INVALID"),
            (dict(v2, schema_version="fxpm.end-grid-row.v3"), "ROW_SCHEMA_VERSION_INVALID"),
            ({key: value for key, value in v2.items() if key != "schema_version"}, "ROW_KEYS_INVALID"),
        )
        for stored, code in cases:
            with self.subTest(code=code), self.assertRaises(grid.EndGridError) as caught:
                grid.row_from_dict(stored)
            self.assertEqual(str(caught.exception), code)
        self.refuse("ROW_SCHEMA_VERSION_INVALID", schema_version="fxpm.end-grid-row.v3")
        self.refuse("NO_TRADE_RUN_RECEIPT_NOT_IN_V1", schema_version=grid.END_GRID_SCHEMA_VERSION_V1)
        legacy = grid.build_end_grid_row(**base_fields(schema_version=grid.END_GRID_SCHEMA_VERSION_V1,
                                                       no_trade_run_receipt_sha256=None))
        self.assertEqual(legacy.to_dict(), v1)


if __name__ == "__main__":
    unittest.main()
