"""SYNTHETIC_UNIT_DATA_ONLY: no Native authority, process issuer or case proof."""
from __future__ import annotations

import ast
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import unittest
import uuid

REPO = Path.cwd().resolve()
if not (REPO / "pm_end_grid.py").is_file():
    raise RuntimeError("Run this source-unit QA from the actual repository root")
sys.path.insert(0, str(REPO))
import pm_authority_spool as spool
import pm_end_grid as grid

if any(Path(module.__file__).resolve().parent != REPO for module in (spool, grid)):
    raise RuntimeError("Original Grid/spool origin mismatch")
SOURCE = Path(__file__).resolve().parents[1] / "pm_historical_scheduler.py"
PROVENANCE = "SYNTHETIC_UNIT_DATA_ONLY_NO_NATIVE_AUTHORITY_OR_CASE_PROOF"


class _CodedUnitError(Exception):
    """Ordinary coded software error; never a public verifier stand-in."""
    def __init__(self, code):
        self.code = code
        super().__init__("ARBITRARY_MESSAGE_MUST_NOT_BECOME_A_REASON")


def _selected():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    defs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    scope = {"__builtins__": __builtins__, "spool": spool, "grid": grid}
    helper = copy.deepcopy(defs["_declared_m5_exception_reason"])
    exec(compile(ast.fix_missing_locations(ast.Module(body=[helper], type_ignores=[])),
        str(SOURCE), "exec"), scope)
    run = defs["_declared_m5_run_one_cell"]
    handler = next(n for n in ast.walk(run) if isinstance(n, ast.ExceptHandler) and n.name == "exc")
    wrapper = ast.parse("def _unit_catch(exc):\n    pass\n").body[0]
    wrapper.body = ast.parse("result = {}\nroot = None\n").body + copy.deepcopy(handler.body)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])),
        str(SOURCE), "exec"), scope)
    capture = defs["_declared_m5_capture_source_abort"]
    assignments = [copy.deepcopy(n) for n in capture.body if isinstance(n, ast.Assign)
        and any(isinstance(x, ast.Name) and x.id in {"code", "reason"}
            for target in n.targets for x in ast.walk(target))]
    if len(assignments) != 2:
        raise RuntimeError("Unexpected original source-abort reason assignment shape")
    wrapper = ast.parse("def _unit_abort_reason(source_error):\n    pass\n").body[0]
    wrapper.body = assignments + [ast.Return(value=ast.Name(id="reason", ctx=ast.Load()))]
    exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])),
        str(SOURCE), "exec"), scope)
    recorder = defs["_declared_m5_record_terminal_refusal"]
    scope["_stop_assignment"] = next(copy.deepcopy(n) for n in ast.walk(recorder)
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "stopped" for t in n.targets))
    scope["_defs"] = defs
    return scope


def _original_stop(scope, reason):
    values = {"grid": grid, "grid_identity": {}, "abort_report": None,
        "row": {"cell": {"symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"}},
        "refusal": {"cycle_start_utc": "2020-01-01T00:00:00+00:00",
            "cycle_end_utc": "2020-01-29T00:00:00+00:00",
            "information_cutoff_utc": "2019-12-31T00:00:00+00:00", "reason_code": reason},
        "process": type("UnitDataProcessIdentity", (), {"receipt_sha256": "1" * 64})(),
        "code": {"pm_historical_scheduler.py": hashlib.sha256(SOURCE.read_bytes()).hexdigest()},
        "produced": "2026-01-01T00:00:00+00:00"}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[copy.deepcopy(scope["_stop_assignment"])],
        type_ignores=[])), str(SOURCE), "exec"), values)
    return values["stopped"]


class M5UncodedStopReasonTests(unittest.TestCase):
    def setUp(self):
        self.scope = _selected()
        self.reason = self.scope["_declared_m5_exception_reason"]
        self.caught = self.scope["_unit_catch"]
        self.abort_reason = self.scope["_unit_abort_reason"]

    def test_uncoded_keyerror_keeps_original_type_and_uses_safe_failed_stop_reason(self):
        error = KeyError("ARBITRARY_MESSAGE_MUST_NOT_BECOME_A_REASON")
        result = self.caught(error)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["reason_code"], "UNEXPECTED_EXCEPTION")
        self.assertEqual(result["exception_diagnostics"]["exception_type"], "KeyError")
        self.assertEqual(self.abort_reason(error), result["reason_code"])
        row = _original_stop(self.scope, result["reason_code"])
        self.assertEqual(row.outcome, "STOP")
        self.assertEqual(row.reason_code, "UNEXPECTED_EXCEPTION")
        self.assertEqual(row.code_sha256["pm_historical_scheduler.py"],
            hashlib.sha256(SOURCE.read_bytes()).hexdigest())
        for name in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
            self.assertIs(getattr(row, name), False)
        self.assertIsNone(row.source_contract_sha256)
        self.assertIsNone(row.consumer_binding_sha256)

    def test_known_spool_and_grid_carriers_preserve_safe_prefix_for_both_colon_forms(self):
        for error_type in (spool.AuthoritySpoolError, grid.EndGridError):
            for text in ("VALID_CODE", "VALID_CODE:DETAIL", "VALID_CODE: DETAIL"):
                with self.subTest(error_type=error_type.__name__, text=text):
                    error = error_type(text)
                    result = self.caught(error)
                    self.assertEqual(result["reason_code"], "VALID_CODE")
                    self.assertEqual(result["status"], "FAILED")
                    self.assertEqual(result["exception_diagnostics"]["exception_type"], error_type.__name__)
                    self.assertEqual(self.abort_reason(error), "VALID_CODE")
                    self.assertEqual(_original_stop(self.scope, result["reason_code"]).reason_code, "VALID_CODE")

    def test_valid_explicit_codes_and_existing_classification_are_unchanged_at_both_calls(self):
        for code, status, expected in (("M5_VIEW_LEASE_NOT_OWNED", "FAILED", "M5_VIEW_LEASE_NOT_OWNED"),
                ("M5:VALID_CODE", "FAILED", "M5:VALID_CODE"),
                ("DECLARED_M5_DISK_LOW", "NOT_RUN", "DISK_LOW"),
                ("DECLARED_SLOT_RUN_ACTIVE", "NOT_RUN", "BUSY")):
            with self.subTest(code=code):
                result = self.caught(_CodedUnitError(code))
                self.assertEqual((result["status"], result["reason_code"]), (status, expected))
                self.assertEqual(self.reason(_CodedUnitError(code))[0], code)
                self.assertEqual(self.abort_reason(_CodedUnitError(code)), expected)
        for name in ("_declared_m5_run_one_cell", "_declared_m5_capture_source_abort"):
            calls = [n for n in ast.walk(self.scope["_defs"][name]) if isinstance(n, ast.Call)
                and isinstance(n.func, ast.Name) and n.func.id == "_declared_m5_exception_reason"]
            self.assertEqual(len(calls), 1)

    def test_invalid_codes_fall_back_with_bounded_plain_diagnostics_and_messages_are_not_codes(self):
        for candidate in ("lowercase", "X" * 129, "", 17, {"untrusted": "value"}):
            with self.subTest(candidate_type=type(candidate).__name__):
                reason, diagnostics = self.reason(_CodedUnitError(candidate))
                self.assertEqual(reason, "UNEXPECTED_EXCEPTION")
                self.assertEqual(diagnostics["exception_type"], "_CodedUnitError")
                self.assertEqual(diagnostics["rejected_reason_candidate_type"], type(candidate).__name__)
                self.assertEqual(diagnostics["rejected_reason_candidate"],
                    candidate[:256] if type(candidate) is str else None)
                self.assertLessEqual(len(json.dumps(diagnostics, ensure_ascii=True)), 1024)
                self.assertEqual(_original_stop(self.scope, reason).reason_code, reason)
        self.assertEqual(self.reason(RuntimeError("VALID_CODE"))[0], "UNEXPECTED_EXCEPTION")
        self.assertEqual(self.reason(grid.EndGridError("lowercase:DETAIL"))[0], "UNEXPECTED_EXCEPTION")
        with self.assertRaisesRegex(grid.EndGridError, "REASON_CODE_INVALID"):
            _original_stop(self.scope, "KeyError")


class M5UncodedStopGridReadbackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        raw = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if raw is None:
            raise unittest.SkipTest("requires explicitly precreated retained QA root")
        root = Path(raw)
        if not raw or not root.is_absolute() or not root.exists() or not root.is_dir():
            raise RuntimeError("FXPM_DERIVED_QA_ROOT must be an existing absolute owned directory")
        if root.is_symlink() or getattr(root.lstat(), "st_file_attributes", 0) & 0x400:
            raise RuntimeError("FXPM_DERIVED_QA_ROOT must not be a symlink or reparse point")
        cls.root = root

    def test_original_stop_writer_readback_retains_uncoded_failure_without_any_source_completion(self):
        scope = _selected()
        result = scope["_unit_catch"](KeyError("SYNTHETIC_UNIT_FAILURE"))
        row = _original_stop(scope, result["reason_code"])
        case = self.root / ("case006-uncoded-stop-" + uuid.uuid4().hex)
        case.mkdir(exist_ok=False)
        written = grid.EndGridWriter(case).append(row)
        restored = list(grid.read_end_grid(case, "XAGUSD"))
        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0].row_sha256, written["row_sha256"])
        self.assertEqual(restored[0].to_bytes(), row.to_bytes())
        self.assertEqual(restored[0].reason_code, "UNEXPECTED_EXCEPTION")
        self.assertEqual(result["exception_diagnostics"]["exception_type"], "KeyError")
        self.assertEqual(restored[0].source_identity, {})
        self.assertEqual(restored[0].cost_tier, "NONE")
        for field in ("job_binding_sha256", "consumer_binding_sha256", "view_key", "view_contract_sha256",
                "source_contract_sha256", "no_trade_run_receipt_sha256"):
            self.assertIsNone(getattr(restored[0], field))
        # Retain the owned fixture and Grid row; there is no cleanup hook.


if __name__ == "__main__":
    unittest.main()
