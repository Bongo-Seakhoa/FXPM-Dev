"""Invented authority-field DATA only; no horizon plan or Native capability is forged."""
import ast
from pathlib import Path
import unittest


def _helper():
    path = Path(__file__).resolve().parents[1] / "pm_historical_anchor_orchestration.py"
    tree = ast.parse(path.read_bytes())
    names = {"_HORIZON_PLAN_SCHEMA", "_CURRENT_HORIZON_PLAN_SCHEMA", "_M5_HORIZON_PLAN_SCHEMA", "_HORIZON_SCOPE"}
    constants = [n for n in tree.body if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name) and n.target.id in names]
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_horizon_plan_authority_fields_valid")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    scope = {"__name__": "invented_horizon_authority_fields_only"}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *constants, fn], type_ignores=[])), str(path), "exec"), scope)
    return scope


class HorizonAuthorityFieldTests(unittest.TestCase):
    def test_native_versions_and_explicit_m5_metadata_are_accepted(self):
        h = _helper()
        for version in (h["_HORIZON_PLAN_SCHEMA"], h["_CURRENT_HORIZON_PLAN_SCHEMA"], h["_M5_HORIZON_PLAN_SCHEMA"]):
            data = {"schema_version": version, "authority_scope": h["_HORIZON_SCOPE"],
                    "confers_trading_or_capital_authority": False, "confers_final_oos_authority": False}
            if version == h["_M5_HORIZON_PLAN_SCHEMA"]:
                data["source_kind"] = "m5_derived"
            with self.subTest(version=version):
                self.assertTrue(h["_horizon_plan_authority_fields_valid"](data))

    def test_m5_source_scope_and_exact_false_fields_stay_strict(self):
        h = _helper()
        good = {"schema_version": h["_M5_HORIZON_PLAN_SCHEMA"], "source_kind": "m5_derived",
                "authority_scope": h["_HORIZON_SCOPE"], "confers_trading_or_capital_authority": False,
                "confers_final_oos_authority": False}
        cases = ({"source_kind": None}, {"source_kind": "broker_native"},
                 {"source_kind": "m5_derived_control_clock.v1"}, {"schema_version": "unknown"},
                 {"authority_scope": "LIVE"}, {"confers_trading_or_capital_authority": 0},
                 {"confers_final_oos_authority": True})
        for change in cases:
            with self.subTest(change=change):
                self.assertFalse(h["_horizon_plan_authority_fields_valid"]({**good, **change}))
