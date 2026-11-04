"""Software-only C1 branch tests; NO_NATIVE_VERIFICATION_PERFORMED.

The active terms resolver and pointer I/O are mocked at their existing boundary.
These tests exercise the candidate method and original pure event projection,
not broker authentication, full Backtester initialization or financial results.
"""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import patch

import pandas as pd


_CANDIDATE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_CANDIDATE_ROOT))
import pm_core as core

if Path(core.__file__).resolve() != _CANDIDATE_ROOT / "pm_core.py":
    raise RuntimeError("C1_TEST_REQUIRES_EXACT_CANDIDATE_CORE")


POINTER_SHA = "1" * 64
MANIFEST_SHA = "2" * 64
BARS_SHA = "3" * 64
SIDECAR_SHA = "4" * 64
VIEW_SHA = "5" * 64
SYMBOL = "NO_NATIVE_VERIFICATION_SOFTWARE_FIXTURE"


def _binding(timeframe="H1"):
    return {
        "symbol": SYMBOL,
        "timeframe": "M5",
        "activation_id": "NO_NATIVE_VERIFICATION_SOFTWARE_FIXTURE",
        "activation_manifest_sha256": MANIFEST_SHA,
        "bars_sha256": BARS_SHA,
        "sidecar_sha256": SIDECAR_SHA,
        "source_kind": "m5_derived",
        "view_timeframe": timeframe,
        "m5_view_contract_sha256": VIEW_SHA,
    }


def _frame(timeframe="H1"):
    step = pd.Timedelta(hours=1) if timeframe == "H1" else pd.Timedelta(minutes=15)
    opens = pd.date_range("2024-01-01", periods=3, freq=step, tz="UTC")
    frame = pd.DataFrame({"_BAR_CLOSE_TS": opens + step}, index=opens)
    frame.attrs[core.NATIVE_SOURCE_BINDING_ATTR] = _binding(timeframe)
    return frame


def _contract():
    return {
        "terms_active_pointer_sha256": POINTER_SHA,
        "price_activation_id": "NO_NATIVE_VERIFICATION_SOFTWARE_FIXTURE",
        "price_activation_manifest_sha256": MANIFEST_SHA,
        "target_bars_sha256": BARS_SHA,
        "target_sidecar_sha256": SIDECAR_SHA,
        "events": [
            {"event_utc": "2024-01-01T00:30:00Z", "factor": 1},
            {"event_utc": "2024-01-01T01:30:00Z", "factor": 3},
        ],
        "terms": {"fixture": "NO_NATIVE_VERIFICATION_PERFORMED"},
    }


def _state():
    return SimpleNamespace(
        config=SimpleNamespace(data_dir=Path("NO_NATIVE_VERIFICATION_UNUSED_DATA_ROOT")),
        _research_swap_context=None,
        _swap_terms_pointer_pin=None,
        _swap_contract_cache={},
        _swap_contract_cache_max=4,
    )


class M5BrokerSwapProjectionTests(unittest.TestCase):
    def setUp(self):
        self.state = _state()
        self.spec = SimpleNamespace(symbol=SYMBOL)
        self.resolved = _contract()
        self.file_patch = patch.object(core.Path, "is_file", return_value=True)
        self.link_patch = patch.object(core.Path, "is_symlink", return_value=False)
        self.hash_patch = patch.object(core, "broker_terms_sha256_file", return_value=POINTER_SHA)
        self.resolve_patch = patch.object(core, "resolve_active_swap_contract", return_value=self.resolved)
        for item in (self.file_patch, self.link_patch, self.hash_patch):
            item.start()
            self.addCleanup(item.stop)
        self.resolver = self.resolve_patch.start()
        self.addCleanup(self.resolve_patch.stop)

    def project(self, frame, timeframe="H1"):
        return core.Backtester._resolve_broker_swap_projection(self.state, frame, self.spec, timeframe)

    def test_resolves_genuine_m5_binding_and_preserves_events_and_contract(self):
        before = deepcopy(self.resolved)
        frame = _frame()
        projection, resolved = self.project(frame)
        self.resolver.assert_called_once_with(
            self.state.config.data_dir / "native", SYMBOL, "M5")
        self.assertIs(resolved, self.resolved)
        self.assertEqual(resolved, before)
        self.assertEqual(projection.pre_open_units.tolist(), [0, 0, 0])
        self.assertEqual(projection.at_close_units.tolist(), [1, 3, 0])
        self.assertEqual(frame.attrs[core.NATIVE_SOURCE_BINDING_ATTR]["timeframe"], "M5")

    def test_fake_native_h1_binding_is_refused_before_terms_lookup(self):
        frame = _frame()
        frame.attrs[core.NATIVE_SOURCE_BINDING_ATTR]["timeframe"] = "H1"
        with self.assertRaisesRegex(core.BrokerTermsError, "M5_DERIVED_SWAP_VIEW_BINDING_INVALID"):
            self.project(frame)
        self.resolver.assert_not_called()

    def test_view_marker_must_match_cell_timeframe_and_have_exact_digest(self):
        for name, value in (("view_timeframe", "M30"), ("m5_view_contract_sha256", "bad")):
            with self.subTest(name=name):
                frame = _frame()
                frame.attrs[core.NATIVE_SOURCE_BINDING_ATTR][name] = value
                with self.assertRaisesRegex(core.BrokerTermsError, "M5_DERIVED_SWAP_VIEW_BINDING_INVALID"):
                    self.project(frame)
        self.resolver.assert_not_called()

    def test_base_m5_binding_must_equal_original_terms_target(self):
        frame = _frame()
        frame.attrs[core.NATIVE_SOURCE_BINDING_ATTR]["bars_sha256"] = "6" * 64
        with self.assertRaisesRegex(core.BrokerTermsError, "M5_DERIVED_SWAP_BASE_BINDING_MISMATCH"):
            self.project(frame)

    def test_research_override_cannot_replace_active_m5_terms(self):
        self.state._research_swap_context = object()
        with self.assertRaisesRegex(core.BrokerTermsError, "M5_DERIVED_SWAP_RESEARCH_OVERRIDE_REFUSED"):
            self.project(_frame())
        self.resolver.assert_not_called()

    def test_missing_terms_pointer_refuses_instead_of_disabling_swap(self):
        with patch.object(core.Path, "is_file", return_value=False):
            with self.assertRaisesRegex(core.BrokerTermsError, "M5_DERIVED_SWAP_ACTIVE_TERMS_REQUIRED"):
                self.project(_frame())
        self.resolver.assert_not_called()

    def test_resolved_pointer_must_match_pinned_active_pointer(self):
        self.resolved["terms_active_pointer_sha256"] = "7" * 64
        with self.assertRaisesRegex(core.BrokerTermsError, "activation changed during swap contract resolution"):
            self.project(_frame())
        self.assertEqual(self.state._swap_contract_cache, {})

    def test_shared_m5_terms_cache_reprojects_each_views_actual_closes(self):
        hourly, _ = self.project(_frame("H1"), "H1")
        quarter, _ = self.project(_frame("M15"), "M15")
        self.resolver.assert_called_once()
        self.assertEqual(hourly.at_close_units.tolist(), [1, 3, 0])
        self.assertEqual(quarter.at_close_units.tolist(), [0, 1, 0])
        self.assertEqual(set(self.state._swap_contract_cache), {(POINTER_SHA, SYMBOL, "M5")})


if __name__ == "__main__":
    unittest.main()
