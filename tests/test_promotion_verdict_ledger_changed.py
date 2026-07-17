"""Regression: a genuinely PROMOTED generation over byte-identical winner
bytes reports ledger_changed=False, and that must NOT be a CLI rejection
reason (seq1686/1691).  Promotion is proven by status/promoted-flag/sha; the
book-byte delta only governs runtime cache invalidation.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

from pm_main import _governed_generation_cli_verdict

_LEDGER_CHANGED_VETO = "OPTIMIZATION_GENERATION_LEDGER_CHANGED_FLAG_FALSE"


def _promoted_outcome(tmp_path: Path, *, ledger_changed: bool) -> tuple:
    ledger = b'{"winners": []}'
    ledger_path = tmp_path / "pm_configs.json"
    ledger_path.write_bytes(ledger)
    outcome = SimpleNamespace(
        generation_id="generation-1",
        status="PROMOTED",
        promoted=True,
        ledger_changed=ledger_changed,
        technical_failures={},
        promoted_ledger_sha256=hashlib.sha256(ledger).hexdigest(),
        manifest_path=str(tmp_path / "pm_configs.generation.active.json"),
    )
    return ledger_path, outcome


def test_unchanged_ledger_is_not_a_promotion_rejection_reason(tmp_path: Path) -> None:
    # The byte-identical promotion: ledger_changed=False must NOT appear as a
    # rejection code.  (Downstream authority-chain checks still fail because we
    # do not build the full manifest here, but the removed veto must be gone.)
    ledger_path, outcome = _promoted_outcome(tmp_path, ledger_changed=False)
    _accepted, errors = _governed_generation_cli_verdict(outcome, ledger_path)
    assert _LEDGER_CHANGED_VETO not in errors


def test_promotion_status_and_flag_are_accepted_for_unchanged_ledger(
    tmp_path: Path,
) -> None:
    # The status/promoted/sha checks pass for an unchanged-ledger promotion;
    # only the (unrelated) authority-chain receipt is missing here.
    ledger_path, outcome = _promoted_outcome(tmp_path, ledger_changed=False)
    _accepted, errors = _governed_generation_cli_verdict(outcome, ledger_path)
    assert "OPTIMIZATION_GENERATION_STATUS_NOT_PROMOTED:PROMOTED" not in errors
    assert "OPTIMIZATION_GENERATION_PROMOTED_FLAG_FALSE" not in errors
    assert "OPTIMIZATION_GENERATION_LEDGER_SHA256_MISMATCH" not in errors


def test_veto_absent_regardless_of_ledger_changed_value(tmp_path: Path) -> None:
    # The removed veto must never appear for either ledger_changed value.
    for changed in (True, False):
        ledger_path, outcome = _promoted_outcome(tmp_path, ledger_changed=changed)
        _accepted, errors = _governed_generation_cli_verdict(outcome, ledger_path)
        assert _LEDGER_CHANGED_VETO not in errors
