"""INVENTED chain metadata and reader stand-ins; real dispatcher and binding guards only."""
import json
from types import SimpleNamespace

import pytest

import pm_authoritative_search_execution as execution


@pytest.mark.parametrize("damage", (None, "fitted_binding", "cross_binding"),
    ids=("v17-public-v9-dispatch", "fitted-binding-still-required", "cross-binding-still-required"))
def test_bound_fit_v17_dispatch_preserves_chain_guards(monkeypatch, damage):
    raw = execution.raw_winner
    body = {"artifact_namespace": "INVENTED-READER", "symbol": "INVENTED",
        "timeframe": "H1", "regime": "TREND"}
    prefix = "INVENTED-READER/winner-search/H1/TREND/frozen/"
    descriptor = {"relative_path": "INVENTED-receipt.json", "sha256": "a" * 64,
        "size_bytes": 1, "format": raw.ASCII_JSON_FORMAT}
    fitted = {"artifact_id": prefix + "fitted", "descriptor": {**descriptor,
        "relative_path": "INVENTED-fitted.json"}}
    receipt = {"artifact_id": prefix + "receipt", "descriptor": descriptor}
    cross_descriptor = {**descriptor, "relative_path": "INVENTED-cross.json"}
    cross_entry = {"artifact_id": "INVENTED-cross", "artifact_type":
        "historical_horizon_winner_cross_binding", "descriptor": cross_descriptor}
    header = {"artifact_roster": [receipt, fitted, cross_entry],
        "frozen_plan_receipt": descriptor, "fitted_state_entry": fitted,
        "horizon_winner_cross_binding": cross_descriptor}
    context = SimpleNamespace(generation_id="INVENTED-GENERATION", context_sha256="b" * 64,
        native_universe_sha256="c" * 64, config_sha256="d" * 64)
    frozen = SimpleNamespace(bound_fitted_state_entry=fitted)
    expected_frozen = frozen
    receipt_bytes = json.dumps({"schema_version": raw.BOUND_FIT_V17_FROZEN_PLAN_SCHEMA}).encode("ascii")
    cross_bytes, cross_value = b'{"fixture":"INVENTED_CROSS"}', {"fixture": "INVENTED_CROSS"}
    restored, verified = [], []

    def read(_context, entry, *, expected_format):
        assert _context is context and expected_format == raw.ASCII_JSON_FORMAT
        return (receipt_bytes if entry["artifact_id"] == receipt["artifact_id"]
            else cross_bytes if entry["artifact_id"] == cross_entry["artifact_id"] else b"{}")

    def restore(**kwargs):
        restored.append(kwargs)
        assert kwargs["context_identity"] == {"generation_id": context.generation_id,
            "generation_context_sha256": context.context_sha256,
            "native_universe_sha256": context.native_universe_sha256,
            "config_sha256": context.config_sha256}
        assert (kwargs["symbol"], kwargs["timeframe"], kwargs["regime"], kwargs["artifact_namespace"]) == (
            body["symbol"], body["timeframe"], body["regime"], body["artifact_namespace"])
        assert kwargs["manifest_entries"] == [receipt, fitted]
        assert kwargs["body_bytes"] == [receipt_bytes, b"{}"]
        return frozen

    def verify(encoded, *, frozen):
        assert frozen is expected_frozen and encoded == cross_bytes
        verified.append(frozen)
        return cross_value, None, None

    def legacy(**_kwargs):
        pytest.fail("V17 frozen.v9 was routed to a legacy reader")

    monkeypatch.setattr(execution, "_bound_fit_entry_bytes", read)
    monkeypatch.setattr(raw, "restore_frozen_winner_search_plan_v9", restore)
    monkeypatch.setattr(raw, "restore_frozen_winner_search_plan_v6", legacy)
    monkeypatch.setattr(raw, "restore_frozen_winner_search_plan_v7", legacy)
    monkeypatch.setattr(raw, "verify_historical_frozen_winner_cross_binding", verify)
    if damage == "fitted_binding":
        header["fitted_state_entry"] = {"fixture": "INVENTED_CROSSED_FITTED_REFERENCE"}
    elif damage == "cross_binding":
        header["horizon_winner_cross_binding"] = {"fixture": "INVENTED_MISSING_CROSS_REFERENCE"}

    if damage is None:
        result = execution._bound_fit_frozen_chain(context, body, header)
        assert result == (frozen, cross_value) and verified == [frozen]
    else:
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as error:
            execution._bound_fit_frozen_chain(context, body, header)
        assert error.value.code == ("BOUND_FIT_FROZEN_BINDING_INVALID" if damage == "fitted_binding"
            else "BOUND_FIT_CROSS_BINDING_MISSING")
        assert verified == []
    assert len(restored) == 1
