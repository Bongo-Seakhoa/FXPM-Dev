"""Invented gate/refusal metadata only; no source, fit or model is accepted.

Every Raw call intentionally fails at a real validation boundary. Reaching the
next roster refusal proves the generation/mode gate without fabricating a
positive authenticated M5 cross-binding or issuing any trading authority.
"""
from __future__ import annotations

from dataclasses import asdict
import json
from types import SimpleNamespace

import pytest

import pm_core as core
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_historical_search_producer as producer
import pm_production_search_planner as planner
import pm_winner_raw_evidence as raw
import pm_winner_selection as winner
from pm_spread_profile import ADMISSION_POLICY, ADMISSION_SCHEMA, FrozenSpreadTier, TIER_SCHEMA
from pm_trade_power import build_numerical_execution_cost_policy
from tests.test_full_panel_completion_transport import issued_process
from tests.test_m5_raw_count_binding import descriptors, window


_PROVENANCE = "INVENTED_GATE_REFUSAL_METADATA_NO_SOURCE_FIT_MODEL_OR_FINANCIAL_ACCEPTANCE"
_GENERATIONS = (winner.V16_GENERATION, winner.V17_GENERATION, winner.V18_GENERATION)


def _cost_pair(*, derived):
    """Use real cost identity builders; their public contract denies qualification."""
    timeframe = "H1" if derived else "M5"
    spec = core.InstrumentSpec("EURUSD", 4, point=0.0001, tick_size=0.0001,
        tick_value=1.0, pip_value=10.0)
    spec_hash = core.Backtester._cost_attestation_json_sha256({
        "schema_version": core.BACKTEST_INSTRUMENT_SPEC_SCHEMA_VERSION,
        "instrument_spec": asdict(spec)})
    marked = descriptors(window()) if derived else [{"tier": tier} for tier in ("P75", "P95")]
    rows = []
    for ordinal, descriptor in enumerate(marked):
        tier = descriptor["tier"]
        points = ordinal + 1
        descriptor.update(schema_version=TIER_SCHEMA, symbol="EURUSD", timeframe=timeframe,
            source_timeframe="M5", point_price=spec.point, spread_points=points,
            full_spread_price=points * spec.point, qualification_authority=False,
            fixture_provenance=_PROVENANCE)
        frozen_tier = FrozenSpreadTier(json.dumps(descriptor, sort_keys=True, allow_nan=False))
        frozen_tier.validate_execution("EURUSD", timeframe, spec.point)
        model = {"mode": "conservative_max", "source": _PROVENANCE,
            "frozen_spread_tier": frozen_tier.descriptor(),
            "entry_admission_policy": ADMISSION_POLICY, "entry_admission_schema": ADMISSION_SCHEMA}
        rows.append({"tier": tier, "frozen_spread_tier": frozen_tier.descriptor(),
            "numerical_execution_cost_policy": build_numerical_execution_cost_policy(
                use_spread=True, spread_mode=core.build_backtest_spread_mode(model),
                instrument_spec_sha256=spec_hash)})
    return core.validate_backtest_frozen_tier_pair_contract({
        "schema_version": core.BACKTEST_FROZEN_TIER_PAIR_CONTRACT_SCHEMA_VERSION,
        "admission_schema": ADMISSION_SCHEMA, "admission_policy": ADMISSION_POLICY,
        "tiers": rows, "qualification_authority": False})


def _binding_probe(generation, *, derived=True, current=False):
    pair = _cost_pair(derived=derived)
    timeframe = "H1" if derived else "M5"
    # The internal gate needs only these properties. This is not a sealed Frozen plan.
    frozen = SimpleNamespace(selection_generation=generation, symbol="EURUSD",
        timeframe=timeframe, regime="TREND", artifact_namespace="invented-m5-gate",
        frozen_tier_pair_contract=pair, frozen_tier_pair_contract_bytes=raw._canonical_bytes(pair))
    historical_schema, current_schema = raw.horizon_winner_cross_binding_schemas_for_generation(generation)
    digest = raw._sha_json({"fixture_provenance": _PROVENANCE})
    binding = {"schema_version": current_schema if current else historical_schema,
        "source_group_decision_sha256": digest, "regime_fit_decision_sha256": digest,
        "native_prefix_index_sha256": digest, "symbol": "EURUSD", "timeframe": timeframe,
        "regime": "TREND", "artifact_namespace": frozen.artifact_namespace,
        "horizon_roster": {}, "winner_roster": {}, "mapping_count": 0,
        "mapping_sha256": raw._sha_json([]), "mapping": [],
        "bijective_coverage": True, "created_before_winner_evidence_access": True,
        "confers_causal_current_live_or_capital_authority": False}
    if current:
        binding.update(mode=raw.CURRENT_CROSS_BINDING_MODE, cell_plan_receipt_sha256=digest,
            causal_precommit_receipt_sha256=digest, search_process_receipt_sha256=digest)
    else:
        binding.update(producer_request_sha256=digest, anchor_id="INVENTED-M5-GATE",
            selection_cutoff_utc="2024-01-02T00:00:00Z", process_precommit_receipt_sha256=digest)
    return frozen, raw._canonical_bytes(binding)


def _refusal(generation, *, derived=True, current=False):
    frozen, encoded = _binding_probe(generation, derived=derived, current=current)
    with pytest.raises(raw.WinnerRawEvidenceError) as caught:
        raw._validated_horizon_winner_cross_binding(encoded, sink=None, frozen=frozen,
            attempt_artifacts=())
    return caught.value.code


@pytest.mark.parametrize("generation", _GENERATIONS, ids=("v16", "v17", "v18"))
def test_historical_m5_gate_reaches_real_roster_refusal(generation):
    assert _refusal(generation) == "PROVENANCE_CROSS_BINDING_ROSTER_INVALID"


@pytest.mark.parametrize("generation", _GENERATIONS, ids=("v16", "v17", "v18"))
def test_current_mode_m5_still_refuses_before_roster(generation):
    assert _refusal(generation, current=True) == "PROVENANCE_M5_CROSS_BINDING_MODE_INVALID"


def test_v15_m5_still_refuses_before_roster():
    assert _refusal(winner.CURRENT_GENERATION) == "PROVENANCE_M5_CROSS_BINDING_MODE_INVALID"


@pytest.mark.parametrize("generation", (winner.CURRENT_GENERATION, *_GENERATIONS),
    ids=("v15", "v16", "v17", "v18"))
def test_unmarked_native_m5_keeps_the_next_real_roster_refusal(generation):
    assert _refusal(generation, derived=False) == "PROVENANCE_CROSS_BINDING_ROSTER_INVALID"


def test_typed_v16_envelope_routes_route_a_to_matched_generation(issued_process):
    case = issued_process()
    edition = orchestration.DECLARED_WINNER_TRANSPORT_V16
    assert planner.full_panel_generation(case.process) is winner.V17_GENERATION
    assert producer._declared_observed_cell_executor(case.context, process=case.process,
        transport_edition=edition) is producer.search_execution.execute_route_a_full_panel_historical_observed_cell
    assert producer._declared_capture_opener(case.context, process=case.process,
        transport_edition=edition) is adapter.open_matched_floor_declared_slot_capture
