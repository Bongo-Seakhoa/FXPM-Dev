"""INVENTED SOFTWARE FIXTURES ONLY: no market, source, or profitability evidence."""
import hashlib
import json

import pytest

import pm_book_multiplicity as book
import pm_winner_selection as winner


def _sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _fold_fixture():
    generation = winner.V19_GENERATION
    axis = {"oos_start_date_utc": "2024-01-01", "oos_end_date_utc": "2024-01-03",
        "daily_dates_utc": ["2024-01-01", "2024-01-02", "2024-01-03"]}
    pair = {"admission_schema": "INVENTED", "admission_policy": {}, "tiers": [
        {"tier": tier, "numerical_execution_cost_policy": {"INVENTED": tier}}
        for tier in ("P75", "P95")]}
    policy = {"frozen_tier_pair_contract": pair,
        "risk_unit_contract_sha256": generation.risk_unit_contract_sha256}
    rows = {}
    for tier in ("P75", "P95"):
        row = {"spread_tier": tier, "source_contract_sha256": _sha(["INVENTED", tier]),
            "cost_contract_sha256": _sha({"INVENTED": tier}),
            "entry_spread_admission_policy_sha256": _sha({"schema_version": "INVENTED", "entry_admission_policy": {}}),
            "independent_signal_r_witness_sha256": _sha(["INVENTED_WITNESS", tier]),
            "resolved_trade_entry_offset_seconds": [0, 60, 172800],
            "resolved_trade_exit_bar_open_offset_seconds": [0, 60, 255600],
            "resolved_trade_exit_observed_at_offset_seconds": [60, 120, 259200],
            "resolved_trade_event_order_anchor": 1,
            "resolved_trade_entry_event_order_offsets": [0, 4, 8],
            "resolved_trade_exit_event_order_offsets": [2, 6, 10],
            "resolved_trade_opportunity_id_digests_base64": winner._encode_position_id_digests(
                ["iso:" + _sha(["INVENTED", tier, index]) for index in range(3)], "fixture", prefix="iso:"),
            "daily_resolved_trade_r_e12_i64be_base64": winner._encode_r_e12_i64be_base64(
                [60 * 10**12, 0, 250000000000], maximum_abs_r=generation.max_abs_daily_r, code="fixture"),
            "resolved_trade_net_r_e12_i64be_base64": winner._encode_r_e12_i64be_base64(
                [30 * 10**12, 30 * 10**12, 250000000000], maximum_abs_r=winner.MAX_ABS_TRADE_R, code="fixture"),
            "trade_power_binding": None}
        rows[tier] = row
    policy["source_contract_sha256"] = _sha({tier: row["source_contract_sha256"] for tier, row in rows.items()})
    policy["cost_contract_sha256"] = _sha({tier: row["cost_contract_sha256"] for tier, row in rows.items()})
    fold = {key: _sha(["INVENTED", key]) for key in winner._COMMON_FOLD_KEYS}
    fold.update(fold_ordinal=0, tier_evidence=rows)
    _rehash(fold, axis, policy)
    return fold, axis, policy


def _rehash(fold, axis, policy):
    generation = winner.V19_GENERATION
    for row in fold["tier_evidence"].values():
        lineage = {key: row[key] for key in winner._TIER_LINEAGE_FIELDS}
        row["daily_surface_sha256"] = _sha({"shared_fold_axis_row_sha256": _sha(axis), **lineage,
            "risk_unit_contract_sha256": policy["risk_unit_contract_sha256"],
            "daily_resolved_trade_r_e12_i64be_base64": row["daily_resolved_trade_r_e12_i64be_base64"]})
        row["trade_vector_sha256"] = _sha({"shared_fold_axis_row_sha256": _sha(axis), **lineage,
            "accounting_identity_kind": generation.policy["accounting_identity"],
            "completed_trade_unit": generation.policy["daily_unit"],
            "inventory_boundary_kind": generation.policy["inventory_boundary"],
            **{key: row[key] for key in generation.serialized_identity_fields},
            "resolved_trade_net_r_e12_i64be_base64": row["resolved_trade_net_r_e12_i64be_base64"],
            "independent_signal_r_witness_sha256": row["independent_signal_r_witness_sha256"],
            "risk_unit_contract_sha256": policy["risk_unit_contract_sha256"], "trade_power_binding": None})


def _validate(fold, axis, policy):
    return winner._validate_current_fold(fold, ordinal=0, shared_axis=axis, policy=policy,
        attempt_payload_sha=_sha("INVENTED"), attempted_trials=1, authenticated=False,
        _generation=winner.V19_GENERATION)


def test_additive_utility_has_no_wealth_domain_and_legacy_contract_is_retained():
    for value in (-1000.0, -1.0, 0.0, 0.25, 60.0):
        assert winner._daily_primary_utility_from_r(value, _generation=winner.V19_GENERATION) == value
    for generation in (winner.V17_GENERATION, winner.V18_GENERATION):
        assert winner._daily_primary_utility_from_r(0.25, _generation=generation) == winner._daily_log_growth_from_r(0.25, _generation=generation)
        assert generation.tier_fold_keys == winner._CURRENT_TIER_FOLD_KEYS
        assert generation.risk_unit_contract_sha256 == winner.PINNED_RISK_UNIT_CONTRACT_SHA256
        assert generation.selection_addendum_evidence_version == "fxpm.matched-floor-selection-evidence.v1"
    winner._validate_primary_policy_contract(_generation=winner.V19_GENERATION)


def test_recorded_exit_buckets_samebar_end_observation_zero_day_and_overlap_sum():
    fold, axis, policy = _fold_fixture()
    result = _validate(fold, axis, policy)["tier_evidence"]["P75"]
    assert result["daily_resolved_trade_r"] == [60.0, 0.0, 0.25]
    assert sum(result["daily_resolved_trade_r"]) == sum(result["resolved_trade_net_r"])
    assert result["resolved_trade_entry_utc"][0] == result["resolved_trade_exit_bar_open_utc"][0]
    assert result["resolved_trade_exit_observed_at_utc"][-1] == "2024-01-04T00:00:00Z"
    assert result["resolved_trade_exit_bar_open_utc"][-1] == "2024-01-03T23:00:00Z"
    assert result["daily_resolved_trade_r"][0] > winner.MAX_ABS_DAILY_R
    assert all(value.startswith("iso:") for value in result["resolved_trade_opportunity_ids"])


def test_conserved_total_cannot_move_a_trade_to_the_observed_day():
    fold, axis, policy = _fold_fixture()
    for row in fold["tier_evidence"].values():
        row["daily_resolved_trade_r_e12_i64be_base64"] = winner._encode_r_e12_i64be_base64(
            [0, 60 * 10**12, 250000000000], maximum_abs_r=winner.V19_GENERATION.max_abs_daily_r, code="fixture")
    _rehash(fold, axis, policy)
    with pytest.raises(ValueError, match="RECORDED_EXIT_DAY_ATTRIBUTION_MISMATCH"):
        _validate(fold, axis, policy)


@pytest.mark.parametrize("field,offset", [
    ("resolved_trade_entry_offset_seconds", 259200),
    ("resolved_trade_exit_bar_open_offset_seconds", 259200),
    ("resolved_trade_exit_observed_at_offset_seconds", 259201),
])
def test_endpoint_rules_refuse_end_open_end_barlabel_and_late_observation(field, offset):
    fold, axis, policy = _fold_fixture()
    fold["tier_evidence"]["P75"][field][-1] = offset
    _rehash(fold, axis, policy)
    with pytest.raises(ValueError):
        _validate(fold, axis, policy)


def test_policy_changes_seed_bytes_and_draws_without_changing_derivation_or_counts():
    old, new = winner.V17_GENERATION, winner.V19_GENERATION
    keys = ("moving_block_bootstrap_seed_domain", "moving_block_bootstrap_seed_rule",
        "moving_block_bootstrap_prng", "moving_block_bootstrap_repetitions",
        "moving_block_bootstrap_max_resampled_blocks_per_scale", "moving_block_bootstrap_operation_limit")
    assert {key: old.policy[key] for key in keys} == {key: new.policy[key] for key in keys}
    old_state, old_sha = winner._bootstrap_seed("INVENTED_CANDIDATE", "P75", "PRIMARY", 5, _generation=old)
    new_state, new_sha = winner._bootstrap_seed("INVENTED_CANDIDATE", "P75", "PRIMARY", 5, _generation=new)
    assert old.policy_sha256 != new.policy_sha256 and old_sha != new_sha
    expected = {"seed_domain": new.policy["moving_block_bootstrap_seed_domain"],
        "selection_policy_sha256": new.policy_sha256, "process_candidate_id": "INVENTED_CANDIDATE",
        "lane": "P75", "scale_role": "PRIMARY", "block_days": 5,
        "repetitions": new.policy["moving_block_bootstrap_repetitions"]}
    assert new_sha == _sha(expected)
    assert winner._splitmix64_next(old_state)[1] != winner._splitmix64_next(new_state)[1]


def test_legacy_v17_v18_policy_bytes_and_keys_remain_the_accepted_contract():
    from tests.test_noise101_winner_generation import _old_v13_policy
    old, old_sha = _old_v13_policy()
    assert _sha(old) == old_sha == winner.V17_GENERATION.policy_sha256
    assert json.dumps(old, sort_keys=True) == json.dumps(dict(winner.V17_SELECTION_POLICY), sort_keys=True)
    newer = dict(winner.V18_SELECTION_POLICY)
    assert newer.keys() == old.keys()
    assert {key for key in old if old[key] != newer[key]} == {"policy_version", "moving_block_bootstrap_operation_limit"}


def test_matched_floor_equality_and_aliases_remain_stable():
    assert not winner._v17_below_matched_stress_economic_floor([0.1, 0.2], [0.3, 0.0])
    assert winner._v17_below_matched_stress_economic_floor([0.1, 0.2], [0.3, 0.000000000001])
    assert winner.CURRENT_WINNER_SELECTION_CONTRACT_VERSION == "fxpm.winner-selection.v15"
    assert book.CURRENT_BOOK_MULTIPLICITY_CONTRACT_VERSION == "fxpm.book-multiplicity.v15"
    assert winner.V19_GENERATION.max_attempts == 300
    assert book.book_generation_for_winner(winner.V19_GENERATION).version == "fxpm.book-multiplicity.v19"
    assert book.V19_GENERATION.p_value_contract_sha256 == winner.V19_BOOK_P_VALUE_CONTRACT_SHA256
    assert winner.V19_GENERATION.selection_addendum_contract["floor"] == winner.V17_SELECTION_ADDENDUM_CONTRACT["floor"]
    contract = winner.build_independent_signal_risk_unit_contract()
    contract["commission"] = "CHANGED_FIXTURE"
    assert winner.build_independent_signal_risk_unit_contract()["commission"] == "NOT_MODELED"


@pytest.mark.parametrize("generation", [book.ARCHIVED_GENERATION, book.V19_GENERATION])
def test_downstream_book_entry_version_and_identity_are_bound_together(generation):
    import pm_pipeline as pipeline
    entry = {key: None for key in pipeline._CURRENT_REFIT_BOOK_ENTRY_FIELDS}
    entry.update(symbol="INVENTED", timeframe="M5", regime="TREND", historical_state="WINNER",
        historical_local_candidate_pending_causal=True, local_process_candidate_id="INVENTED",
        local_process_sha256=_sha("INVENTED_PROCESS"), winner_decision_sha256=_sha("INVENTED_WINNER"),
        regime_receipt_sha256=_sha("INVENTED_REGIME"), regime_detector_record_sha256=_sha("INVENTED_DETECTOR"))
    identity = {key: value for key, value in entry.items() if key not in {
        "adjusted_q_value", "by_qualified", "historical_local_candidate_pending_causal", "historical_book_entry_sha256"}}
    entry["historical_book_entry_sha256"] = _sha({"book_contract_version": generation.version, **identity})
    contract_version = pipeline._current_refit_book_entry_contract_version(generation.cell_entry_version)
    assert contract_version == generation.version
    assert pipeline._current_refit_validate_book_entry(entry, request=entry,
        book_contract_version=contract_version)[1] == entry["historical_book_entry_sha256"]
    if generation.independent_signal_r_enabled:
        assert generation.cell_entry_version == "fxpm.book-multiplicity-cell.v10"
        with pytest.raises(ValueError, match="CURRENT_REFIT_BOOK_ENTRY_INVALID"):
            pipeline._current_refit_validate_book_entry(entry, request=entry)
    else:
        assert generation.cell_entry_version == pipeline.CURRENT_REFIT_BOOK_ENTRY_VERSION == "fxpm.book-multiplicity-cell.v9"


def test_downstream_unknown_cell_version_is_refused_and_format_defaults_stay_legacy():
    import pm_current_refit_production as capture
    import pm_pipeline as pipeline
    with pytest.raises(ValueError, match="CURRENT_REFIT_BOOK_ENTRY_VERSION_INVALID"):
        pipeline._current_refit_book_entry_contract_version("fxpm.book-multiplicity-cell.INVENTED")
    assert capture._RAW_FORMATS[capture.ART_BOOK_ENTRY] == "fxpm.book-multiplicity-cell.v9"
    capture._assert_pipeline_format_contract()


def _origin_fixture(generation):
    origin = {key: _sha(["INVENTED", key]) for key in generation.stage_origin_fields}
    origin["schema_version"] = generation.stage_origin_version
    if generation.independent_signal_r_enabled:
        origin.update(attempt_ordinal=0, attempt_id="INVENTED_ATTEMPT")
    else:
        origin.update(stage_size_bytes=1, unit={"ordinal": 0, "raw_attempt_id": "INVENTED_ATTEMPT"})
    return origin


@pytest.mark.parametrize("generation", [winner.V17_GENERATION, winner.V18_GENERATION, winner.V19_GENERATION])
def test_direct_origin_is_explicit_and_legacy_stage_origin_stays_compatible(generation):
    origin = _origin_fixture(generation)
    assert winner._validate_selection_stage_origin(origin,
        policy_sha=origin["precommitment_sha256"], attempt={"attempt_id": "INVENTED_ATTEMPT"},
        ordinal=0, witness_sha=origin["matched_base_facts_sha256"], _generation=generation) == origin
    if generation.independent_signal_r_enabled:
        assert "unit" not in origin and "stage_size_bytes" not in origin
        assert origin["schema_version"] == "fxpm.independent-signal-stage-origin-binding.v1"
    else:
        assert origin["schema_version"] == "fxpm.matched-floor-stage-origin-binding.v1"


@pytest.mark.parametrize("mutation", ["extra_legacy_stage", "attempt_splice", "invalid_hash"])
def test_direct_origin_rejects_extra_legacy_stage_spliced_attempt_and_invalid_hash(mutation):
    origin = _origin_fixture(winner.V19_GENERATION)
    if mutation == "extra_legacy_stage":
        origin["unit"] = {"INVENTED": True}
    elif mutation == "attempt_splice":
        origin["attempt_id"] = "DIFFERENT_INVENTED_ATTEMPT"
    else:
        origin["bound_fitted_state_sha256"] = "INVALID"
    with pytest.raises(ValueError):
        winner._validate_selection_stage_origin(origin,
            policy_sha=origin["precommitment_sha256"], attempt={"attempt_id": "INVENTED_ATTEMPT"},
            ordinal=0, witness_sha=origin["matched_base_facts_sha256"], _generation=winner.V19_GENERATION)
