"""INVENTED SOFTWARE FIXTURES ONLY: no market, source, or profitability evidence."""
from copy import deepcopy
from datetime import date, datetime, timedelta
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


def _v19_witness_ledger_inputs(*, repeated_witness):
    from pm_authority_spool import ARTIFACT_DESCRIPTOR_SCHEMA
    from pm_winner_raw_evidence import _evidence_entry

    generation = winner.V19_GENERATION
    fold, axis, policy = _fold_fixture()
    cell = ("EURUSD", "D1", "TREND")
    policy.update(contract_version=generation.precommitment_version,
        run_id="INVENTED_V19_WITNESS_REPLAY", cell_id=winner.compute_cell_id(*cell),
        symbol=cell[0], timeframe=cell[1], regime=cell[2],
        frozen_tier_pair_contract_sha256=_sha(policy["frozen_tier_pair_contract"]))
    policy.update({name: _sha(["INVENTED", name]) for name in (
        "partition_cost_precommitments_sha256", "process_registry_sha256",
        "horizon_registry_sha256", "trial_registry_sha256")})
    attempt = {
        "ordinal": 0, "attempt_id": _sha("INVENTED_ATTEMPT")[:40],
        "process_candidate_id": _sha("INVENTED_CANDIDATE")[:40],
        "strategy_id": "INVENTED_STRATEGY", "precommitted_evidence_role": "EVALUATION_REQUIRED",
        **{name: _sha(["INVENTED", name]) for name in (
            "process_sha256", "horizon_process_sha256", "trial_definition_sha256", "refit_recipe_sha256")},
        **{name: policy[name] for name in ("source_contract_sha256", "cost_contract_sha256",
            generation.pair_field, "risk_unit_contract_sha256")},
    }
    complexity = {key: attempt[key] for key in ("ordinal", "process_candidate_id",
        "strategy_id", "trial_definition_sha256", "refit_recipe_sha256")}
    complexity.update(dict.fromkeys(("source_strategy_domain_sha256", "parameter_domain",
        "parameter_domain_sha256", "finite_domain_cardinality", "active_free_parameter_count")))
    policy["selection_complexity_roster"] = [complexity]
    policy["selection_complexity_roster_sha256"] = _sha([complexity])
    policy_sha, attempt_sha = _sha(policy), _sha({"attempts": [attempt]})

    start = date.fromisoformat(axis["oos_start_date_utc"])
    days = generation.policy["min_days_per_fold"]
    dates = [(start + timedelta(days=offset)).isoformat() for offset in range(days)]
    planned = {"fold_ordinal": 0, "fold_id": "INVENTED_FOLD",
        "anchor_as_of_utc": "2023-12-31T00:00:00Z", "oos_start_date_utc": dates[0],
        "oos_end_date_utc": dates[-1]}
    partition = {
        "schema_version": generation.shared_partition_version, "causal_fold_plan_row_sha256": _sha(planned),
        **{name: policy[name] for name in ("cell_id", "symbol", "timeframe", "regime",
            "source_contract_sha256", "cost_contract_sha256", generation.pair_field, "risk_unit_contract_sha256")},
        **{name: _sha(["INVENTED", name]) for name in (
            "native_prefix_index_sha256", "native_prefix_content_sha256", "native_source_sha256",
            "activation_manifest_sha256", "source_group_decision_sha256", "regime_fit_decision_sha256",
            "execution_spec_receipt_sha256")},
    }
    axis = {**planned, "daily_dates_utc": dates,
        "shared_oos_partition": {**partition, "receipt_sha256": _sha(partition)}}
    for row in fold["tier_evidence"].values():
        row["daily_resolved_trade_r_e12_i64be_base64"] = winner._encode_r_e12_i64be_base64(
            [60 * 10**12, 0, 250000000000] + [0] * (days - 3),
            maximum_abs_r=generation.max_abs_daily_r, code="INVENTED_DAILY")
    if repeated_witness:
        p75, p95 = (fold["tier_evidence"][tier] for tier in ("P75", "P95"))
        p95[generation.witness_field] = p75[generation.witness_field]
    _rehash(fold, axis, policy)
    entries = [_evidence_entry(attempt, status="PROCESS_REJECTED", folds=[fold])]

    account = _validate(fold, axis, policy)["tier_evidence"]["P75"]
    identity = {key: account[key] for key in generation.trade_identity_fields}
    floor = {"floor_status": "UNAVAILABLE_BASE_FILL_OR_POSITION_PARITY_MISMATCH",
        "floor_reason_codes": ["INVENTED_TIER_PARITY_MISMATCH"]}
    partition_statuses = [
        {"fold_ordinal": 0, "partition_role": role, "witness_sha256": _sha(["INVENTED_WITNESS", role]), **floor}
        for role in ("optimizer_train", "tuning_validation", "final_lockbox")
    ]
    origin = {**_origin_fixture(generation), "precommitment_sha256": policy_sha,
        "attempt_id": attempt["attempt_id"]}
    witness_digest = _sha("INVENTED_MATCHED_BASE_WITNESSES_DESCRIPTOR")
    addendum_rows = [{
        **{key: attempt[key] for key in ("ordinal", "attempt_id", "process_candidate_id")},
        "stage_origin_binding": origin,
        "partition_witnesses_descriptor": {
            "artifact_id": "invented/attempt-0000/matched-base-witnesses",
            "artifact_type": "winner_matched_base_witnesses",
            "descriptor": {"schema_version": ARTIFACT_DESCRIPTOR_SCHEMA, "sha256": witness_digest,
                "relative_path": f"objects/sha256/{witness_digest[:2]}/{witness_digest}.fxpm-ascii-json-v1",
                "size_bytes": 64, "format": "fxpm-ascii-json-v1"},
        },
        "full_witnesses_sha256": origin["matched_base_facts_sha256"],
        "partition_statuses": partition_statuses,
        "final_folds": [{"fold_ordinal": 0, "shared_fold_axis_row_sha256": _sha(axis),
            "raw_shared_oos_partition_sha256": _sha(axis["shared_oos_partition"]),
            "base_opportunity_identity": identity, "base_opportunity_identity_sha256": _sha(identity),
            "base_actual_initial_risk_net_r": None, "matched_p95_spread_cost_r": None, **floor}],
    }]
    boundary = "2024-04-01T00:00:00Z"
    completed = "2024-04-01T00:00:01Z"
    payload = {
        **{name: policy[name] for name in ("run_id", "cell_id", "symbol", "timeframe", "regime",
            "partition_cost_precommitments_sha256", "source_contract_sha256", "cost_contract_sha256",
            generation.pair_field, "risk_unit_contract_sha256", "process_registry_sha256",
            "horizon_registry_sha256", "trial_registry_sha256")},
        "contract_version": generation.evidence_version,
        "generated_at_utc": completed, "evidence_access_started_at_utc": boundary,
        "evidence_access_completed_at_utc": completed, "latest_evidence_boundary_utc": boundary,
        "precommitment_payload_sha256": policy_sha, "attempt_ledger_payload_sha256": attempt_sha,
        "shared_fold_axis_count": 1, "shared_fold_axis_sha256": _sha([axis]), "shared_fold_axis": [axis],
        "attempt_count": 1, "entries_sha256": _sha(entries), "entries": entries,
        "evidence_scope": "PROCESS_LEVEL_ONLY", "parameter_evidence_inherited": False,
        "selection_addendum": {"contract_version": generation.selection_addendum_evidence_version,
            "precommitment_payload_sha256": policy_sha, "attempt_ledger_payload_sha256": attempt_sha,
            "selection_complexity_roster_sha256": policy["selection_complexity_roster_sha256"],
            "entries": addendum_rows, "entries_sha256": _sha(addendum_rows)},
    }
    return dict(payload=payload, policy=policy, policy_sha=policy_sha, attempt_payload_sha=attempt_sha,
        frozen_at=datetime.fromisoformat("2023-12-31T00:00:00+00:00"), cell=(policy["cell_id"], *cell),
        attempts=[attempt], fold_plan=[planned], latest_evidence_boundary=datetime.fromisoformat(boundary),
        verification_time=datetime.fromisoformat(completed), request_sha=_sha("INVENTED_REQUEST"),
        _generation=generation)


def _v19_witness_ledger(*, repeated_witness):
    # Exercise the real ledger boundary with one invented fold.
    return winner._validate_evidence_ledger(**_v19_witness_ledger_inputs(repeated_witness=repeated_witness))


def test_v19_evidence_ledger_accepts_distinct_tier_witnesses():
    _, entries, axes = _v19_witness_ledger(repeated_witness=False)
    assert len(entries) == len(axes) == 1
    tiers = entries[0]["folds"][0]["tier_evidence"]
    field = winner.V19_GENERATION.witness_field
    assert tiers["P75"][field] != tiers["P95"][field]


def test_v19_evidence_ledger_refuses_cross_tier_witness_replay():
    with pytest.raises(winner._ContractError) as caught:
        _v19_witness_ledger(repeated_witness=True)
    assert str(caught.value) == "EVIDENCE_0000_POSITION_ACTIVITY_LEDGER_SHA256_REPLAYED_ACROSS_EVIDENCE_DOMAINS"


_WITNESS_BINDING_REFUSAL = "INDEPENDENT_R_RESOLVED_WITNESS_BINDING_CHANGED"


def _invented_owner_provenance(capture, lanes, reference):
    from test_authoritative_search_execution import _fold_provenance_transport_fixture
    import pm_authoritative_search_execution as execution
    import pm_winner_raw_evidence as raw

    references = dict.fromkeys(raw._PARTITIONS, reference)
    template, _, _ = _fold_provenance_transport_fixture(source_observation_references=references)
    lane_hashes = {name: _sha(lane) for name, lane in lanes.items()}
    execution_receipt = raw.build_independent_tier_execution_receipt(lanes, _sha("INVENTED_TIER_PAIR"))
    signals = capture["signals"]
    signal_digest = hashlib.sha256(signals.to_numpy(dtype="<i4").tobytes()).hexdigest()
    signal_vector = execution._receipt(execution.SIGNAL_VECTOR_VERSION, {
        "encoding": "little-endian-signed-int32", "observation_count": len(signals),
        "values": signals.tolist(), "signal_vector_sha256": signal_digest,
    })
    schema = raw.INDEPENDENT_AUTHORITATIVE_PROVENANCE_SCHEMA
    specific = raw._provenance_specific_keys(current=True, referenced=True, independent=True)
    replay = template["attempt_replay_receipt"]
    del replay["position_events_are_genuine_backtester_v2"]
    replay.update(lane_sha256=lane_hashes, independent_tier_execution_receipt=execution_receipt,
        independent_tier_execution_receipt_sha256=execution_receipt["receipt_sha256"],
        signal_vector_receipts=dict.fromkeys(raw._PARTITIONS, signal_vector),
        genuine_independent_signal_r_capture=True)
    template["anchor_trial_ledger"]["attempted_trials_exact"] = 2
    for ordinal in range(2):
        provenance = {}
        for role, receipt in template.items():
            body = {key: value for key, value in receipt.items() if key not in {"receipt_sha256", "schema_version"}}
            body.update(attempt_ordinal=ordinal, horizon_attempt_id=f"invented-owner-{ordinal}",
                timeframe="D1", fold_id="INVENTED_FOLD")
            sealed = execution._receipt(schema, body)
            provenance[role] = raw._validated_self_hashed_receipt(sealed, role=role,
                expected_keys=raw._PROVENANCE_COMMON_KEYS | specific[role], schema_version=schema)
        yield provenance


@pytest.fixture(scope="module")
def invented_v19_witness_owners(tmp_path_factory):
    """Two invented receipt owners share a genuine independent-R batch."""
    from dataclasses import asdict, replace
    from test_independent_signal_r import _evaluate
    from test_independent_signal_r_production_raw import _capture_case
    from test_signal_opportunities import _provenance
    from test_winner_raw_evidence import _source_observation_transport_case
    import pm_authority_spool as spool
    import pm_winner_raw_evidence as raw

    case = _source_observation_transport_case(tmp_path_factory.mktemp("witness-owners"),
        generation_id="invented-v19-witness-owners")
    context, sink = case["context"], case["sink"]
    frozen = replace(case["frozen"], timeframe="D1", artifact_namespace="invented-v19-witness-owners")
    capture = _capture_case(daily=True)
    base = capture["lane"]
    axes = [bar.isoformat().replace("+00:00", "Z") for bar in capture["frame"].index]
    observations = [{"target_open_utc": timestamp, "expected_prior_fine_close_utc": timestamp,
        "prior_fine_close_utc": timestamp, "points": 9.0} for timestamp in axes]
    receipt_body = {key: value for key, value in case["receipt"].items() if key != "receipt_sha256"}
    receipt_body.update(timeframe="D1", observations=observations, entry_observations_sha256=_sha(observations),
        source_product={**receipt_body["source_product"], "cutoff_utc": base["batch"]["cutoff_utc"]})
    reference = raw.publish_source_observation_receipt(sink, frozen=frozen,
        receipt={**receipt_body, "receipt_sha256": _sha(receipt_body)})
    p95_batch = asdict(_evaluate(capture["frame"], capture["signals"].tolist(),
        strategy=capture["strategy"], spec=capture["spec"], pricing=capture["pricing"], timeframe="D1",
        provenance=_provenance(capture["strategy"], tier="P95")))
    p95_batch["records"] = list(p95_batch["records"])
    final_lanes = {
        "P75": {**base, "source_reference": reference},
        "P95": {**base, "source_reference": reference, "batch": p95_batch,
            "swap_accrual_contract": p95_batch["cost_model"]["swap_contract"]},
    }
    records = {tier: raw._independent_r_records(lane, regime="TREND") for tier, lane in final_lanes.items()}
    assert records["P75"] and records["P95"]
    lanes = {role + "_" + tier.lower(): lane
        for role in raw._PARTITIONS for tier, lane in final_lanes.items()}
    retained_sink = spool.open_symbol_sink(context, "EURUSD", manifest_domain=spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN)
    root = raw._artifact_prefix(frozen.artifact_namespace, frozen.timeframe, frozen.regime)
    owners = {}
    for attempt_ordinal, provenance in enumerate(_invented_owner_provenance(capture, lanes, reference)):
        prefix = f"{root}/attempt-{attempt_ordinal:04d}/fold-000"
        receipts = {role: raw._put_ascii_json(sink, prefix + "/provenance/" + role, "winner_" + role, receipt)
            for role, receipt in provenance.items()}
        replay = provenance["attempt_replay_receipt"]
        replay_sha = receipts["attempt_replay_receipt"]["descriptor"]["sha256"]
        assert replay["attempt_ordinal"] == attempt_ordinal
        assert replay["fold_ordinal"] == 0
        assert replay_sha == _sha(replay) != replay["receipt_sha256"]
        ledger_fold = {"fold_ordinal": 0,
            "tier_evidence": {tier: {"spread_tier": tier} for tier in final_lanes},
            **{("attempt_oos_source_receipt" if role == "oos_source_receipt" else role) + "_sha256":
                entry["descriptor"]["sha256"] for role, entry in receipts.items()}}
        original_fold = deepcopy(ledger_fold)
        old_entries, witnesses = {}, {}
        for tier, lane in final_lanes.items():
            field = winner.V19_GENERATION.witness_field
            old_witness = {"schema_version": "fxpm.independent-signal-resolved-witness.v1",
                "source_reference": reference, "partition_axis": lane["trade_power_partition_axis"],
                "method_edition": lane["batch"]["method_edition"], "records": records[tier],
                "batch_sha256": _sha(lane["batch"])}
            witness = raw._independent_resolved_witness(lane, records[tier],
                attempt_replay_receipt_sha256=replay_sha, spread_tier=tier)
            assert witness["schema_version"] == "fxpm.independent-signal-resolved-witness.v2"
            assert {key: value for key, value in witness.items() if key not in ("owner", "schema_version")} == {
                key: value for key, value in old_witness.items() if key != "schema_version"}
            art_id = prefix + "/resolved-" + tier.lower()
            art_type = "winner_independent_signal_r_witness"
            old_entries[art_id] = raw._put_ascii_json(retained_sink, art_id, art_type, old_witness)
            original_fold["tier_evidence"][tier][field] = old_entries[art_id]["descriptor"]["sha256"]
            entry = raw._put_ascii_json(sink, art_id, art_type, witness)
            ledger_fold["tier_evidence"][tier][field] = entry["descriptor"]["sha256"]
            witnesses[tier] = witness
        owners[attempt_ordinal] = dict(prefix=prefix, attempt_ordinal=attempt_ordinal,
            ledger_fold=ledger_fold, final_lanes=final_lanes, final_records=records,
            original_fold=original_fold, old_entries=old_entries, witnesses=witnesses)

    field = winner.V19_GENERATION.witness_field
    assert owners[0]["original_fold"]["tier_evidence"]["P75"][field] == owners[1]["original_fold"]["tier_evidence"]["P75"][field]
    first = owners[0]
    wrong_tier = raw._independent_resolved_witness(first["final_lanes"]["P75"], first["final_records"]["P75"],
        attempt_replay_receipt_sha256=first["ledger_fold"]["attempt_replay_receipt_sha256"], spread_tier="P95")
    wrong_tier_entry = raw._put_ascii_json(sink, first["prefix"] + "/swapped-tier-control",
        "winner_independent_signal_r_witness", wrong_tier)
    def sealed_entries(owner_sink):
        manifest = spool.verify_symbol_receipt(context.root, owner_sink.seal(),
            expected_generation_id=context.generation_id, expected_symbol="EURUSD",
            expected_native_universe_sha256=context.native_universe_sha256, expected_config_sha256=context.config_sha256,
            expected_manifest_domain=owner_sink.manifest_domain)
        return {entry["artifact_id"]: entry for entry in manifest["artifacts"]}

    entries = sealed_entries(sink)
    retained_entries = sealed_entries(retained_sink)
    for owner in owners.values():
        assert all(retained_entries[key] == entry for key, entry in owner["old_entries"].items())
        owner["old_entries"] = {key: retained_entries[key] for key in owner["old_entries"]}
    return context, owners, entries, wrong_tier_entry


def _verify_witness_owner(raw, context, owner, entries, *, retained=False, allow_retained_v1=False):
    def read(artifact_id):
        return raw._source_observation_closure_body(context, entries[artifact_id])

    raw._verify_independent_resolved_witness_fold(prefix=owner["prefix"], attempt_ordinal=owner["attempt_ordinal"],
        ledger_fold=owner["original_fold"] if retained else owner["ledger_fold"],
        entries=entries, read=read, final_lanes=owner["final_lanes"], final_records=owner["final_records"],
        replay_receipt=read(owner["prefix"] + "/provenance/attempt_replay_receipt"), allow_retained_v1=allow_retained_v1)


def _two_owner_ledger(owners, *, repeat_witness=False):
    from pm_winner_raw_evidence import _evidence_entry

    # Use the scratch publications as the provenance of the invented ledger.
    inputs = _v19_witness_ledger_inputs(repeated_witness=False)
    payload, policy = inputs["payload"], inputs["policy"]
    template_fold = payload["entries"][0]["folds"][0]
    addendum = payload["selection_addendum"]
    template_addendum = addendum["entries"][0]
    template_attempt = inputs["attempts"][0]
    template_complexity = policy["selection_complexity_roster"][0]
    axis = payload["shared_fold_axis"][0]
    attempts, evidence, addenda, complexities = [], [], [], []
    field = winner.V19_GENERATION.witness_field
    for ordinal, owner in enumerate(owners.values()):
        actual_fold = owner["ledger_fold"]
        attempt = {**template_attempt, "ordinal": ordinal,
            "attempt_id": _sha(["INVENTED_TWO_OWNER_ATTEMPT", ordinal])[:40],
            "process_candidate_id": _sha(["INVENTED_TWO_OWNER_CANDIDATE", ordinal])[:40]}
        attempts.append(attempt)
        complexities.append({**template_complexity, **{key: attempt[key]
            for key in ("ordinal", "process_candidate_id")}})
        fold = deepcopy(template_fold)
        fold.update({key: actual_fold[key] for key in actual_fold if key != "tier_evidence"})
        for tier, row in fold["tier_evidence"].items():
            row[field] = actual_fold["tier_evidence"][tier][field]
        if repeat_witness and ordinal == 1:
            fold["tier_evidence"]["P75"][field] = owners[0]["ledger_fold"]["tier_evidence"]["P75"][field]
        _rehash(fold, axis, policy)
        evidence.append(_evidence_entry(attempt, status="PROCESS_REJECTED", folds=[fold]))
        row = deepcopy(template_addendum)
        row.update({key: attempt[key] for key in ("ordinal", "attempt_id", "process_candidate_id")})
        row["stage_origin_binding"].update(attempt_ordinal=ordinal, attempt_id=attempt["attempt_id"])
        addenda.append(row)
    policy.update(selection_complexity_roster=complexities, selection_complexity_roster_sha256=_sha(complexities))
    policy_sha, attempt_sha = _sha(policy), _sha({"attempts": attempts})
    for row in addenda:
        row["stage_origin_binding"]["precommitment_sha256"] = policy_sha
    addendum.update(precommitment_payload_sha256=policy_sha, attempt_ledger_payload_sha256=attempt_sha,
        selection_complexity_roster_sha256=policy["selection_complexity_roster_sha256"],
        entries=addenda, entries_sha256=_sha(addenda))
    payload.update(precommitment_payload_sha256=policy_sha, attempt_ledger_payload_sha256=attempt_sha,
        attempt_count=len(attempts), entries=evidence, entries_sha256=_sha(evidence))
    inputs.update(policy_sha=policy_sha, attempt_payload_sha=attempt_sha, attempts=attempts)
    return winner._validate_evidence_ledger(**inputs)


def test_v19_distinct_resolved_witness_owners_pass_raw_and_winner(invented_v19_witness_owners):
    import pm_winner_raw_evidence as raw

    context, owners, entries, _ = invented_v19_witness_owners
    first, second = (owners[ordinal]["witnesses"]["P75"] for ordinal in (0, 1))
    assert {key: value for key, value in first.items() if key != "owner"} == {
        key: value for key, value in second.items() if key != "owner"}
    assert first["owner"] != second["owner"]
    for owner in owners.values():
        _verify_witness_owner(raw, context, owner, entries)
    _, verified, _ = _two_owner_ledger(owners)
    field = winner.V19_GENERATION.witness_field
    assert [row["folds"][0]["tier_evidence"]["P75"][field] for row in verified] == [_sha(first), _sha(second)]
    assert _sha(first) != _sha(second)


def test_v19_winner_refuses_same_resolved_witness_owner_replay(invented_v19_witness_owners):
    _, owners, _, _ = invented_v19_witness_owners
    with pytest.raises(winner._ContractError) as caught:
        _two_owner_ledger(owners, repeat_witness=True)
    assert str(caught.value) == "EVIDENCE_0001_POSITION_ACTIVITY_LEDGER_SHA256_REPLAYED_ACROSS_EVIDENCE_DOMAINS"


@pytest.mark.parametrize("splice", ["owner", "tier"])
def test_v19_raw_refuses_swapped_resolved_witness(splice, invented_v19_witness_owners):
    import pm_winner_raw_evidence as raw

    context, owners, entries, wrong_tier = invented_v19_witness_owners
    owner = deepcopy(owners[0])
    art_id = owner["prefix"] + "/resolved-p75"
    source = entries[owners[1]["prefix"] + "/resolved-p75"] if splice == "owner" else wrong_tier
    # Move both references to the exact swapped body so identity comparison
    # reaches the owner field rather than stopping at a digest mismatch.
    read_view = {**entries, art_id: {**entries[art_id], "descriptor": source["descriptor"]}}
    owner["ledger_fold"]["tier_evidence"]["P75"][winner.V19_GENERATION.witness_field] = source["descriptor"]["sha256"]
    with pytest.raises(raw.WinnerRawEvidenceError, match="^" + _WITNESS_BINDING_REFUSAL + "$"):
        _verify_witness_owner(raw, context, owner, read_view)


def test_v19_raw_reads_exact_v1_only_as_retained_evidence(invented_v19_witness_owners):
    import pm_winner_raw_evidence as raw

    context, owners, entries, _ = invented_v19_witness_owners
    for owner in owners.values():
        retained_entries = {**entries, **owner["old_entries"]}
        _verify_witness_owner(raw, context, owner, retained_entries, retained=True, allow_retained_v1=True)
        with pytest.raises(raw.WinnerRawEvidenceError, match="^" + _WITNESS_BINDING_REFUSAL + "$"):
            _verify_witness_owner(raw, context, owner, retained_entries, retained=True)


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
    changed = {key for key in old if json.dumps(old[key], sort_keys=True) != json.dumps(newer[key], sort_keys=True)}
    assert changed == {"policy_version", "moving_block_bootstrap_operation_limit"}


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
