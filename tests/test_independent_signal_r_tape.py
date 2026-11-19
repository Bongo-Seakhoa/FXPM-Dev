"""Tiny invented v2 cold tapes; no producer, native source, or account replay."""
import json
from copy import deepcopy

import pytest

import pm_signal_opportunity_tape as tape
from pm_signal_opportunities import independent_episode_diagnostics
from test_signal_opportunity_tape import _bytes, _first, _groups, _publish, _relink_horizon, _sha


def _v2_case(case, *, admitted=False, unavailable_swap=False):
    request = case["request"]
    request.update(method_edition=tape.INDEPENDENT_R_METHOD,
        source_sha256={"executor": tape.INDEPENDENT_R_WRITER_SHA256,
            "labeler": tape.INDEPENDENT_R_LABELER_SHA256})
    case["expected_totals"]["not_eligible_count"] = 0
    batch, labels = None, []

    def finish():
        if batch is not None:
            batch["descriptive_groups"] = _groups(labels)
            batch["episode_diagnostics"] = independent_episode_diagnostics(labels)

    for row in case["rows"]:
        if row["record_type"] == "BATCH":
            finish()
            batch, labels = row, []
            batch.update(method_edition=tape.INDEPENDENT_R_METHOD,
                cost_model={"declared_model": "SPREAD_PLUS_SWAP", "commission": "NOT_MODELED", "account_compounding": False,
                    "swap_contract": {"status": "UNAVAILABLE" if unavailable_swap else "AVAILABLE",
                        "provenance": "HAND_BUILT_NOT_SWAP_CONVERSION_EVIDENCE"}})
        elif row["record_type"] == "OPPORTUNITY":
            label = row["label"]
            label.update(eligible=False, eligibility_reason=None, signal_regime="RANGE", episode_id=None,
                initial_risk_per_unit=None, price_pnl_per_unit=None, spread_cost_per_unit=None,
                swap_per_unit=None, price_r=None, spread_r=None, swap_r=None, modeled_net_r=None,
                cost_status="NOT_ENTERED")
            status = label["status"]
            if status == "NO_NEXT_OBSERVATION":
                label.update(stop_loss_pips=None, take_profit_pips=None, eligibility_reason="NO_NEXT_OBSERVATION")
            elif status == "RIGHT_CENSORED" and not admitted:
                label.update(status="NOT_ELIGIBLE", eligibility_reason="ENTRY_ADMISSION_" + label["entry_admission"],
                    stop_loss_pips=None, take_profit_pips=None, entry_price=None,
                    stop_loss_price=None, take_profit_price=None)
                case["expected_totals"]["right_censored_count"] -= 1
                case["expected_totals"]["not_eligible_count"] += 1
            else:
                risk = .05
                label.update(eligible=True, initial_risk_per_unit=risk,
                    episode_id="SYNTHUSD/H1/" + label["entry_regime"] + "/" + label["entry_open_utc"],
                    cost_status="UNRESOLVED")
                if status == "CLOSED":
                    price, spread, swap = label["signed_price_delta"], .002, -.001
                    label.update(price_pnl_per_unit=price, spread_cost_per_unit=spread,
                        price_r=price / risk, spread_r=-spread / risk,
                        cost_status="UNAVAILABLE_SWAP" if unavailable_swap else "MODELED_AVAILABLE")
                    if not unavailable_swap:
                        label.update(swap_per_unit=swap, swap_r=swap / risk, modeled_net_r=(price + swap) / risk)
            labels.append(label)
        elif row["record_type"] == "COMPLETE":
            finish()
            row.update(case["expected_totals"])


def _noise303_case(case):
    request, horizon, cell = case["request"], case["horizon"], case["cell_plan"]
    additions = []
    for ordinal in range(300, 303):
        roster = dict(request["attempt_roster"][-1], ordinal=ordinal,
            horizon_attempt_id=f"SYNTH-NOISE-ATTEMPT-{ordinal}", process_candidate_id=f"SYNTH-NOISE-PROCESS-{ordinal}",
            trial_definition_sha256=_sha(["synthetic noise current trial", ordinal]))
        request["attempt_roster"].append(roster)
        trial = deepcopy(horizon["trials"][-1])
        trial["trial_id"] = f"SYNTH-NOISE-TRIAL-{ordinal}"
        trial["trial_definition_sha256"] = _sha({key: value for key, value in trial.items()
            if key != "trial_definition_sha256"})
        horizon["trials"].append(trial)
        horizon["trial_registry"].append({"synthetic_trial": trial["trial_id"]})
        horizon["attempts"].append(dict(horizon["attempts"][-1], attempt_id=roster["horizon_attempt_id"],
            trial_id=trial["trial_id"], trial_definition_sha256=trial["trial_definition_sha256"]))
        cell["attempt_artifact_sha256"].append(_sha(["synthetic noise attempt artifact", ordinal]))
        additions.append(dict(record_type="ATTEMPT", **roster))
    request.update(attempt_count=303, attempt_roster_sha256=_sha(request["attempt_roster"]),
        domain_roster_sha256=_sha("HAND_BUILT_NO_NOISE101_CATALOGUE"))
    request["source_edition"] = dict(edition_id="fxpm.noise101-per-timeframe-source-edition.v1",
        process_schema="fxpm.production-search-process-precommit.v4",
        execution_edition="fxpm.route-a-noise101-per-timeframe-owner-execution.v1",
        catalogue_schema="fxpm.strategy-candidate-catalogue-by-timeframe.v1", winner_generation_id="SYNTHETIC_V18",
        ordered_strategy_ids=[f"SYNTHETIC_STRATEGY_{i:03d}" for i in range(101)], ordered_timeframes=["H1"],
        strategy_count=101, nondefault_variants_per_strategy=2, attempt_count=303,
        candidate_catalogue_sha256=request["candidate_domain_sha256"],
        domain_roster_sha256_by_timeframe={"H1": request["domain_roster_sha256"]},
        state_registry_sha256=_sha("HAND_BUILT_NO_STATE_REGISTRY"), source_release_sha256=_sha("HAND_BUILT_NO_RELEASE"))
    cell.update(attempt_count=303, attempt_roster_sha256=_sha(cell["attempt_artifact_sha256"]))
    _relink_horizon(case)
    case["rows"][-1:-1] = additions
    case["expected_totals"]["roster_count"] += 3
    case["expected_totals"]["presearch_rejected_count"] += 3
    case["rows"][-1].update(case["expected_totals"])


def _publish_v2(root, monkeypatch, mutate=None, *, admitted=False, unavailable_swap=False, noise303=False):
    def adjust(case):
        _v2_case(case, admitted=admitted, unavailable_swap=unavailable_swap)
        if noise303:
            _noise303_case(case)
        if mutate is not None:
            mutate(case)

    def header(raw):
        rows = [json.loads(line) for line in raw.splitlines()]
        rows[0]["swap_status"] = "PER_TRADE_MODELED"
        return b"".join(_bytes(row) + b"\n" for row in rows)

    # Reuse the v1 fixture builder only; restore its module constants before reading.
    with monkeypatch.context() as fixture:
        for name in ("REQUEST_SCHEMA", "TAPE_SCHEMA", "TAPE_FORMAT", "LABEL_SCHEMA"):
            fixture.setattr(tape, name, getattr(tape, "INDEPENDENT_R_" + name))
        return _publish(root, adjust, admission="ADMIT" if admitted else "REFUSE",
            raw_transform=header, horizon_publisher=True, all_rejected=noise303)


def _refuse_v2(tmp_path, monkeypatch, mutate, code):
    call, _, _ = _publish_v2(tmp_path / "spool", monkeypatch, mutate)
    with pytest.raises(tape.SignalOpportunityTapeError) as found:
        tape.read_independent_signal_r_tape(**call)
    assert found.value.code == code


def test_v2_cold_roundtrip_keeps_nonentered_observations_and_batch_episode_scope(tmp_path, monkeypatch):
    call, case, _ = _publish_v2(tmp_path / "spool", monkeypatch)
    result = tape.read_independent_signal_r_tape(**call).to_dict()
    assert result["schema_version"] == "fxpm.signal-opportunity-descriptive-summary.v2"
    assert result["method_edition"] == tape.INDEPENDENT_R_METHOD
    assert result["totals"] == case["expected_totals"]
    assert result["totals"]["not_eligible_count"] > 0
    assert result["complete_cost_net_profit_available"] is result["qualification_authority"] is False
    assert result["swap_status"] == "PER_TRADE_MODELED"
    diagnostics = result["episode_diagnostics"]
    assert len(diagnostics) == result["totals"]["batch_count"]
    assert all(row["entry_regime"] == "TREND" for row in diagnostics)
    assert len({tuple(row[key] for key in ("attempt_ordinal", "fold_ordinal", "partition", "tier"))
        for row in diagnostics}) == len(diagnostics)
    assert diagnostics[0]["traded_episode_count"] == diagnostics[0]["resolved_episode_count"] == 1
    assert diagnostics[0]["profitable_episode_share"] == 1.0
    labels = [row["label"] for row in case["rows"] if row["record_type"] == "OPPORTUNITY"]
    refused = next(row for row in labels if row["status"] == "NOT_ELIGIBLE")
    assert refused["entry_bar"] is not None and refused["entry_price"] is None
    assert labels[0]["entry_regime"] == "TREND" and labels[0]["signal_regime"] == "RANGE"


def test_v2_censored_or_unavailable_swap_episode_stays_unresolved(tmp_path, monkeypatch):
    call, _, _ = _publish_v2(tmp_path / "spool", monkeypatch, admitted=True, unavailable_swap=True)
    result = tape.read_independent_signal_r_tape(**call).to_dict()
    first = result["episode_diagnostics"][0]
    assert first["traded_episode_count"] == first["unresolved_episode_count"] == 1
    assert first["resolved_episode_count"] == 0
    assert first["profitable_episode_share"] is None
    assert result["totals"]["right_censored_count"] > 0


def test_v2_session_exit_is_observed_at_exit_open(tmp_path, monkeypatch):
    def session(case):
        label = _first(case, "OPPORTUNITY")["label"]
        label.update(exit_reason="SESSION", exit_fill="SESSION_OPEN", exit_observed_at_utc=label["exit_bar_open_utc"])
    call, _, _ = _publish_v2(tmp_path / "spool", monkeypatch, session)
    assert tape.read_independent_signal_r_tape(**call).to_dict()["status"] == "DESCRIPTIVE_COMPLETE"


@pytest.mark.parametrize("kind,code", [
    ("double_spread", "TAPE_R_ARITHMETIC_INVALID"),
    ("nonentered_r", "TAPE_R_NOT_ENTERED_INVALID"),
    ("episode_share", "TAPE_R_EPISODE_RECONCILIATION_FAILED"),
    ("commission", "TAPE_R_COST_MODEL_INVALID"),
])
def test_v2_refuses_inconsistent_r_cost_or_episode_evidence(tmp_path, monkeypatch, kind, code):
    def mutate(case):
        if kind == "double_spread":
            label = _first(case, "OPPORTUNITY")["label"]
            label["modeled_net_r"] -= label["spread_cost_per_unit"] / label["initial_risk_per_unit"]
        elif kind == "nonentered_r":
            label = next(row["label"] for row in case["rows"] if row["record_type"] == "OPPORTUNITY"
                and row["label"]["status"] == "NOT_ELIGIBLE")
            label["modeled_net_r"] = 0.0
        elif kind == "episode_share":
            _first(case, "BATCH")["episode_diagnostics"]["TREND"]["profitable_episode_share"] = 0.0
        else:
            _first(case, "BATCH")["cost_model"]["commission"] = "MODELED"
    _refuse_v2(tmp_path, monkeypatch, mutate, code)


def test_reader_entrypoints_require_their_explicit_edition(tmp_path, monkeypatch):
    v2_call, _, _ = _publish_v2(tmp_path / "v2", monkeypatch)
    v1_call, _, _ = _publish(tmp_path / "v1")
    for reader, call in ((tape.read_signal_opportunity_tape, v2_call), (tape.read_independent_signal_r_tape, v1_call)):
        with pytest.raises(tape.SignalOpportunityTapeError) as found:
            reader(**call)
        assert found.value.code == "TAPE_ARTIFACT_ROSTER_BINDING_INVALID"


def test_current_v1_pair_is_accepted_without_allowing_mixed_historical_sources(tmp_path):
    current = {"executor": tape.CURRENT_COMPATIBLE_V1_WRITER_SHA256,
        "labeler": tape.CURRENT_COMPATIBLE_V1_LABELER_SHA256}

    def compatible(case):
        case["request"]["source_sha256"] = current

    call, _, _ = _publish(tmp_path / "compatible", compatible, horizon_publisher=True)
    assert tape.read_signal_opportunity_tape(**call).to_dict()["writer_sha256"] == current["executor"]

    def mixed(case):
        case["request"]["source_sha256"] = dict(current, labeler=tape.LABELER_SHA256)

    call, _, _ = _publish(tmp_path / "mixed", mixed)
    with pytest.raises(tape.SignalOpportunityTapeError) as found:
        tape.read_signal_opportunity_tape(**call)
    assert found.value.code == "TAPE_REQUEST_BINDING_INVALID"


def test_v2_binds_existing_noise303_request_metadata_without_catalogue_or_market_fixture(tmp_path, monkeypatch):
    call, case, _ = _publish_v2(tmp_path / "spool", monkeypatch, noise303=True)
    result = tape.read_independent_signal_r_tape(**call).to_dict()
    assert result["totals"] == case["expected_totals"]
    assert result["totals"]["roster_count"] == result["totals"]["presearch_rejected_count"] == 303
    assert result["episode_diagnostics"] == []


def test_v2_refuses_crossed_noise_timeframe_domain_roster(tmp_path, monkeypatch):
    def crossed(case):
        case["request"]["domain_roster_sha256"] = _sha("crossed synthetic domain")
    # The public preimage publisher must refuse before any tape is sealed.
    with pytest.raises(tape.SignalOpportunityTapeError) as found:
        _publish_v2(tmp_path / "spool", monkeypatch, crossed, noise303=True)
    assert found.value.code == "TAPE_SOURCE_EDITION_BINDING_INVALID"
