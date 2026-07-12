"""Tuning-only calibration of the funded total initial-stop risk ceiling.

The cap is selected on the tuning-validation partition, frozen in a sealed
artifact, and may only be accepted or rejected by the one-shot final lockbox.
Final-lockbox results are never inputs to this selector.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, is_dataclass, replace
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

import pandas as pd


CAP_CALIBRATION_SCHEMA_VERSION = "2026-07-11-tuning-cap-calibration-v3"
CAP_CALIBRATION_SEAL_ALGORITHM = "sha256-canonical-json-v1"
CAP_SWEEP_PCT = tuple(value / 4.0 for value in range(12, 361))
CAP_SELECTION_RULE = "highest_feasible_cap_from_complete_0_25pct_aligned_3_to_90_pct_sweep"


def canonical_json_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def cap_neutral_config_sha256(config_payload: Mapping[str, Any]) -> str:
    """Hash calibration inputs without creating a selected-cap circularity."""
    neutral = json.loads(json.dumps(dict(config_payload), default=str))
    pipeline = neutral.get("pipeline") if isinstance(neutral.get("pipeline"), dict) else {}
    pipeline["max_total_open_risk_pct"] = "SELECTED_BY_TUNING_CAP_ARTIFACT"
    # The immutable roster is created before these config pointers can be
    # filled.  Its semantic/file hashes are bound explicitly in the artifact
    # lineage, so neutralize only the later pointer fields here.
    pipeline["production_prelockbox_roster_path"] = ""
    pipeline["production_prelockbox_roster_sha256"] = ""
    pipeline["production_cap_calibration_path"] = ""
    pipeline["production_cap_calibration_sha256"] = ""
    # The final replay is generated only after the cap has been frozen, so its
    # output pointer cannot be an input to the earlier tuning calibration.
    pipeline["production_portfolio_replay_evidence_path"] = ""
    # These are post-verdict release pointers/controls.  Their exact artifacts
    # are verified independently; including their paths here would make the
    # earlier Phase-A roster self-referential and impossible to pin later.
    pipeline["winner_ledger_path"] = "RELEASED_LEDGER_POINTER"
    pipeline["production_promotion_authorization_path"] = ""
    pipeline["production_promotion_authorization_sha256"] = ""
    pipeline["manual_new_entries_enabled"] = "OPERATOR_RELEASE_CONTROL"
    neutral["pipeline"] = pipeline
    return canonical_json_sha256(neutral)


def cap_neutral_policy_contract_sha256(policies: Mapping[float, Any]) -> str:
    """Hash every replay policy while neutralizing only the selected total cap."""
    contract = []
    for balance, policy in sorted(policies.items(), key=lambda item: float(item[0])):
        if is_dataclass(policy):
            payload = asdict(policy)
        elif isinstance(policy, Mapping):
            payload = dict(policy)
        else:
            raise TypeError("cap-calibration policies must be dataclasses or mappings")
        payload["max_open_risk_pct"] = "SELECTED_BY_TUNING_CAP_ARTIFACT"
        contract.append(
            {
                "starting_balance": float(balance),
                "policy": payload,
            }
        )
    return canonical_json_sha256(contract)


def seal_cap_calibration(payload: Mapping[str, Any]) -> Dict[str, Any]:
    record = json.loads(json.dumps(dict(payload), default=str))
    record.pop("content_seal", None)
    record["schema_version"] = CAP_CALIBRATION_SCHEMA_VERSION
    digest = canonical_json_sha256(record)
    record["content_seal"] = {
        "algorithm": CAP_CALIBRATION_SEAL_ALGORITHM,
        "sha256": digest,
    }
    return record


def cap_calibration_seal_valid(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    seal = payload.get("content_seal")
    if not isinstance(seal, Mapping):
        return False
    if str(seal.get("algorithm") or "") != CAP_CALIBRATION_SEAL_ALGORITHM:
        return False
    claimed = str(seal.get("sha256") or "").lower()
    unsigned = json.loads(json.dumps(dict(payload), default=str))
    unsigned.pop("content_seal", None)
    return len(claimed) == 64 and claimed == canonical_json_sha256(unsigned)


@dataclass(frozen=True)
class CapCalibrationConstraints:
    minimum_calendar_days: int = 730
    minimum_accepted_positions: int = 100
    minimum_bootstrap_daily_return_observations: int = 500
    minimum_profit_factor: float = 1.05
    minimum_return_pct: float = 0.0
    maximum_mtm_drawdown_pct: float = 20.0
    maximum_initial_stop_stress_drawdown_pct: float = 20.0
    maximum_bootstrap_ruin_upper_95pct: float = 1.0
    maximum_bootstrap_p95_drawdown_pct: float = 20.0
    minimum_mtm_coverage_pct: float = 100.0
    minimum_cap_utilization_fraction: float = 0.80
    initial_stop_gap_multiple: float = 2.5
    maximum_gap_stress_loss_pct: float = 20.0

    def validate(self) -> None:
        numeric_values = asdict(self)
        if any(
            not math.isfinite(float(value))
            for value in numeric_values.values()
        ):
            raise ValueError("cap calibration constraints must all be finite")
        if self.minimum_calendar_days < 730:
            raise ValueError("cap calibration requires at least 730 tuning calendar days")
        if self.minimum_accepted_positions < 100:
            raise ValueError("cap calibration requires at least 100 accepted positions")
        if self.minimum_bootstrap_daily_return_observations < 500:
            raise ValueError(
                "cap calibration requires at least 500 bootstrap daily-return observations"
            )
        if self.minimum_profit_factor < 1.05:
            raise ValueError("cap calibration profit-factor floor cannot be below 1.05")
        if self.minimum_return_pct < 0.0:
            raise ValueError("cap calibration return floor cannot be negative")
        if not (0.0 < self.maximum_mtm_drawdown_pct <= 20.0):
            raise ValueError("cap calibration MTM drawdown cap must be in (0, 20]")
        if not (0.0 < self.maximum_initial_stop_stress_drawdown_pct <= 20.0):
            raise ValueError("cap calibration stop-stress drawdown cap must be in (0, 20]")
        if not (0.0 <= self.maximum_bootstrap_ruin_upper_95pct <= 1.0):
            raise ValueError(
                "bootstrap ruin Wilson upper bound cannot exceed 1 percentage point"
            )
        if not (0.0 < self.maximum_bootstrap_p95_drawdown_pct <= 20.0):
            raise ValueError("bootstrap P95 drawdown cap must be in (0, 20]")
        if self.minimum_mtm_coverage_pct < 100.0:
            raise ValueError("cap calibration requires complete MTM candidate coverage")
        if not (0.80 <= self.minimum_cap_utilization_fraction <= 1.0):
            raise ValueError("cap utilization fraction must be in [0.80, 1.0]")
        if self.initial_stop_gap_multiple < 2.5:
            raise ValueError("initial-stop gap multiple cannot be below 2.5x")
        if not (
            0.0 < self.maximum_gap_stress_loss_pct <= 20.0
            and self.maximum_gap_stress_loss_pct
            <= self.maximum_initial_stop_stress_drawdown_pct
        ):
            raise ValueError(
                "gap-stress loss cap must be positive, no greater than 20%, "
                "and no weaker than the initial-stop drawdown cap"
            )


def _finite(value: Any, default: float = float("nan")) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if math.isfinite(number) else default


def _scenario_failures(
    summary: Mapping[str, Any],
    *,
    cap_pct: float,
    constraints: CapCalibrationConstraints,
) -> list[str]:
    from pm_portfolio_replay import PORTFOLIO_REPLAY_SCHEMA_VERSION

    failures: list[str] = []
    if (
        cap_pct * constraints.initial_stop_gap_multiple
        > constraints.maximum_gap_stress_loss_pct + 1e-12
    ):
        failures.append("DETERMINISTIC_GAP_STRESS_TOO_HIGH")
    if str(summary.get("schema_version") or "") != PORTFOLIO_REPLAY_SCHEMA_VERSION:
        failures.append("SUMMARY_SCHEMA_INVALID")
    if int(_finite(summary.get("accepted_positions"), 0.0)) < constraints.minimum_accepted_positions:
        failures.append("ACCEPTED_POSITIONS_TOO_LOW")
    profit_factor = summary.get("profit_factor")
    pf = float("inf") if str(profit_factor).lower() == "inf" else _finite(profit_factor)
    if not math.isfinite(pf) and pf != float("inf"):
        failures.append("PROFIT_FACTOR_UNAVAILABLE")
    elif pf < constraints.minimum_profit_factor:
        failures.append("PROFIT_FACTOR_TOO_LOW")
    return_pct = _finite(summary.get("return_pct"))
    if not math.isfinite(return_pct):
        failures.append("RETURN_UNAVAILABLE")
    elif return_pct < constraints.minimum_return_pct:
        failures.append("RETURN_TOO_LOW")
    mtm_drawdown = _finite(summary.get("max_mark_to_market_drawdown_pct"))
    if not math.isfinite(mtm_drawdown):
        failures.append("MTM_DRAWDOWN_UNAVAILABLE")
    elif mtm_drawdown > constraints.maximum_mtm_drawdown_pct:
        failures.append("MTM_DRAWDOWN_TOO_HIGH")
    stop_stress_drawdown = _finite(
        summary.get("max_initial_stop_stress_drawdown_pct")
    )
    if not math.isfinite(stop_stress_drawdown):
        failures.append("INITIAL_STOP_STRESS_DRAWDOWN_UNAVAILABLE")
    elif stop_stress_drawdown > constraints.maximum_initial_stop_stress_drawdown_pct:
        failures.append("INITIAL_STOP_STRESS_DRAWDOWN_TOO_HIGH")
    mtm_coverage = _finite(summary.get("mtm_candidate_coverage_pct"))
    if not math.isfinite(mtm_coverage):
        failures.append("MTM_COVERAGE_UNAVAILABLE")
    elif mtm_coverage < constraints.minimum_mtm_coverage_pct:
        failures.append("MTM_COVERAGE_INCOMPLETE")
    if bool(summary.get("observed_ruin", True)):
        failures.append("OBSERVED_RUIN")
    bootstrap = summary.get("exploratory_sequence_bootstrap")
    if not isinstance(bootstrap, Mapping):
        failures.append("BOOTSTRAP_RUIN_UNAVAILABLE")
    else:
        daily_observations = _finite(
            bootstrap.get("daily_return_observations"), float("nan")
        )
        if not (
            math.isfinite(daily_observations)
            and daily_observations.is_integer()
        ):
            failures.append("BOOTSTRAP_DAILY_OBSERVATIONS_UNAVAILABLE")
        elif (
            daily_observations
            < constraints.minimum_bootstrap_daily_return_observations
        ):
            failures.append("BOOTSTRAP_DAILY_OBSERVATIONS_TOO_LOW")
        interval = bootstrap.get("ruin_probability_wilson_95pct")
        lower = (
            _finite(interval[0])
            if isinstance(interval, (list, tuple)) and len(interval) == 2
            else float("nan")
        )
        upper = (
            _finite(interval[1])
            if isinstance(interval, (list, tuple)) and len(interval) == 2
            else float("nan")
        )
        if not (
            math.isfinite(lower)
            and math.isfinite(upper)
            and 0.0 <= lower <= upper <= 100.0
        ):
            failures.append("BOOTSTRAP_RUIN_UNAVAILABLE")
        elif upper > constraints.maximum_bootstrap_ruin_upper_95pct:
            failures.append("BOOTSTRAP_RUIN_TOO_HIGH")
        bootstrap_p95_drawdown = _finite(bootstrap.get("p95_max_drawdown_pct"))
        if not (
            math.isfinite(bootstrap_p95_drawdown)
            and 0.0 <= bootstrap_p95_drawdown <= 100.0
        ):
            failures.append("BOOTSTRAP_P95_DRAWDOWN_UNAVAILABLE")
        elif bootstrap_p95_drawdown > constraints.maximum_bootstrap_p95_drawdown_pct:
            failures.append("BOOTSTRAP_P95_DRAWDOWN_TOO_HIGH")
    # The funded cap is enforced only at chronological admission and every
    # exposure is expressed on min(MTM equity, balance - open stop loss).
    # A later MTM move can raise pathwise risk percentages without any new
    # allocation decision, so neither fresh-equity path risk nor post-entry
    # drift identifies which cap was actually exercised by the allocator.
    observed_cap_use = _finite(
        summary.get("max_admission_total_stop_risk_pct")
    )
    if not math.isfinite(observed_cap_use):
        failures.append("CAP_UTILIZATION_UNAVAILABLE")
    elif observed_cap_use > cap_pct + 1e-9:
        failures.append("CAP_BREACHED")
    elif observed_cap_use + 1e-9 < cap_pct * constraints.minimum_cap_utilization_fraction:
        failures.append("CAP_NOT_EMPIRICALLY_EXERCISED")
    return failures


def calibrate_total_stop_risk_cap(
    *,
    candidates: Iterable[Any],
    base_policies: Mapping[float, Any],
    evaluation_start: Any,
    evaluation_end: Any,
    lineage: Mapping[str, Any],
    constraints: CapCalibrationConstraints,
    correlation_lookup: Optional[Callable[[str, str, pd.Timestamp], Optional[float]]] = None,
) -> Dict[str, Any]:
    """Sweep 3–90% in 0.25% steps on tuning and seal the highest feasible cap."""
    from pm_portfolio_replay import SynchronizedPortfolioSimulator

    constraints.validate()
    start, end = pd.Timestamp(evaluation_start), pd.Timestamp(evaluation_end)
    if start.tzinfo is not None:
        start = start.tz_convert("UTC").tz_localize(None)
    if end.tzinfo is not None:
        end = end.tz_convert("UTC").tz_localize(None)
    if end <= start:
        raise ValueError("tuning calibration window must have positive duration")
    if (end - start).days < constraints.minimum_calendar_days:
        raise ValueError("tuning calibration window is shorter than the constraint")
    if not base_policies:
        raise ValueError("at least one starting-balance policy is required")

    normalized_candidates = tuple(candidate.normalized() for candidate in candidates)
    if not normalized_candidates:
        raise ValueError("tuning calibration requires candidate positions")
    for candidate in normalized_candidates:
        final_exit = max(leg.exit_time for leg in candidate.legs)
        if (
            candidate.entry_time < start
            or candidate.entry_time >= end
            or final_exit >= end
        ):
            raise ValueError(
                f"candidate {candidate.candidate_id} escapes the tuning-validation window"
            )

    candidate_contract_sha = canonical_json_sha256(
        [asdict(candidate) for candidate in normalized_candidates]
    )
    policy_contract_sha = cap_neutral_policy_contract_sha256(base_policies)
    required_balances = sorted(float(balance) for balance in base_policies)
    if any(not math.isfinite(balance) or balance <= 0.0 for balance in required_balances):
        raise ValueError("starting-balance policies require finite positive keys")
    cap_records: list[Dict[str, Any]] = []
    feasible_caps: list[float] = []
    for cap_pct in CAP_SWEEP_PCT:
        scenarios: list[Dict[str, Any]] = []
        cap_failures: list[str] = []
        for balance, base_policy in sorted(base_policies.items(), key=lambda item: float(item[0])):
            policy = replace(base_policy, max_open_risk_pct=cap_pct)
            result = SynchronizedPortfolioSimulator(policy, correlation_lookup).run(
                normalized_candidates
            )
            summary = result["summary"]
            failures = _scenario_failures(
                summary,
                cap_pct=cap_pct,
                constraints=constraints,
            )
            cap_failures.extend(f"{float(balance):.2f}:{failure}" for failure in failures)
            scenarios.append(
                {
                    "starting_balance": float(balance),
                    "policy": result["policy"],
                    "summary": summary,
                    "failures": failures,
                }
            )
        feasible = not cap_failures
        if feasible:
            feasible_caps.append(cap_pct)
        cap_records.append(
            {
                "cap_pct": cap_pct,
                "feasible": feasible,
                "failures": sorted(set(cap_failures)),
                "scenarios": scenarios,
            }
        )

    selected = max(feasible_caps) if feasible_caps else None
    artifact = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "partition": "tuning_validation",
        "selection_rule": CAP_SELECTION_RULE,
        "profitability_guarantee": False,
        "places_trades": False,
        "window": {
            "evaluation_start": start.isoformat(),
            "evaluation_end": end.isoformat(),
            "calendar_days": int((end - start).days),
        },
        "lineage": {
            **dict(lineage),
            "candidate_contract_sha256": candidate_contract_sha,
            "base_policy_contract_sha256": policy_contract_sha,
            "required_starting_balances": required_balances,
        },
        "constraints": asdict(constraints),
        "sweep_caps_pct": list(CAP_SWEEP_PCT),
        "cap_records": cap_records,
        "selection": {
            "status": "PASS" if selected is not None else "NO_FEASIBLE_CAP",
            "selected_max_total_stop_risk_pct": selected,
            "feasible_caps_pct": feasible_caps,
            "provisional_previous_cap_pct": 8.0,
        },
        "limitations": [
            "the selected cap is conditional on tuning-period candidates, costs, broker specifications and dependence",
            "winner and cap selection share the tuning surface and are therefore selection-contaminated by design",
            "the untouched final lockbox may reject but must never select or increase the cap",
            "a cap not reached to at least the configured utilization fraction is not empirically identified",
            "the cap must also survive the predeclared 2.5x aggregate initial-stop gap shock within the 20% loss budget",
            "offline margin and bar-close MTM can understate intrabar or gap losses",
        ],
    }
    return seal_cap_calibration(artifact)


def validate_cap_calibration_artifact(
    artifact: Any,
    *,
    expected_lineage: Mapping[str, Any],
    expected_selected_cap_pct: float,
) -> list[str]:
    """Verify partition, full sweep, constraints, lineage and highest-cap rule."""
    errors: list[str] = []
    if not isinstance(artifact, Mapping):
        return ["CAP_CALIBRATION_UNREADABLE"]
    if str(artifact.get("schema_version") or "") != CAP_CALIBRATION_SCHEMA_VERSION:
        errors.append("CAP_CALIBRATION_SCHEMA_MISMATCH")
    if not cap_calibration_seal_valid(artifact):
        errors.append("CAP_CALIBRATION_SEAL_INVALID")
    if str(artifact.get("partition") or "") != "tuning_validation":
        errors.append("CAP_CALIBRATION_NOT_TUNING_ONLY")
    if str(artifact.get("selection_rule") or "") != CAP_SELECTION_RULE:
        errors.append("CAP_CALIBRATION_SELECTION_RULE_INVALID")
    if list(artifact.get("sweep_caps_pct") or []) != list(CAP_SWEEP_PCT):
        errors.append("CAP_CALIBRATION_SWEEP_INCOMPLETE")

    constraints_raw = artifact.get("constraints")
    try:
        constraints = CapCalibrationConstraints(**dict(constraints_raw or {}))
        constraints.validate()
    except Exception:
        constraints = None
        errors.append("CAP_CALIBRATION_CONSTRAINTS_TOO_WEAK")

    lineage = artifact.get("lineage") if isinstance(artifact.get("lineage"), Mapping) else {}
    for key, expected in expected_lineage.items():
        if canonical_json_sha256(lineage.get(key)) != canonical_json_sha256(expected):
            errors.append(f"CAP_CALIBRATION_LINEAGE_MISMATCH:{key}")

    window = artifact.get("window") if isinstance(artifact.get("window"), Mapping) else {}
    try:
        window_start = pd.Timestamp(window.get("evaluation_start"))
        window_end = pd.Timestamp(window.get("evaluation_end"))
        if window_start.tzinfo is not None:
            window_start = window_start.tz_convert("UTC").tz_localize(None)
        if window_end.tzinfo is not None:
            window_end = window_end.tz_convert("UTC").tz_localize(None)
        calendar_days = int((window_end - window_start).days)
        if window_end <= window_start or int(window.get("calendar_days", -1)) != calendar_days:
            raise ValueError("inconsistent window")
        if constraints is not None and calendar_days < constraints.minimum_calendar_days:
            errors.append("CAP_CALIBRATION_WINDOW_TOO_SHORT")
    except Exception:
        errors.append("CAP_CALIBRATION_WINDOW_INVALID")

    raw_required_balances = lineage.get("required_starting_balances")
    try:
        required_balances = sorted(float(value) for value in raw_required_balances)
        if not required_balances or any(
            not math.isfinite(value) or value <= 0.0 for value in required_balances
        ):
            raise ValueError("invalid balances")
    except Exception:
        required_balances = []
        errors.append("CAP_CALIBRATION_REQUIRED_BALANCES_INVALID")
    expected_policy_contract = str(lineage.get("base_policy_contract_sha256") or "")
    if len(expected_policy_contract) != 64:
        errors.append("CAP_CALIBRATION_POLICY_CONTRACT_MISSING")

    records = artifact.get("cap_records") if isinstance(artifact.get("cap_records"), list) else []
    if len(records) != len(CAP_SWEEP_PCT):
        errors.append("CAP_CALIBRATION_RECORD_COUNT_INVALID")
    recomputed_feasible: list[float] = []
    if constraints is not None and len(records) == len(CAP_SWEEP_PCT):
        for expected_cap, record in zip(CAP_SWEEP_PCT, records):
            if not isinstance(record, Mapping) or _finite(record.get("cap_pct")) != expected_cap:
                errors.append("CAP_CALIBRATION_RECORD_ORDER_INVALID")
                break
            scenario_failures: list[str] = []
            cap_failure_labels: list[str] = []
            scenarios = record.get("scenarios") if isinstance(record.get("scenarios"), list) else []
            if not scenarios:
                scenario_failures.append("SCENARIOS_MISSING")
            scenario_policies: Dict[float, Mapping[str, Any]] = {}
            for scenario in scenarios:
                summary = scenario.get("summary") if isinstance(scenario, Mapping) else {}
                scenario_balance = _finite(
                    scenario.get("starting_balance") if isinstance(scenario, Mapping) else None
                )
                policy = scenario.get("policy") if isinstance(scenario, Mapping) else None
                summary_starting_balance = _finite(
                    summary.get("starting_balance") if isinstance(summary, Mapping) else None
                )
                if (
                    not math.isfinite(scenario_balance)
                    or scenario_balance <= 0.0
                    or not isinstance(policy, Mapping)
                    or not math.isfinite(_finite(policy.get("starting_balance")))
                    or abs(_finite(policy.get("starting_balance")) - scenario_balance) > 1e-6
                    or not math.isfinite(_finite(policy.get("max_open_risk_pct")))
                    or abs(_finite(policy.get("max_open_risk_pct")) - expected_cap) > 1e-12
                    or not math.isfinite(summary_starting_balance)
                    or abs(summary_starting_balance - scenario_balance) > 1e-6
                    or scenario_balance in scenario_policies
                ):
                    scenario_failures.append("SCENARIO_POLICY_INVALID")
                else:
                    scenario_policies[scenario_balance] = policy
                computed_scenario_failures = _scenario_failures(
                    summary,
                    cap_pct=expected_cap,
                    constraints=constraints,
                )
                scenario_failures.extend(computed_scenario_failures)
                cap_failure_labels.extend(
                    f"{scenario_balance:.2f}:{failure}"
                    for failure in computed_scenario_failures
                )
                stored_scenario_failures = (
                    scenario.get("failures")
                    if isinstance(scenario, Mapping)
                    and isinstance(scenario.get("failures"), list)
                    else []
                )
                if sorted(set(stored_scenario_failures)) != sorted(
                    set(computed_scenario_failures)
                ):
                    errors.append(
                        "CAP_CALIBRATION_SCENARIO_FAILURES_MISMATCH:"
                        f"{expected_cap:.2f}:{scenario_balance:.2f}"
                    )
            if sorted(scenario_policies) != required_balances:
                scenario_failures.append("REQUIRED_BALANCE_SCENARIOS_MISMATCH")
            elif (
                len(expected_policy_contract) == 64
                and cap_neutral_policy_contract_sha256(scenario_policies)
                != expected_policy_contract
            ):
                scenario_failures.append("BASE_POLICY_CONTRACT_MISMATCH")
            feasible = not scenario_failures
            stored_cap_failures = (
                record.get("failures")
                if isinstance(record.get("failures"), list)
                else []
            )
            if sorted(set(stored_cap_failures)) != sorted(set(cap_failure_labels)):
                errors.append(
                    f"CAP_CALIBRATION_CAP_FAILURES_MISMATCH:{expected_cap:.2f}"
                )
            if bool(record.get("feasible")) != feasible:
                errors.append(f"CAP_CALIBRATION_FEASIBILITY_MISMATCH:{expected_cap:.2f}")
            if feasible:
                recomputed_feasible.append(expected_cap)

    selection = artifact.get("selection") if isinstance(artifact.get("selection"), Mapping) else {}
    selected = _finite(selection.get("selected_max_total_stop_risk_pct"))
    recomputed_selected = max(recomputed_feasible) if recomputed_feasible else float("nan")
    stored_feasible_caps = selection.get("feasible_caps_pct")
    if not isinstance(stored_feasible_caps, list) or stored_feasible_caps != recomputed_feasible:
        errors.append("CAP_CALIBRATION_FEASIBLE_SET_MISMATCH")
    if not math.isfinite(recomputed_selected):
        errors.append("CAP_CALIBRATION_NO_FEASIBLE_CAP")
        if str(selection.get("status") or "") != "NO_FEASIBLE_CAP":
            errors.append("CAP_CALIBRATION_SELECTION_STATUS_INVALID")
    elif selected != recomputed_selected:
        errors.append("CAP_CALIBRATION_NOT_HIGHEST_FEASIBLE")
    elif str(selection.get("status") or "") != "PASS":
        errors.append("CAP_CALIBRATION_SELECTION_STATUS_INVALID")
    if not math.isfinite(selected) or abs(selected - float(expected_selected_cap_pct)) > 1e-12:
        errors.append("CAP_CALIBRATION_CONFIG_CAP_MISMATCH")
    return list(dict.fromkeys(errors))


def write_immutable_cap_calibration(directory: Path, artifact: Mapping[str, Any]) -> Path:
    sealed = seal_cap_calibration(artifact)
    digest = str((sealed.get("content_seal") or {}).get("sha256") or "")
    if len(digest) != 64:
        raise ValueError("cap calibration seal unavailable")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"cap_calibration_{digest}.json"
    encoded = json.dumps(sealed, indent=2, sort_keys=True, default=str)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise RuntimeError("content-addressed cap calibration collision")
        return path
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(encoded, encoding="utf-8")
    temp.replace(path)
    return path
