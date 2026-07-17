"""Bounded V8 capital evidence adapter for synchronized replay outputs."""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import fields
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional

import pm_cap_calibration as cap
from pm_portfolio_replay import (
    LEGACY_PORTFOLIO_REPLAY_SCHEMA_VERSION,
    MAX_PORTFOLIO_REPLAY_CANDIDATES,
    PORTFOLIO_REPLAY_INPUT_RECEIPT_VERSION,
    PORTFOLIO_REPLAY_PATH_CONTRACT_VERSION,
    PORTFOLIO_REPLAY_SCHEMA_VERSION,
    PortfolioPolicy,
    canonical_json_sha256 as replay_json_sha256,
    portfolio_replay_candidate_ids_sha256,
)


CAP_REPLAY_OUTPUT_BINDING_VERSION = (
    "fxpm.capital-envelope-synchronized-output-binding.v1"
)
CAP_REPLAY_PRODUCER_VERSION = "fxpm.capital-envelope-replay-producer.v1"
MAX_REPLAY_OUTPUT_COMPONENT_BYTES = 512 * 1024 * 1024
MAX_REPLAY_PATH_ROWS = 2_000_000
FLAT_MARGIN_LEVEL_PCT = cap.FLAT_MARGIN_LEVEL_PCT

_RESULT_KEYS = frozenset(
    {
        "input_receipt",
        "summary",
        "policy",
        "closed_positions",
        "blocked_positions",
        "path",
    }
)
_INPUT_RECEIPT_KEYS = frozenset(
    {
        "schema_version",
        "candidate_count",
        "candidate_ids_sha256",
        "candidate_roster_sha256",
        "candidate_roster_hash_algorithm",
        "policy_sha256",
        "execution_authority_mode",
        "path_contract_version",
    }
)
_FROZEN_ROSTER_KEYS = frozenset(
    {
        "schema_version",
        "candidate_count",
        "candidate_ids_sha256",
        "candidate_roster_sha256",
        "candidate_roster_hash_algorithm",
    }
)
_PATH_KEYS = frozenset(
    {
        "timestamp",
        "balance",
        "unrealized_pnl",
        "mtm_equity",
        "stop_equity",
        "open_risk",
        "open_risk_pct",
        "conservative_open_risk_pct",
        "risk_capital",
        "total_stop_risk_pct",
        "factor_stop_risk_pct",
        "same_symbol_stop_risk_pct",
        "margin_used",
        "margin_utilization_pct",
        "margin_level_pct",
        "free_margin",
        "open_positions",
        "admission_events",
        "realized_pnl_at_timestamp",
    }
)
_CALIBRATED_REPLAY_POLICY_FIELDS = frozenset(
    {
        "max_open_risk_pct",
        "max_factor_group_open_risk_pct",
        "max_correlated_cluster_risk_pct",
        "max_same_symbol_open_risk_pct",
        "max_margin_utilization_pct",
        "min_margin_level_pct",
        "min_free_margin_pct",
    }
)
_CONTRACT_LINEAGE_KEYS = frozenset(
    {
        "book_contract_version",
        "refit_contract_version",
        "cost_contract_sha256",
        "native_source_contract_sha256",
        "replay_contract_sha256",
        "margin_contract_sha256",
    }
)
_POLICY_FIELDS = frozenset(item.name for item in fields(PortfolioPolicy))


def _sha(value: Any) -> str:
    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    digest = hashlib.sha256()
    size = 0
    try:
        for chunk in encoder.iterencode(value):
            encoded = chunk.encode("utf-8")
            size += len(encoded)
            if size > MAX_REPLAY_OUTPUT_COMPONENT_BYTES:
                raise ValueError("replay output component exceeds its byte bound")
            digest.update(encoded)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError("replay output is not bounded canonical JSON") from exc
    return digest.hexdigest()


def _number(value: Any, name: str) -> float:
    if type(value) not in {int, float}:
        raise ValueError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _date_window(value: Any) -> tuple[dict[str, str], date, date]:
    if type(value) is not dict or set(value) != {"start_date", "end_date"}:
        raise ValueError("capital attempt window schema is invalid")
    try:
        start = date.fromisoformat(value["start_date"])
        end = date.fromisoformat(value["end_date"])
    except (TypeError, ValueError) as exc:
        raise ValueError("capital attempt window is not canonical") from exc
    if value != {"start_date": start.isoformat(), "end_date": end.isoformat()}:
        raise ValueError("capital attempt window is not canonical")
    if end <= start:
        raise ValueError("capital attempt window is empty")
    return dict(value), start, end


def _timestamp(value: Any) -> datetime:
    if type(value) is not str:
        raise ValueError("replay timestamp must be a string")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("replay timestamp is invalid") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _close(left: float, right: float) -> bool:
    return abs(left - right) <= max(2e-6, 2e-8 * max(abs(left), abs(right)))


def _policy_point(value: Any) -> dict[str, Any]:
    try:
        return cap._policy(value)
    except cap._Blocked as exc:
        raise ValueError(str(exc)) from exc


def _lineage_without_output(value: Any) -> dict[str, Any]:
    try:
        return cap._current_lineage(value)
    except cap._Blocked as exc:
        raise ValueError(str(exc)) from exc


def _frozen_roster(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _FROZEN_ROSTER_KEYS:
        raise ValueError("frozen replay candidate roster schema is invalid")
    result = dict(value)
    if result["schema_version"] != PORTFOLIO_REPLAY_INPUT_RECEIPT_VERSION:
        raise ValueError("frozen replay candidate roster version is invalid")
    count = result["candidate_count"]
    if type(count) is not int or not 1 <= count <= MAX_PORTFOLIO_REPLAY_CANDIDATES:
        raise ValueError("frozen replay candidate count is invalid")
    if not _is_sha256(result["candidate_ids_sha256"]) or not _is_sha256(
        result["candidate_roster_sha256"]
    ):
        raise ValueError("frozen replay candidate hashes are invalid")
    if result["candidate_roster_hash_algorithm"] != (
        "sha256-domain-length-framed-canonical-record-sha256-v1"
    ):
        raise ValueError("frozen replay candidate hash algorithm is invalid")
    return result


def capital_policy_for_point(
    base_policy: PortfolioPolicy,
    policy_point: Mapping[str, Any],
) -> PortfolioPolicy:
    """Apply only precommitted capital axes to one frozen replay policy."""

    if type(base_policy) is not PortfolioPolicy:
        raise ValueError("exact PortfolioPolicy base is required")
    point = _policy_point(policy_point)
    payload = {
        item.name: getattr(base_policy, item.name) for item in fields(PortfolioPolicy)
    }
    payload.update(
        {
            "max_open_risk_pct": point["max_total_stop_risk_pct"],
            "max_factor_group_open_risk_pct": point[
                "max_factor_stop_risk_pct"
            ],
            "max_correlated_cluster_risk_pct": point[
                "max_factor_stop_risk_pct"
            ],
            "max_same_symbol_open_risk_pct": point[
                "max_same_symbol_stop_risk_pct"
            ],
            "max_margin_utilization_pct": point["max_margin_utilization_pct"],
            "min_margin_level_pct": point["min_margin_level_pct"],
            "min_free_margin_pct": point["min_free_margin_pct"],
        }
    )
    result = PortfolioPolicy(**payload)
    result.validate()
    return result


def _validate_policy(result_policy: Any, point: Mapping[str, Any]) -> dict[str, Any]:
    if type(result_policy) is not dict or set(result_policy) != _POLICY_FIELDS:
        raise ValueError("synchronized replay policy schema is invalid")
    policy = dict(result_policy)
    expected = {
        "max_open_risk_pct": point["max_total_stop_risk_pct"],
        "max_factor_group_open_risk_pct": point["max_factor_stop_risk_pct"],
        "max_correlated_cluster_risk_pct": point["max_factor_stop_risk_pct"],
        "max_same_symbol_open_risk_pct": point[
            "max_same_symbol_stop_risk_pct"
        ],
        "max_margin_utilization_pct": point["max_margin_utilization_pct"],
        "min_margin_level_pct": point["min_margin_level_pct"],
        "min_free_margin_pct": point["min_free_margin_pct"],
    }
    for name, expected_value in expected.items():
        if abs(_number(policy[name], f"replay policy {name}") - expected_value) > 1e-9:
            raise ValueError(f"synchronized replay policy axis mismatch: {name}")
    if replay_json_sha256(policy) != replay_json_sha256(result_policy):
        raise ValueError("synchronized replay policy is not stable JSON")
    return policy


def _daily_observations(
    path: Any,
    *,
    window: Mapping[str, str],
    policy: Mapping[str, Any],
    policy_point: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    if type(path) is not list or not 1 <= len(path) <= MAX_REPLAY_PATH_ROWS:
        raise ValueError("synchronized replay path is outside its bound")
    _, start, end = _date_window(window)
    rows_by_date: dict[date, list[dict[str, Any]]] = {}
    previous_stamp: Optional[datetime] = None
    for raw in path:
        if type(raw) is not dict or set(raw) != _PATH_KEYS:
            raise ValueError("synchronized replay path row schema is invalid")
        stamp = _timestamp(raw["timestamp"])
        if previous_stamp is not None and stamp <= previous_stamp:
            raise ValueError("synchronized replay path is reordered or duplicated")
        previous_stamp = stamp
        if not start <= stamp.date() < end:
            raise ValueError("synchronized replay path escapes its attempt window")
        total = _number(raw["total_stop_risk_pct"], "total stop risk")
        factor = _number(raw["factor_stop_risk_pct"], "factor stop risk")
        symbol = _number(raw["same_symbol_stop_risk_pct"], "symbol stop risk")
        margin = _number(raw["margin_utilization_pct"], "margin utilization")
        risk_capital = _number(raw["risk_capital"], "risk capital")
        equity = _number(raw["mtm_equity"], "MTM equity")
        stop_equity = _number(raw["stop_equity"], "stop equity")
        balance = _number(raw["balance"], "balance")
        unrealized = _number(raw["unrealized_pnl"], "unrealized P&L")
        open_risk = _number(raw["open_risk"], "open risk")
        margin_used = _number(raw["margin_used"], "used margin")
        free_margin = _number(raw["free_margin"], "free margin")
        if min(total, factor, symbol, margin) < 0.0 or min(
            risk_capital, equity, stop_equity
        ) <= 0.0 or min(open_risk, margin_used) < 0.0:
            raise ValueError("synchronized replay path has invalid capital state")
        if not _close(equity, balance + unrealized):
            raise ValueError("synchronized replay MTM equity does not reconcile")
        if not _close(stop_equity, balance - open_risk):
            raise ValueError("synchronized replay stop equity does not reconcile")
        if not _close(risk_capital, min(equity, stop_equity)):
            raise ValueError("synchronized replay risk capital does not reconcile")
        if not _close(total, open_risk / risk_capital * 100.0):
            raise ValueError("synchronized replay total stop risk does not reconcile")
        expected_margin = margin_used / equity * 100.0
        if not _close(margin, expected_margin):
            raise ValueError("synchronized replay margin utilization does not reconcile")
        if not _close(free_margin, equity - margin_used):
            raise ValueError("synchronized replay free margin does not reconcile")
        raw_margin_level = raw["margin_level_pct"]
        if margin_used > 0.0:
            level = _number(raw_margin_level, "margin level")
            if not _close(level, 10_000.0 / margin):
                raise ValueError("synchronized replay margin level does not reconcile")
        elif raw_margin_level is not None:
            raise ValueError("flat synchronized replay margin level must be null")
        if symbol > factor + 1e-7 or factor > total + 1e-7:
            raise ValueError("synchronized replay stop-risk hierarchy is invalid")
        if total > policy_point["max_total_stop_risk_pct"] + 1e-7:
            raise ValueError("synchronized replay exceeds total policy")
        if factor > policy_point["max_factor_stop_risk_pct"] + 1e-7:
            raise ValueError("synchronized replay exceeds factor policy")
        if symbol > policy_point["max_same_symbol_stop_risk_pct"] + 1e-7:
            raise ValueError("synchronized replay exceeds symbol policy")
        if margin > policy_point["max_margin_utilization_pct"] + 1e-7:
            raise ValueError("synchronized replay exceeds margin policy")
        events = raw["admission_events"]
        if type(events) is not int or type(events) is bool or events < 0:
            raise ValueError("synchronized replay admission count is invalid")
        rows_by_date.setdefault(stamp.date(), []).append(raw)

    starting_balance = _number(policy["starting_balance"], "starting balance")
    prior_equity = starting_balance
    last_equity = starting_balance
    last_stop_equity = starting_balance
    peak_equity = starting_balance
    peak_stop_equity = starting_balance
    accepted = 0
    output: list[dict[str, Any]] = []
    cursor = start
    gap_multiple = policy_point["gap_stress_multiple"]
    while cursor < end:
        day_mtm_dd = max(0.0, (peak_equity - last_equity) / peak_equity * 100.0)
        day_stop_dd = max(
            0.0,
            (peak_stop_equity - last_stop_equity) / peak_stop_equity * 100.0,
        )
        total = factor = symbol = margin = 0.0
        admissions = 0
        for raw in rows_by_date.get(cursor, []):
            last_equity = _number(raw["mtm_equity"], "MTM equity")
            last_stop_equity = _number(raw["stop_equity"], "stop equity")
            peak_equity = max(peak_equity, last_equity)
            peak_stop_equity = max(peak_stop_equity, last_stop_equity)
            day_mtm_dd = max(
                day_mtm_dd,
                (peak_equity - last_equity) / peak_equity * 100.0,
            )
            day_stop_dd = max(
                day_stop_dd,
                (peak_stop_equity - last_stop_equity)
                / peak_stop_equity
                * 100.0,
            )
            total = max(total, float(raw["total_stop_risk_pct"]))
            factor = max(factor, float(raw["factor_stop_risk_pct"]))
            symbol = max(symbol, float(raw["same_symbol_stop_risk_pct"]))
            margin = max(margin, float(raw["margin_utilization_pct"]))
            admissions += int(raw["admission_events"])
        if last_equity <= 0.0 or prior_equity <= 0.0:
            raise ValueError("synchronized replay equity is non-positive")
        net_return = last_equity / prior_equity - 1.0
        if not -1.0 < net_return <= 10.0:
            raise ValueError("synchronized replay daily return is outside its bound")
        accepted += admissions
        output.append(
            {
                "date": cursor.isoformat(),
                "stress_net_return": net_return,
                "mtm_drawdown_pct": day_mtm_dd,
                "initial_stop_drawdown_pct": day_stop_dd,
                "total_stop_risk_pct": total,
                "factor_stop_risk_pct": factor,
                "same_symbol_stop_risk_pct": symbol,
                "joint_gap_loss_pct": total * gap_multiple,
                "factor_gap_loss_pct": factor * gap_multiple,
                "margin_utilization_pct": margin,
                "margin_level_pct": (
                    10_000.0 / margin if margin > 0.0 else FLAT_MARGIN_LEVEL_PCT
                ),
                "free_margin_pct": 100.0 - margin,
                "admission_events": admissions,
            }
        )
        prior_equity = last_equity
        cursor += timedelta(days=1)
    return output, accepted


def _write_binding(root: Path, binding: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        binding,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    digest = cap.canonical_json_sha256(binding)
    directory = root / "replay_output_receipts"
    directory.mkdir(parents=True, exist_ok=True)
    if cap._is_link_or_reparse(root) or cap._is_link_or_reparse(directory):
        raise ValueError("capital replay output receipt directory is unsafe")
    path = directory / f"replay_output_{digest}.json"
    if path.exists():
        if cap._is_link_or_reparse(path) or path.stat().st_nlink != 1:
            raise ValueError("capital replay output receipt path is unsafe")
        if path.read_bytes() != encoded:
            raise ValueError("immutable replay output receipt contains different bytes")
    else:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
            0o600,
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    return digest


def build_capital_v8_attempt_from_synchronized_output(
    replay_output: Mapping[str, Any],
    *,
    output_root: Path,
    anchor_id: str,
    phase: str,
    window: Mapping[str, str],
    policy_point: Mapping[str, Any],
    lineage: Mapping[str, Any],
    frozen_candidate_roster: Mapping[str, Any],
) -> dict[str, Any]:
    """Convert one canonical replay result into one complete V8 attempt."""

    if type(replay_output) is not dict or set(replay_output) != _RESULT_KEYS:
        raise ValueError("synchronized replay output schema is invalid")
    point = _policy_point(policy_point)
    window_copy, _, _ = _date_window(window)
    if anchor_id == "CURRENT":
        if phase != "CURRENT_SELECTION":
            raise ValueError("current capital replay phase is invalid")
    elif phase not in {"INNER_SELECTION", "OUTER_EVALUATION"}:
        raise ValueError("historical capital replay phase is invalid")
    if type(anchor_id) is not str or not anchor_id or "|" in anchor_id:
        raise ValueError("capital replay anchor id is invalid")

    summary = replay_output["summary"]
    if type(summary) is not dict:
        raise ValueError("synchronized replay summary schema is invalid")
    source_version = summary.get("schema_version")
    if source_version == LEGACY_PORTFOLIO_REPLAY_SCHEMA_VERSION:
        raise ValueError("LEGACY_V7_CAPITAL_REPLAY_DISABLED")
    if source_version != PORTFOLIO_REPLAY_SCHEMA_VERSION:
        raise ValueError("synchronized replay source version is invalid")
    if summary.get("capital_path_contract") != PORTFOLIO_REPLAY_PATH_CONTRACT_VERSION:
        raise ValueError("synchronized replay capital path contract is invalid")

    receipt = replay_output["input_receipt"]
    if type(receipt) is not dict or set(receipt) != _INPUT_RECEIPT_KEYS:
        raise ValueError("synchronized replay input receipt schema is invalid")
    frozen = _frozen_roster(frozen_candidate_roster)
    if any(receipt.get(key) != frozen[key] for key in _FROZEN_ROSTER_KEYS):
        raise ValueError("synchronized replay candidate roster was not frozen")
    if receipt["path_contract_version"] != PORTFOLIO_REPLAY_PATH_CONTRACT_VERSION:
        raise ValueError("synchronized replay input path contract is invalid")
    expected_mode = (
        "CURRENT_REFIT_REVERIFIED"
        if anchor_id == "CURRENT"
        else "STANDARD_SYNCHRONIZED_REPLAY"
    )
    if receipt["execution_authority_mode"] != expected_mode:
        raise ValueError("synchronized replay execution authority mode is invalid")
    if summary.get("candidate_roster_sha256") != frozen[
        "candidate_roster_sha256"
    ]:
        raise ValueError("synchronized replay summary roster mismatch")

    policy = _validate_policy(replay_output["policy"], point)
    if receipt["policy_sha256"] != replay_json_sha256(policy):
        raise ValueError("synchronized replay policy receipt mismatch")
    closed = replay_output["closed_positions"]
    blocked = replay_output["blocked_positions"]
    if type(closed) is not list or type(blocked) is not list:
        raise ValueError("synchronized replay disposition schema is invalid")
    if len(closed) + len(blocked) != frozen["candidate_count"]:
        raise ValueError("synchronized replay disposition roster is incomplete")
    identifiers: list[str] = []
    for row in [*closed, *blocked]:
        if type(row) is not dict or type(row.get("candidate_id")) is not str:
            raise ValueError("synchronized replay disposition identity is invalid")
        identifiers.append(row["candidate_id"])
    if portfolio_replay_candidate_ids_sha256(identifiers) != frozen[
        "candidate_ids_sha256"
    ]:
        raise ValueError("synchronized replay disposition roster mismatch")
    if summary.get("candidate_positions") != frozen["candidate_count"]:
        raise ValueError("synchronized replay summary candidate count mismatch")
    if summary.get("accepted_positions") != len(closed):
        raise ValueError("synchronized replay accepted count mismatch")
    if summary.get("blocked_positions") != len(blocked):
        raise ValueError("synchronized replay blocked count mismatch")

    daily, accepted = _daily_observations(
        replay_output["path"],
        window=window_copy,
        policy=policy,
        policy_point=point,
    )
    if accepted != len(closed):
        raise ValueError("synchronized replay daily admissions do not reconcile")
    attempt_id = f"{anchor_id}|{phase}|{point['policy_id']}"
    binding = {
        "schema_version": CAP_REPLAY_OUTPUT_BINDING_VERSION,
        "producer_version": CAP_REPLAY_PRODUCER_VERSION,
        "attempt_id": attempt_id,
        "window": window_copy,
        "policy_sha256": cap.canonical_json_sha256(point),
        "input_receipt_sha256": cap.canonical_json_sha256(receipt),
        "summary_sha256": _sha(summary),
        "closed_positions_sha256": _sha(closed),
        "blocked_positions_sha256": _sha(blocked),
        "path_sha256": _sha(replay_output["path"]),
        "daily_observations_sha256": cap.canonical_json_sha256(daily),
        "accepted_positions": accepted,
    }
    exact_lineage = _lineage_without_output(lineage)
    expected_role = (
        "PROCESS_VALIDATED_CURRENT_REFIT"
        if anchor_id == "CURRENT"
        else "ANCHOR_LOCAL_REFIT"
    )
    if exact_lineage["book_role"] != expected_role:
        raise ValueError("capital replay book role is invalid")
    replay_output_sha256 = _write_binding(Path(output_root), binding)
    exact_lineage["replay_output_sha256"] = replay_output_sha256
    attempt: dict[str, Any] = {
        "attempt_id": attempt_id,
        "anchor_id": anchor_id,
        "phase": phase,
        "policy_id": point["policy_id"],
        "policy_sha256": cap.canonical_json_sha256(point),
        "status": "COMPLETE",
        "failure_code": None,
        "window": window_copy,
        "lineage": exact_lineage,
        "daily_observations": daily,
        "accepted_positions": accepted,
    }
    attempt["attempt_sha256"] = cap.canonical_json_sha256(attempt)
    return attempt


def _canonical_utc(value: Any) -> tuple[str, datetime]:
    text = str(value or "").strip()
    probe = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(probe)
    except ValueError as exc:
        raise ValueError("capital replay generation time is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("capital replay generation time must be timezone-aware")
    parsed = parsed.astimezone(timezone.utc)
    canonical = parsed.isoformat().replace("+00:00", "Z")
    if text != canonical:
        raise ValueError("capital replay generation time is not canonical")
    return canonical, parsed


def produce_unsigned_capital_v8_replay_body(
    *,
    output_root: Path,
    precommitment_receipt: Mapping[str, Any],
    trusted_precommitment_public_keys: Mapping[str, bytes],
    constraints: cap.CapCalibrationConstraints,
    frozen_candidate_rosters_by_phase: Mapping[str, Mapping[str, Any]],
    lineage_by_phase: Mapping[str, Mapping[str, Any]],
    replay_output_loader: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    generated_at_utc: Any,
    shard_attempt_limit: int = 8,
) -> dict[str, Any]:
    """Write bounded attempt shards and return the body for external signing."""

    if type(constraints) is not cap.CapCalibrationConstraints:
        raise ValueError("exact CapCalibrationConstraints are required")
    constraints.validate()
    if not callable(replay_output_loader):
        raise ValueError("capital replay output loader is required")
    precommit_body, precommit_meta = cap._verified_envelope(
        precommitment_receipt,
        kind=cap.PRECOMMITMENT_SIGNATURE_KIND,
        trusted_public_keys=trusted_precommitment_public_keys,
        missing_reason="CAP_PRECOMMITMENT_MISSING",
    )
    policies, anchors, precommit = cap._precommitment(
        precommit_body, constraints
    )
    generated_text, generated = _canonical_utc(generated_at_utc)
    if generated < precommit["not_before"]:
        raise ValueError("capital replay precedes precommitted evidence access")

    phases: list[dict[str, Any]] = []
    for anchor in anchors:
        for phase, window_name in (
            ("INNER_SELECTION", "inner_window"),
            ("OUTER_EVALUATION", "outer_window"),
        ):
            phases.append(
                {
                    "anchor_id": anchor["anchor_id"],
                    "phase": phase,
                    "window": anchor[window_name],
                }
            )
    phases.append(
        {
            "anchor_id": "CURRENT",
            "phase": "CURRENT_SELECTION",
            "window": precommit["current_window"],
        }
    )
    phase_ids = {
        f"{item['anchor_id']}|{item['phase']}" for item in phases
    }
    if type(frozen_candidate_rosters_by_phase) is not dict or set(
        frozen_candidate_rosters_by_phase
    ) != phase_ids:
        raise ValueError("capital replay frozen phase roster is incomplete")
    if type(lineage_by_phase) is not dict or set(lineage_by_phase) != phase_ids:
        raise ValueError("capital replay phase lineage is incomplete")
    frozen = {
        key: _frozen_roster(value)
        for key, value in frozen_candidate_rosters_by_phase.items()
    }
    lineages = {
        key: _lineage_without_output(value)
        for key, value in lineage_by_phase.items()
    }
    for lineage in lineages.values():
        if any(
            lineage[name] != precommit["contract_lineage"][name]
            for name in _CONTRACT_LINEAGE_KEYS
        ):
            raise ValueError("capital replay lineage was not precommitted")

    neutral_policy_by_phase: dict[str, str] = {}
    global_neutral_policy_sha: list[str] = []

    def attempts() -> Iterable[dict[str, Any]]:
        for phase_spec in phases:
            phase_id = f"{phase_spec['anchor_id']}|{phase_spec['phase']}"
            for policy_id in sorted(policies):
                policy = policies[policy_id]
                load_spec = {
                    "attempt_id": (
                        f"{phase_spec['anchor_id']}|{phase_spec['phase']}|{policy_id}"
                    ),
                    "anchor_id": phase_spec["anchor_id"],
                    "phase": phase_spec["phase"],
                    "window": dict(phase_spec["window"]),
                    "policy": dict(policy),
                    "frozen_candidate_roster": dict(frozen[phase_id]),
                }
                replay_output = replay_output_loader(load_spec)
                attempt = build_capital_v8_attempt_from_synchronized_output(
                    replay_output,
                    output_root=Path(output_root),
                    anchor_id=phase_spec["anchor_id"],
                    phase=phase_spec["phase"],
                    window=phase_spec["window"],
                    policy_point=policy,
                    lineage=lineages[phase_id],
                    frozen_candidate_roster=frozen[phase_id],
                )
                result_policy = dict(replay_output["policy"])
                for name in _CALIBRATED_REPLAY_POLICY_FIELDS:
                    result_policy[name] = f"PRECOMMITTED_AXIS:{name}"
                neutral_sha = cap.canonical_json_sha256(result_policy)
                prior = neutral_policy_by_phase.setdefault(phase_id, neutral_sha)
                if prior != neutral_sha:
                    raise ValueError(
                        "capital replay changed a non-calibrated policy field"
                    )
                result_policy["allowed_promotion_statuses"] = (
                    "PHASE_BOUND_STATUS_ALLOWLIST"
                )
                global_sha = cap.canonical_json_sha256(result_policy)
                if not global_neutral_policy_sha:
                    global_neutral_policy_sha.append(global_sha)
                elif global_neutral_policy_sha[0] != global_sha:
                    raise ValueError(
                        "capital replay base policy changed across phases"
                    )
                yield attempt

    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    storage = cap.write_capital_replay_attempt_shards(
        root,
        attempts(),
        shard_attempt_limit=shard_attempt_limit,
    )
    return {
        "schema_version": cap.CAP_REPLAY_SCHEMA_VERSION,
        "precommitment_sha256": precommit_meta["body_sha256"],
        "precommitment_envelope_sha256": precommit_meta["envelope_sha256"],
        "generated_at_utc": generated_text,
        **storage,
    }


__all__ = [
    "CAP_REPLAY_OUTPUT_BINDING_VERSION",
    "CAP_REPLAY_PRODUCER_VERSION",
    "FLAT_MARGIN_LEVEL_PCT",
    "build_capital_v8_attempt_from_synchronized_output",
    "capital_policy_for_point",
    "produce_unsigned_capital_v8_replay_body",
]
