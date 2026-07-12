#!/usr/bin/env python3
# ruff: noqa: E402
"""Draft or explicitly approve an immutable funded-promotion authorization.

This is deliberately a two-command workflow.  ``--draft`` produces a sealed,
non-authoritative review packet.  ``--approve DRAFT`` re-verifies every current
input, records an explicit review decision, and emits an APPROVED authorization.
Neither command edits config.json, the winner ledger, or the manual-entry flag.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import sys
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pm_cap_calibration import (
    cap_neutral_config_sha256,
    cap_neutral_policy_contract_sha256,
    validate_cap_calibration_artifact,
)
from pm_core import PipelineConfig
from pm_pipeline import build_artifact_meta
from pm_portfolio_replay import (
    active_native_portfolio_contract,
    build_ledger_cell_roster,
    canonical_json_sha256,
    portfolio_cost_contract,
    portfolio_replay_evidence_seal_valid,
    validate_funded_portfolio_replay_evidence,
)
from pm_promotion_authorization import (
    AUTHORIZATION_SCHEMA,
    authorization_payload_sha256,
    build_cell_contract,
    build_review_scope,
    canonical_sha256,
    cell_contract_sha256,
    sha256_file,
    strict_json_loads,
)
from pm_two_phase_lockbox import (
    critical_module_code_contract,
    load_pinned_prelockbox_roster,
    prelockbox_evidence_policy_from_config_payload,
    validate_global_final_lockbox_access,
    validate_released_ledger_against_prelockbox,
)


DRAFT_SCHEMA = "fxpm-funded-promotion-authorization-draft-v1"
DRAFT_SEAL_ALGORITHM = "sha256-canonical-json-v1"
AUTHORING_CHAIN_SCHEMA = "fxpm-funded-promotion-authoring-chain-v1"


class AuthoringFailure(ValueError):
    """A fail-closed promotion-authoring rejection with stable reason codes."""

    def __init__(self, *errors: str):
        normalized = tuple(dict.fromkeys(str(item) for item in errors if str(item)))
        self.errors = normalized or ("PROMOTION_AUTHORING_REJECTED",)
        super().__init__("; ".join(self.errors))


@dataclass(frozen=True)
class VerifiedAuthoringContext:
    config_path: Path
    config_payload: Dict[str, Any]
    config: PipelineConfig
    ledger_path: Path
    ledger: Dict[str, Any]
    ledger_sha256: str
    contracts: tuple[Dict[str, Any], ...]
    authoring_chain: Dict[str, Any]
    max_age_days: int
    earliest_cell_expiry_utc: datetime


def _aware_utc(value: Any, code: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise AuthoringFailure(code)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise AuthoringFailure(code) from exc
    if parsed.tzinfo is None:
        raise AuthoringFailure(code)
    return parsed.astimezone(timezone.utc)


def _cell_expiry_utc(value: Any, cell_id: str) -> datetime:
    """Parse the ledger's legacy local-naive schedule convention explicitly."""
    text = str(value or "").strip()
    if not text:
        raise AuthoringFailure(f"CELL_EXPIRY_INVALID:{cell_id}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise AuthoringFailure(f"CELL_EXPIRY_INVALID:{cell_id}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed.astimezone(timezone.utc)


def _strict_mapping(path: Path, label: str) -> Dict[str, Any]:
    try:
        payload = strict_json_loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise AuthoringFailure(f"{label}_UNREADABLE:{type(exc).__name__}") from exc
    if not isinstance(payload, Mapping):
        raise AuthoringFailure(f"{label}_NOT_OBJECT")
    return dict(payload)


def _resolve(raw: Any, config_path: Path, code: str) -> Path:
    value = str(raw or "").strip()
    if not value:
        raise AuthoringFailure(code)
    path = Path(value)
    if not path.is_absolute():
        path = config_path.resolve().parent / path
    return path.resolve()


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _is_sha1(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 40 and all(char in "0123456789abcdef" for char in text)


def _pipeline_config(config_path: Path, payload: Mapping[str, Any]) -> PipelineConfig:
    allowed = {item.name for item in fields(PipelineConfig)}
    pipeline = payload.get("pipeline")
    if not isinstance(pipeline, Mapping):
        raise AuthoringFailure("CONFIG_PIPELINE_NOT_OBJECT")
    cfg = PipelineConfig(
        **{key: value for key, value in pipeline.items() if key in allowed}
    )
    cfg._deployment_config_path = str(config_path.resolve())
    data_dir = Path(cfg.data_dir)
    if not data_dir.is_absolute():
        cfg.data_dir = config_path.resolve().parent / data_dir
    return cfg


def _symbols(payload: Mapping[str, Any]) -> list[str]:
    raw = payload.get("symbols")
    if not isinstance(raw, list):
        raise AuthoringFailure("CONFIG_SYMBOL_ROSTER_INVALID")
    symbols = [str(item or "").strip().upper() for item in raw]
    if not symbols or any(not item for item in symbols) or len(set(symbols)) != len(symbols):
        raise AuthoringFailure("CONFIG_SYMBOL_ROSTER_INVALID")
    return sorted(symbols)


def _float(value: Any, code: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise AuthoringFailure(code) from exc
    if not math.isfinite(number):
        raise AuthoringFailure(code)
    return number


def _broker_sha256(config_payload: Mapping[str, Any], config_path: Path) -> str:
    path = _resolve(
        config_payload.get("broker_specs_path"),
        config_path,
        "BROKER_SPECS_PATH_MISSING",
    )
    if not path.is_file():
        raise AuthoringFailure("BROKER_SPECS_MISSING")
    return sha256_file(path)


def _starting_balances(cfg: PipelineConfig) -> list[float]:
    balances = sorted(
        {
            _float(value, "PORTFOLIO_STARTING_BALANCE_INVALID")
            for value in (
                getattr(
                    cfg,
                    "production_portfolio_replay_required_starting_balances",
                    [],
                )
                or []
            )
        }
    )
    if not balances or any(value <= 0.0 for value in balances):
        raise AuthoringFailure("PORTFOLIO_STARTING_BALANCES_INVALID")
    return balances


def _tradeable_contracts(
    ledger: Mapping[str, Any],
    allowed_statuses: Iterable[str],
) -> tuple[Dict[str, Any], ...]:
    allowed = {str(value or "").strip().upper() for value in allowed_statuses}
    allowed -= {"", "BLOCKED", "PAPER_ONLY", "NO_TRADE", "UNKNOWN"}
    contracts: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw_symbol, symbol_payload in ledger.items():
        if not isinstance(symbol_payload, Mapping):
            continue
        symbol = str(symbol_payload.get("symbol") or raw_symbol).strip().upper()
        regimes = symbol_payload.get("regime_configs")
        if not isinstance(regimes, Mapping):
            continue
        for raw_timeframe, regime_map in regimes.items():
            if not isinstance(regime_map, Mapping):
                continue
            timeframe = str(raw_timeframe or "").strip().upper()
            for raw_regime, cell in regime_map.items():
                if not isinstance(cell, Mapping):
                    continue
                regime = str(raw_regime or "").strip().upper()
                strategy = str(cell.get("strategy_name") or "").strip()
                meta = cell.get("artifact_meta")
                meta = meta if isinstance(meta, Mapping) else {}
                status = str(cell.get("promotion_status") or "BLOCKED").upper()
                if (
                    strategy
                    and strategy.upper() != "NO_TRADE"
                    and not bool(meta.get("no_trade"))
                    and status in allowed
                ):
                    contract = build_cell_contract(
                        symbol,
                        timeframe,
                        regime,
                        cell,
                        symbol_payload,
                    )
                    cell_id = str(contract.get("cell_id") or "")
                    if cell_id in seen:
                        raise AuthoringFailure(f"DUPLICATE_TRADEABLE_CELL:{cell_id}")
                    seen.add(cell_id)
                    contracts.append(contract)
    contracts.sort(key=lambda row: str(row.get("cell_id") or ""))
    if not contracts:
        raise AuthoringFailure("NO_TRADEABLE_CELLS_TO_AUTHORIZE")
    return tuple(contracts)


def _contract_authoring_errors(contract: Mapping[str, Any]) -> list[str]:
    """Mirror the runtime's non-negotiable per-cell lineage requirements."""
    cell_id = str(contract.get("cell_id") or "<unknown>")
    errors: list[str] = []
    native = contract.get("native_lineage")
    native = native if isinstance(native, Mapping) else {}
    if not (
        native.get("source_kind") == "broker_native"
        and bool(native.get("provenance_valid"))
        and bool(native.get("timeframe_evidence_passed"))
        and _is_sha256(native.get("bars_sha256"))
        and _is_sha256(native.get("sidecar_sha256"))
        and _is_sha256(native.get("activation_manifest_sha256"))
        and str(native.get("timeframe_evidence_source_sha256") or "").lower()
        == str(native.get("bars_sha256") or "").lower()
        and bool(native.get("activation_id"))
        and _is_sha1(native.get("data_window_fingerprint"))
        and bool(native.get("evidence_contract_version"))
    ):
        errors.append(f"AUTHORIZATION_NATIVE_LINEAGE_INCOMPLETE:{cell_id}")
    deployment = contract.get("deployment_contract")
    deployment = deployment if isinstance(deployment, Mapping) else {}
    if not (
        deployment.get("contract") == "funded_live_deployment_v1"
        and deployment.get("status") == "PASS"
        and _is_sha1(deployment.get("sha1"))
    ):
        errors.append(f"AUTHORIZATION_DEPLOYMENT_CONTRACT_INVALID:{cell_id}")
    if (
        str(contract.get("promotion_status") or "").upper() != "PROMOTED"
        or not str(contract.get("strategy_name") or "").strip()
        or not str(contract.get("artifact_version") or "").strip()
        or not str(contract.get("trained_at") or "").strip()
    ):
        errors.append(f"AUTHORIZATION_CELL_IDENTITY_INCOMPLETE:{cell_id}")
    return errors


def _validate_final_pass_shape(evidence: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if not portfolio_replay_evidence_seal_valid(evidence):
        errors.append("FINAL_PORTFOLIO_EVIDENCE_SEAL_INVALID")
    if str(evidence.get("verdict") or "").strip().upper() != "PASS":
        errors.append("FINAL_PORTFOLIO_VERDICT_NOT_PASS")
    if evidence.get("failures"):
        errors.append("FINAL_PORTFOLIO_FAILURES_PRESENT")
    replay = evidence.get("replay") if isinstance(evidence.get("replay"), Mapping) else {}
    window = replay.get("window") if isinstance(replay.get("window"), Mapping) else {}
    if str(window.get("partition") or "") != "final_lockbox":
        errors.append("FINAL_PORTFOLIO_PARTITION_INVALID")
    if bool(replay.get("partial_run")) or bool(replay.get("allow_cell_errors")):
        errors.append("FINAL_PORTFOLIO_PARTIAL_OR_PERMISSIVE")
    return errors


def load_verified_authoring_context(
    config_path: Path,
    *,
    ledger_override: Optional[Path] = None,
    now: Optional[datetime] = None,
) -> VerifiedAuthoringContext:
    """Rebuild and verify the complete chain that an authorization may attest."""
    config_path = Path(config_path).resolve()
    config_payload = _strict_mapping(config_path, "CONFIG")
    cfg = _pipeline_config(config_path, config_payload)
    pipeline = config_payload.get("pipeline")
    assert isinstance(pipeline, Mapping)  # established by _pipeline_config
    symbols = _symbols(config_payload)
    probe = now or datetime.now(timezone.utc)
    if probe.tzinfo is None:
        raise AuthoringFailure("AUTHORING_CLOCK_NOT_AWARE")
    probe = probe.astimezone(timezone.utc)

    ledger_path = (
        Path(ledger_override).resolve()
        if ledger_override is not None
        else _resolve(
            pipeline.get("winner_ledger_path"),
            config_path,
            "WINNER_LEDGER_PATH_MISSING",
        )
    )
    ledger = _strict_mapping(ledger_path, "RELEASED_LEDGER")
    ledger_sha = sha256_file(ledger_path)
    broker_sha = _broker_sha256(config_payload, config_path)

    native_contract, native_errors = active_native_portfolio_contract(
        Path(cfg.data_dir), symbols
    )
    if native_errors:
        raise AuthoringFailure(*native_errors)
    artifact_meta = build_artifact_meta(cfg)
    deployment = dict(artifact_meta.get("deployment_contract") or {})
    code_contract = critical_module_code_contract(ROOT)
    if code_contract.get("missing"):
        raise AuthoringFailure(
            "CRITICAL_MODULES_MISSING:" + ",".join(code_contract["missing"])
        )
    code_sha = str(code_contract.get("aggregate_sha256") or "")
    expected_pre_lineage = {
        "cap_neutral_config_sha256": cap_neutral_config_sha256(config_payload),
        "artifact_contract_sha256": canonical_json_sha256(artifact_meta),
        "deployment_contract_sha256": canonical_json_sha256(deployment),
        "broker_specs_sha256": broker_sha,
        "native_activation_id": native_contract.get("activation_id"),
        "native_activation_manifest_sha256": native_contract.get(
            "activation_manifest_sha256"
        ),
        "native_activation_roster_sha256": native_contract.get(
            "activation_roster_sha256"
        ),
        "native_bars_sha256_by_source": native_contract.get(
            "bars_sha256_by_source"
        ),
        "native_sidecar_sha256_by_source": native_contract.get(
            "sidecar_sha256_by_source"
        ),
        "critical_module_code_contract": code_contract,
        "critical_module_code_contract_sha256": code_sha,
    }
    prelockbox, pre_path, pre_errors = load_pinned_prelockbox_roster(
        config_payload=config_payload,
        config_path=config_path,
        expected_symbols=symbols,
        expected_lineage=expected_pre_lineage,
    )
    if pre_errors or not isinstance(prelockbox, Mapping) or pre_path is None:
        raise AuthoringFailure(*(pre_errors or ["PRELOCKBOX_ARTIFACT_INVALID"]))
    pre_file_sha = sha256_file(pre_path)
    pre_roster = prelockbox.get("roster")
    pre_roster = pre_roster if isinstance(pre_roster, Mapping) else {}
    pre_roster_sha = str(pre_roster.get("roster_sha256") or "")
    pre_seal_sha = str((prelockbox.get("content_seal") or {}).get("sha256") or "")

    frozen_ledger = prelockbox.get("frozen_ledger") or {}
    expected_roster, roster_errors = build_ledger_cell_roster(
        frozen_ledger,
        symbols,
        allowed_promotion_statuses=("FROZEN_PRELOCKBOX",),
    )
    if roster_errors:
        raise AuthoringFailure(*roster_errors)

    balances = _starting_balances(cfg)
    tuning_policies = {
        balance: prelockbox_evidence_policy_from_config_payload(
            config_payload, balance
        )
        for balance in balances
    }
    cap_path = _resolve(
        pipeline.get("production_cap_calibration_path"),
        config_path,
        "CAP_CALIBRATION_PATH_MISSING",
    )
    cap_payload = _strict_mapping(cap_path, "CAP_CALIBRATION")
    cap_file_sha = sha256_file(cap_path)
    configured_cap_sha = str(
        pipeline.get("production_cap_calibration_sha256") or ""
    ).lower()
    if not _is_sha256(configured_cap_sha) or configured_cap_sha != cap_file_sha:
        raise AuthoringFailure("CAP_CALIBRATION_FILE_SHA256_MISMATCH")
    expected_cap_lineage = {
        "cap_neutral_config_sha256": cap_neutral_config_sha256(config_payload),
        "prelockbox_roster_sha256": pre_roster_sha,
        "prelockbox_artifact_file_sha256": pre_file_sha,
        "prelockbox_artifact_seal_sha256": pre_seal_sha,
        "critical_module_code_contract_sha256": code_sha,
        "broker_specs_sha256": broker_sha,
        "native_activation_id": native_contract.get("activation_id"),
        "native_activation_manifest_sha256": native_contract.get(
            "activation_manifest_sha256"
        ),
        "native_activation_roster_sha256": native_contract.get(
            "activation_roster_sha256"
        ),
        "native_bars_sha256_by_source": native_contract.get(
            "bars_sha256_by_source"
        ),
        "native_sidecar_sha256_by_source": native_contract.get(
            "sidecar_sha256_by_source"
        ),
        "base_policy_contract_sha256": cap_neutral_policy_contract_sha256(
            tuning_policies
        ),
        "required_starting_balances": balances,
    }
    selected_cap = _float(
        pipeline.get("max_total_open_risk_pct"),
        "SELECTED_TOTAL_STOP_RISK_CAP_INVALID",
    )
    cap_errors = validate_cap_calibration_artifact(
        cap_payload,
        expected_lineage=expected_cap_lineage,
        expected_selected_cap_pct=selected_cap,
    )
    if cap_errors:
        raise AuthoringFailure(*cap_errors)

    evidence_path = _resolve(
        pipeline.get("production_portfolio_replay_evidence_path"),
        config_path,
        "FINAL_PORTFOLIO_EVIDENCE_PATH_MISSING",
    )
    evidence = _strict_mapping(evidence_path, "FINAL_PORTFOLIO_EVIDENCE")
    shape_errors = _validate_final_pass_shape(evidence)
    if shape_errors:
        raise AuthoringFailure(*shape_errors)
    evidence_file_sha = sha256_file(evidence_path)
    cap_selection = cap_payload.get("selection")
    cap_selection = cap_selection if isinstance(cap_selection, Mapping) else {}
    cap_selected = _float(
        cap_selection.get("selected_max_total_stop_risk_pct"),
        "CAP_CALIBRATION_SELECTED_CAP_INVALID",
    )
    evidence_lineage = {
        "artifact_contract_sha256": canonical_json_sha256(artifact_meta),
        "deployment_contract_sha256": canonical_json_sha256(deployment),
        "prelockbox_roster_sha256": pre_roster_sha,
        "prelockbox_artifact_file_sha256": pre_file_sha,
        "prelockbox_artifact_seal_sha256": pre_seal_sha,
        "critical_module_code_contract_sha256": code_sha,
        "broker_specs_sha256": broker_sha,
        "native_activation_id": native_contract.get("activation_id"),
        "native_activation_manifest_sha256": native_contract.get(
            "activation_manifest_sha256"
        ),
        "native_activation_roster_sha256": native_contract.get(
            "activation_roster_sha256"
        ),
        "native_bars_sha256_by_source": native_contract.get(
            "bars_sha256_by_source"
        ),
        "native_sidecar_sha256_by_source": native_contract.get(
            "sidecar_sha256_by_source"
        ),
        "cap_calibration_sha256": cap_file_sha,
        "cap_calibration_selected_max_total_stop_risk_pct": cap_selected,
    }
    final_policies = {
        balance: asdict(
            prelockbox_evidence_policy_from_config_payload(
                config_payload, balance
            )
        )
        for balance in balances
    }
    cap_constraints = cap_payload.get("constraints")
    cap_constraints = cap_constraints if isinstance(cap_constraints, Mapping) else {}
    evidence_errors = validate_funded_portfolio_replay_evidence(
        evidence,
        expected_lineage=evidence_lineage,
        expected_roster=expected_roster,
        expected_cost_contract=portfolio_cost_contract(config_payload),
        expected_policies=final_policies,
        minimum_calendar_days=_float(
            pipeline.get("production_portfolio_replay_min_calendar_days"),
            "PORTFOLIO_MIN_CALENDAR_DAYS_INVALID",
        ),
        minimum_accepted_positions=int(
            _float(
                pipeline.get("production_portfolio_replay_min_positions"),
                "PORTFOLIO_MIN_POSITIONS_INVALID",
            )
        ),
        minimum_profit_factor=_float(
            pipeline.get("production_portfolio_replay_min_profit_factor"),
            "PORTFOLIO_MIN_PROFIT_FACTOR_INVALID",
        ),
        minimum_return_pct=_float(
            pipeline.get("production_portfolio_replay_min_return_pct"),
            "PORTFOLIO_MIN_RETURN_INVALID",
        ),
        maximum_drawdown_pct=_float(
            pipeline.get("production_portfolio_replay_max_drawdown_pct"),
            "PORTFOLIO_MAX_DRAWDOWN_INVALID",
        ),
        require_mtm=bool(
            pipeline.get("production_portfolio_replay_require_mtm", True)
        ),
        maximum_bootstrap_ruin_upper_95pct=_float(
            cap_constraints.get("maximum_bootstrap_ruin_upper_95pct"),
            "CAP_BOOTSTRAP_RUIN_LIMIT_INVALID",
        ),
        maximum_bootstrap_p95_drawdown_pct=_float(
            cap_constraints.get("maximum_bootstrap_p95_drawdown_pct"),
            "CAP_BOOTSTRAP_DRAWDOWN_LIMIT_INVALID",
        ),
        minimum_bootstrap_daily_return_observations=_float(
            cap_constraints.get("minimum_bootstrap_daily_return_observations"),
            "CAP_BOOTSTRAP_DAILY_OBSERVATIONS_INVALID",
        ),
    )
    replay = evidence.get("replay")
    replay = replay if isinstance(replay, Mapping) else {}
    evidence_errors.extend(
        validate_global_final_lockbox_access(
            config=cfg,
            config_path=config_path,
            access_id=str(replay.get("global_lockbox_access_id") or ""),
            frozen_ledger=frozen_ledger,
            roster_sha256=pre_roster_sha,
            prelockbox_file_sha256=pre_file_sha,
        )
    )
    if evidence_errors:
        raise AuthoringFailure(*evidence_errors)

    release_errors = validate_released_ledger_against_prelockbox(
        ledger,
        prelockbox,
        expected_portfolio_evidence_sha256=evidence_file_sha,
    )
    if release_errors:
        raise AuthoringFailure(*release_errors)

    allowed_statuses = pipeline.get("live_allowed_promotion_statuses") or [
        "PROMOTED"
    ]
    if isinstance(allowed_statuses, str):
        allowed_statuses = [
            value.strip() for value in allowed_statuses.split(",") if value.strip()
        ]
    contracts = _tradeable_contracts(ledger, allowed_statuses)
    expected_tradeable = sorted(pre_roster.get("tradeable_cell_ids") or [])
    observed_tradeable = sorted(str(row.get("cell_id") or "") for row in contracts)
    if observed_tradeable != expected_tradeable:
        raise AuthoringFailure("AUTHORIZATION_TRADEABLE_ROSTER_MISMATCH")
    contract_errors = [
        error
        for contract in contracts
        for error in _contract_authoring_errors(contract)
    ]
    if contract_errors:
        raise AuthoringFailure(*contract_errors)
    expiries = [
        _cell_expiry_utc(contract.get("valid_until"), str(contract.get("cell_id")))
        for contract in contracts
    ]
    earliest_expiry = min(expiries)
    if earliest_expiry <= probe:
        raise AuthoringFailure("AUTHORIZATION_SCOPE_CONTAINS_EXPIRED_CELL")

    authoring_chain = {
        "schema_version": AUTHORING_CHAIN_SCHEMA,
        "winner_ledger_file_sha256": ledger_sha,
        "winner_ledger_semantic_sha256": canonical_sha256(ledger),
        "prelockbox_artifact_file_sha256": pre_file_sha,
        "prelockbox_artifact_seal_sha256": pre_seal_sha,
        "prelockbox_roster_sha256": pre_roster_sha,
        "cap_calibration_file_sha256": cap_file_sha,
        "cap_calibration_seal_sha256": str(
            (cap_payload.get("content_seal") or {}).get("sha256") or ""
        ),
        "selected_max_total_stop_risk_pct": cap_selected,
        "final_portfolio_evidence_file_sha256": evidence_file_sha,
        "final_portfolio_evidence_seal_sha256": str(
            (evidence.get("content_seal") or {}).get("sha256") or ""
        ),
        "global_final_lockbox_access_id": str(
            replay.get("global_lockbox_access_id") or ""
        ),
        "critical_module_code_contract_sha256": code_sha,
        "artifact_contract_sha256": canonical_json_sha256(artifact_meta),
        "deployment_contract_sha256": canonical_json_sha256(deployment),
        "native_activation_id": native_contract.get("activation_id"),
        "native_activation_manifest_sha256": native_contract.get(
            "activation_manifest_sha256"
        ),
        "native_activation_roster_sha256": native_contract.get(
            "activation_roster_sha256"
        ),
        "release_scope": "ALL_FROZEN_TRADEABLE_CELLS",
        "survivor_pruning_performed": False,
        "source_excluded_no_trade_cells_preserved": True,
    }
    max_age_days = max(
        1,
        int(
            _float(
                pipeline.get("production_promotion_authorization_max_age_days", 31),
                "PROMOTION_AUTHORIZATION_MAX_AGE_INVALID",
            )
        ),
    )
    return VerifiedAuthoringContext(
        config_path=config_path,
        config_payload=config_payload,
        config=cfg,
        ledger_path=ledger_path,
        ledger=ledger,
        ledger_sha256=ledger_sha,
        contracts=contracts,
        authoring_chain=authoring_chain,
        max_age_days=max_age_days,
        earliest_cell_expiry_utc=earliest_expiry,
    )


def _approval_rows(
    contracts: Sequence[Mapping[str, Any]], *, decision: str
) -> list[Dict[str, Any]]:
    return [
        {
            "cell_id": str(contract.get("cell_id") or "").upper(),
            "decision": decision,
            "contract_sha256": cell_contract_sha256(contract),
            "contract": copy.deepcopy(dict(contract)),
        }
        for contract in contracts
    ]


def draft_seal_sha256(payload: Mapping[str, Any]) -> str:
    unsigned = dict(payload)
    unsigned.pop("content_seal", None)
    return canonical_sha256(unsigned)


def draft_seal_valid(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    seal = payload.get("content_seal")
    return bool(
        isinstance(seal, Mapping)
        and seal.get("algorithm") == DRAFT_SEAL_ALGORITHM
        and str(seal.get("sha256") or "").lower() == draft_seal_sha256(payload)
    )


def build_authorization_draft(
    context: VerifiedAuthoringContext,
    *,
    prepared_by: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    preparer = str(prepared_by or "").strip()
    if not preparer:
        raise AuthoringFailure("PROMOTION_PREPARER_ID_MISSING")
    prepared_at = now or datetime.now(timezone.utc)
    if prepared_at.tzinfo is None:
        raise AuthoringFailure("DRAFT_PREPARED_AT_NOT_AWARE")
    prepared_at = prepared_at.astimezone(timezone.utc)
    scope = build_review_scope(context.ledger_sha256, context.contracts)
    body: Dict[str, Any] = {
        "schema_version": DRAFT_SCHEMA,
        "draft_status": "DRAFT_NON_AUTHORITATIVE",
        "authorizes_trading": False,
        "prepared_at": prepared_at.isoformat(),
        "prepared_by": preparer,
        "winner_ledger_sha256": context.ledger_sha256,
        "review_scope": scope,
        "review_scope_sha256": canonical_sha256(scope),
        "authoring_chain": copy.deepcopy(context.authoring_chain),
        "authoring_chain_sha256": canonical_sha256(context.authoring_chain),
        "earliest_cell_expiry_utc": context.earliest_cell_expiry_utc.isoformat(),
        "cells": _approval_rows(context.contracts, decision="PENDING_REVIEW"),
    }
    body["draft_id"] = "DRAFT-" + canonical_sha256(body)[:24].upper()
    body["content_seal"] = {
        "algorithm": DRAFT_SEAL_ALGORITHM,
        "sha256": draft_seal_sha256(body),
    }
    return body


def _draft_matches_context(
    draft: Mapping[str, Any], context: VerifiedAuthoringContext
) -> list[str]:
    errors: list[str] = []
    if str(draft.get("schema_version") or "") != DRAFT_SCHEMA:
        errors.append("PROMOTION_DRAFT_SCHEMA_INVALID")
    if str(draft.get("draft_status") or "") != "DRAFT_NON_AUTHORITATIVE":
        errors.append("PROMOTION_DRAFT_STATUS_INVALID")
    if draft.get("authorizes_trading") is not False:
        errors.append("PROMOTION_DRAFT_AUTHORITY_FLAG_INVALID")
    if not draft_seal_valid(draft):
        errors.append("PROMOTION_DRAFT_SEAL_INVALID")
    scope = build_review_scope(context.ledger_sha256, context.contracts)
    if canonical_sha256(draft.get("review_scope")) != canonical_sha256(scope):
        errors.append("PROMOTION_DRAFT_SCOPE_STALE_OR_TAMPERED")
    if str(draft.get("review_scope_sha256") or "").lower() != canonical_sha256(
        scope
    ):
        errors.append("PROMOTION_DRAFT_SCOPE_SHA256_INVALID")
    if canonical_sha256(draft.get("authoring_chain")) != canonical_sha256(
        context.authoring_chain
    ):
        errors.append("PROMOTION_DRAFT_CHAIN_STALE_OR_TAMPERED")
    if str(draft.get("authoring_chain_sha256") or "").lower() != canonical_sha256(
        context.authoring_chain
    ):
        errors.append("PROMOTION_DRAFT_CHAIN_SHA256_INVALID")
    expected_rows = _approval_rows(context.contracts, decision="PENDING_REVIEW")
    if canonical_sha256(draft.get("cells")) != canonical_sha256(expected_rows):
        errors.append("PROMOTION_DRAFT_CELLS_STALE_OR_TAMPERED")
    if str(draft.get("winner_ledger_sha256") or "").lower() != context.ledger_sha256:
        errors.append("PROMOTION_DRAFT_LEDGER_STALE_OR_TAMPERED")
    return errors


def approve_authorization_draft(
    draft: Mapping[str, Any],
    context: VerifiedAuthoringContext,
    *,
    review_method: str,
    reviewer_id: str,
    decision_reference: str,
    expires_at: Any,
    control_id: str = "",
    control_version: str = "",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    errors = _draft_matches_context(draft, context)
    if errors:
        raise AuthoringFailure(*errors)
    method = str(review_method or "").strip().upper()
    if method not in {"HUMAN", "CONTROLLED"}:
        raise AuthoringFailure("PROMOTION_REVIEW_METHOD_INVALID")
    preparer = str(draft.get("prepared_by") or "").strip()
    reviewer = str(reviewer_id or "").strip()
    reference = str(decision_reference or "").strip()
    if not preparer:
        raise AuthoringFailure("PROMOTION_PREPARER_ID_MISSING")
    if not reviewer:
        raise AuthoringFailure("PROMOTION_REVIEWER_ID_MISSING")
    if not reference:
        raise AuthoringFailure("PROMOTION_REVIEW_DECISION_REFERENCE_MISSING")
    if method == "HUMAN" and reviewer.casefold() == preparer.casefold():
        raise AuthoringFailure("PROMOTION_REVIEW_SEPARATION_OF_DUTIES_FAILED")
    control = str(control_id or "").strip()
    version = str(control_version or "").strip()
    if method == "CONTROLLED" and (not control or not version):
        raise AuthoringFailure("PROMOTION_CONTROL_IDENTITY_MISSING")

    issued_at = now or datetime.now(timezone.utc)
    if issued_at.tzinfo is None:
        raise AuthoringFailure("PROMOTION_ISSUED_AT_NOT_AWARE")
    issued_at = issued_at.astimezone(timezone.utc)
    expiry = _aware_utc(expires_at, "PROMOTION_EXPIRES_AT_INVALID")
    if expiry <= issued_at:
        raise AuthoringFailure("PROMOTION_AUTHORIZATION_TIME_RANGE_INVALID")
    if expiry > issued_at + timedelta(days=context.max_age_days):
        raise AuthoringFailure("PROMOTION_AUTHORIZATION_EXPIRY_EXCEEDS_MAX_AGE")
    if expiry > context.earliest_cell_expiry_utc:
        raise AuthoringFailure("PROMOTION_AUTHORIZATION_OUTLIVES_CELL")

    scope = build_review_scope(context.ledger_sha256, context.contracts)
    review: Dict[str, Any] = {
        "status": "APPROVED",
        "method": method,
        "prepared_by": preparer,
        "reviewer_id": reviewer,
        "reviewed_at": issued_at.isoformat(),
        "decision_reference": reference,
        "scope_sha256": canonical_sha256(scope),
    }
    if method == "CONTROLLED":
        review["control_id"] = control
        review["control_version"] = version
    identity_seed = {
        "draft_seal_sha256": str((draft.get("content_seal") or {}).get("sha256") or ""),
        "review": review,
        "issued_at": issued_at.isoformat(),
        "expires_at": expiry.isoformat(),
    }
    payload: Dict[str, Any] = {
        "schema_version": AUTHORIZATION_SCHEMA,
        "authorization_id": "AUTH-" + canonical_sha256(identity_seed)[:24].upper(),
        "authorization_status": "APPROVED",
        "issued_at": issued_at.isoformat(),
        "expires_at": expiry.isoformat(),
        "winner_ledger_sha256": context.ledger_sha256,
        "source_draft_id": str(draft.get("draft_id") or ""),
        "source_draft_seal_sha256": str(
            (draft.get("content_seal") or {}).get("sha256") or ""
        ),
        "authoring_chain": copy.deepcopy(context.authoring_chain),
        "review": review,
        "cells": _approval_rows(context.contracts, decision="APPROVE"),
    }
    payload["payload_sha256"] = authorization_payload_sha256(payload)
    return payload


def _write_content_addressed(
    output_dir: Path,
    *,
    prefix: str,
    digest: str,
    payload: Mapping[str, Any],
) -> Path:
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{prefix}_{digest}.json"
    serialized = json.dumps(
        payload,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ) + "\n"
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
    except FileExistsError:
        existing = strict_json_loads(path.read_text(encoding="utf-8"))
        if canonical_sha256(existing) != canonical_sha256(payload):
            raise AuthoringFailure("CONTENT_ADDRESSED_OUTPUT_COLLISION")
    return path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--draft",
        action="store_true",
        help=(
            "Write a sealed DRAFT_NON_AUTHORITATIVE review packet. It cannot "
            "authorize funded entries and must later be passed to --approve."
        ),
    )
    mode.add_argument(
        "--approve",
        metavar="DRAFT_JSON",
        help=(
            "Explicitly approve this sealed draft after re-verifying the full "
            "current final-PASS chain; writes a separate immutable authorization."
        ),
    )
    parser.add_argument("--config", default=str(ROOT / "config.json"))
    parser.add_argument(
        "--ledger",
        default="",
        help="Optional released-ledger override; otherwise uses winner_ledger_path.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(
            ROOT
            / "agent_audit"
            / "performance"
            / "data"
            / "promotion_authorizations"
        ),
    )
    parser.add_argument("--prepared-by", default="", help="Required with --draft.")
    parser.add_argument(
        "--review-method",
        choices=("HUMAN", "CONTROLLED"),
        help="Required with --approve.",
    )
    parser.add_argument("--reviewer-id", default="", help="Required with --approve.")
    parser.add_argument(
        "--decision-reference", default="", help="Required with --approve."
    )
    parser.add_argument(
        "--expires-at",
        default="",
        help=(
            "Required with --approve: offset-aware ISO-8601 UTC expiry, no later "
            "than config max age or the earliest approved cell expiry."
        ),
    )
    parser.add_argument(
        "--control-id",
        default="",
        help="Required for a CONTROLLED approval.",
    )
    parser.add_argument(
        "--control-version",
        default="",
        help="Required for a CONTROLLED approval.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config_path = Path(args.config).resolve()
        context = load_verified_authoring_context(
            config_path,
            ledger_override=(Path(args.ledger).resolve() if args.ledger else None),
        )
        output_dir = Path(args.out_dir).resolve()
        if args.draft:
            if not args.prepared_by:
                raise AuthoringFailure("--prepared-by is required with --draft")
            payload = build_authorization_draft(
                context,
                prepared_by=args.prepared_by,
            )
            digest = str((payload.get("content_seal") or {}).get("sha256") or "")
            path = _write_content_addressed(
                output_dir,
                prefix="promotion_authorization_draft",
                digest=digest,
                payload=payload,
            )
            print("STATUS=DRAFT_NON_AUTHORITATIVE")
            print("AUTHORIZES_TRADING=false")
            print(f"DRAFT_PATH={path}")
            print(f"DRAFT_FILE_SHA256={sha256_file(path)}")
            print(f"DRAFT_SEAL_SHA256={digest}")
            print(
                "NEXT=review the exact packet, then run this command with "
                f"--approve \"{path}\" and the explicit review fields"
            )
            return 0

        if args.prepared_by:
            raise AuthoringFailure(
                "--prepared-by belongs to --draft; approval uses the sealed preparer identity"
            )
        if not args.review_method:
            raise AuthoringFailure("--review-method is required with --approve")
        draft_path = Path(args.approve).resolve()
        draft = _strict_mapping(draft_path, "PROMOTION_DRAFT")
        payload = approve_authorization_draft(
            draft,
            context,
            review_method=args.review_method,
            reviewer_id=args.reviewer_id,
            decision_reference=args.decision_reference,
            expires_at=args.expires_at,
            control_id=args.control_id,
            control_version=args.control_version,
        )
        digest = str(payload.get("payload_sha256") or "")
        path = _write_content_addressed(
            output_dir,
            prefix="promotion_authorization",
            digest=digest,
            payload=payload,
        )
        file_sha = sha256_file(path)
        display_path: Path = path
        try:
            display_path = path.relative_to(config_path.parent)
        except ValueError:
            pass
        print("STATUS=APPROVED_IMMUTABLE_EXACT")
        print(f"AUTHORIZATION_PATH={path}")
        print(f"AUTHORIZATION_FILE_SHA256={file_sha}")
        print(f"AUTHORIZATION_PAYLOAD_SHA256={digest}")
        print("CONFIG_PINS_BEGIN")
        print(
            '"production_promotion_authorization_path": '
            + json.dumps(str(display_path).replace("\\", "/"))
            + ","
        )
        print(
            '"production_promotion_authorization_sha256": '
            + json.dumps(file_sha)
        )
        print("CONFIG_PINS_END")
        print("CONFIG_NOT_MODIFIED=true")
        print("LEDGER_NOT_MODIFIED=true")
        print("MANUAL_NEW_ENTRIES_NOT_MODIFIED=true")
        return 0
    except AuthoringFailure as exc:
        print("STATUS=REJECTED", file=sys.stderr)
        for error in exc.errors:
            print(f"ERROR={error}", file=sys.stderr)
        return 2
    except Exception as exc:
        print("STATUS=REJECTED", file=sys.stderr)
        print(f"ERROR=UNEXPECTED_FAIL_CLOSED:{type(exc).__name__}:{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
