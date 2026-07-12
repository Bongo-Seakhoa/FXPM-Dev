"""Sealed two-phase production optimization lifecycle.

Phase A selects one immutable specialist (or an explicit ``NO_TRADE`` marker)
for every symbol x timeframe x regime cell using optimizer-train and tuning
data only.  The resulting global roster is content sealed before any final
lockbox return is evaluated.  Phase B may evaluate that exact roster jointly
and return PASS or FAIL; it is not allowed to remove, replace, or re-rank a
cell after seeing the final lockbox.

The module is deliberately independent of the live executor.  The special
``FROZEN_PRELOCKBOX`` status is accepted only by the offline evidence replay
policy.  A funded ledger is materialized all-or-none after a joint PASS and
still requires the detached promotion authorization used by live trading.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple


PRELOCKBOX_ROSTER_SCHEMA_VERSION = "2026-07-11-global-prelockbox-roster-v2"
PRELOCKBOX_ROSTER_SEAL_ALGORITHM = "sha256-canonical-json-v1"
PRELOCKBOX_PHASE = "A_PRELOCKBOX_SELECTION_FROZEN"
FROZEN_PRELOCKBOX_STATUS = "FROZEN_PRELOCKBOX"
REQUIRED_TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")
REQUIRED_REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")
CRITICAL_MODULE_CODE_CONTRACT_VERSION = "2026-07-11-lockbox-executable-code-v3"
CRITICAL_MODULE_PATHS = (
    "pm_core.py",
    "pm_pipeline.py",
    "pm_main.py",
    "pm_mt5.py",
    "pm_live_readiness.py",
    "pm_live_outcomes.py",
    "pm_institutional_supervisor.py",
    "pm_cycle_drift.py",
    "pm_event_context.py",
    "pm_storage.py",
    "pm_optuna.py",
    "pm_portfolio_replay.py",
    "pm_statistics.py",
    "pm_risk_admission.py",
    "pm_symbol.py",
    "pm_regime.py",
    "pm_regime_tuner.py",
    "pm_regime_audit.py",
    "pm_strategies.py",
    "pm_position.py",
    "pm_metrics.py",
    "pm_evidence.py",
    "pm_fairness_audit.py",
    "pm_enhancement_seams.py",
    "pm_order_governance.py",
    "pm_cap_calibration.py",
    "pm_model_lifecycle.py",
    "pm_native_activation.py",
    "pm_native_parity.py",
    "pm_promotion_authorization.py",
    "pm_two_phase_lockbox.py",
    "agent_audit/performance/scripts/run_promoted_cell_replay.py",
    "agent_audit/performance/scripts/run_synchronized_portfolio_replay.py",
    "tools/acquire_native_history.py",
    "tools/audit_activate_native_history.py",
    "tools/audit_funded_config_surface.py",
    "tools/build_postlockbox_edge_challenger.py",
    "tools/calibrate_capital_envelope.py",
    "tools/issue_promotion_authorization.py",
    "tools/promotion_replay_audit.py",
    "tools/seal_prelockbox_roster.py",
)


def canonical_json_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def critical_module_code_contract(repo_root: Optional[Path] = None) -> Dict[str, Any]:
    """Hash every implementation that can alter selection or replay outcomes."""
    root = Path(repo_root or Path(__file__).resolve().parent).resolve()
    files: Dict[str, str] = {}
    missing: List[str] = []
    for relative in CRITICAL_MODULE_PATHS:
        path = root / Path(relative)
        if not path.is_file():
            missing.append(relative)
            continue
        files[relative.replace("\\", "/")] = sha256_file(path)
    contract = {
        "schema_version": CRITICAL_MODULE_CODE_CONTRACT_VERSION,
        "files": dict(sorted(files.items())),
        "missing": sorted(missing),
    }
    contract["aggregate_sha256"] = canonical_json_sha256(
        {
            "schema_version": contract["schema_version"],
            "files": contract["files"],
            "missing": contract["missing"],
        }
    )
    return contract


def _json_copy(payload: Any) -> Any:
    return json.loads(json.dumps(payload, default=str))


def _cell_contract(cell: Mapping[str, Any]) -> Dict[str, Any]:
    """Return immutable model bytes, excluding the later all-or-none release.

    Phase B may change only promotion status/reason and append its release
    pointer.  Everything capable of changing signals, sizing, selection, or
    order governance remains in this hash.
    """
    contract = _json_copy(dict(cell))
    contract.pop("promotion_status", None)
    contract.pop("promotion_reasons", None)
    evidence = contract.get("validation_evidence")
    if isinstance(evidence, dict):
        evidence.pop("two_phase_release", None)
    return contract


def _symbol_contract(symbol_payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Freeze symbol-level bytes while allowing only documented release fields.

    Phase B changes leaf promotion statuses and may set the symbol's convenience
    ``is_validated`` flag.  The separately generated edge book is also allowed
    to be attached later.  Every other symbol-level field remains immutable.
    """
    contract = _json_copy(dict(symbol_payload))
    contract.pop("regime_configs", None)
    contract.pop("is_validated", None)
    contract.pop("edge_challenger_book", None)
    return contract


def effective_replay_priority_contract(cell: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the single auditable priority value used by joint replay.

    Priority is frozen explicitly in the roster as well as implicitly in the
    full cell hash.  This prevents a later replay implementation from silently
    changing tie ordering without invalidating the critical-code contract.
    """
    evidence = (
        cell.get("validation_evidence")
        if isinstance(cell.get("validation_evidence"), Mapping)
        else {}
    )
    val_metrics = (
        cell.get("val_metrics")
        if isinstance(cell.get("val_metrics"), Mapping)
        else {}
    )
    choices = (
        ("quality_score", cell.get("quality_score")),
        ("validation_evidence.selection_score_raw", evidence.get("selection_score_raw")),
        ("val_metrics.return_drawdown_ratio", val_metrics.get("return_drawdown_ratio")),
    )
    for source, raw in choices:
        try:
            value = float(raw)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(value):
            return {"source": source, "value": value}
    return {"source": "constant_zero_fallback", "value": 0.0}


def _is_no_trade(cell: Mapping[str, Any]) -> bool:
    artifact = cell.get("artifact_meta")
    strategy_is_no_trade = (
        str(cell.get("strategy_name") or "").strip().upper() == "NO_TRADE"
    )
    artifact_is_no_trade = (
        bool(artifact.get("no_trade")) if isinstance(artifact, Mapping) else False
    )
    return strategy_is_no_trade or artifact_is_no_trade


def _symbol_payloads(ledger: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    payloads: Dict[str, Mapping[str, Any]] = {}
    for raw_symbol, raw_payload in ledger.items():
        if not isinstance(raw_payload, Mapping):
            continue
        symbol = str(raw_payload.get("symbol") or raw_symbol).strip().upper()
        if symbol:
            payloads[symbol] = raw_payload
    return payloads


def build_frozen_roster(
    ledger: Mapping[str, Any],
    symbols: Iterable[str],
) -> Tuple[Dict[str, Any], List[str]]:
    """Fingerprint every independent cell and reject final-outcome leakage."""
    requested = sorted(
        {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    )
    payloads = _symbol_payloads(ledger)
    rows: List[Dict[str, Any]] = []
    tradeable_ids: List[str] = []
    errors: List[str] = []
    raw_keys_by_symbol: Dict[str, List[str]] = {}
    for raw_symbol, raw_payload in ledger.items():
        if not isinstance(raw_payload, Mapping):
            continue
        symbol = str(raw_payload.get("symbol") or raw_symbol).strip().upper()
        if not symbol:
            continue
        raw_keys_by_symbol.setdefault(symbol, []).append(str(raw_symbol))
        if str(raw_symbol) != symbol:
            errors.append(f"PRELOCKBOX_SYMBOL_KEY_NONCANONICAL:{raw_symbol}->{symbol}")
    for symbol, raw_keys in sorted(raw_keys_by_symbol.items()):
        if len(raw_keys) != 1:
            errors.append(
                f"PRELOCKBOX_SYMBOL_DUPLICATE:{symbol}:" + ",".join(sorted(raw_keys))
            )

    for symbol in requested:
        symbol_payload = payloads.get(symbol)
        if not isinstance(symbol_payload, Mapping):
            errors.append(f"PRELOCKBOX_SYMBOL_MISSING:{symbol}")
            continue
        if dict(symbol_payload.get("holdout_metrics") or {}):
            errors.append(f"PRELOCKBOX_SYMBOL_HAS_FINAL_OUTCOMES:{symbol}")
        edge_book = symbol_payload.get("edge_challenger_book")
        if isinstance(edge_book, Mapping) and dict(edge_book.get("regime_configs") or {}):
            errors.append(f"PRELOCKBOX_EDGE_REFIT_PRESENT:{symbol}")
        grid = symbol_payload.get("regime_configs")
        if not isinstance(grid, Mapping):
            errors.append(f"PRELOCKBOX_GRID_MISSING:{symbol}")
            grid = {}
        for timeframe in REQUIRED_TIMEFRAMES:
            regime_map = grid.get(timeframe)
            if not isinstance(regime_map, Mapping):
                regime_map = {}
            for regime in REQUIRED_REGIMES:
                cell_id = f"{symbol}|{timeframe}|{regime}"
                cell = regime_map.get(regime)
                if not isinstance(cell, Mapping):
                    errors.append(f"PRELOCKBOX_CELL_MISSING:{cell_id}")
                    continue
                no_trade = _is_no_trade(cell)
                strategy = str(cell.get("strategy_name") or "").strip()
                status = str(cell.get("promotion_status") or "").strip().upper()
                if dict(cell.get("holdout_metrics") or {}):
                    errors.append(f"PRELOCKBOX_CELL_HAS_FINAL_OUTCOMES:{cell_id}")
                temporal = dict(
                    (cell.get("validation_evidence") or {}).get("temporal_lineage")
                    or {}
                ) if isinstance(cell.get("validation_evidence"), Mapping) else {}
                evidenced_through = temporal.get("performance_evidenced_through_utc")
                selection_cutoff = temporal.get("parameter_selection_cutoff_utc")
                if evidenced_through not in (None, "", selection_cutoff):
                    errors.append(f"PRELOCKBOX_TEMPORAL_LEAK:{cell_id}")
                if no_trade:
                    if status not in {"NO_TRADE", "BLOCKED"}:
                        errors.append(f"PRELOCKBOX_NO_TRADE_STATUS_INVALID:{cell_id}:{status}")
                else:
                    if not strategy:
                        errors.append(f"PRELOCKBOX_STRATEGY_MISSING:{cell_id}")
                    if status != FROZEN_PRELOCKBOX_STATUS:
                        errors.append(f"PRELOCKBOX_STATUS_INVALID:{cell_id}:{status or '<missing>'}")
                    tradeable_ids.append(cell_id)
                rows.append(
                    {
                        "cell_id": cell_id,
                        "strategy_name": strategy,
                        "source_promotion_status": status,
                        "no_trade": no_trade,
                        "frozen_candidate": not no_trade,
                        "parameters_sha256": canonical_json_sha256(
                            dict(cell.get("parameters") or {})
                        ),
                        "frozen_cell_contract_sha256": canonical_json_sha256(
                            _cell_contract(cell)
                        ),
                        "frozen_cell_sha256": canonical_json_sha256(cell),
                        "effective_replay_priority": effective_replay_priority_contract(
                            cell
                        ),
                    }
                )

    extra_symbols = sorted(set(payloads) - set(requested))
    if extra_symbols:
        errors.append("PRELOCKBOX_EXTRA_SYMBOLS:" + ",".join(extra_symbols))
    expected_count = len(requested) * len(REQUIRED_TIMEFRAMES) * len(REQUIRED_REGIMES)
    if len(rows) != expected_count:
        errors.append(f"PRELOCKBOX_CELL_COUNT:{len(rows)}!={expected_count}")
    try:
        final_start, final_end = global_final_lockbox_window(
            ledger, expected_symbols=requested
        )
    except ValueError as exc:
        errors.append(f"PRELOCKBOX_GLOBAL_FINAL_WINDOW_INVALID:{exc}")
        final_window = None
    else:
        final_window = {"start_ts": final_start, "end_ts": final_end}
    roster = {
        "symbols": requested,
        "required_timeframes": list(REQUIRED_TIMEFRAMES),
        "required_regimes": list(REQUIRED_REGIMES),
        "expected_cell_count": expected_count,
        "tradeable_cell_ids": sorted(tradeable_ids),
        "rows": sorted(rows, key=lambda row: str(row["cell_id"])),
        "symbol_contract_sha256": {
            symbol: canonical_json_sha256(_symbol_contract(payloads[symbol]))
            for symbol in requested
            if symbol in payloads
        },
        "global_final_lockbox_window": final_window,
    }
    roster["roster_sha256"] = canonical_json_sha256(roster["rows"])
    return roster, list(dict.fromkeys(errors))


def seal_prelockbox_roster(payload: Mapping[str, Any]) -> Dict[str, Any]:
    record = _json_copy(dict(payload))
    record.pop("content_seal", None)
    record["schema_version"] = PRELOCKBOX_ROSTER_SCHEMA_VERSION
    digest = canonical_json_sha256(record)
    record["content_seal"] = {
        "algorithm": PRELOCKBOX_ROSTER_SEAL_ALGORITHM,
        "sha256": digest,
    }
    return record


def prelockbox_roster_seal_valid(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    seal = payload.get("content_seal")
    if not isinstance(seal, Mapping):
        return False
    if str(seal.get("algorithm") or "") != PRELOCKBOX_ROSTER_SEAL_ALGORITHM:
        return False
    unsigned = _json_copy(dict(payload))
    unsigned.pop("content_seal", None)
    return str(seal.get("sha256") or "").lower() == canonical_json_sha256(unsigned)


def build_prelockbox_roster_artifact(
    *,
    ledger: Mapping[str, Any],
    symbols: Iterable[str],
    lineage: Mapping[str, Any],
) -> Dict[str, Any]:
    roster, errors = build_frozen_roster(ledger, symbols)
    if errors:
        raise ValueError("; ".join(errors))
    artifact = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phase": PRELOCKBOX_PHASE,
        "status": "FROZEN",
        "places_trades": False,
        "profitability_guarantee": False,
        "contract": {
            "selection_surfaces": ["optimizer_train", "tuning_validation"],
            "final_lockbox_outcomes_observed": False,
            "final_lockbox_may_only_accept_or_reject_joint_roster": True,
            "final_lockbox_cell_pruning_prohibited": True,
            "final_lockbox_reranking_prohibited": True,
            "survivor_only_replay_prohibited": True,
            "release_requires_detached_authorization": True,
        },
        "lineage": _json_copy(dict(lineage)),
        "roster": roster,
        "frozen_ledger": _json_copy(dict(ledger)),
    }
    return seal_prelockbox_roster(artifact)


def validate_prelockbox_roster_artifact(
    artifact: Any,
    *,
    expected_symbols: Optional[Iterable[str]] = None,
    expected_lineage: Optional[Mapping[str, Any]] = None,
) -> List[str]:
    errors: List[str] = []
    if not isinstance(artifact, Mapping):
        return ["PRELOCKBOX_ARTIFACT_UNREADABLE"]
    if str(artifact.get("schema_version") or "") != PRELOCKBOX_ROSTER_SCHEMA_VERSION:
        errors.append("PRELOCKBOX_SCHEMA_MISMATCH")
    if not prelockbox_roster_seal_valid(artifact):
        errors.append("PRELOCKBOX_SEAL_INVALID")
    if str(artifact.get("phase") or "") != PRELOCKBOX_PHASE:
        errors.append("PRELOCKBOX_PHASE_INVALID")
    if str(artifact.get("status") or "").upper() != "FROZEN":
        errors.append("PRELOCKBOX_NOT_FROZEN")
    contract = artifact.get("contract")
    if not isinstance(contract, Mapping):
        errors.append("PRELOCKBOX_CONTRACT_MISSING")
    else:
        required_true = (
            "final_lockbox_may_only_accept_or_reject_joint_roster",
            "final_lockbox_cell_pruning_prohibited",
            "final_lockbox_reranking_prohibited",
            "survivor_only_replay_prohibited",
            "release_requires_detached_authorization",
        )
        if bool(contract.get("final_lockbox_outcomes_observed", True)):
            errors.append("PRELOCKBOX_FINAL_OUTCOME_FLAG_INVALID")
        for key in required_true:
            if not bool(contract.get(key, False)):
                errors.append(f"PRELOCKBOX_CONTRACT_FALSE:{key}")
    frozen = artifact.get("frozen_ledger")
    roster = artifact.get("roster")
    if not isinstance(frozen, Mapping) or not isinstance(roster, Mapping):
        errors.append("PRELOCKBOX_FROZEN_LEDGER_OR_ROSTER_MISSING")
    else:
        symbols = list(expected_symbols) if expected_symbols is not None else list(
            roster.get("symbols") or []
        )
        rebuilt, rebuild_errors = build_frozen_roster(frozen, symbols)
        errors.extend(rebuild_errors)
        if canonical_json_sha256(rebuilt) != canonical_json_sha256(roster):
            errors.append("PRELOCKBOX_ROSTER_REBUILD_MISMATCH")
    if expected_symbols is not None and isinstance(roster, Mapping):
        expected = sorted(
            {str(symbol).strip().upper() for symbol in expected_symbols if str(symbol).strip()}
        )
        if list(roster.get("symbols") or []) != expected:
            errors.append("PRELOCKBOX_SYMBOL_ROSTER_MISMATCH")
    lineage = artifact.get("lineage") if isinstance(artifact.get("lineage"), Mapping) else {}
    for key, expected in dict(expected_lineage or {}).items():
        if canonical_json_sha256(lineage.get(key)) != canonical_json_sha256(expected):
            errors.append(f"PRELOCKBOX_LINEAGE_MISMATCH:{key}")
    return list(dict.fromkeys(errors))


def write_immutable_prelockbox_roster(
    output_dir: Path,
    artifact: Mapping[str, Any],
) -> Path:
    sealed = seal_prelockbox_roster(artifact)
    digest = str((sealed.get("content_seal") or {}).get("sha256") or "")
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"prelockbox_roster_{digest}.json"
    serialized = json.dumps(sealed, indent=2, sort_keys=True, ensure_ascii=False)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.write("\n")
    except FileExistsError:
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing != sealed:
            raise RuntimeError(f"immutable pre-lockbox path contains different bytes: {path}")
    return path


def load_pinned_prelockbox_roster(
    *,
    config_payload: Mapping[str, Any],
    config_path: Path,
    expected_symbols: Optional[Iterable[str]] = None,
    expected_lineage: Optional[Mapping[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[Path], List[str]]:
    pipeline = config_payload.get("pipeline")
    pipeline = pipeline if isinstance(pipeline, Mapping) else {}
    raw_path = str(pipeline.get("production_prelockbox_roster_path") or "").strip()
    configured_sha = str(
        pipeline.get("production_prelockbox_roster_sha256") or ""
    ).strip().lower()
    errors: List[str] = []
    if not raw_path:
        return None, None, ["PRELOCKBOX_PATH_MISSING"]
    if len(configured_sha) != 64 or any(ch not in "0123456789abcdef" for ch in configured_sha):
        return None, None, ["PRELOCKBOX_FILE_SHA256_NOT_PINNED"]
    path = Path(raw_path)
    if not path.is_absolute():
        path = Path(config_path).resolve().parent / path
    try:
        observed_sha = sha256_file(path)
        artifact = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return None, path, [f"PRELOCKBOX_UNREADABLE:{type(exc).__name__}"]
    if observed_sha.lower() != configured_sha:
        errors.append("PRELOCKBOX_FILE_SHA256_MISMATCH")
    errors.extend(
        validate_prelockbox_roster_artifact(
            artifact,
            expected_symbols=expected_symbols,
            expected_lineage=expected_lineage,
        )
    )
    return dict(artifact), path, list(dict.fromkeys(errors))


def iter_frozen_tradeable_cells(
    artifact: Mapping[str, Any],
) -> Iterator[Dict[str, Any]]:
    """Yield exact Phase-A cells without changing their evidence-only status."""
    frozen = artifact.get("frozen_ledger")
    roster = artifact.get("roster")
    if not isinstance(frozen, Mapping) or not isinstance(roster, Mapping):
        return
    ids = {str(value) for value in roster.get("tradeable_cell_ids") or []}
    payloads = _symbol_payloads(frozen)
    for cell_id in sorted(ids):
        try:
            symbol, timeframe, regime = cell_id.split("|")
            cell = payloads[symbol]["regime_configs"][timeframe][regime]
        except Exception:
            continue
        if not isinstance(cell, Mapping):
            continue
        payload = _json_copy(dict(cell))
        payload.update(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "regime": regime,
                "prelockbox_cell_id": cell_id,
            }
        )
        yield payload


def prelockbox_evidence_policy_from_config_payload(
    config_payload: Mapping[str, Any],
    starting_balance: float,
) -> Any:
    """Build replay/live-parity policy with only the evidence status changed."""
    from pm_portfolio_replay import portfolio_policy_from_config_payload

    live_policy = portfolio_policy_from_config_payload(config_payload, starting_balance)
    return replace(
        live_policy,
        allowed_promotion_statuses=(FROZEN_PRELOCKBOX_STATUS,),
    )


def global_final_lockbox_window(
    frozen_ledger: Mapping[str, Any],
    *,
    expected_symbols: Optional[Iterable[str]] = None,
) -> Tuple[str, str]:
    """Return one normalized final interval shared by every requested symbol."""
    payloads = _symbol_payloads(frozen_ledger)
    requested = (
        sorted(
            {
                str(symbol).strip().upper()
                for symbol in expected_symbols
                if str(symbol).strip()
            }
        )
        if expected_symbols is not None
        else sorted(payloads)
    )
    if not requested:
        raise ValueError("pre-lockbox ledger has no symbols")
    missing_symbols = sorted(set(requested) - set(payloads))
    if missing_symbols:
        raise ValueError(
            "pre-lockbox final window symbols missing: " + ",".join(missing_symbols)
        )
    pairs: set[Tuple[str, str]] = set()
    for symbol in requested:
        payload = payloads[symbol]
        data_meta = payload.get("data_window_meta")
        windows = data_meta.get("windows") if isinstance(data_meta, Mapping) else {}
        final = windows.get("final_lockbox") if isinstance(windows, Mapping) else {}
        if not isinstance(final, Mapping):
            raise ValueError(f"pre-lockbox final window missing for {symbol}")
        start = str(final.get("start_ts") or "").strip()
        end = str(final.get("end_ts") or final.get("end_ts_exclusive") or "").strip()
        if not start or not end:
            raise ValueError(f"pre-lockbox final window incomplete for {symbol}")
        try:
            parsed_start = datetime.fromisoformat(start.replace("Z", "+00:00"))
            parsed_end = datetime.fromisoformat(end.replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                f"pre-lockbox final window timestamp invalid for {symbol}"
            ) from exc
        if parsed_start.tzinfo is None:
            parsed_start = parsed_start.replace(tzinfo=timezone.utc)
        if parsed_end.tzinfo is None:
            parsed_end = parsed_end.replace(tzinfo=timezone.utc)
        parsed_start = parsed_start.astimezone(timezone.utc)
        parsed_end = parsed_end.astimezone(timezone.utc)
        if parsed_start >= parsed_end:
            raise ValueError(f"pre-lockbox final window non-positive for {symbol}")
        pairs.add((parsed_start.isoformat(), parsed_end.isoformat()))
    if len(pairs) != 1:
        raise ValueError("pre-lockbox ledger does not carry one common final window")
    return next(iter(pairs))


def resolve_lockbox_registry_path(config: Any, config_path: Path) -> Path:
    raw = str(getattr(config, "lockbox_access_registry_path", "") or "").strip()
    base = Path(config_path).resolve().parent
    if raw:
        path = Path(raw)
        return path if path.is_absolute() else base / path
    output = Path(getattr(config, "output_dir", "pm_outputs/production_live"))
    if not output.is_absolute():
        output = base / output
    return output / "lockbox_access_registry.json"


def consume_global_final_lockbox_once(
    *,
    config: Any,
    config_path: Path,
    frozen_ledger: Mapping[str, Any],
    roster_sha256: str,
    prelockbox_file_sha256: str,
) -> str:
    """Atomically burn Phase B before a caller opens any final price source."""
    from pm_statistics import LockboxAccessRegistry

    start, end = global_final_lockbox_window(frozen_ledger)
    decision = LockboxAccessRegistry(
        resolve_lockbox_registry_path(config, config_path)
    ).consume(
        cell={
            "symbol": "__GLOBAL_PORTFOLIO__",
            "timeframe": "ALL",
            "regime": "ALL",
        },
        lineage={
            "source_sha256": str(prelockbox_file_sha256),
            "window_start": start,
            "window_end": end,
            "data_window_fingerprint": str(roster_sha256),
            "evidence_contract_version": "two_phase_joint_final_lockbox_v1",
        },
    )
    if not decision.allowed:
        raise RuntimeError(
            "FINAL_LOCKBOX_ALREADY_CONSUMED_OR_OVERLAPPING: "
            f"{decision.reason}; access_id={decision.access_id}"
        )
    return decision.access_id


def validate_global_final_lockbox_access(
    *,
    config: Any,
    config_path: Path,
    access_id: str,
    frozen_ledger: Mapping[str, Any],
    roster_sha256: str,
    prelockbox_file_sha256: str,
) -> List[str]:
    """Verify the sealed PASS points to the persisted exact one-shot record."""
    errors: List[str] = []
    path = resolve_lockbox_registry_path(config, config_path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        accesses = payload.get("accesses")
        record = accesses.get(str(access_id)) if isinstance(accesses, Mapping) else None
    except Exception as exc:
        return [f"GLOBAL_LOCKBOX_REGISTRY_UNREADABLE:{type(exc).__name__}"]
    if not isinstance(record, Mapping):
        return ["GLOBAL_LOCKBOX_ACCESS_RECORD_MISSING"]
    expected_start, expected_end = global_final_lockbox_window(frozen_ledger)
    cell = record.get("cell") if isinstance(record.get("cell"), Mapping) else {}
    lineage = record.get("lineage") if isinstance(record.get("lineage"), Mapping) else {}
    if cell != {
        "symbol": "__GLOBAL_PORTFOLIO__",
        "timeframe": "ALL",
        "regime": "ALL",
    }:
        errors.append("GLOBAL_LOCKBOX_ACCESS_CELL_INVALID")
    expected = {
        "source_sha256": str(prelockbox_file_sha256),
        "window_start": expected_start,
        "window_end": expected_end,
        "data_window_fingerprint": str(roster_sha256),
        "evidence_contract_version": "two_phase_joint_final_lockbox_v1",
    }
    for key, value in expected.items():
        if str(lineage.get(key) or "") != str(value):
            errors.append(f"GLOBAL_LOCKBOX_ACCESS_LINEAGE_MISMATCH:{key}")
    if str(record.get("access_id") or "") != str(access_id):
        errors.append("GLOBAL_LOCKBOX_ACCESS_ID_MISMATCH")
    return errors


def materialize_joint_pass_ledger(
    artifact: Mapping[str, Any],
    *,
    portfolio_evidence_sha256: str,
) -> Dict[str, Any]:
    """Release the complete frozen roster after one joint PASS, never survivors."""
    errors = validate_prelockbox_roster_artifact(artifact)
    if errors:
        raise ValueError("invalid pre-lockbox artifact: " + ", ".join(errors))
    evidence_sha = str(portfolio_evidence_sha256 or "").strip().lower()
    if len(evidence_sha) != 64 or any(ch not in "0123456789abcdef" for ch in evidence_sha):
        raise ValueError("portfolio evidence SHA-256 must be pinned before release")
    released = _json_copy(artifact["frozen_ledger"])
    released_payloads = _symbol_payloads(released)
    tradeable = set((artifact.get("roster") or {}).get("tradeable_cell_ids") or [])
    for cell_id in sorted(tradeable):
        symbol, timeframe, regime = cell_id.split("|")
        cell = released_payloads[symbol]["regime_configs"][timeframe][regime]
        cell["promotion_status"] = "PROMOTED"
        cell["promotion_reasons"] = [
            "complete frozen global roster passed the one-shot joint final-lockbox replay",
            "funded execution still requires detached exact-cell authorization",
        ]
        evidence = cell.get("validation_evidence")
        evidence = dict(evidence) if isinstance(evidence, Mapping) else {}
        evidence["two_phase_release"] = {
            "prelockbox_roster_sha256": (artifact.get("roster") or {}).get(
                "roster_sha256"
            ),
            "portfolio_evidence_sha256": evidence_sha,
            "release_scope": "ALL_FROZEN_TRADEABLE_CELLS",
            "survivor_pruning_performed": False,
            "detached_authorization_required": True,
        }
        cell["validation_evidence"] = evidence
        released_payloads[symbol]["is_validated"] = True
    return released


def validate_released_ledger_against_prelockbox(
    ledger: Mapping[str, Any],
    artifact: Mapping[str, Any],
    *,
    expected_portfolio_evidence_sha256: Optional[str] = None,
) -> List[str]:
    """Prove that a funded ledger is an all-or-none status-only release."""
    errors = validate_prelockbox_roster_artifact(artifact)
    if errors:
        return errors
    rows = {
        str(row.get("cell_id")): row
        for row in (artifact.get("roster") or {}).get("rows") or []
        if isinstance(row, Mapping)
    }
    required = set((artifact.get("roster") or {}).get("tradeable_cell_ids") or [])
    expected_cells = set(rows)
    expected_symbols = set((artifact.get("roster") or {}).get("symbols") or [])
    symbol_contracts = dict(
        (artifact.get("roster") or {}).get("symbol_contract_sha256") or {}
    )
    frozen_roster_sha = str(
        (artifact.get("roster") or {}).get("roster_sha256") or ""
    ).lower()
    expected_evidence_sha = str(expected_portfolio_evidence_sha256 or "").lower()
    payloads = _symbol_payloads(ledger)
    raw_keys_by_symbol: Dict[str, List[str]] = {}
    for raw_symbol, raw_payload in ledger.items():
        if not isinstance(raw_payload, Mapping):
            continue
        symbol = str(raw_payload.get("symbol") or raw_symbol).strip().upper()
        if not symbol:
            continue
        raw_keys_by_symbol.setdefault(symbol, []).append(str(raw_symbol))
        if str(raw_symbol) != symbol:
            errors.append(f"RELEASED_SYMBOL_KEY_NONCANONICAL:{raw_symbol}->{symbol}")
    for symbol, raw_keys in sorted(raw_keys_by_symbol.items()):
        if len(raw_keys) != 1:
            errors.append(
                f"RELEASED_SYMBOL_DUPLICATE:{symbol}:" + ",".join(sorted(raw_keys))
            )
    for symbol in sorted(expected_symbols - set(payloads)):
        errors.append(f"RELEASED_SYMBOL_MISSING:{symbol}")
    for symbol in sorted(set(payloads) - expected_symbols):
        errors.append(f"RELEASED_EXTRA_SYMBOL:{symbol}")
    for symbol in sorted(expected_symbols & set(payloads)):
        expected_symbol_contract = str(symbol_contracts.get(symbol) or "")
        if (
            len(expected_symbol_contract) != 64
            or canonical_json_sha256(_symbol_contract(payloads[symbol]))
            != expected_symbol_contract
        ):
            errors.append(f"RELEASED_SYMBOL_MUTATED:{symbol}")
    observed_promoted: set[str] = set()
    for cell_id in sorted(expected_cells):
        try:
            symbol, timeframe, regime = cell_id.split("|")
            cell = payloads[symbol]["regime_configs"][timeframe][regime]
        except Exception:
            errors.append(f"RELEASED_CELL_MISSING:{cell_id}")
            continue
        if not isinstance(cell, Mapping):
            errors.append(f"RELEASED_CELL_INVALID:{cell_id}")
            continue
        if cell_id not in required:
            if canonical_json_sha256(cell) != str(
                rows[cell_id].get("frozen_cell_sha256") or ""
            ):
                errors.append(f"RELEASED_NO_TRADE_CELL_MUTATED:{cell_id}")
            continue
        if str(cell.get("promotion_status") or "").upper() != "PROMOTED":
            errors.append(f"RELEASED_CELL_NOT_PROMOTED:{cell_id}")
        else:
            observed_promoted.add(cell_id)
        validation_evidence = cell.get("validation_evidence")
        release = (
            validation_evidence.get("two_phase_release")
            if isinstance(validation_evidence, Mapping)
            and isinstance(validation_evidence.get("two_phase_release"), Mapping)
            else {}
        )
        release_evidence_sha = str(
            release.get("portfolio_evidence_sha256") or ""
        ).lower()
        if (
            str(release.get("prelockbox_roster_sha256") or "").lower()
            != frozen_roster_sha
            or release.get("release_scope") != "ALL_FROZEN_TRADEABLE_CELLS"
            or release.get("survivor_pruning_performed") is not False
            or release.get("detached_authorization_required") is not True
            or len(release_evidence_sha) != 64
        ):
            errors.append(f"RELEASED_CELL_RELEASE_MARKER_INVALID:{cell_id}")
        elif expected_evidence_sha and release_evidence_sha != expected_evidence_sha:
            errors.append(f"RELEASED_CELL_EVIDENCE_MISMATCH:{cell_id}")
        expected_contract = str(rows[cell_id].get("frozen_cell_contract_sha256") or "")
        if canonical_json_sha256(_cell_contract(cell)) != expected_contract:
            errors.append(f"RELEASED_CELL_MUTATED:{cell_id}")
    for symbol, payload in payloads.items():
        grid = payload.get("regime_configs") if isinstance(payload, Mapping) else {}
        if not isinstance(grid, Mapping):
            continue
        for timeframe, regime_map in grid.items():
            if not isinstance(regime_map, Mapping):
                continue
            for regime, cell in regime_map.items():
                cell_id = f"{symbol}|{str(timeframe).upper()}|{str(regime).upper()}"
                if cell_id not in expected_cells:
                    errors.append(f"RELEASED_EXTRA_CELL:{cell_id}")
                if not isinstance(cell, Mapping):
                    continue
                status = str(cell.get("promotion_status") or "").upper()
                if status == "PROMOTED" and cell_id not in required:
                    errors.append(f"RELEASED_EXTRA_PROMOTED_CELL:{cell_id}")
    if observed_promoted != required:
        errors.append("RELEASED_ROSTER_NOT_ALL_OR_NONE")
    return list(dict.fromkeys(errors))


def write_immutable_released_ledger(output_dir: Path, ledger: Mapping[str, Any]) -> Path:
    payload = _json_copy(dict(ledger))
    digest = canonical_json_sha256(payload)
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"funded_winner_ledger_{digest}.json"
    serialized = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.write("\n")
    except FileExistsError:
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing != payload:
            raise RuntimeError(f"immutable released-ledger path contains different bytes: {path}")
    return path


__all__ = [
    "FROZEN_PRELOCKBOX_STATUS",
    "PRELOCKBOX_PHASE",
    "PRELOCKBOX_ROSTER_SCHEMA_VERSION",
    "build_frozen_roster",
    "build_prelockbox_roster_artifact",
    "canonical_json_sha256",
    "critical_module_code_contract",
    "consume_global_final_lockbox_once",
    "effective_replay_priority_contract",
    "global_final_lockbox_window",
    "iter_frozen_tradeable_cells",
    "load_pinned_prelockbox_roster",
    "materialize_joint_pass_ledger",
    "prelockbox_evidence_policy_from_config_payload",
    "prelockbox_roster_seal_valid",
    "sha256_file",
    "validate_global_final_lockbox_access",
    "validate_prelockbox_roster_artifact",
    "validate_released_ledger_against_prelockbox",
    "write_immutable_prelockbox_roster",
    "write_immutable_released_ledger",
]
