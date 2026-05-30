"""Fail-closed live-readiness validation for FXPM."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class ReadinessFinding:
    severity: str
    code: str
    message: str


@dataclass
class ReadinessReport:
    ok: bool
    findings: List[ReadinessFinding] = field(default_factory=list)

    @property
    def errors(self) -> List[ReadinessFinding]:
        return [finding for finding in self.findings if finding.severity == "error"]

    @property
    def warnings(self) -> List[ReadinessFinding]:
        return [finding for finding in self.findings if finding.severity == "warning"]


def _load_json_file(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _resolve_relative_to_config(path_raw: Any, config_path: str) -> Path:
    path = Path(str(path_raw))
    if path.is_absolute():
        return path
    return Path(config_path).resolve().parent / path


def _broker_specs_by_symbol(specs: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    mapping: Dict[str, Dict[str, Any]] = {}
    for key, payload in specs.items():
        if not isinstance(payload, dict):
            continue
        for candidate in (key, payload.get("symbol"), payload.get("broker_symbol")):
            if str(candidate or "").strip():
                mapping[str(candidate).upper()] = payload
    return mapping


def _broker_spec_field_issues(specs: Dict[str, Any], symbols: Iterable[str]) -> List[str]:
    required_positive = (
        "point",
        "tick_size",
        "tick_value",
        "contract_size",
        "volume_min",
        "volume_max",
        "volume_step",
    )
    issues: List[str] = []
    spec_by_symbol = _broker_specs_by_symbol(specs)
    for symbol in symbols:
        payload = spec_by_symbol.get(str(symbol).upper())
        if not isinstance(payload, dict):
            continue
        if payload.get("digits") is None:
            issues.append(f"{symbol}: missing digits")
        else:
            try:
                if int(payload.get("digits")) < 0:
                    issues.append(f"{symbol}: invalid digits={payload.get('digits')}")
            except Exception:
                issues.append(f"{symbol}: invalid digits={payload.get('digits')}")
        if payload.get("spread") is None:
            issues.append(f"{symbol}: missing spread")
        else:
            try:
                if float(payload.get("spread")) < 0.0:
                    issues.append(f"{symbol}: invalid spread={payload.get('spread')}")
            except Exception:
                issues.append(f"{symbol}: invalid spread={payload.get('spread')}")
        for field_name in required_positive:
            try:
                value = float(payload.get(field_name, 0.0) or 0.0)
            except Exception:
                value = 0.0
            if value <= 0.0:
                issues.append(f"{symbol}: invalid {field_name}={payload.get(field_name)}")
    return issues


def _iter_symbol_configs(ledger: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(ledger, dict):
        if isinstance(ledger.get("symbols"), dict):
            for value in ledger["symbols"].values():
                if isinstance(value, dict):
                    yield value
            return
        if isinstance(ledger.get("symbol_configs"), dict):
            for value in ledger["symbol_configs"].values():
                if isinstance(value, dict):
                    yield value
            return
        if all(isinstance(value, dict) for value in ledger.values()):
            for value in ledger.values():
                yield value
            return
    if isinstance(ledger, list):
        for value in ledger:
            if isinstance(value, dict):
                yield value


def _config_has_tradeable_winner(config: Dict[str, Any]) -> bool:
    def _tradeable(payload: Dict[str, Any]) -> bool:
        name = str(payload.get("strategy_name", "") or "").upper()
        meta = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
        status = str(payload.get("promotion_status", "PROMOTED") or "PROMOTED").upper()
        non_live_statuses = {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}
        return bool(name and name != "NO_TRADE" and not meta.get("no_trade") and status not in non_live_statuses)

    if isinstance(config.get("default_config"), dict) and _tradeable(config["default_config"]):
        return True
    regimes = config.get("regime_configs")
    if isinstance(regimes, dict):
        for tf_payload in regimes.values():
            if not isinstance(tf_payload, dict):
                continue
            for regime_payload in tf_payload.values():
                if isinstance(regime_payload, dict) and _tradeable(regime_payload):
                    return True
    return _tradeable(config)


def validate_live_readiness(config: Any,
                            *,
                            symbols: Iterable[str],
                            config_path: str = "config.json",
                            winner_ledger_path: Optional[str] = None) -> ReadinessReport:
    findings: List[ReadinessFinding] = []

    def add(severity: str, code: str, message: str) -> None:
        findings.append(ReadinessFinding(severity=severity, code=code, message=message))

    if str(getattr(config, "live_artifact_drift_policy", "block") or "").lower() != "block":
        add("error", "artifact_drift_not_blocking", "live_artifact_drift_policy must be 'block' for live trading")

    if not bool(getattr(config, "institutional_supervisor_enabled", True)):
        add("warning", "supervisor_disabled", "institutional supervisor is disabled")
    else:
        # Check whether a previous run latched the supervisor, while preserving
        # the supervisor's manual_or_next_cycle reopen semantics.
        try:
            raw_log_dir = getattr(config, "log_dir", "logs")
            log_dir = Path(raw_log_dir if isinstance(raw_log_dir, (str, Path)) else "logs")
            supervisor_state_path = log_dir / "institutional_supervisor_state.json"
            if supervisor_state_path.exists():
                state = _load_json_file(supervisor_state_path) or {}
                if bool(state.get("active_block")):
                    reopen_mode = str(
                        getattr(config, "hard_block_reopen_mode", "manual_or_next_cycle") or ""
                    ).strip().lower()
                    blocked_cycle = str(state.get("cycle_id", "") or "")
                    try:
                        current_cycle = str(config.get_last_retrain_slot(datetime.now()).isoformat())
                    except Exception:
                        current_cycle = ""
                    next_cycle_reopens = (
                        reopen_mode in {"manual_or_next_cycle", "next_cycle"}
                        and bool(current_cycle)
                        and bool(blocked_cycle)
                        and current_cycle != blocked_cycle
                    )
                    if next_cycle_reopens:
                        add(
                            "warning",
                            "supervisor_latch_from_previous_cycle",
                            "institutional supervisor latch belongs to a previous cycle; supervisor will reopen on evaluation",
                        )
                    else:
                        reasons = ", ".join(str(r) for r in (state.get("block_reasons") or [])) or "unknown"
                        add(
                            "error",
                            "supervisor_latched_from_previous_run",
                            f"institutional supervisor is latched (reasons: {reasons}); reset before live trading",
                        )
        except Exception:
            pass
    if not bool(getattr(config, "walk_forward_audit_enabled", False)):
        add("warning", "walk_forward_disabled", "walk-forward audit is disabled; promotion falls back to validation/historical audit")

    regime_params_file = str(getattr(config, "regime_params_file", "") or "")
    if regime_params_file and not Path(regime_params_file).exists():
        add("warning", "regime_params_missing", f"regime params file not found: {regime_params_file}")

    requested_symbols = sorted({str(symbol).upper() for symbol in symbols if str(symbol).strip()})
    config_payload = _load_json_file(Path(config_path)) or {}
    broker_specs_raw = config_payload.get("broker_specs_path") if isinstance(config_payload, dict) else None
    if not broker_specs_raw:
        add(
            "warning",
            "broker_specs_path_missing",
            "broker_specs_path is not configured; backtest/live contract parity cannot be verified before launch",
        )
    else:
        broker_specs_path = _resolve_relative_to_config(broker_specs_raw, config_path)
        broker_specs = _load_json_file(broker_specs_path)
        if not isinstance(broker_specs, dict) or not broker_specs:
            add(
                "error",
                "broker_specs_unreadable",
                f"broker specs file is missing, empty, or unreadable: {broker_specs_path}",
            )
        else:
            covered_symbols = set(_broker_specs_by_symbol(broker_specs))
            missing_specs = sorted(set(requested_symbols) - covered_symbols)
            if missing_specs:
                add(
                    "error",
                    "broker_specs_missing_symbols",
                    f"broker specs missing symbols: {', '.join(missing_specs)}",
                )
            field_issues = _broker_spec_field_issues(broker_specs, requested_symbols)
            if field_issues:
                preview = "; ".join(field_issues[:8])
                if len(field_issues) > 8:
                    preview += f"; +{len(field_issues) - 8} more"
                add("error", "broker_specs_invalid_fields", f"broker specs contain invalid execution fields: {preview}")

    ledger_raw = winner_ledger_path or getattr(config, "winner_ledger_path", "pm_configs.json")
    ledger_path = Path(str(ledger_raw))
    if not ledger_path.is_absolute():
        ledger_path = Path(config_path).resolve().parent / ledger_path
    ledger = _load_json_file(ledger_path)
    if ledger is None:
        add("error", "winner_ledger_unreadable", f"winner ledger is missing or unreadable: {ledger_path}")
    else:
        symbol_configs = list(_iter_symbol_configs(ledger))
        if not symbol_configs:
            add("error", "winner_ledger_empty", f"winner ledger has no symbol configs: {ledger_path}")
        elif not any(_config_has_tradeable_winner(payload) for payload in symbol_configs):
            add("error", "winner_ledger_no_tradeable_winners", "winner ledger contains no tradeable winners")
        ledger_symbols = {
            str(payload.get("symbol", "") or "").upper()
            for payload in symbol_configs
            if str(payload.get("symbol", "") or "").strip()
        }
        missing = sorted(set(requested_symbols) - ledger_symbols)
        if missing:
            add("warning", "symbols_missing_from_ledger", f"symbols missing from winner ledger: {', '.join(missing)}")

    return ReadinessReport(ok=not any(f.severity == "error" for f in findings), findings=findings)


def log_readiness_report(logger: Any, report: ReadinessReport) -> None:
    for finding in report.findings:
        line = f"Live readiness {finding.severity.upper()} [{finding.code}]: {finding.message}"
        if finding.severity == "error":
            logger.error(line)
        elif finding.severity == "warning":
            logger.warning(line)
        else:
            logger.info(line)
