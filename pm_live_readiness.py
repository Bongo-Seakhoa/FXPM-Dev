"""Fail-closed live-readiness validation for FXPM."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
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
    if not bool(getattr(config, "walk_forward_audit_enabled", False)):
        add("warning", "walk_forward_disabled", "walk-forward audit is disabled; promotion falls back to validation/historical audit")

    regime_params_file = str(getattr(config, "regime_params_file", "") or "")
    if regime_params_file and not Path(regime_params_file).exists():
        add("warning", "regime_params_missing", f"regime params file not found: {regime_params_file}")

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
        missing = sorted({str(symbol).upper() for symbol in symbols} - ledger_symbols)
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
