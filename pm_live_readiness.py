"""Fail-closed live-readiness validation for FXPM."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set


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

    has_nested_configs = False
    if isinstance(config.get("default_config"), dict) and _tradeable(config["default_config"]):
        return True
    if isinstance(config.get("default_config"), dict):
        has_nested_configs = True
    regimes = config.get("regime_configs")
    if isinstance(regimes, dict):
        has_nested_configs = True
        for tf_payload in regimes.values():
            if not isinstance(tf_payload, dict):
                continue
            for regime_payload in tf_payload.values():
                if isinstance(regime_payload, dict) and _tradeable(regime_payload):
                    return True
    if has_nested_configs:
        return False
    return _tradeable(config)


def _tradeable_cell_ids_from_ledger(ledger: Any) -> Set[str]:
    """Return deployable SYMBOL|TIMEFRAME|REGIME cells from a winner ledger."""

    def _tradeable(payload: Dict[str, Any]) -> bool:
        name = str(payload.get("strategy_name", "") or "").upper()
        meta = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
        status = str(payload.get("promotion_status", "PROMOTED") or "PROMOTED").upper()
        return bool(
            name
            and name != "NO_TRADE"
            and not meta.get("no_trade")
            and status not in {"NO_TRADE", "BLOCKED", "PAPER_ONLY"}
        )

    cells: Set[str] = set()
    for symbol_payload in _iter_symbol_configs(ledger):
        symbol = str(symbol_payload.get("symbol", "") or "").strip().upper()
        if not symbol:
            continue
        regimes = symbol_payload.get("regime_configs")
        if isinstance(regimes, dict):
            for timeframe, regime_payload in regimes.items():
                tf = str(timeframe or "").strip().upper()
                if not isinstance(regime_payload, dict) or not tf:
                    continue
                for regime, payload in regime_payload.items():
                    rg = str(regime or "").strip().upper()
                    if isinstance(payload, dict) and rg and _tradeable(payload):
                        cells.add(f"{symbol}|{tf}|{rg}")
            continue
        if _tradeable(symbol_payload):
            tf = str(symbol_payload.get("timeframe", "") or "").strip().upper() or "LEGACY"
            cells.add(f"{symbol}|{tf}|LEGACY")
    return cells


def _cells_missing_tuned_regime_params(cells: Iterable[str], regime_params_file: str) -> Set[str]:
    """Return deployable regime cells that lack explicit tuned regime params."""
    try:
        from pm_regime import has_tuned_regime_params
    except Exception:
        return {str(cell) for cell in cells}

    missing: Set[str] = set()
    for cell in cells:
        parts = str(cell or "").split("|")
        if len(parts) != 3:
            missing.add(str(cell))
            continue
        symbol, timeframe, _regime = (part.strip().upper() for part in parts)
        if not symbol or not timeframe:
            missing.add(str(cell))
            continue
        if timeframe == "LEGACY":
            continue
        if not has_tuned_regime_params(symbol, timeframe, regime_params_file):
            missing.add(f"{symbol}|{timeframe}|{_regime}")
    return missing


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if not str(value or "").strip():
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _ledger_live_expiry_issues(ledger: Any, now: datetime, grace_minutes: int = 0) -> List[str]:
    issues: List[str] = []
    grace = timedelta(minutes=max(0, int(grace_minutes or 0)))
    for symbol_payload in _iter_symbol_configs(ledger):
        if not _config_has_tradeable_winner(symbol_payload):
            continue
        symbol = _symbol_label(symbol_payload)
        valid_until = _parse_iso_datetime(symbol_payload.get("valid_until"))
        if valid_until is None:
            issues.append(f"{symbol}: no expiry date")
            continue
        if now >= valid_until + grace:
            issues.append(f"{symbol}: expired at {valid_until.strftime('%Y-%m-%d %H:%M')}")
    return issues


def _promote_cell_ids_from_review(review_payload: Any) -> Set[str]:
    cells: Set[str] = set()
    if not isinstance(review_payload, dict):
        return cells
    for row in review_payload.get("rows") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("final_verdict", "") or "").upper() != "PROMOTE":
            continue
        symbol = str(row.get("symbol", "") or "").strip().upper()
        timeframe = str(row.get("timeframe", "") or "").strip().upper()
        regime = str(row.get("regime", "") or "").strip().upper()
        if symbol and timeframe and regime:
            cells.add(f"{symbol}|{timeframe}|{regime}")
    return cells


def _preview_set(values: Iterable[str], limit: int = 12) -> str:
    ordered = sorted(str(value) for value in values)
    preview = ", ".join(ordered[:limit])
    if len(ordered) > limit:
        preview += f", +{len(ordered) - limit} more"
    return preview


def _symbol_label(config: Dict[str, Any], fallback: str = "unknown") -> str:
    value = str(config.get("symbol", "") or "").strip().upper()
    return value or fallback


def validate_live_readiness(config: Any,
                            *,
                            symbols: Iterable[str],
                            config_path: str = "config.json",
                            winner_ledger_path: Optional[str] = None,
                            mt5_config: Optional[Any] = None,
                            account_info: Optional[Any] = None,
                            position_config: Optional[Any] = None) -> ReadinessReport:
    findings: List[ReadinessFinding] = []

    def add(severity: str, code: str, message: str) -> None:
        findings.append(ReadinessFinding(severity=severity, code=code, message=message))

    if str(getattr(config, "live_artifact_drift_policy", "block") or "").lower() != "block":
        add("error", "artifact_drift_not_blocking", "live_artifact_drift_policy must be 'block' for live trading")

    manual_entries = bool(getattr(config, "manual_new_entries_enabled", True))
    if not manual_entries:
        add(
            "error",
            "manual_new_entries_disabled",
            "manual_new_entries_enabled is false; live entry startup is intentionally blocked",
        )
    manual_path_raw = str(getattr(config, "manual_kill_switch_path", "") or "").strip()
    if manual_path_raw:
        manual_path = _resolve_relative_to_config(manual_path_raw, config_path)
        if manual_path.exists():
            try:
                content = manual_path.read_text(encoding="utf-8").strip().lower()
            except Exception:
                content = "unreadable"
            if content in {"0", "false", "off", "block", "blocked", "pause", "paused", "kill"}:
                add(
                    "error",
                    "manual_kill_switch_active",
                    f"manual kill switch blocks new entries: {manual_path}",
                )

    if mt5_config is not None:
        expected_login = int(getattr(mt5_config, "expected_login", 0) or 0)
        expected_server = str(getattr(mt5_config, "expected_server", "") or "").strip()
        expected_currency = str(getattr(mt5_config, "expected_currency", "") or "").strip()
        if not (expected_login or expected_server or expected_currency):
            add(
                "error",
                "mt5_account_identity_not_pinned",
                "real live trading requires non-secret mt5.expected_login/server/currency pinning",
            )
        if expected_login or expected_server or expected_currency:
            if account_info is None:
                add(
                    "error",
                    "mt5_account_identity_unverified",
                    "expected MT5 account identity is configured but account_info is unavailable",
                )
            else:
                actual_login = int(getattr(account_info, "login", 0) or 0)
                actual_server = str(getattr(account_info, "server", "") or "").strip()
                actual_currency = str(getattr(account_info, "currency", "") or "").strip()
                if expected_login and actual_login != expected_login:
                    add(
                        "error",
                        "mt5_account_login_mismatch",
                        f"MT5 account login mismatch: expected {expected_login}, actual {actual_login}",
                    )
                if expected_server and actual_server.lower() != expected_server.lower():
                    add(
                        "error",
                        "mt5_account_server_mismatch",
                        f"MT5 server mismatch: expected {expected_server}, actual {actual_server or 'unknown'}",
                    )
                if expected_currency and actual_currency.upper() != expected_currency.upper():
                    add(
                        "error",
                        "mt5_account_currency_mismatch",
                        f"MT5 account currency mismatch: expected {expected_currency}, actual {actual_currency or 'unknown'}",
                    )

    min_account_equity = 0.0
    try:
        min_account_equity = float(getattr(config, "live_min_account_equity", 0.0) or 0.0)
    except (TypeError, ValueError, OverflowError):
        min_account_equity = 0.0
    if min_account_equity > 0.0:
        if account_info is None:
            add(
                "error",
                "account_equity_unverified",
                "live_min_account_equity is configured but account_info is unavailable",
            )
        else:
            try:
                actual_equity = float(getattr(account_info, "equity", 0.0) or 0.0)
            except (TypeError, ValueError, OverflowError):
                actual_equity = 0.0
            if actual_equity < min_account_equity:
                add(
                    "error",
                    "account_equity_below_live_minimum",
                    f"connected MT5 equity {actual_equity:.2f} is below live_min_account_equity {min_account_equity:.2f}",
                )
            min_currency = str(getattr(config, "live_min_account_equity_currency", "") or "").strip()
            actual_currency = str(getattr(account_info, "currency", "") or "").strip()
            if min_currency and actual_currency and actual_currency.upper() != min_currency.upper():
                add(
                    "error",
                    "account_equity_currency_mismatch",
                    f"connected MT5 currency {actual_currency!r} does not match live minimum currency {min_currency!r}",
                )

    if position_config is not None and account_info is not None:
        try:
            equity = float(getattr(account_info, "equity", 0.0) or 0.0)
            risk_pct = float(getattr(position_config, "risk_per_trade_pct", 0.0) or 0.0)
            max_risk_pct = float(getattr(position_config, "max_risk_pct", 0.0) or 0.0)
        except (TypeError, ValueError, OverflowError):
            equity = risk_pct = max_risk_pct = 0.0
        if equity > 0.0 and risk_pct > 0.0:
            risk_amount = equity * risk_pct / 100.0
            if risk_amount <= 0.0:
                add(
                    "error",
                    "account_risk_budget_invalid",
                    f"risk budget is non-positive at equity={equity:.2f}, risk_per_trade_pct={risk_pct:.4f}",
                )
            if max_risk_pct > 0.0 and risk_pct > max_risk_pct:
                add(
                    "error",
                    "risk_pct_exceeds_hard_cap",
                    f"risk_per_trade_pct {risk_pct:.4f}% exceeds max_risk_pct {max_risk_pct:.4f}%",
                )

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

    storage_floor_raw = getattr(config, "storage_pause_entries_below_free_gb", None)
    if storage_floor_raw not in (None, "", False):
        try:
            storage_floor = float(storage_floor_raw)
        except Exception:
            storage_floor = 0.0
        if storage_floor > 0.0:
            try:
                check_root = Path(config_path).resolve().parent
                free_gb = shutil.disk_usage(check_root).free / (1024 ** 3)
                if free_gb < storage_floor:
                    add(
                        "warning",
                        "storage_free_space_below_advisory_floor",
                        f"free disk space {free_gb:.2f}GB is below storage advisory floor {storage_floor:.2f}GB",
                    )
            except Exception as exc:
                add(
                    "warning",
                    "storage_free_space_check_failed",
                    f"could not verify free disk space before launch: {type(exc).__name__}: {exc}",
                )

    regime_params_file = str(getattr(config, "regime_params_file", "") or "")
    regime_params_missing = bool(regime_params_file and not Path(regime_params_file).exists())

    requested_symbols = sorted({str(symbol).upper() for symbol in symbols if str(symbol).strip()})
    symbol_pool_target = 0
    try:
        symbol_pool_target = int(getattr(config, "production_symbol_pool_target_count", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        symbol_pool_target = 0
    if symbol_pool_target > 0 and len(requested_symbols) != symbol_pool_target:
        add(
            "error",
            "production_symbol_pool_count_mismatch",
            f"configured symbol pool has {len(requested_symbols)} symbols, expected {symbol_pool_target}",
        )
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
        expiry_issues = _ledger_live_expiry_issues(
            ledger,
            datetime.now(),
            grace_minutes=int(getattr(config, "live_config_expiry_grace_minutes", 0) or 0),
        )
        if expiry_issues:
            preview = "; ".join(expiry_issues[:8])
            if len(expiry_issues) > 8:
                preview += f"; +{len(expiry_issues) - 8} more"
            add(
                "error",
                "winner_ledger_not_live_current",
                f"winner ledger is not current for live trading: {preview}",
            )
        ledger_symbols = {
            str(payload.get("symbol", "") or "").upper()
            for payload in symbol_configs
            if str(payload.get("symbol", "") or "").strip()
        }
        missing = sorted(set(requested_symbols) - ledger_symbols)
        if missing:
            allow_partial_live = bool(getattr(config, "live_allow_partial_symbol_ledger", False))
            severity = (
                "warning"
                if allow_partial_live
                else ("error" if bool(getattr(config, "production_require_full_symbol_ledger", False)) else "warning")
            )
            code = "production_symbol_ledger_incomplete" if severity == "error" else "symbols_missing_from_ledger"
            preview = ", ".join(missing[:20])
            if len(missing) > 20:
                preview += f", +{len(missing) - 20} more"
            add(severity, code, f"symbols missing from winner ledger: {preview}")
        try:
            from pm_pipeline import artifact_contract_matches, build_artifact_meta

            current_artifact_meta = build_artifact_meta(config)
            drifted_symbols = []
            regime_missing_matches_artifact = True
            for idx, payload in enumerate(symbol_configs):
                if not _config_has_tradeable_winner(payload):
                    continue
                stored_artifact = payload.get("artifact_meta") if isinstance(payload.get("artifact_meta"), dict) else {}
                if not artifact_contract_matches(stored_artifact, current_artifact_meta):
                    drifted_symbols.append(_symbol_label(payload, fallback=f"entry_{idx}"))
                stored_regime = stored_artifact.get("regime_params")
                if regime_params_missing and isinstance(stored_regime, dict):
                    if stored_regime.get("mtime_ns") is not None or stored_regime.get("size") is not None:
                        regime_missing_matches_artifact = False
                elif regime_params_missing:
                    regime_missing_matches_artifact = False
            if drifted_symbols:
                preview = ", ".join(drifted_symbols[:12])
                if len(drifted_symbols) > 12:
                    preview += f", +{len(drifted_symbols) - 12} more"
                add(
                    "error",
                    "winner_ledger_artifact_drift",
                    f"winner ledger artifacts do not match the current optimization/cost contract: {preview}",
                )
            if regime_params_missing and not regime_missing_matches_artifact:
                add("warning", "regime_params_missing", f"regime params file not found: {regime_params_file}")
        except Exception as exc:
            add(
                "warning",
                "winner_ledger_artifact_check_failed",
                f"could not verify winner ledger artifact contract: {type(exc).__name__}: {exc}",
            )
            if regime_params_missing:
                add("warning", "regime_params_missing", f"regime params file not found: {regime_params_file}")

    tradeable_cells = _tradeable_cell_ids_from_ledger(ledger) if ledger is not None else set()
    if bool(getattr(config, "live_require_tuned_regime_params", False)) and tradeable_cells:
        missing_tuned_params = _cells_missing_tuned_regime_params(tradeable_cells, regime_params_file)
        if missing_tuned_params:
            add(
                "error",
                "live_tuned_regime_params_missing",
                "live entries require explicitly tuned regime params; missing tuned entries for "
                f"{_preview_set(missing_tuned_params)}"
                + (f" in {regime_params_file}" if regime_params_file else " because regime_params_file is empty"),
            )

    if bool(getattr(config, "production_require_promotion_gate", False)):
        gate_mode = str(getattr(config, "production_promotion_gate_mode", "external_review_exact") or "").strip().lower()
        if gate_mode in {"ledger", "current_ledger", "ledger_authoritative"}:
            gate_mode = "ledger_status"
        elif gate_mode in {"external", "exact", "review", "review_exact"}:
            gate_mode = "external_review_exact"
        if gate_mode not in {"external_review_exact", "ledger_status"}:
            add(
                "error",
                "production_promotion_gate_mode_invalid",
                f"invalid production_promotion_gate_mode: {getattr(config, 'production_promotion_gate_mode', None)!r}",
            )
            gate_mode = "external_review_exact"
        if gate_mode == "ledger_status":
            if ledger is not None and not tradeable_cells:
                add(
                    "error",
                    "production_promotion_ledger_no_tradeable_cells",
                    "production promotion gate uses ledger_status but the winner ledger has no tradeable promoted cells",
                )
            return ReadinessReport(ok=not any(f.severity == "error" for f in findings), findings=findings)

        review_raw = str(getattr(config, "production_promotion_review_path", "") or "").strip()
        safety_raw = str(getattr(config, "production_promotion_cell_safety_path", "") or "").strip()
        marker_raw = str(getattr(config, "production_completion_marker_path", "") or "").strip()

        if not review_raw:
            add(
                "error",
                "production_promotion_review_missing",
                "production_require_promotion_gate is enabled but production_promotion_review_path is empty",
            )
            review_payload = None
        else:
            review_path = _resolve_relative_to_config(review_raw, config_path)
            review_payload = _load_json_file(review_path)
            if not isinstance(review_payload, dict):
                add(
                    "error",
                    "production_promotion_review_unreadable",
                    f"promotion review artifact is missing or unreadable: {review_path}",
                )

        promote_cells = _promote_cell_ids_from_review(review_payload)
        if review_raw and isinstance(review_payload, dict) and not promote_cells:
            add(
                "error",
                "production_promotion_review_no_promotes",
                "promotion review artifact contains no final PROMOTE cells",
            )

        expected_cells = {
            str(cell or "").strip().upper()
            for cell in (getattr(config, "production_expected_promote_cells", []) or [])
            if str(cell or "").strip()
        }
        if expected_cells and promote_cells and promote_cells != expected_cells:
            add(
                "error",
                "production_expected_promote_cells_mismatch",
                "expected production promote cells do not match promotion review: "
                f"missing={_preview_set(expected_cells - promote_cells) or 'none'}; "
                f"extra={_preview_set(promote_cells - expected_cells) or 'none'}",
            )
        if promote_cells and tradeable_cells != promote_cells:
            add(
                "error",
                "production_winner_ledger_promotion_mismatch",
                "tradeable winner ledger cells do not exactly match corrected promotion PROMOTE cells: "
                f"ledger_only={_preview_set(tradeable_cells - promote_cells) or 'none'}; "
                f"review_only={_preview_set(promote_cells - tradeable_cells) or 'none'}",
            )

        if not safety_raw:
            add(
                "error",
                "production_promotion_safety_missing",
                "production_require_promotion_gate is enabled but production_promotion_cell_safety_path is empty",
            )
        else:
            safety_path = _resolve_relative_to_config(safety_raw, config_path)
            safety_payload = _load_json_file(safety_path)
            if not isinstance(safety_payload, dict):
                add(
                    "error",
                    "production_promotion_safety_unreadable",
                    f"promotion cell-safety artifact is missing or unreadable: {safety_path}",
                )
            else:
                summary = (
                    safety_payload.get("navigation_counts")
                    if isinstance(safety_payload.get("navigation_counts"), dict)
                    else None
                )
                if summary is None:
                    summary = safety_payload.get("summary") if isinstance(safety_payload.get("summary"), dict) else safety_payload

                def _safety_count(name: str) -> int:
                    value = summary.get(name, safety_payload.get(name, 0))
                    if isinstance(value, list):
                        return len(value)
                    return int(value or 0)

                unsafe = _safety_count("unsafe_promotes")
                fail_not_reject = _safety_count("fail_not_reject")
                final_promotes = _safety_count("final_promotes")
                if unsafe or fail_not_reject:
                    add(
                        "error",
                        "production_promotion_safety_failed",
                        f"promotion/cell safety artifact reports unsafe_promotes={unsafe}, fail_not_reject={fail_not_reject}",
                    )
                if promote_cells and final_promotes != len(promote_cells):
                    add(
                        "error",
                        "production_promotion_safety_count_mismatch",
                        f"promotion safety final_promotes={final_promotes} but review PROMOTE count={len(promote_cells)}",
                    )

        if not marker_raw:
            add(
                "error",
                "production_completion_marker_missing",
                "production_require_promotion_gate is enabled but production_completion_marker_path is empty",
            )
        else:
            marker_path = _resolve_relative_to_config(marker_raw, config_path)
            marker_payload = _load_json_file(marker_path)
            if not isinstance(marker_payload, dict):
                add(
                    "error",
                    "production_completion_marker_unreadable",
                    f"completion marker is missing or unreadable: {marker_path}",
                )
            else:
                verdict = str(marker_payload.get("verdict", "") or "").upper()
                if not bool(marker_payload.get("review_ready", False)):
                    add(
                        "error",
                        "production_completion_marker_not_review_ready",
                        "completion marker is not marked review_ready",
                    )
                if bool(marker_payload.get("stale")):
                    add("error", "production_completion_marker_stale", "completion marker is marked stale")
                if bool(marker_payload.get("pid_alive")):
                    add("error", "production_completion_marker_active_pid", "completion marker still reports pid_alive=true")
                if "CELL_SAFE" not in verdict:
                    add(
                        "error",
                        "production_completion_marker_not_cell_safe",
                        f"completion marker verdict is not cell-safe: {marker_payload.get('verdict')}",
                    )

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
