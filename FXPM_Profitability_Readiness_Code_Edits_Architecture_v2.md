# FXPM Profitability-Readiness Blueprint v2

**Document purpose:** This is a revised implementation blueprint after incorporating the FXPM philosophy that each `(symbol, timeframe, regime)` winner acts as an autonomous specialist trader. The revised architecture avoids unnecessary top-down interference while adding the minimum institutional controls needed to protect profitability, sustainability, and live funding confidence.

**Core position:** I do **not** vehemently disagree with your specialist-manager model. I think it is the correct philosophical center for FXPM. My earlier portfolio-risk recommendation should be refined: the portfolio layer should **not** become a constant correlation veto or top-down allocator that suppresses the best local opportunities. Instead, it should become a **circuit-breaker and institutional supervisor layer** that intervenes only when aggregate system behavior becomes dangerous, clearly degraded, or operationally untrustworthy.

The highest-profitability version of FXPM should therefore be:

```text
Autonomous specialist managers
    +
Local order-management tournaments
    +
Causal profitability-aligned regime selection
    +
Fast two-week reselection cycle
    +
Lightweight institutional safety breakpoints
    +
Fail-closed deployment readiness
```

not:

```text
Autonomous specialists
    +
Heavy portfolio optimizer that second-guesses every signal
```

and not:

```text
Research winners
    +
No institutional protection once many specialists are active simultaneously
```

---

## 1. Revised verdict after your clarification

Your clarification materially changes how the blueprint should be framed.

### Where I agree with you

I agree that FXPM should not be turned into a conventional top-down portfolio allocation engine. The core value proposition is that each local context can identify its best specialist:

```text
symbol × timeframe × regime → best strategy + best params + best order policy
```

If that local specialist is valid, current, and live-eligible, suppressing it merely because another signal looks correlated can easily harm profitability. In a system designed to discover independent local edges, excessive portfolio-level filters can remove the very opportunity breadth that makes FXPM powerful.

I also agree that the two-week retrain cadence is not incidental. It is part of the edge. The system is trying to capture a fresh market-surface advantage before it decays. Adding a long extra validation layer between optimization and live trading can reduce the expected edge.

I further agree that the live order-management pool is underdeveloped relative to the strategy pool. With 47 entry strategies but only around four governance/order-management policies, the system may be leaving a large portion of profitability on the table after entry. This is now elevated to a top-priority profitability upgrade.

### Where I partially disagree

I do not think a two-week retrain cadence eliminates the need for live drift controls. It reduces the need for a heavy model-lifecycle apparatus, but it does not protect against:

```text
broker spread regime changing inside the two-week cycle
data-feed discontinuity
wrong active regime selection
execution slippage expanding
MT5 symbol metadata changing
a local winner suffering immediate live mismatch
margin stress from multiple active specialists
platform restart / stale artifact / stale ledger issues
```

So the right compromise is not “heavy drift monitor.” It is a **lightweight cycle guard**. It should not constantly derate or second-guess local winners. It should only detect severe within-cycle failure modes and pause, derate, or require review when the evidence is strong.

### Where I strongly disagree

I strongly disagree with having no real aggregate fail-safe. Even if every specialist is locally valid, the combined book can still become unsustainable. This is not a contradiction of the specialist model. It is the same reason a trading house allows specialists to trade independently but still has firm-level drawdown, margin, and catastrophic-loss rules.

The key is that the institutional layer should not say:

```text
“Do not take EURUSD because GBPUSD is already long.”
```

It should say:

```text
“New entries are paused because aggregate open risk, margin stress, daily loss, or execution degradation has crossed a hard breakpoint.”
```

That is a very different architecture. The first suppresses local edge. The second protects survival.

---

## 2. Revised target architecture

### 2.1 Architecture in one sentence

FXPM should be structured as a federation of autonomous specialist managers supervised by a lightweight institutional safety layer.

### 2.2 Specialist manager model

Each local manager is defined by:

```text
SpecialistManagerKey:
    symbol
    timeframe
    regime

SpecialistManagerArtifact:
    strategy_name
    parameters
    validation evidence
    regime evidence
    cost evidence
    order_management_policy
    artifact fingerprint
    valid_until
    promotion_status
```

A specialist is allowed to trade only when:

```text
1. Its artifact is current.
2. Its validation evidence meets gates.
3. Its regime label is live-causal and valid.
4. Its order-management policy is known.
5. Broker and execution preflight pass.
6. The institutional supervisor has not triggered a hard breakpoint.
```

The institutional supervisor should not normally rank, reallocate, or filter the specialists. It should only stop new risk when the system is unsafe.

### 2.3 Institutional supervisor model

The supervisor is not a portfolio optimizer. It is a capital-preservation shell.

It directly evaluates:

```text
daily realized loss
session realized loss
weekly realized loss
open drawdown
margin level
total open risk
```

Companion live gates and sentinels monitor the remaining operational evidence:

```text
broker execution degradation
artifact validity
ledger completeness
severe live-cycle drift
data continuity
```

It normally returns:

```python
SupervisorDecision(allow_new_entries=True, risk_multiplier=1.0, reason="normal")
```

It intervenes only under hard breakpoints:

```python
SupervisorDecision(allow_new_entries=False, risk_multiplier=0.0, reason="daily_loss_hard_block")
```

or, optionally, under a narrow derating mode:

```python
SupervisorDecision(allow_new_entries=True, risk_multiplier=0.5, reason="execution_degradation")
```

### 2.4 Revised architecture diagram

```text
                       ┌──────────────────────────┐
                       │       config.json         │
                       │ risk, validation, modes   │
                       └─────────────┬────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         OPTIMIZATION FACTORY                         │
│                                                                      │
│  M5 data window → features → causal regimes → strategy tournament     │
│      → per-regime winners → governance tournament → artifacts         │
│                                                                      │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        WINNER LEDGER / ARTIFACTS                     │
│                                                                      │
│  symbol → timeframe → regime → strategy + params + policy + evidence │
│                                                                      │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                            LIVE TRADING LOOP                         │
│                                                                      │
│  New bar → features → REGIME_LIVE → exact local winner lookup         │
│      → signal → local order policy → broker preflight                 │
│                                                                      │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    INSTITUTIONAL SUPERVISOR                          │
│                                                                      │
│  Fail-closed deployment checks                                        │
│  Hard system drawdown/loss breakpoints                                │
│  Margin and execution safety                                          │
│  Lightweight two-week-cycle drift sentinel                            │
│                                                                      │
│  Normal behavior: allow specialists to act                            │
│  Emergency behavior: pause/derate/block new entries                   │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         MT5 EXECUTION                                │
│                                                                      │
│  symbol resolution, tick, broker metadata, order_check, order_send    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 3. Updated recommendation map

| Topic | Revised stance |
|---|---|
| Portfolio/correlation control | Do **not** add constant correlation vetoes. Add institutional hard breakpoints only. |
| Same-symbol controls | Keep existing same-symbol geometry controls; they are local safety, not top-down suppression. |
| Portfolio-wide risk | Add high-threshold system risk circuit breakers, not per-signal allocation interference. |
| Live drift | Add lightweight cycle drift sentinel only. Do not build a heavy drift platform. |
| Walk-forward validation | Do not add a stale intermediary layer. Add same-cycle fragility audit and severe-failure blocker. |
| Order management | Elevate to top priority. Expand governance pool and validate per specialist. |
| Strategy pool | Expand from 47 to 50-52 only with genuinely distinct edge archetypes. |
| Regime accuracy | Treat as central. Add regime profitability audit, regime confidence metrics, and boundary diagnostics. |
| Carry/news/currency strength | Implement as optional strategy/archetype or cost modeling, not broad portfolio suppression. |
| Fair comparison | Preserve common assumptions; add fairness telemetry and family-size-aware validation metrics. |

---

# Part A — Required config edits

## A1. Immediate config edits

### File

```text
config.json
```

### Current issues to address

The current active config contains:

```json
"live_artifact_drift_policy": "warn",
"walk_forward_audit_enabled": false,
"local_governance_live_mode": "on",
"daily_loss_advisory_pct": 10.0,
"session_loss_advisory_pct": 10.0,
"market_driven_exit_pack_mode": "off"
```

The revised version should distinguish between:

```text
1. autonomous specialist freedom
2. local order-management expansion
3. institutional hard breakpoints
4. lightweight cycle monitoring
```

### Recommended patch

```json
{
  "pipeline": {
    "live_artifact_drift_policy": "block",

    "institutional_supervisor_enabled": true,
    "institutional_supervisor_mode": "circuit_breaker",
    "portfolio_control_policy": "hard_breakpoints_only",

    "max_total_open_risk_pct": 8.0,
    "max_new_entries_when_total_open_risk_above_pct": 6.0,
    "open_risk_action": "pause_new_entries",

    "daily_loss_hard_block_pct": 3.0,
    "session_loss_hard_block_pct": 2.0,
    "weekly_loss_hard_block_pct": 6.0,
    "open_drawdown_hard_block_pct": 8.0,
    "hard_block_reopen_mode": "manual_or_next_cycle",

    "cycle_drift_monitor_enabled": true,
    "cycle_drift_mode": "lightweight",
    "cycle_drift_min_trades": 6,
    "cycle_drift_consecutive_losses": 5,
    "cycle_drift_execution_slippage_mult": 2.5,
    "cycle_drift_spread_degradation_mult": 2.0,
    "cycle_drift_action": "pause_context",

    "walk_forward_audit_enabled": true,
    "walk_forward_audit_mode": "severe_fragility_only",
    "walk_forward_promotion_policy": "block_severe_only",
    "walk_forward_do_not_displace_latest_surface": true,

    "local_governance_tournament_enabled": true,
    "local_governance_live_mode": "shadow",
    "local_governance_candidate_policies": [
      "control_fixed",
      "breakeven_1r",
      "stepped_breakeven",
      "atr_trail_capped",
      "chandelier_atr",
      "structure_trail",
      "partial_tp_runner",
      "time_decay_exit",
      "volatility_contraction_exit",
      "regime_flip_reduce"
    ],

    "market_driven_exit_pack_mode": "paper",

    "swap_model_enabled": true,
    "swap_model_triple_rollover_day": "wednesday",
    "swap_model_min_holding_hours": 20,

    "regime_profitability_audit_enabled": true,
    "regime_boundary_audit_enabled": true,
    "regime_ambiguity_policy": "observe_only",
    "regime_min_live_gap_warn": 0.05
  }
}
```

### Why these settings fit FXPM philosophy

`portfolio_control_policy = "hard_breakpoints_only"` explicitly prevents the institutional layer from acting as a constant signal suppressor.

`max_total_open_risk_pct` and hard loss gates protect the account when the aggregate system is clearly misaligned. They do not block a valid specialist merely because it is correlated with another signal.

`local_governance_live_mode = "shadow"` is recommended initially because the governance pool is about to expand. Once the expanded policies have artifact evidence, it can be set to `"on"`.

`market_driven_exit_pack_mode = "paper"` lets the system measure possible exit improvements without changing live orders prematurely.

`walk_forward_promotion_policy = "block_severe_only"` avoids destroying the fresh two-week edge while still blocking obviously fragile winners.

---

# Part B — PipelineConfig additions

## B1. Add new config fields

### File

```text
pm_core.py
```

### Insertion point

Inside:

```python
@dataclass
class PipelineConfig:
```

after existing live/advisory risk fields, near:

```python
daily_loss_advisory_pct: float = 0.0
session_loss_advisory_pct: float = 0.0
```

### Add

```python
    # ===== INSTITUTIONAL SUPERVISOR (HARD BREAKPOINTS ONLY) =====
    # This is not a top-down portfolio optimizer. It is a safety shell.
    institutional_supervisor_enabled: bool = True
    institutional_supervisor_mode: str = "circuit_breaker"  # off|observe|circuit_breaker
    portfolio_control_policy: str = "hard_breakpoints_only"

    # Aggregate open-risk safety. High threshold by design: protects survival
    # without suppressing normal specialist-manager opportunity capture.
    max_total_open_risk_pct: float = 8.0
    max_new_entries_when_total_open_risk_above_pct: float = 6.0
    open_risk_action: str = "pause_new_entries"  # observe|pause_new_entries|derate

    # Hard system-level loss gates. These are firm-level breakpoints.
    daily_loss_hard_block_pct: float = 0.0
    session_loss_hard_block_pct: float = 0.0
    weekly_loss_hard_block_pct: float = 0.0
    open_drawdown_hard_block_pct: float = 0.0
    hard_block_reopen_mode: str = "manual_or_next_cycle"

    # ===== LIGHTWEIGHT CYCLE DRIFT SENTINEL =====
    # Two-week retraining reduces model-age drift, but does not eliminate
    # severe within-cycle execution/data/regime failures.
    cycle_drift_monitor_enabled: bool = True
    cycle_drift_mode: str = "lightweight"  # off|observe|lightweight
    cycle_drift_min_trades: int = 6
    cycle_drift_consecutive_losses: int = 5
    cycle_drift_execution_slippage_mult: float = 2.5
    cycle_drift_spread_degradation_mult: float = 2.0
    cycle_drift_action: str = "pause_context"  # observe|pause_context|derate_context

    # ===== WALK-FORWARD / FRAGILITY AUDIT =====
    # Must not displace the latest live selection surface unless severe
    # fragility is detected.
    walk_forward_audit_mode: str = "severe_fragility_only"
    walk_forward_promotion_policy: str = "block_severe_only"
    walk_forward_do_not_displace_latest_surface: bool = True

    # ===== COST REALISM =====
    swap_model_enabled: bool = True
    swap_model_triple_rollover_day: str = "wednesday"
    swap_model_min_holding_hours: float = 20.0

    # ===== REGIME PROFITABILITY AUDIT =====
    regime_profitability_audit_enabled: bool = True
    regime_boundary_audit_enabled: bool = True
    regime_ambiguity_policy: str = "observe_only"  # observe_only|derate|no_trade_when_extreme
    regime_min_live_gap_warn: float = 0.05
```

### Normalization rules to add in `PipelineConfig.__post_init__`

Find or create the normalization block in `PipelineConfig.__post_init__`.

Add:

```python
        valid_supervisor_modes = {"off", "observe", "circuit_breaker"}
        self.institutional_supervisor_mode = str(
            getattr(self, "institutional_supervisor_mode", "circuit_breaker")
        ).strip().lower()
        if self.institutional_supervisor_mode not in valid_supervisor_modes:
            logger.warning(
                "Invalid institutional_supervisor_mode=%r; using circuit_breaker",
                self.institutional_supervisor_mode,
            )
            self.institutional_supervisor_mode = "circuit_breaker"

        valid_portfolio_policies = {"hard_breakpoints_only"}
        self.portfolio_control_policy = str(
            getattr(self, "portfolio_control_policy", "hard_breakpoints_only")
        ).strip().lower()
        if self.portfolio_control_policy not in valid_portfolio_policies:
            logger.warning(
                "Unsupported portfolio_control_policy=%r; using hard_breakpoints_only",
                self.portfolio_control_policy,
            )
            self.portfolio_control_policy = "hard_breakpoints_only"

        valid_drift_modes = {"off", "observe", "lightweight"}
        self.cycle_drift_mode = str(
            getattr(self, "cycle_drift_mode", "lightweight")
        ).strip().lower()
        if self.cycle_drift_mode not in valid_drift_modes:
            logger.warning("Invalid cycle_drift_mode=%r; using lightweight", self.cycle_drift_mode)
            self.cycle_drift_mode = "lightweight"

        valid_wf_modes = {"off", "observe", "severe_fragility_only"}
        self.walk_forward_audit_mode = str(
            getattr(self, "walk_forward_audit_mode", "severe_fragility_only")
        ).strip().lower()
        if self.walk_forward_audit_mode not in valid_wf_modes:
            logger.warning("Invalid walk_forward_audit_mode=%r; using severe_fragility_only",
                           self.walk_forward_audit_mode)
            self.walk_forward_audit_mode = "severe_fragility_only"

        valid_wf_policies = {"observe", "block_severe_only", "block_any_failure"}
        self.walk_forward_promotion_policy = str(
            getattr(self, "walk_forward_promotion_policy", "block_severe_only")
        ).strip().lower()
        if self.walk_forward_promotion_policy not in valid_wf_policies:
            logger.warning("Invalid walk_forward_promotion_policy=%r; using block_severe_only",
                           self.walk_forward_promotion_policy)
            self.walk_forward_promotion_policy = "block_severe_only"
```

---

# Part C — Live readiness gate

## C1. Add a fail-closed deployment readiness module

### New file

```text
pm_live_readiness.py
```

### Purpose

This is the missing bridge between research and live deployment. It does not optimize or suppress strategy signals. It answers one question before live trading starts:

```text
Are artifact drift policy and the active winner ledger safe enough to allow funded live startup?
```

### Code skeleton

```python
# pm_live_readiness.py

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ReadinessFinding:
    severity: str          # pass|warn|block
    code: str
    message: str
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReadinessReport:
    allow_live: bool
    findings: List[ReadinessFinding] = field(default_factory=list)

    def add(self, severity: str, code: str, message: str, **context: Any) -> None:
        self.findings.append(ReadinessFinding(severity, code, message, context))

    @property
    def blockers(self) -> List[ReadinessFinding]:
        return [f for f in self.findings if f.severity == "block"]


def _load_json_file(path: Path) -> Optional[Any]:
    try:
        if not path.exists():
            return None
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        return json.loads(raw)
    except Exception as exc:
        logger.warning("Failed reading %s: %s", path, exc)
        return None


def validate_live_readiness(
    *,
    pipeline_config: Any,
    config_data: Dict[str, Any],
    repo_root: Path = Path("."),
    require_regime_params: bool = False,
) -> ReadinessReport:
    report = ReadinessReport(allow_live=False)

    ledger_path = Path(getattr(pipeline_config, "winner_ledger_path", "pm_configs.json"))
    if not ledger_path.is_absolute():
        ledger_path = repo_root / ledger_path

    ledger = _load_json_file(ledger_path)
    if not isinstance(ledger, dict) or not ledger:
        report.add(
            "block",
            "winner_ledger_empty",
            "Active winner ledger is missing or empty. Live trading must not start.",
            path=str(ledger_path),
        )

    drift_policy = str(getattr(pipeline_config, "live_artifact_drift_policy", "block")).lower()
    if drift_policy != "block":
        report.add(
            "block",
            "artifact_drift_not_blocking",
            "live_artifact_drift_policy must be 'block' before funded live trading.",
            current=drift_policy,
        )

    if require_regime_params:
        regime_path = Path(getattr(pipeline_config, "regime_params_file", "regime_params.json"))
        if not regime_path.is_absolute():
            regime_path = repo_root / regime_path
        regime_params = _load_json_file(regime_path)
        if not isinstance(regime_params, dict) or not regime_params:
            report.add(
                "block",
                "regime_params_missing",
                "Regime params are required by deployment policy but missing/empty.",
                path=str(regime_path),
            )
    else:
        report.add(
            "warn",
            "regime_params_optional",
            "Regime params not required by policy. Defaults may be used if explicitly accepted.",
        )

    if bool(getattr(pipeline_config, "walk_forward_audit_enabled", False)):
        wf_policy = str(getattr(pipeline_config, "walk_forward_promotion_policy", "observe")).lower()
        if wf_policy == "observe":
            report.add(
                "warn",
                "walk_forward_observe_only",
                "Walk-forward audit is observe-only. This preserves edge but does not block severe fragility.",
            )
    else:
        report.add(
            "warn",
            "walk_forward_disabled",
            "Walk-forward fragility audit is disabled.",
        )

    supervisor_enabled = bool(getattr(pipeline_config, "institutional_supervisor_enabled", False))
    if not supervisor_enabled:
        report.add(
            "warn",
            "institutional_supervisor_disabled",
            "Institutional supervisor is disabled. Local specialists may trade without aggregate breakpoints.",
        )

    symbols = config_data.get("symbols") or []
    if not symbols:
        report.add(
            "warn",
            "symbols_missing",
            "No explicit symbols list in config. Runtime may fall back to defaults.",
        )

    report.allow_live = len(report.blockers) == 0
    return report


def log_readiness_report(report: ReadinessReport) -> None:
    for finding in report.findings:
        log_fn = logger.info
        if finding.severity == "warn":
            log_fn = logger.warning
        elif finding.severity == "block":
            log_fn = logger.error
        log_fn("[%s] %s: %s %s", finding.severity.upper(), finding.code, finding.message, finding.context)
```

## C2. Call readiness gate before live trading

### File

```text
pm_main.py
```

### Insertion point

Before constructing or starting the live trading loop in the CLI branch for `--trade`.

### Add import

```python
from pm_live_readiness import validate_live_readiness, log_readiness_report
```

### Add call

```python
        readiness = validate_live_readiness(
            pipeline_config=pipeline_config,
            config_data=config_data,
            repo_root=Path("."),
            require_regime_params=False,
        )
        log_readiness_report(readiness)

        if not readiness.allow_live and not args.paper:
            logger.error("Live readiness gate failed; refusing funded live trading.")
            return 2
```

### Why this fits FXPM

This is not a top-down trading constraint. It does not second-guess valid specialists. It only ensures the deployment surface is complete and trustworthy before money is at risk.

---

# Part D — Institutional supervisor, hard breakpoints only

## D1. Add `pm_institutional_supervisor.py`

### New file

```text
pm_institutional_supervisor.py
```

### Purpose

Protect the whole FXPM system when aggregate behavior becomes unacceptable. It does not block ordinary specialist opportunities due to correlation or overlap.

### Code skeleton

```python
# pm_institutional_supervisor.py

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


@dataclass
class SupervisorContext:
    account_balance: float
    account_equity: float
    account_margin_level: float
    open_positions: List[Any] = field(default_factory=list)
    estimated_total_open_risk_pct: float = 0.0
    realized_daily_loss_pct: float = 0.0
    realized_session_loss_pct: float = 0.0
    realized_weekly_loss_pct: float = 0.0
    open_drawdown_pct: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SupervisorDecision:
    allow_new_entries: bool = True
    risk_multiplier: float = 1.0
    reason: str = "normal"
    severity: str = "pass"  # pass|warn|block
    notes: List[str] = field(default_factory=list)


class InstitutionalSupervisor:
    # Firm-level guardrail for FXPM.
    #
    # This class intentionally does NOT:
    # - rank strategies,
    # - veto ordinary correlation,
    # - allocate capital top-down,
    # - choose between specialists.
    #
    # It only blocks/derates when hard breakpoints are reached.

    def __init__(self, config: Any, state_path: str = "pm_outputs/institutional_supervisor_state.json"):
        self.config = config
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        try:
            if self.state_path.exists():
                return json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to load supervisor state: %s", exc)
        return {
            "blocked": False,
            "block_reason": "",
            "blocked_at": None,
            "block_cycle_id": None,
        }

    def _save_state(self) -> None:
        tmp = self.state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.state, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.state_path)

    def evaluate(self, context: SupervisorContext) -> SupervisorDecision:
        if not bool(getattr(self.config, "institutional_supervisor_enabled", True)):
            return SupervisorDecision(True, 1.0, "supervisor_disabled", "warn")

        mode = str(getattr(self.config, "institutional_supervisor_mode", "circuit_breaker")).lower()
        if mode == "off":
            return SupervisorDecision(True, 1.0, "supervisor_off", "warn")

        if self.state.get("blocked") and str(getattr(self.config, "hard_block_reopen_mode", "")).lower() == "manual_or_next_cycle":
            return SupervisorDecision(
                allow_new_entries=False,
                risk_multiplier=0.0,
                reason=self.state.get("block_reason", "persistent_hard_block"),
                severity="block",
                notes=["Supervisor remains blocked until manual reset or next approved retrain cycle."],
            )

        checks = [
            ("daily_loss_hard_block", context.realized_daily_loss_pct, getattr(self.config, "daily_loss_hard_block_pct", 0.0)),
            ("session_loss_hard_block", context.realized_session_loss_pct, getattr(self.config, "session_loss_hard_block_pct", 0.0)),
            ("weekly_loss_hard_block", context.realized_weekly_loss_pct, getattr(self.config, "weekly_loss_hard_block_pct", 0.0)),
            ("open_drawdown_hard_block", context.open_drawdown_pct, getattr(self.config, "open_drawdown_hard_block_pct", 0.0)),
        ]

        for code, observed, threshold in checks:
            try:
                threshold = float(threshold or 0.0)
                observed = float(observed or 0.0)
            except Exception:
                continue
            if threshold > 0 and observed >= threshold:
                return self._block(code, observed=observed, threshold=threshold)

        max_total_open = float(getattr(self.config, "max_total_open_risk_pct", 0.0) or 0.0)
        if max_total_open > 0 and context.estimated_total_open_risk_pct >= max_total_open:
            return self._block(
                "max_total_open_risk",
                observed=context.estimated_total_open_risk_pct,
                threshold=max_total_open,
            )

        pause_above = float(getattr(self.config, "max_new_entries_when_total_open_risk_above_pct", 0.0) or 0.0)
        if pause_above > 0 and context.estimated_total_open_risk_pct >= pause_above:
            return SupervisorDecision(
                allow_new_entries=False,
                risk_multiplier=0.0,
                reason="open_risk_pause_new_entries",
                severity="warn",
                notes=[
                    f"Total open risk {context.estimated_total_open_risk_pct:.2f}% "
                    f">= pause threshold {pause_above:.2f}%."
                ],
            )

        return SupervisorDecision(True, 1.0, "normal", "pass")

    def _block(self, reason: str, **payload: Any) -> SupervisorDecision:
        self.state.update({
            "blocked": True,
            "block_reason": reason,
            "blocked_at": datetime.now().isoformat(),
            "payload": payload,
        })
        self._save_state()
        return SupervisorDecision(
            allow_new_entries=False,
            risk_multiplier=0.0,
            reason=reason,
            severity="block",
            notes=[f"Hard breakpoint triggered: {reason} {payload}"],
        )

    def manual_reset(self, note: str = "") -> None:
        self.state = {
            "blocked": False,
            "block_reason": "",
            "blocked_at": None,
            "manual_reset_at": datetime.now().isoformat(),
            "manual_reset_note": note,
        }
        self._save_state()
```

## D2. Integrate in `LiveTrader`

### File

```text
pm_main.py
```

### Insertion point

Inside `LiveTrader.__init__`, after enhancement seams and storage manager initialization.

### Add import

```python
from pm_institutional_supervisor import InstitutionalSupervisor, SupervisorContext
```

### Add field

```python
        self._institutional_supervisor = InstitutionalSupervisor(
            pipeline_config,
            state_path=str(Path(getattr(pipeline_config, "output_dir", "./pm_outputs")) / "institutional_supervisor_state.json"),
        )
```

### Add evaluation before placing any new entry

Find the live decision point immediately before final order send.

Add:

```python
        supervisor_ctx = self._build_supervisor_context(open_positions_snapshot)
        supervisor_decision = self._institutional_supervisor.evaluate(supervisor_ctx)

        if not supervisor_decision.allow_new_entries:
            self.logger.warning(
                "[%s] Institutional supervisor blocked new entry: %s (%s)",
                symbol,
                supervisor_decision.reason,
                supervisor_decision.notes,
            )
            self._decision_throttle.record_decision(
                symbol=symbol,
                decision_key=decision_key,
                bar_time_iso=bar_time_iso,
                timeframe=timeframe,
                regime=regime,
                strategy_name=strategy.name,
                direction=signal,
                action="SKIPPED_INSTITUTIONAL_SUPERVISOR",
                context={
                    "reason": supervisor_decision.reason,
                    "severity": supervisor_decision.severity,
                    "notes": supervisor_decision.notes,
                },
            )
            return
```

### Add helper

```python
    def _build_supervisor_context(self, open_positions_snapshot) -> SupervisorContext:
        account = self.mt5.get_account_info()
        balance = float(getattr(account, "balance", 0.0) or 0.0)
        equity = float(getattr(account, "equity", 0.0) or 0.0)
        margin_level = float(getattr(account, "margin_level", 0.0) or 0.0)

        estimated_total_open_risk_pct = self._estimate_total_open_risk_pct(open_positions_snapshot, equity)

        return SupervisorContext(
            account_balance=balance,
            account_equity=equity,
            account_margin_level=margin_level,
            open_positions=list(open_positions_snapshot or []),
            estimated_total_open_risk_pct=estimated_total_open_risk_pct,
            realized_daily_loss_pct=self._estimate_realized_loss_pct("daily", balance),
            realized_session_loss_pct=self._estimate_realized_loss_pct("session", balance),
            realized_weekly_loss_pct=self._estimate_realized_loss_pct("weekly", balance),
            open_drawdown_pct=self._estimate_open_drawdown_pct(balance, equity),
            metadata={"mode": "hard_breakpoints_only"},
        )
```

### Note

The helper functions can start conservative and improve over time. The first release can use available position geometry and account data. The key is to create the institutional shell without turning it into a constant signal suppressor.

---

# Part E — Live order-management expansion

This is now the highest-probability direct profitability upgrade.

## E1. Current issue

The current governance policy list is too small for the size and ambition of FXPM. Entry strategies compete across 47 strategies, multiple symbols, multiple timeframes, and multiple regimes. But after entry, the current policy surface is approximately:

```text
control_fixed
breakeven_1r
atr_trail_capped
pure_atr
```

The downstream management surface is probably too narrow. Many strategies lose profitability not because entries are bad, but because exits and trade management are mismatched to the regime.

## E2. Expand the governance candidate set

### File

```text
pm_order_governance.py
```

### Current constant

```python
DEFAULT_GOVERNANCE_CANDIDATES: Sequence[str] = (
    "control_fixed",
    "breakeven_1r",
    "atr_trail_capped",
    "pure_atr",
)
```

### Replace with

```python
DEFAULT_GOVERNANCE_CANDIDATES: Sequence[str] = (
    "control_fixed",
    "breakeven_1r",
    "stepped_breakeven",
    "atr_trail_capped",
    "chandelier_atr",
    "structure_trail",
    "partial_tp_runner",
    "time_decay_exit",
    "volatility_contraction_exit",
    "regime_flip_reduce",
    "pure_atr",
)
```

## E3. Update policy allowlist

### Insertion point

In `candidate_policy_names`, update the allowlist:

```python
        if name not in {
            "control_fixed",
            "breakeven_1r",
            "stepped_breakeven",
            "atr_trail_capped",
            "chandelier_atr",
            "structure_trail",
            "partial_tp_runner",
            "time_decay_exit",
            "volatility_contraction_exit",
            "regime_flip_reduce",
            "pure_atr",
        }:
            continue
```

## E4. Add aliases

### Insertion point

Inside `normalize_policy_name`.

Add:

```python
        "step_be": "stepped_breakeven",
        "stepped_be": "stepped_breakeven",
        "chandelier": "chandelier_atr",
        "structure": "structure_trail",
        "swing_trail": "structure_trail",
        "partial_runner": "partial_tp_runner",
        "time_decay": "time_decay_exit",
        "vol_contraction": "volatility_contraction_exit",
        "regime_flip": "regime_flip_reduce",
```

## E5. Expand `make_policy` defaults

### Insertion point

Inside `make_policy`.

Replace the default params with:

```python
    params: Dict[str, Any] = {
        "name": name,

        # Existing policies
        "trail_activation_atr_mult": 1.5,
        "trail_atr_mult": 2.5,
        "breakeven_r": 1.0,
        "breakeven_offset_pips": 0.0,
        "release_tp_after_r": None,

        # Stepped breakeven
        "step1_r": 1.0,
        "step1_lock_r": 0.0,
        "step2_r": 1.5,
        "step2_lock_r": 0.5,
        "step3_r": 2.0,
        "step3_lock_r": 1.0,

        # Chandelier ATR
        "chandelier_atr_mult": 3.0,
        "chandelier_activation_r": 1.0,

        # Structure trail
        "structure_lookback": 20,
        "structure_buffer_atr_mult": 0.25,
        "structure_activation_r": 0.8,

        # Partial TP runner
        "partial_tp_r": 1.0,
        "partial_close_pct": 50.0,
        "runner_trail_atr_mult": 2.5,

        # Time decay exit
        "time_decay_bars": 48,
        "time_decay_min_r": 0.0,
        "time_decay_action": "tighten",  # tighten|exit

        # Volatility contraction exit
        "vol_contract_lookback": 20,
        "vol_contract_threshold": 0.65,
        "vol_contract_action": "tighten",

        # Regime flip reduce
        "regime_flip_action": "tighten",  # tighten|exit|release_tp
        "regime_flip_tighten_to_r": 0.0,
    }
```

## E6. Extend `GovernanceContext`

### Current context

```python
@dataclass
class GovernanceContext:
    symbol: str
    timeframe: str
    regime: str
    ...
```

### Add fields

```python
    bars_held: int = 0
    entry_regime: str = ""
    current_regime: str = ""
    current_regime_gap: float = 0.0
    recent_high: float = 0.0
    recent_low: float = 0.0
    atr_percentile: float = 0.0
    realized_vol_ratio: float = 1.0
    partial_taken: bool = False
```

## E7. Add policy evaluation branches

### Insertion point

Inside `evaluate_policy`.

### Add helper

```python
def _stop_from_r(context: GovernanceContext, lock_r: float) -> float:
    initial_r = context.initial_r
    if initial_r <= 0:
        return context.current_stop_loss
    if context.is_long:
        return context.entry_price + lock_r * initial_r
    return context.entry_price - lock_r * initial_r
```

### Add branch: stepped breakeven

```python
    if name == "stepped_breakeven":
        lock_r = None
        if context.favorable_r >= float(normalized["step3_r"]):
            lock_r = float(normalized["step3_lock_r"])
        elif context.favorable_r >= float(normalized["step2_r"]):
            lock_r = float(normalized["step2_lock_r"])
        elif context.favorable_r >= float(normalized["step1_r"]):
            lock_r = float(normalized["step1_lock_r"])

        if lock_r is not None:
            candidate = _stop_from_r(context, lock_r)
            tightened = _tighten_stop(
                context.current_stop_loss,
                _sanitize_stop_candidate(candidate, context),
                context.direction,
            )
            if tightened is not None:
                decision.stop_loss = tightened
                decision.notes.append(f"stepped_breakeven_lock_{lock_r:.2f}r")
        return decision
```

### Add branch: chandelier ATR

```python
    if name == "chandelier_atr":
        if context.current_atr > 0 and context.favorable_r >= float(normalized["chandelier_activation_r"]):
            mult = float(normalized["chandelier_atr_mult"])
            if context.is_long:
                anchor = context.highest_since_entry or context.recent_high or context.current_price
                candidate = float(anchor) - mult * float(context.current_atr)
            else:
                anchor = context.lowest_since_entry or context.recent_low or context.current_price
                candidate = float(anchor) + mult * float(context.current_atr)

            tightened = _tighten_stop(
                context.current_stop_loss,
                _sanitize_stop_candidate(candidate, context),
                context.direction,
            )
            if tightened is not None:
                decision.stop_loss = tightened
                decision.notes.append("chandelier_atr")
        return decision
```

### Add branch: structure trail

```python
    if name == "structure_trail":
        if context.current_atr > 0 and context.favorable_r >= float(normalized["structure_activation_r"]):
            buffer_price = float(normalized["structure_buffer_atr_mult"]) * float(context.current_atr)
            if context.is_long and context.recent_low > 0:
                candidate = float(context.recent_low) - buffer_price
            elif (not context.is_long) and context.recent_high > 0:
                candidate = float(context.recent_high) + buffer_price
            else:
                return decision

            tightened = _tighten_stop(
                context.current_stop_loss,
                _sanitize_stop_candidate(candidate, context),
                context.direction,
            )
            if tightened is not None:
                decision.stop_loss = tightened
                decision.notes.append("structure_trail")
        return decision
```

### Add branch: partial TP runner

```python
    if name == "partial_tp_runner":
        if (not context.partial_taken) and context.favorable_r >= float(normalized["partial_tp_r"]):
            decision.partial_close_pct = float(normalized["partial_close_pct"])
            decision.notes.append(f"partial_tp_runner_signal_{decision.partial_close_pct:.1f}%")
            if context.current_atr > 0:
                if context.is_long:
                    candidate = context.highest_since_entry - float(normalized["runner_trail_atr_mult"]) * context.current_atr
                else:
                    candidate = context.lowest_since_entry + float(normalized["runner_trail_atr_mult"]) * context.current_atr
                tightened = _tighten_stop(
                    context.current_stop_loss,
                    _sanitize_stop_candidate(candidate, context),
                    context.direction,
                )
                if tightened is not None:
                    decision.stop_loss = tightened
                    decision.notes.append("runner_trail_after_partial")
        return decision
```

### Add branch: time decay exit

```python
    if name == "time_decay_exit":
        bars_held = int(context.bars_held or 0)
        if bars_held >= int(normalized["time_decay_bars"]) and context.favorable_r <= float(normalized["time_decay_min_r"]):
            action = str(normalized["time_decay_action"]).lower()
            if action == "exit":
                decision.take_profit = context.current_price
                decision.exit_now = True
                decision.notes.append("time_decay_exit_now")
            else:
                candidate = _stop_from_r(context, 0.0)
                tightened = _tighten_stop(
                    context.current_stop_loss,
                    _sanitize_stop_candidate(candidate, context),
                    context.direction,
                )
                if tightened is not None:
                    decision.stop_loss = tightened
                    decision.notes.append("time_decay_tighten")
        return decision
```

### Add branch: volatility contraction exit

```python
    if name == "volatility_contraction_exit":
        ratio = float(context.realized_vol_ratio or 1.0)
        if ratio <= float(normalized["vol_contract_threshold"]):
            action = str(normalized["vol_contract_action"]).lower()
            if action == "exit":
                decision.take_profit = context.current_price
                decision.exit_now = True
                decision.notes.append("volatility_contraction_exit")
            else:
                candidate = _stop_from_r(context, 0.0)
                tightened = _tighten_stop(
                    context.current_stop_loss,
                    _sanitize_stop_candidate(candidate, context),
                    context.direction,
                )
                if tightened is not None:
                    decision.stop_loss = tightened
                    decision.notes.append("volatility_contraction_tighten")
        return decision
```

### Add branch: regime flip reduce

```python
    if name == "regime_flip_reduce":
        entry_regime = str(context.entry_regime or context.regime or "").upper()
        current_regime = str(context.current_regime or context.regime or "").upper()
        if entry_regime and current_regime and entry_regime != current_regime:
            action = str(normalized["regime_flip_action"]).lower()
            if action == "exit":
                decision.take_profit = context.current_price
                decision.exit_now = True
                decision.notes.append(f"regime_flip_exit_{entry_regime}_to_{current_regime}")
            elif action == "release_tp":
                decision.take_profit = math.inf if context.is_long else -math.inf
                decision.tp_released = True
                decision.notes.append(f"regime_flip_tp_release_{entry_regime}_to_{current_regime}")
            else:
                lock_r = float(normalized["regime_flip_tighten_to_r"])
                candidate = _stop_from_r(context, lock_r)
                tightened = _tighten_stop(
                    context.current_stop_loss,
                    _sanitize_stop_candidate(candidate, context),
                    context.direction,
                )
                if tightened is not None:
                    decision.stop_loss = tightened
                    decision.notes.append(f"regime_flip_tighten_{entry_regime}_to_{current_regime}")
        return decision
```

## E8. Extend `GovernanceDecision` for partial closes

### File

```text
pm_order_governance.py
```

### Current

```python
@dataclass
class GovernanceDecision:
    policy_name: str
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    tp_released: bool = False
    notes: List[str] = field(default_factory=list)
```

### Replace with

```python
@dataclass
class GovernanceDecision:
    policy_name: str
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    tp_released: bool = False
    partial_close_pct: float = 0.0
    exit_now: bool = False
    notes: List[str] = field(default_factory=list)
```

### Live executor insertion

In `pm_main.py`, wherever governance decisions are applied to open positions, add:

```python
if decision.partial_close_pct > 0:
    self.logger.info(
        "[%s] Governance policy %s requests partial close %.1f%%",
        position.symbol,
        decision.policy_name,
        decision.partial_close_pct,
    )
```

### Add partial close helper to `pm_mt5.py`

```python
    def close_position_partial(
        self,
        position: MT5Position,
        close_volume: float,
        deviation: int = 30,
        magic: int = 0,
        comment: str = "",
    ) -> MT5OrderResult:
        if close_volume <= 0:
            return MT5OrderResult(False, 10014, "Invalid partial close volume")

        symbol_info = self.get_symbol_info(position.symbol)
        if symbol_info is None:
            return MT5OrderResult(False, -1, "Symbol info unavailable")

        close_volume = self._normalize_volume(close_volume, symbol_info)
        if close_volume <= 0 or close_volume > position.volume:
            return MT5OrderResult(False, 10014, "Invalid normalized partial close volume")

        clone = MT5Position(**{**position.__dict__, "volume": close_volume})
        return self.close_position(clone, deviation=deviation, magic=magic, comment=comment or "Partial close")
```

---

# Part F — Strategy pool expansion to 50-52

The target of 50-52 strategies is reasonable if additions are genuinely distinct and improve coverage. Do not add more simple indicator variants unless they fill a missing behavioral niche.

## F1. Recommended additions

### 1. Session VWAP Reversion Strategy

**Purpose:** Capture session-specific mean reversion around a rolling/session VWAP proxy.

**Why it fits:** Adds time/session behavior without top-down portfolio suppression.

**Category:** Mean reversion.

**Name:** `SessionVWAPReversionStrategy`

**Core logic:**

```text
If price deviates far from session VWAP proxy and regime is RANGE/CHOP, fade back to VWAP.
```

### 2. Liquidity Sweep Reversal Strategy

**Purpose:** Trade false breaks / stop-runs around prior highs/lows.

**Why it fits:** Adds price-action microstructure not already covered by standard oscillators.

**Category:** Mean reversion / reversal.

**Name:** `LiquiditySweepReversalStrategy`

**Core logic:**

```text
If current high sweeps prior N-bar high but closes back below it, short.
If current low sweeps prior N-bar low but closes back above it, long.
```

### 3. Regime Transition Breakout Strategy

**Purpose:** Trade CHOP/RANGE → BREAKOUT transition.

**Why it fits:** Directly addresses regime transition as edge, not just static regime lookup.

**Category:** Breakout.

**Name:** `RegimeTransitionBreakoutStrategy`

**Core logic:**

```text
If previous live regime was CHOP/RANGE and current live regime becomes BREAKOUT with sufficient gap,
trade breakout direction.
```

### 4. Carry-Filtered Trend Continuation Strategy

**Purpose:** Trend following that can optionally require favorable carry or avoid strongly negative carry.

**Why it fits:** Converts carry from a portfolio restriction into a local strategy archetype.

**Category:** Trend.

**Name:** `CarryFilteredTrendContinuationStrategy`

**Core logic:**

```text
Trend signal must pass EMA/ADX trend condition.
Optional carry gate adjusts or filters direction based on swap_long/swap_short.
```

### 5. Multi-Pair Currency Strength Momentum Strategy

**Purpose:** Trade pair direction based on relative strength of base vs quote currency.

**Why it fits:** This should be added as a strategy candidate, not as a portfolio-level veto.

**Category:** Trend / momentum.

**Name:** `CurrencyStrengthMomentumStrategy`

**Core logic:**

```text
If base currency strength > quote currency strength by threshold, long.
If quote currency strength > base currency strength by threshold, short.
```

### Recommended target

Add **three first**, not all five at once:

```text
1. LiquiditySweepReversalStrategy
2. RegimeTransitionBreakoutStrategy
3. SessionVWAPReversionStrategy
```

Then add carry and currency-strength only after the core live readiness work is stable.

## F2. Strategy registry insertion

### File

```text
pm_strategies.py
```

### Insertion point

Add classes near related strategy families, then update the registry map where `StrategyRegistry` registers all strategy classes.

### Example class skeleton: Liquidity Sweep Reversal

```python
class LiquiditySweepReversalStrategy(BaseStrategy):
    @property
    def name(self) -> str:
        return "LiquiditySweepReversalStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.MEAN_REVERSION

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 20,
            "close_back_pct": 0.25,
            "atr_filter_mult": 0.5,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 2.5,
            "use_structural_sl": True,
            "structural_sl_lookback": 20,
        }

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [10, 20, 30, 50],
            "close_back_pct": [0.15, 0.25, 0.35],
            "atr_filter_mult": [0.0, 0.5, 1.0],
            "sl_atr_mult": _GLOBAL_SL_GRID,
            "tp_atr_mult": _GLOBAL_TP_GRID,
            "use_structural_sl": [False, True],
        }

    def get_required_features(self) -> Set[str]:
        return {"ATR_14"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        p = self.params
        lookback = int(p["lookback"])
        close_back_pct = float(p["close_back_pct"])
        atr_mult = float(p["atr_filter_mult"])

        signals = pd.Series(0, index=features.index, dtype=int)
        prior_high = features["High"].shift(1).rolling(lookback).max()
        prior_low = features["Low"].shift(1).rolling(lookback).min()
        atr = features.get("ATR_14", (features["High"] - features["Low"]).rolling(14).mean())

        bar_range = (features["High"] - features["Low"]).replace(0, np.nan)

        swept_high = features["High"] > prior_high
        closed_back_below = features["Close"] < prior_high - close_back_pct * atr
        enough_range = bar_range >= atr_mult * atr

        swept_low = features["Low"] < prior_low
        closed_back_above = features["Close"] > prior_low + close_back_pct * atr

        signals[swept_high & closed_back_below & enough_range] = -1
        signals[swept_low & closed_back_above & enough_range] = 1

        return self._zero_warmup(signals, max(lookback, 50))
```

### Example class skeleton: Regime Transition Breakout

```python
class RegimeTransitionBreakoutStrategy(BaseStrategy):
    @property
    def name(self) -> str:
        return "RegimeTransitionBreakoutStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.BREAKOUT

    def get_default_params(self) -> Dict[str, Any]:
        return {
            "lookback": 20,
            "min_regime_gap": 0.05,
            "atr_buffer_mult": 0.10,
            "sl_atr_mult": 2.0,
            "tp_atr_mult": 3.0,
        }

    def get_param_grid(self) -> Dict[str, List]:
        return {
            "lookback": [10, 20, 30, 50],
            "min_regime_gap": [0.03, 0.05, 0.08, 0.10],
            "atr_buffer_mult": [0.0, 0.10, 0.20],
            "sl_atr_mult": _GLOBAL_SL_GRID,
            "tp_atr_mult": _GLOBAL_TP_GRID,
        }

    def get_required_features(self) -> Set[str]:
        return {"ATR_14", "REGIME_LIVE", "REGIME_GAP"}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        p = self.params
        lookback = int(p["lookback"])
        min_gap = float(p["min_regime_gap"])
        atr_buffer = float(p["atr_buffer_mult"])

        signals = pd.Series(0, index=features.index, dtype=int)
        regime = features["REGIME_LIVE"] if "REGIME_LIVE" in features else features.get("REGIME")
        gap = features.get("REGIME_GAP", pd.Series(0.0, index=features.index))
        atr = features.get("ATR_14", (features["High"] - features["Low"]).rolling(14).mean())

        prev_regime = regime.shift(1)
        transition = prev_regime.isin(["CHOP", "RANGE"]) & (regime == "BREAKOUT") & (gap >= min_gap)

        prior_high = features["High"].shift(1).rolling(lookback).max()
        prior_low = features["Low"].shift(1).rolling(lookback).min()

        long_break = features["Close"] > prior_high + atr_buffer * atr
        short_break = features["Close"] < prior_low - atr_buffer * atr

        signals[transition & long_break] = 1
        signals[transition & short_break] = -1

        return self._zero_warmup(signals, max(lookback, 50))
```

---

# Part G — Regime accuracy and profitability alignment

Regime accuracy is central to FXPM. The issue is not whether regimes are conceptually valid. The issue is whether the live system selects the correct specialist often enough to preserve profitability.

## G1. Add a regime profitability audit

### New file

```text
pm_regime_audit.py
```

### Purpose

Measure whether regime labels are helping the strategy-selection process.

### Metrics

```text
1. Regime distribution per symbol/timeframe.
2. Regime transition counts.
3. Average REGIME_GAP per label.
4. PnL by detected regime.
5. PnL by previous regime → current regime transition.
6. Winner coverage by regime.
7. No-trade marker frequency by regime.
8. Near-boundary trade outcomes where REGIME_GAP is low.
```

### Code skeleton

```python
# pm_regime_audit.py

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List
import pandas as pd
import numpy as np


@dataclass
class RegimeAuditReport:
    symbol: str
    timeframe: str
    summary: Dict[str, Any] = field(default_factory=dict)
    regime_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    transition_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    boundary_metrics: Dict[str, Any] = field(default_factory=dict)


def _trade_regime(trade: Dict[str, Any], features: pd.DataFrame) -> str:
    signal_bar = trade.get("signal_bar", trade.get("signal_bar_index", None))
    if signal_bar is None:
        return "UNKNOWN"
    try:
        i = int(signal_bar)
        col = "REGIME_LIVE" if "REGIME_LIVE" in features.columns else "REGIME"
        return str(features[col].iat[i])
    except Exception:
        return "UNKNOWN"


def audit_regime_profitability(
    *,
    symbol: str,
    timeframe: str,
    features: pd.DataFrame,
    trades: List[Dict[str, Any]],
    low_gap_threshold: float = 0.05,
) -> RegimeAuditReport:
    report = RegimeAuditReport(symbol=symbol, timeframe=timeframe)

    regime_col = "REGIME_LIVE" if "REGIME_LIVE" in features.columns else "REGIME"
    gap_col = "REGIME_GAP" if "REGIME_GAP" in features.columns else None

    regime_counts = features[regime_col].value_counts(dropna=False).to_dict()
    report.summary["regime_counts"] = {str(k): int(v) for k, v in regime_counts.items()}

    by_regime: Dict[str, List[float]] = {}
    low_gap_pnls: List[float] = []
    normal_gap_pnls: List[float] = []

    for trade in trades or []:
        regime = _trade_regime(trade, features)
        pnl = float(trade.get("pnl_dollars", trade.get("pnl", 0.0)) or 0.0)
        by_regime.setdefault(regime, []).append(pnl)

        signal_bar = trade.get("signal_bar", trade.get("signal_bar_index", None))
        if gap_col and signal_bar is not None:
            try:
                gap = float(features[gap_col].iat[int(signal_bar)] or 0.0)
                if gap < low_gap_threshold:
                    low_gap_pnls.append(pnl)
                else:
                    normal_gap_pnls.append(pnl)
            except Exception:
                pass

    for regime, pnls in by_regime.items():
        arr = np.array(pnls, dtype=float)
        report.regime_metrics[regime] = {
            "trades": int(len(arr)),
            "total_pnl": float(arr.sum()) if len(arr) else 0.0,
            "mean_pnl": float(arr.mean()) if len(arr) else 0.0,
            "win_rate": float((arr > 0).mean() * 100.0) if len(arr) else 0.0,
        }

    report.boundary_metrics = {
        "low_gap_threshold": low_gap_threshold,
        "low_gap_trades": len(low_gap_pnls),
        "low_gap_total_pnl": float(np.sum(low_gap_pnls)) if low_gap_pnls else 0.0,
        "normal_gap_trades": len(normal_gap_pnls),
        "normal_gap_total_pnl": float(np.sum(normal_gap_pnls)) if normal_gap_pnls else 0.0,
    }

    return report
```

## G2. Add regime audit to winner artifacts

### File

```text
pm_pipeline.py
```

### Insertion point

After a candidate winner is selected and before `RegimeConfig` is finalized.

### Add

```python
from pm_regime_audit import audit_regime_profitability
```

Then:

```python
regime_audit = audit_regime_profitability(
    symbol=symbol,
    timeframe=timeframe,
    features=selection_features,
    trades=selection_result.get("trades", []),
    low_gap_threshold=float(getattr(self.config, "regime_min_live_gap_warn", 0.05)),
)

regime_config.validation_evidence["regime_audit"] = {
    "summary": regime_audit.summary,
    "regime_metrics": regime_audit.regime_metrics,
    "boundary_metrics": regime_audit.boundary_metrics,
}
```

## G3. Live regime boundary handling

Do not immediately block low-gap regime decisions. That would risk suppressing too much edge. Start with observe-only:

```text
regime_ambiguity_policy = observe_only
```

The system should record:

```text
symbol
timeframe
detected regime
top score
gap
selected winner
signal
trade result
```

After enough evidence, you can decide whether low-gap cases should:

```text
1. remain fully tradable
2. use lower risk
3. defer if both top regimes have conflicting winners
4. use a transition strategy
```

---

# Part H — Walk-forward without killing live edge

Your concern is valid: an extra validation layer that forces the live system to trade older parameters can harm the fresh two-week edge.

## H1. Revised walk-forward principle

Walk-forward should be used as a **fragility audit**, not as a replacement for the latest selection surface.

Use the newest surface for live selection.

Use walk-forward only to detect severe instability:

```text
If the latest winner is strong but collapses catastrophically across recent rolling windows,
block or demote it.
Otherwise, allow it to trade.
```

## H2. Implementation rule

Do not add:

```text
optimize on old window → validate on newer window → then trade still older params
```

Instead add:

```text
latest optimization remains the champion
rolling audit checks whether champion behavior is catastrophically unstable
severe fail = no live promotion
non-severe fail = warning / paper tag
pass = live eligible
```

## H3. Artifact fields

### File

```text
pm_pipeline.py
```

### Insertion point

`RegimeConfig.validation_evidence`

Add:

```python
"fresh_edge_policy": {
    "live_surface": "latest_stage2_or_stage3",
    "walk_forward_role": "severe_fragility_audit",
    "do_not_displace_latest_surface": True
}
```

## H4. Promotion labels

Add to each `RegimeConfig`:

```python
promotion_status: str = "PROMOTED"  # PROMOTED|PROMOTE_SMALL|PAPER_ONLY|NO_TRADE|BLOCKED
promotion_reasons: List[str] = field(default_factory=list)
```

### Insertion point

`pm_pipeline.py`, inside `RegimeConfig`.

```python
    promotion_status: str = "PROMOTED"
    promotion_reasons: List[str] = field(default_factory=list)
```

Update `to_dict` and `from_dict` accordingly.

### Promotion logic

```python
def classify_promotion_status(regime_config: RegimeConfig, config: PipelineConfig) -> str:
    wf = regime_config.walk_forward_audit or {}
    summary = wf.get("summary", {}) if isinstance(wf, dict) else {}

    if regime_config.is_no_trade_marker():
        return "NO_TRADE"

    if summary.get("severe_failure") and getattr(config, "walk_forward_promotion_policy", "") == "block_severe_only":
        return "BLOCKED"

    hist = regime_config.historical_audit_metrics or {}
    if hist.get("severe_failure"):
        return "PROMOTE_SMALL"

    return "PROMOTED"
```

### Live lookup rule

In live trading, only allow:

```text
PROMOTED
PROMOTE_SMALL
```

For `PROMOTE_SMALL`, apply a risk multiplier such as 0.5 if enabled. Otherwise, start with observe-only.

---

# Part I — Lightweight live-cycle drift sentinel

## I1. Why it still matters despite two-week retraining

Two-week retraining solves model-age decay. It does not solve within-cycle operational degradation.

Examples:

```text
A broker widens spreads for two days.
A symbol’s execution becomes poor after a server issue.
The regime detector starts selecting CHOP while price is breaking out.
A data cache discontinuity creates bad features.
A local winner has five immediate live losses.
```

A lightweight sentinel catches these without becoming a heavy drift system.

## I2. New file

```text
pm_cycle_drift.py
```

### Code skeleton

```python
# pm_cycle_drift.py

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


@dataclass
class CycleDriftDecision:
    action: str = "allow"  # allow|pause_context|derate_context|observe
    risk_multiplier: float = 1.0
    reason: str = "normal"
    evidence: Dict[str, Any] = field(default_factory=dict)


class CycleDriftMonitor:
    # Lightweight two-week-cycle sentinel.
    #
    # This is not a full model lifecycle system. It only catches severe live mismatch
    # during the current retrain cycle.

    def __init__(self, config: Any, state_path: str = "pm_outputs/cycle_drift_state.json"):
        self.config = config
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load()

    def _load(self) -> Dict[str, Any]:
        try:
            if self.state_path.exists():
                return json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        return {"contexts": {}}

    def _save(self) -> None:
        tmp = self.state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.state, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.state_path)

    @staticmethod
    def context_key(symbol: str, timeframe: str, regime: str, strategy_name: str) -> str:
        return f"{symbol}|{timeframe}|{regime}|{strategy_name}"

    def record_trade_result(
        self,
        *,
        symbol: str,
        timeframe: str,
        regime: str,
        strategy_name: str,
        pnl_dollars: float,
        r_multiple: float = 0.0,
        slippage_pips: float = 0.0,
        spread_pips: float = 0.0,
        expected_spread_pips: float = 0.0,
    ) -> None:
        key = self.context_key(symbol, timeframe, regime, strategy_name)
        ctx = self.state["contexts"].setdefault(key, {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "consecutive_losses": 0,
            "total_pnl": 0.0,
            "max_slippage_pips": 0.0,
            "max_spread_ratio": 0.0,
            "paused": False,
            "pause_reason": "",
        })

        pnl = float(pnl_dollars or 0.0)
        ctx["trades"] += 1
        ctx["total_pnl"] += pnl
        if pnl > 0:
            ctx["wins"] += 1
            ctx["consecutive_losses"] = 0
        else:
            ctx["losses"] += 1
            ctx["consecutive_losses"] += 1

        ctx["max_slippage_pips"] = max(float(ctx.get("max_slippage_pips", 0.0)), float(slippage_pips or 0.0))

        if expected_spread_pips and expected_spread_pips > 0:
            ratio = float(spread_pips or 0.0) / float(expected_spread_pips)
            ctx["max_spread_ratio"] = max(float(ctx.get("max_spread_ratio", 0.0)), ratio)

        ctx["last_update"] = datetime.now().isoformat()
        self._save()

    def evaluate_context(self, *, symbol: str, timeframe: str, regime: str, strategy_name: str) -> CycleDriftDecision:
        if not bool(getattr(self.config, "cycle_drift_monitor_enabled", True)):
            return CycleDriftDecision("allow", 1.0, "cycle_drift_disabled")

        mode = str(getattr(self.config, "cycle_drift_mode", "lightweight")).lower()
        if mode == "off":
            return CycleDriftDecision("allow", 1.0, "cycle_drift_off")

        key = self.context_key(symbol, timeframe, regime, strategy_name)
        ctx = self.state.get("contexts", {}).get(key, {})
        if ctx.get("paused"):
            return CycleDriftDecision("pause_context", 0.0, ctx.get("pause_reason", "context_paused"), ctx)

        min_trades = int(getattr(self.config, "cycle_drift_min_trades", 6) or 6)
        max_consec = int(getattr(self.config, "cycle_drift_consecutive_losses", 5) or 5)

        if int(ctx.get("trades", 0)) >= min_trades and int(ctx.get("consecutive_losses", 0)) >= max_consec:
            return self._action(key, "consecutive_losses", ctx)

        spread_mult = float(getattr(self.config, "cycle_drift_spread_degradation_mult", 2.0) or 2.0)
        if float(ctx.get("max_spread_ratio", 0.0)) >= spread_mult:
            return self._action(key, "spread_degradation", ctx)

        return CycleDriftDecision("allow", 1.0, "normal", ctx)

    def _action(self, key: str, reason: str, ctx: Dict[str, Any]) -> CycleDriftDecision:
        mode = str(getattr(self.config, "cycle_drift_mode", "lightweight")).lower()
        configured_action = str(getattr(self.config, "cycle_drift_action", "pause_context")).lower()

        if mode == "observe":
            return CycleDriftDecision("observe", 1.0, reason, ctx)

        if configured_action == "derate_context":
            return CycleDriftDecision("derate_context", 0.5, reason, ctx)

        ctx["paused"] = True
        ctx["pause_reason"] = reason
        self.state["contexts"][key] = ctx
        self._save()
        return CycleDriftDecision("pause_context", 0.0, reason, ctx)
```

## I3. Integration in `LiveTrader`

### File

```text
pm_main.py
```

### Add import

```python
from pm_cycle_drift import CycleDriftMonitor
```

### In `LiveTrader.__init__`

```python
        self._cycle_drift = CycleDriftMonitor(
            pipeline_config,
            state_path=str(Path(getattr(pipeline_config, "output_dir", "./pm_outputs")) / "cycle_drift_state.json"),
        )
```

### Before placing a new trade

```python
        drift_decision = self._cycle_drift.evaluate_context(
            symbol=symbol,
            timeframe=timeframe,
            regime=regime,
            strategy_name=strategy.name,
        )

        if drift_decision.action == "pause_context":
            self.logger.warning(
                "[%s] Cycle drift paused context %s/%s/%s: %s",
                symbol, timeframe, regime, strategy.name, drift_decision.reason,
            )
            return

        if drift_decision.action == "derate_context":
            risk_pct *= float(drift_decision.risk_multiplier)
```

### After trade close

Where closed trade results are recorded, call:

```python
self._cycle_drift.record_trade_result(
    symbol=symbol,
    timeframe=timeframe,
    regime=regime,
    strategy_name=strategy_name,
    pnl_dollars=pnl_dollars,
    r_multiple=r_multiple,
    slippage_pips=observed_slippage_pips,
    spread_pips=observed_spread_pips,
    expected_spread_pips=expected_spread_pips,
)
```

---

# Part J — Swap/carry modeling without over-filtering

## J1. Revised recommendation

Do not add broad carry restrictions unless evidence proves they improve profitability. But do add swap/carry modeling to the backtest and artifacts. This is not optional for D1/exotics sustainability.

Carry should enter FXPM in two ways:

```text
1. Cost realism: all strategies pay or receive swap when holding through rollover.
2. Strategy archetype: selected carry-aware strategies may use carry as a local signal/filter.
```

It should not become:

```text
"Never trade against carry."
```

That would be too blunt.

## J2. Add post-trade swap adjustment

### File

```text
pm_core.py
```

### Insertion point

Inside `Backtester.run`, after trades are reconstructed from `_backtest_loop_numba` and before performance metrics are calculated.

Because the Numba loop does not receive timestamps, do swap adjustment in Python after trade reconstruction.

### Code skeleton

```python
def _apply_swap_costs_to_trades(
    self,
    trades: List[Dict[str, Any]],
    features: pd.DataFrame,
    symbol: str,
    spec: InstrumentSpec,
) -> List[Dict[str, Any]]:
    if not bool(getattr(self.config, "swap_model_enabled", False)):
        return trades

    triple_day = str(getattr(self.config, "swap_model_triple_rollover_day", "wednesday")).lower()
    triple_weekday = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }.get(triple_day, 2)

    min_hours = float(getattr(self.config, "swap_model_min_holding_hours", 20.0) or 20.0)

    for trade in trades:
        try:
            entry_i = int(trade.get("entry_bar", trade.get("entry_bar_index")))
            exit_i = int(trade.get("exit_bar", trade.get("exit_bar_index")))
            entry_ts = features.index[entry_i]
            exit_ts = features.index[exit_i]
        except Exception:
            continue

        holding_hours = (exit_ts - entry_ts).total_seconds() / 3600.0
        if holding_hours < min_hours:
            trade["swap_cost"] = 0.0
            continue

        direction = int(trade.get("direction", 0))
        volume = float(trade.get("size", trade.get("volume", 0.0)) or 0.0)
        swap_per_lot = float(spec.swap_long if direction == 1 else spec.swap_short)

        rollover_days = 0
        cur = entry_ts.normalize()
        end = exit_ts.normalize()
        while cur < end:
            mult = 3 if cur.weekday() == triple_weekday else 1
            rollover_days += mult
            cur += pd.Timedelta(days=1)

        swap_cost = swap_per_lot * volume * rollover_days
        trade["swap_cost"] = float(swap_cost)
        trade["pnl_dollars"] = float(trade.get("pnl_dollars", 0.0)) + float(swap_cost)

    return trades
```

Then call:

```python
trades = self._apply_swap_costs_to_trades(trades, features, symbol, spec)
```

---

# Part K — News/event logic without over-restricting specialists

## K1. Revised recommendation

Do not add a broad news-event veto by default. That could suppress the system’s best opportunities.

Instead implement news/event handling as:

```text
1. execution-quality context
2. optional imported event calendar
3. local strategy feature if a strategy wants it
4. hard block only for extreme spread/rollover conditions
```

## K2. Add optional event calendar loader

### New file

```text
pm_event_context.py
```

### Code skeleton

```python
# pm_event_context.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd


@dataclass
class EventRisk:
    active: bool = False
    severity: str = "none"  # none|low|medium|high
    event_name: str = ""
    minutes_to_event: float = 999999.0


class EventCalendar:
    def __init__(self, path: str = "data/events.csv"):
        self.path = Path(path)
        self.events = self._load()

    def _load(self) -> pd.DataFrame:
        if not self.path.exists():
            return pd.DataFrame(columns=["time", "currency", "severity", "name"])
        df = pd.read_csv(self.path)
        df["time"] = pd.to_datetime(df["time"])
        return df.sort_values("time")

    def risk_for_symbol(self, symbol: str, timestamp, window_minutes: int = 30) -> EventRisk:
        if self.events.empty:
            return EventRisk(False, "none")

        currencies = _symbol_currencies(symbol)
        ts = pd.Timestamp(timestamp)
        start = ts - pd.Timedelta(minutes=window_minutes)
        end = ts + pd.Timedelta(minutes=window_minutes)

        subset = self.events[
            (self.events["time"] >= start)
            & (self.events["time"] <= end)
            & (self.events["currency"].isin(currencies))
        ]

        if subset.empty:
            return EventRisk(False, "none")

        row = subset.iloc[0]
        minutes = abs((row["time"] - ts).total_seconds()) / 60.0
        return EventRisk(True, str(row.get("severity", "medium")), str(row.get("name", "")), minutes)


def _symbol_currencies(symbol: str):
    s = "".join(ch for ch in str(symbol).upper() if ch.isalpha())
    if len(s) >= 6:
        return [s[:3], s[3:6]]
    return []
```

## K3. Policy

Start as observe-only:

```json
"event_calendar_enabled": false,
"event_calendar_mode": "observe",
"event_calendar_high_impact_action": "observe"
```

Only move to hard blocks if live/paper evidence shows news windows damage expectancy.

---

# Part L — Fairness in strategy comparison

Your fairness concern is important. A strategy tournament can unintentionally favor certain strategy types if:

```text
one strategy has a much larger parameter grid
one strategy has higher trade frequency and therefore more stable metrics
one strategy has easier-to-optimize defaults
one strategy benefits from the shared ATR stop surface more than another
one strategy is penalized by signal sparsity despite strong expectancy
```

FXPM already helps by using a common SL/TP grid and family-size-aware Optuna budgets. Keep that. Add telemetry rather than heavy interference.

## L1. Add fairness metadata to optimization artifacts

### File

```text
pm_pipeline.py
```

### Insertion point

When storing `validation_evidence`.

Add:

```python
"fairness_evidence": {
    "strategy_category": strategy.category.value if hasattr(strategy.category, "value") else str(strategy.category),
    "param_grid_size": strategy.cartesian_grid_size() if hasattr(strategy, "cartesian_grid_size") else None,
    "search_trials": int(search_trials or 0),
    "search_coverage_pct": float(search_trials / max(strategy.cartesian_grid_size(), 1)) if hasattr(strategy, "cartesian_grid_size") else None,
    "signal_count_train": int(train_signals.abs().sum()) if train_signals is not None else None,
    "signal_count_val": int(val_signals.abs().sum()) if val_signals is not None else None,
    "trade_count_train": int(train_metrics.get("total_trades", 0)),
    "trade_count_val": int(val_metrics.get("total_trades", 0)),
}
```

## L2. Add fairness report

### New file

```text
pm_fairness_audit.py
```

### Purpose

This report should not change selection at first. It should reveal whether the tournament is skewing toward a particular class.

### Metrics

```text
Strategy category winner rate
Average param-grid coverage by category
Average validation trade count by category
No-trade rate by regime
Winner concentration by family
```

---

# Part M — Revised validation design

## M1. Keep the fresh edge

Do not add a validation layer that forces the model to trade older stale parameters.

## M2. Add severe-fragility blocker

Add:

```text
latest winner remains live candidate
audit checks severe fragility only
severe failure blocks
moderate concerns become PROMOTE_SMALL or observe
```

## M3. Add promotion status to live selection

### File

```text
pm_pipeline.py / pm_main.py
```

### Live selection rule

When a live winner is found:

```python
promotion_status = getattr(regime_config, "promotion_status", "PROMOTED")
if promotion_status in {"BLOCKED", "PAPER_ONLY", "NO_TRADE"}:
    skip live entry
elif promotion_status == "PROMOTE_SMALL":
    optional risk multiplier = 0.5
else:
    normal trade
```

This gives you robustness without creating a stale intermediate window.

---

# Part N — Specific disagreement/acceptance register

## N1. Portfolio risk

### Your position

Let each specialist act independently. Avoid top-down interference.

### Revised blueprint position

Accepted with modification.

### Implementation

Add institutional breakpoints only.

Do not implement default correlation veto.

Do not implement default currency-leg veto.

Do add high-threshold hard system risk gates.

## N2. Live order management

### Your position

Current pool is likely too small.

### Revised blueprint position

Strongly accepted.

### Implementation

Expand governance pool from 4 to 10+ policies and validate per local winner.

## N3. Live drift

### Your position

Two-week retrain reduces need for heavy drift.

### Revised blueprint position

Accepted with modification.

### Implementation

Add lightweight cycle sentinel only. No heavy drift platform.

## N4. Carry/news/currency strength

### Your position

Need specificity; avoid broad restrictions.

### Revised blueprint position

Accepted.

### Implementation

Carry becomes cost modeling and optional strategy. News becomes optional event context / execution observation. Currency strength becomes a strategy candidate, not a portfolio veto.

## N5. Strategy pool

### Your position

47 is broad but not complete; target 50-52.

### Revised blueprint position

Accepted.

### Implementation

Add 3 first, then 2 later, all distinct.

## N6. Regime accuracy

### Your position

Central to the entire system.

### Revised blueprint position

Strongly accepted.

### Implementation

Add profitability-aligned regime audits, boundary diagnostics, and transition strategy archetype.

## N7. Walk-forward validation

### Your position

Avoid adding a stale layer that decays live edge.

### Revised blueprint position

Strongly accepted.

### Implementation

Use severe-fragility audit only. Latest optimization remains live surface.

---

# Part O — Priority implementation plan

## Phase 1: Deployment safety without suppressing edge

1. Add `pm_live_readiness.py`.
2. Set `live_artifact_drift_policy = "block"`.
3. Add institutional supervisor in hard-breakpoints-only mode.
4. Add hard daily/session/weekly/open-drawdown gates.
5. Keep correlation and currency-leg controls observe-only.

## Phase 2: Order-management profitability expansion

1. Expand governance candidate pool.
2. Add policy fields and evaluation branches.
3. Add partial close support.
4. Run governance in shadow first.
5. Promote policies to live only when artifact evidence supports them.

## Phase 3: Regime accuracy

1. Add `pm_regime_audit.py`.
2. Persist regime audit evidence per winner.
3. Add boundary diagnostics.
4. Add `RegimeTransitionBreakoutStrategy`.

## Phase 4: Cost realism

1. Add swap modeling.
2. Add cost evidence to artifacts.
3. Re-run D1/exotic winners.
4. Consider disabling D1/exotics until swap-adjusted evidence is acceptable.

## Phase 5: Lightweight cycle drift

1. Add `pm_cycle_drift.py`.
2. Record context-level outcomes.
3. Pause only severe underperformance or execution degradation.
4. Reset by retrain cycle or manual operator approval.

## Phase 6: Strategy pool expansion

1. Add `LiquiditySweepReversalStrategy`.
2. Add `RegimeTransitionBreakoutStrategy`.
3. Add `SessionVWAPReversionStrategy`.
4. Evaluate fairness and winner contribution.
5. Add carry/currency-strength only if they pass proof-of-value.

---

# Part P — Tests to add

## P1. Unit tests

```text
tests/test_profitability_readiness_blueprint.py
tests/test_new_strategies_42_50.py
tests/test_strategy_param_grid_consistency.py
tests/test_live_loop_integration.py
tests/test_mt5_connector.py
tests/test_instrument_specs.py
```

## P2. Key test cases

### Live readiness

```text
empty active ledger blocks funded live
warn drift policy blocks funded live
missing regime params warns or blocks depending policy
paper mode can bypass funded-live block
```

### Institutional supervisor

```text
normal context allows entries
daily loss threshold blocks entries
open risk pause threshold pauses new entries
manual reset clears persistent block
supervisor never blocks due only to correlation
```

### Order governance

```text
stepped breakeven only tightens stops
chandelier never loosens stops
structure trail respects executable side of market
partial runner emits partial-close instruction
regime flip reduce reacts only after actual regime change
```

### Swap modeling

```text
intraday trade no swap
overnight long applies long swap
overnight short applies short swap
Wednesday rollover applies triple swap
D1 multi-day trade includes cumulative swap
```

### Regime audit

```text
low-gap trades are counted
PnL is attributed to REGIME_LIVE
regime distribution persisted
boundary metrics persisted
```

---

# Part Q — Final live deployment checklist

Before funded live trading:

```text
[ ] Active winner ledger is non-empty.
[ ] Artifact drift policy is block.
[ ] Live readiness gate passes.
[ ] Institutional supervisor is enabled in hard_breakpoints_only mode.
[ ] Hard daily/session/weekly/open-drawdown breakpoints exist.
[ ] Walk-forward audit is severe_fragility_only, not stale validation displacement.
[ ] Governance policies have been shadow-tested or are artifact-approved.
[ ] Swap modeling has been applied or D1/exotics are excluded.
[ ] Regime audit evidence exists for winners.
[ ] Cycle drift sentinel is enabled in lightweight mode.
[ ] No constant correlation veto is active by default.
[ ] Event/news logic is observe-only unless evidence supports blocking.
[ ] Paper deployment has run on the same broker/account setup.
[ ] MT5 broker metadata is synced and verified.
```

---

# Part R — Context questions for a future refinement pass

These questions are not blockers for the current blueprint, but they would help tune thresholds and code details.

1. What is the intended starting live account size?
2. Is the broker account netting or hedging?
3. Which symbols are actually intended for live deployment, versus only research?
4. Are D1 and exotics intended to trade live immediately?
5. Do your local winner artifacts already contain governance policy evidence for each local winner?
6. Do you have recent paper-mode logs with skipped trades, spread rejections, and order-check failures?
7. Do you want partial close support in live MT5 immediately, or should partial policies remain shadow-only first?
8. Should `PROMOTE_SMALL` apply a fixed 0.5 risk multiplier, or should it only tag the dashboard initially?
9. Is the two-week retrain schedule manually supervised, or should expiry force all new entries off until retraining completes?
10. Do you want regime ambiguity to remain observe-only initially, or derate risk when `REGIME_GAP` is below threshold?

---

# Appendix S — Current-repository grounding

This blueprint was grounded in the following current-repository facts observed during the pre-implementation review. The implementation status is recorded in Appendix T.

1. `config.json` previously set `live_artifact_drift_policy` to `"warn"` while the code default in `PipelineConfig` was `"block"`. For funded live trading, the active config needed to be changed to `"block"`.
2. `config.json` sets `winner_ledger_path` to `pm_configs_high_risk.json`.
3. Winner ledger presence/content must be checked by the live readiness gate before funded trading.
4. `regime_params.json` was not present in the repository snapshot reviewed.
5. `pm_core.py` previously documented that swap was configured but not applied in backtest, and that D1 multi-week holds may see material impact.
6. `pm_order_governance.py` previously exposed a small governance policy surface: `control_fixed`, `breakeven_1r`, `atr_trail_capped`, and `pure_atr`.
7. `pm_main.py` contained a live pipeline crash path where `active_mode` was referenced without being defined.
8. `pm_enhancement_seams.py` already has risk-scalar, exit-pack, portfolio-observatory, and execution-quality seam concepts, so the recommended additions fit the existing design direction.
9. `pm_regime.py` uses causal regime detection and provides shifted `REGIME_LIVE` for decision-time parity.
10. `pm_regime_tuner.py` tunes regime parameters using stability/gap/trend-ratio style metrics; this should be supplemented with profitability-aligned regime audit evidence.

---

# Appendix T — Implementation and QA status

Implementation date: 2026-04-30.

The blueprint has been implemented with the following codebase changes:

1. `config.json` now sets `live_artifact_drift_policy` to `"block"`, enables the institutional supervisor in `circuit_breaker` mode, enables `walk_forward_audit_enabled`, enables swap modeling, expands `local_governance_candidate_policies`, and keeps live governance in `"shadow"` mode until the expanded policies have live broker evidence.
2. `PipelineConfig` now exposes and normalizes the institutional supervisor, hard breakpoint, cycle drift, walk-forward promotion, swap model, regime audit, and event observe-only configuration fields.
3. `pm_live_readiness.py` adds a fail-closed live readiness gate for funded startup: artifact drift must be blocking, the winner ledger must be readable/non-empty, and the ledger must contain at least one live-tradeable winner. `PAPER_ONLY`, `NO_TRADE`, and `BLOCKED` winners are not counted as live-tradeable. `pm_main.py` calls the gate before real live trading and returns exit code `2` if readiness fails.
4. `pm_institutional_supervisor.py` adds hard-breakpoint-only portfolio supervision with persisted latch state and next-cycle/manual reopening. It does not implement a constant correlation veto or top-down allocator.
5. `pm_cycle_drift.py` adds a persisted lightweight cycle drift sentinel. `pm_main.py` records closed strategy outcomes into it by position closure, so partial exits do not inflate cycle trade counts, and the sentinel persists live spread degradation/final quote-adjustment slippage evidence for operator review or reset.
6. `pm_order_governance.py` now includes `stepped_breakeven`, `chandelier_atr`, `structure_trail`, `partial_tp_runner`, `time_decay_exit`, `volatility_contraction_exit`, and `regime_flip_reduce`, with live-cycle propagation in `pm_main.py`.
7. `pm_mt5.py` now supports partial position closes for governance policies that require runner management, and live governance persists successful partial-close state immediately before any follow-up SL/TP modification.
8. `pm_core.Backtester` now applies signed overnight swap/carry costs to trade P&L and the equity curve, including configured triple rollover, avoids charging same-day holds that never cross a rollover boundary, preserves broker swap mode metadata, and models governance partial/forced-exit decisions in the Python governance backtest path.
9. `pm_strategies.py` now registers `SessionVWAPReversionStrategy`, `LiquiditySweepReversalStrategy`, and `RegimeTransitionBreakoutStrategy`; the transition strategy requires source-regime to target-regime movement with `REGIME_GAP` support.
10. `pm_pipeline.py` now persists promotion status/reasons, records fresh-edge/no-best-train-fallback evidence, records search-budget fairness telemetry and category-level fairness reporting, attaches regime profitability/boundary audit evidence into `validation_evidence["regime_audit"]`, and fixes the undefined `active_mode` pipeline crash by using the explicit `stage3_selection` surface.
11. `pm_event_context.py` provides an observe-only CSV event calendar with symbol/currency matching, severity filtering, and `minutes_to_event` annotations. `pm_main.py` now propagates nearby event context into actionable live decisions. It is intentionally non-blocking until there is broker/calendar evidence to justify execution impact.
12. `pm_main.py` now refreshes full portfolio positions immediately before institutional-supervisor checks in real trading, preventing same-sweep portfolio-risk undercounting after an earlier symbol opens a trade.

QA verification completed:

```text
python -m pytest -q -p no:cacheprovider
588 passed, 1 skipped, 356 subtests passed
```

Quality status:

```text
[x] Active code no longer references undefined active_mode.
[x] Live artifact drift is configured to block.
[x] Readiness gate is fail-closed for funded live artifact drift and live-tradeable winner ledger requirements.
[x] Institutional supervision is hard-breakpoint-only.
[x] Institutional hard blocks persist and respect manual/next-cycle reopening.
[x] Daily/session/weekly realized-loss hard breakpoints are estimated from broker closing deals with weekly lookback coverage.
[x] Expanded local governance is propagated to backtest and live management surfaces.
[x] Expanded governance is shadow-mode by default for live execution safety.
[x] Successful live partial closes are persisted before any follow-up modify can fail.
[x] Swap/carry costs are applied in backtest metrics without same-day non-rollover charges.
[x] Promotion status, fairness evidence, and regime audit evidence are persisted.
[x] Event calendar support is observe-only and wired into live actionable context.
[x] New strategies are registered, parameter-grid checked, and signal-contract tested.
[x] Full deterministic regression suite passed.
```

Known intentional limits after implementation:

1. Event/news handling remains observe-only as the blueprint recommended.
2. Realized loss breakpoints use the broker recent-closing-deal feed available to the live loop; a persistent broker-ledger warehouse would improve long-horizon auditability but is not required for the configured daily/session/weekly circuit breaker.
3. `PROMOTE_SMALL` applies a fixed `0.5` live risk multiplier, matching the blueprint question’s suggested default until operator policy is refined.

---

# Final summary

The revised blueprint accepts the central FXPM philosophy: autonomous specialists should be allowed to exploit their local edge. The system should not be burdened with a heavy top-down portfolio optimizer or stale validation layer.

The correct profitability-focused next step is to strengthen the **promotion and supervision shell**, not to suppress the specialist model.

The most important changes are:

```text
1. Fail-closed live readiness.
2. Hard institutional breakpoints only, not constant correlation vetoes.
3. Expanded local order-management tournament.
4. Profitability-aligned regime audit.
5. Severe-fragility walk-forward audit that does not displace the latest edge.
6. Swap/carry cost realism.
7. Lightweight two-week-cycle drift sentinel.
8. Three high-quality new strategy archetypes before expanding further.
```

This keeps FXPM aligned with its intended purpose: a profitability-first system where each local best manager can act, while the whole system remains sustainable enough to fund with real money.
