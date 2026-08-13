"""Shared helpers for the governance (active order management) vs static SL/TP study.

Read-only against the engine, config.json, and the winner ledger.
"""
import hashlib
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PF_CAP = 10.0
DEFAULT_OUT_DIR = REPO / "pm_outputs" / "governance_oos_eval"

COST_PARITY_KEYS = (
    "use_spread", "backtest_spread_mode", "use_commission", "use_slippage",
    "slippage_pips", "swap_model_enabled", "swap_model_min_holding_hours",
    "swap_model_triple_rollover_day", "discovery_risk_per_trade_pct",
)


@dataclass
class CellRef:
    symbol: str
    tf: str
    regime: str
    strategy_name: str
    parameters: Dict[str, Any]
    promotion_status: str
    is_validated: bool
    selected_policy: str
    selection_basis: str
    candidate_results: Dict[str, Dict[str, Any]]
    stored_wf_windows: List[Dict[str, Any]] = field(default_factory=list)
    stored_wf_summary: Dict[str, Any] = field(default_factory=dict)
    data_window_meta: Dict[str, Any] = field(default_factory=dict)
    artifact_meta: Dict[str, Any] = field(default_factory=dict)
    optimized_at: str = ""

    @property
    def cell_id(self) -> str:
        return f"{self.symbol}|{self.tf}|{self.regime}"

    @property
    def combo_key(self) -> str:
        params_sig = hashlib.sha1(
            json.dumps(self.parameters, sort_keys=True, default=str).encode()
        ).hexdigest()[:12]
        return f"{self.symbol}|{self.tf}|{self.strategy_name}|{params_sig}"

    @property
    def is_active(self) -> bool:
        return self.selected_policy != "control_fixed"

    @property
    def live_relevant(self) -> bool:
        return self.promotion_status in ("PROMOTED", "PROMOTE_SMALL")


def load_ledger(path: Optional[Path] = None) -> Dict[str, Any]:
    with open(path or REPO / "pm_configs.json", encoding="utf-8") as f:
        return json.load(f)


def iter_tournament_cells(ledger: Dict[str, Any]) -> Iterator[CellRef]:
    """Yield every regime cell with non-empty governance tournament results."""
    for symbol, entry in ledger.items():
        if not isinstance(entry, dict):
            continue
        dwm = entry.get("data_window_meta") or {}
        for tf, regimes in (entry.get("regime_configs") or {}).items():
            for regime, rc in (regimes or {}).items():
                if not isinstance(rc, dict) or rc.get("strategy_name") == "NO_TRADE":
                    continue
                gp = rc.get("governance_policy") or {}
                cr = gp.get("candidate_results") or {}
                if not cr:
                    continue
                wfa = rc.get("walk_forward_audit") or {}
                yield CellRef(
                    symbol=symbol, tf=tf, regime=regime,
                    strategy_name=str(rc.get("strategy_name", "")),
                    parameters=dict(rc.get("parameters") or {}),
                    promotion_status=str(rc.get("promotion_status", "")),
                    is_validated=bool(rc.get("is_validated", False)),
                    selected_policy=str(gp.get("selected_policy", "control_fixed")),
                    selection_basis=str(gp.get("selection_basis", "")),
                    candidate_results=cr,
                    stored_wf_windows=list(wfa.get("windows") or []),
                    stored_wf_summary=dict(wfa.get("summary") or {}),
                    data_window_meta=dwm,
                    artifact_meta=dict(rc.get("artifact_meta") or entry.get("artifact_meta") or {}),
                    optimized_at=str(rc.get("trained_at") or entry.get("optimized_at") or ""),
                )


def build_config(no_disk_cache: bool = False):
    from pm_core import (
        FeatureComputer,
        PipelineConfig,
        strict_pipeline_config_kwargs,
    )
    payload = json.loads((REPO / "config.json").read_text(encoding="utf-8"))
    pipe = payload.get("pipeline", {})
    cfg = PipelineConfig(**strict_pipeline_config_kwargs(pipe))
    cfg.data_dir = str(REPO / "data")
    cfg.regime_params_file = str(REPO / str(getattr(cfg, "regime_params_file", "regime_params.json")))
    FeatureComputer.configure_cache(
        getattr(cfg, "feature_cache_max_entries", 0),
        disk_enabled=False if no_disk_cache else getattr(cfg, "feature_disk_cache_enabled", False),
        disk_dir=str(REPO / str(getattr(cfg, "feature_disk_cache_dir", "")).lstrip("./")) if getattr(cfg, "feature_disk_cache_dir", "") else "",
        disk_max_gb=getattr(cfg, "feature_disk_cache_max_gb", 0.0),
        disk_min_free_gb=getattr(cfg, "feature_disk_cache_min_free_gb", 0.0),
    )
    return cfg


def cost_parity_mismatches(cfg, artifact_meta: Dict[str, Any]) -> List[str]:
    out = []
    for key in COST_PARITY_KEYS:
        if key not in artifact_meta:
            continue
        have, want = getattr(cfg, key, None), artifact_meta[key]
        if str(have) != str(want):
            out.append(f"{key}: cfg={have!r} ledger={want!r}")
    return out


def sanitize_pf(value: Any) -> float:
    try:
        pf = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pf != pf:
        return 0.0
    if pf in (float("inf"), float("-inf")):
        return PF_CAP if pf > 0 else 0.0
    return max(0.0, min(PF_CAP, pf))


def finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if out == out and abs(out) != float("inf") else default
