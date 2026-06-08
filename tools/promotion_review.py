"""Consolidated regime-level promotion-readiness review.

A discovery winner is only promotable if it clears every gate:
  Gate 1  stress kill-test : survives the configured adverse entry-spread mode
  Gate 2  global PBO       : its timeframe-aligned group is not overfit
  Gate 3  clean holdout    : positive return on the freshest untouched holdout window
  Gate 4  provenance       : spread feed is trustworthy
  Gate 0  parity           : optional fast-run vs sequential reference

Read-only; promotes nothing by itself. This expands the actual production
surface, symbol x timeframe x regime. The legacy one-row-per-symbol default is
not a promotion truth.
"""

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

from benchmark import DataLoader, SPREAD_MODES, _git_head, build_config, evaluate_winner
from regime_winner_review import HOLDOUT_MIN_TRADES, iter_rows


PBO_REVIEW = 0.5


def _row_float(row, key):
    try:
        value = float(row.get(key))
    except (TypeError, ValueError):
        return None
    if math.isnan(value):
        return None
    return value


def _row_trades(row, key):
    try:
        return int(float(row.get(key, 0) or 0))
    except (TypeError, ValueError):
        return 0


def _promotion_status_failures(row):
    """Mirror the production promotion classification in the readiness review.

    The review expands readiness into independent gates, but a precomputed
    BLOCKED/PROMOTE_SMALL status is itself evidence that the winner is not a
    clean live promotion. This keeps the report aligned with the per-cell
    benchmark guard.
    """
    status = str(row.get("promotion_status", "") or "").upper()
    reasons = row.get("promotion_reasons") or []
    if not isinstance(reasons, list):
        reasons = [str(reasons)]
    detail = "; ".join(str(reason) for reason in reasons if str(reason)) or status

    if status.startswith("BLOCK"):
        return [{
            "gate": "promotion_status",
            "severity": "reject",
            "code": "promotion_blocked",
            "detail": f"promotion_status BLOCKED: {detail}",
        }]
    if status.startswith("PROMOTE_SMALL"):
        return [{
            "gate": "promotion_status",
            "severity": "review",
            "code": "promotion_small",
            "detail": f"promotion_status PROMOTE_SMALL: {detail}",
        }]
    return []


def _numeric_evidence_failures(row):
    """Reject sufficient independent evidence that is numerically negative.

    Coarse status fields can miss cell-level failures when the historical audit
    did not set severe_failure. The live-book audit already applies these
    numeric guards, so promotion readiness should not contradict it.
    """
    failures = []
    val_trades = _row_trades(row, "val_trades")
    historical_trades = _row_trades(row, "historical_trades")
    holdout_trades = _row_trades(row, "holdout_trades")

    if val_trades >= HOLDOUT_MIN_TRADES:
        val_pf = _row_float(row, "val_pf")
        val_return = _row_float(row, "val_return_pct")
        if val_pf is not None and val_pf < 1.0:
            failures.append({
                "gate": "validation",
                "severity": "reject",
                "code": "validation_pf_lt_1",
                "detail": f"validation PF {val_pf:.2f} < 1.00",
            })
        if val_return is not None and val_return < 0.0:
            failures.append({
                "gate": "validation",
                "severity": "reject",
                "code": "validation_return_negative",
                "detail": f"validation return {val_return:.2f}% < 0",
            })

    if historical_trades >= HOLDOUT_MIN_TRADES:
        historical_pf = _row_float(row, "historical_pf")
        historical_return = _row_float(row, "historical_return_pct")
        historical_dd = _row_float(row, "historical_max_dd_pct")
        if historical_pf is not None and historical_pf < 1.0:
            failures.append({
                "gate": "historical",
                "severity": "reject",
                "code": "historical_pf_lt_1",
                "detail": f"historical PF {historical_pf:.2f} < 1.00",
            })
        if historical_return is not None and historical_return < 0.0:
            failures.append({
                "gate": "historical",
                "severity": "reject",
                "code": "historical_return_negative",
                "detail": f"historical return {historical_return:.2f}% < 0",
            })
        if historical_dd is not None and historical_dd > 40.0:
            failures.append({
                "gate": "historical",
                "severity": "reject",
                "code": "historical_dd_gt_40",
                "detail": f"historical DD {historical_dd:.2f}% > 40.00%",
            })

    if holdout_trades >= HOLDOUT_MIN_TRADES:
        holdout_pf = _row_float(row, "holdout_pf")
        holdout_return = _row_float(row, "holdout_return_pct")
        if holdout_pf is not None and holdout_pf < 1.0:
            failures.append({
                "gate": "holdout",
                "severity": "reject",
                "code": "holdout_pf_lt_1",
                "detail": f"holdout PF {holdout_pf:.2f} < 1.00",
            })
        if holdout_return is not None and holdout_return < 0.0:
            failures.append({
                "gate": "holdout",
                "severity": "reject",
                "code": "holdout_return_negative",
                "detail": f"holdout return {holdout_return:.2f}% < 0",
            })

    return failures


def _gate_failures(row, stress, stress_mode):
    """Return every non-clean gate, independent of final-verdict priority."""
    failures = []
    failures.extend(_promotion_status_failures(row))
    failures.extend(_numeric_evidence_failures(row))
    stress_verdict = stress.get("stress", "PENDING")
    holdout_status = row.get("holdout_status", "")
    historical_status = row.get("historical_status", "")
    provenance = stress.get("provenance", "")
    tf_pbo = row.get("pbo_timeframe")

    if stress_verdict == "KILL":
        failures.append({
            "gate": "stress",
            "severity": "reject",
            "code": "stress_fragile",
            "detail": f"fails {stress_mode} (spread-spike fragile)",
        })
    elif stress_verdict in ("PENDING", "SKIPPED", "NO_DATA", "ERROR"):
        failures.append({
            "gate": "stress",
            "severity": "review",
            "code": f"stress_{str(stress_verdict).lower()}",
            "detail": f"stress {str(stress_verdict).lower()}",
        })

    if holdout_status == "FAIL":
        failures.append({
            "gate": "holdout",
            "severity": "reject",
            "code": "holdout_fail",
            "detail": "negative/sub-1 holdout",
        })
    elif holdout_status in ("NONE", "INSUFFICIENT"):
        failures.append({
            "gate": "holdout",
            "severity": "review",
            "code": f"holdout_{str(holdout_status).lower()}",
            "detail": f"holdout {str(holdout_status).lower()}",
        })

    if historical_status == "FAIL":
        failures.append({
            "gate": "historical",
            "severity": "reject",
            "code": "historical_fail",
            "detail": "historical audit severe failure",
        })
    elif historical_status in ("NONE", "DISABLED"):
        failures.append({
            "gate": "historical",
            "severity": "review",
            "code": f"historical_{str(historical_status).lower()}",
            "detail": f"historical {str(historical_status).lower()}",
        })

    if isinstance(tf_pbo, (int, float)) and tf_pbo > PBO_REVIEW:
        failures.append({
            "gate": "pbo",
            "severity": "reject",
            "code": "pbo_overfit",
            "detail": f"overfit (timeframe PBO {tf_pbo})",
        })
    elif tf_pbo is None:
        failures.append({
            "gate": "pbo",
            "severity": "review",
            "code": "pbo_missing",
            "detail": "PBO missing",
        })

    if provenance and provenance not in ("fx_historical_spread", "broker_spread", ""):
        failures.append({
            "gate": "provenance",
            "severity": "review",
            "code": f"provenance_{provenance}",
            "detail": f"provenance {provenance}",
        })

    return failures


def _stress_regime(row, cfg_conservative, cfg_stress, loader, collapse_frac):
    winner = {
        "symbol": row["symbol"],
        "timeframe": row["timeframe"],
        "strategy_name": row["strategy_name"],
        "parameters": row.get("parameters", {}) or {},
    }
    try:
        rc = evaluate_winner(cfg_conservative, loader, winner)
        rs = evaluate_winner(cfg_stress, loader, winner)
    except Exception as exc:
        return {"stress": "ERROR", "error": f"{type(exc).__name__}: {exc}"}
    if "error" in rc or "error" in rs:
        return {
            "stress": "NO_DATA",
            "error": rc.get("error") or rs.get("error"),
            "provenance": rc.get("spread_provenance_status", ""),
        }

    ret_c, ret_s = float(rc["return_pct"]), float(rs["return_pct"])
    pf_s = float(rs["profit_factor"])
    lost = ((ret_c - ret_s) / abs(ret_c)) if ret_c != 0 else 0.0
    kill = (pf_s < 1.0) or (ret_s <= 0) or (ret_c > 0 and lost > collapse_frac)
    return {
        "stress": "KILL" if kill else "SURVIVE",
        "asset_class": rc.get("asset_class", ""),
        "provenance": rc.get("spread_provenance_status", ""),
        "ret_conservative": round(ret_c, 2),
        "ret_stress": round(ret_s, 2),
        "pf_conservative": round(float(rc["profit_factor"]), 3),
        "pf_stress": round(pf_s, 3),
        "frac_return_lost": round(lost, 3),
        "stress_entry_column": rs.get("spread_entry_column", ""),
        "stress_entry_model": rs.get("spread_entry_model", ""),
        "stress_bar_column": rs.get("spread_bar_column", ""),
        "stress_mean_to_broker_spread_ratio": round(float(rs.get("mean_to_broker_spread_ratio", 0.0) or 0.0), 3),
        "stress_max_to_broker_spread_ratio": round(float(rs.get("max_to_broker_spread_ratio", 0.0) or 0.0), 3),
    }


def _final_verdict(row, stress, stress_mode):
    failures = _gate_failures(row, stress, stress_mode)
    reject = next((failure for failure in failures if failure["severity"] == "reject"), None)
    if reject:
        return "REJECT", reject["detail"]
    if failures:
        return "REVIEW", "; ".join(failure["detail"] for failure in failures)
    return "PROMOTE", "all gates clean"


def _validate_completion_marker(marker_path: Path, ledger_path: Path):
    if not marker_path.exists():
        return False, f"completion marker missing: {marker_path}"
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return False, f"completion marker unreadable: {type(exc).__name__}: {exc}"
    if not isinstance(marker, dict):
        return False, "completion marker is not a JSON object"
    if not bool(marker.get("review_ready", False)):
        return False, "completion marker is not marked review_ready"
    if bool(marker.get("pid_alive", False)):
        return False, "completion marker still reports pid_alive=true"
    if bool(marker.get("stopped_for_memory_pressure", False)):
        return False, "completion marker reports stopped_for_memory_pressure=true"
    marker_ledger = marker.get("ledger")
    if marker_ledger:
        try:
            if Path(str(marker_ledger)).resolve() != ledger_path.resolve():
                return False, f"completion marker ledger mismatch: {marker_ledger}"
        except Exception as exc:
            return False, f"completion marker ledger check failed: {type(exc).__name__}: {exc}"
    if ledger_path.exists() and marker.get("ledger_count") is not None:
        try:
            ledger = json.loads(ledger_path.read_text(encoding="utf-8-sig"))
            current_count = sum(
                1 for key, value in ledger.items()
                if isinstance(value, dict) and not str(key).startswith("_")
            )
            if int(marker.get("ledger_count") or 0) != current_count:
                return False, (
                    f"completion marker ledger_count mismatch: "
                    f"{marker.get('ledger_count')} vs {current_count}"
                )
        except Exception as exc:
            return False, f"completion marker ledger_count check failed: {type(exc).__name__}: {exc}"
    if marker.get("oos_manifests") is not None and marker.get("ledger_count") is not None:
        try:
            if int(marker.get("oos_manifests") or 0) < int(marker.get("ledger_count") or 0):
                return False, (
                    f"completion marker oos_manifests {marker.get('oos_manifests')} "
                    f"< ledger_count {marker.get('ledger_count')}"
                )
        except Exception as exc:
            return False, f"completion marker OOS coverage check failed: {type(exc).__name__}: {exc}"
    return True, "completion marker valid"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ledger",
        default=str(REPO / "pm_configs.json"),
    )
    parser.add_argument(
        "--evidence-dir",
        default=str(REPO / "pm_outputs" / "production_live" / "oos_evidence"),
    )
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--label", default="promotion_review")
    parser.add_argument(
        "--skip-stress",
        action="store_true",
        help="Do not run the expensive per-regime stress backtest gate.",
    )
    parser.add_argument(
        "--stress-mode",
        default="stress_max",
        choices=[m for m in SPREAD_MODES if m.startswith("stress_")],
        help=(
            "Adverse-cost replay mode for Gate 1. Default preserves the historical "
            "max-spread torture test; stress_p90/stress_p95 are discovery modes."
        ),
    )
    parser.add_argument("--collapse-frac", type=float, default=0.5)
    parser.add_argument("--holdout-min-trades", type=int, default=HOLDOUT_MIN_TRADES)
    parser.add_argument(
        "--completion-marker",
        default="",
        help="Optional JSON marker proving the optimizer has exited for this ledger.",
    )
    parser.add_argument(
        "--require-completion-marker",
        action="store_true",
        help="Fail closed unless --completion-marker exists and reports pid_alive=false.",
    )
    args = parser.parse_args()

    ledger_path = Path(args.ledger)
    completion_marker_valid = None
    completion_marker_reason = ""
    if args.require_completion_marker and not args.completion_marker:
        print("completion marker required but --completion-marker was not provided")
        return 2
    if args.completion_marker:
        marker_path = Path(args.completion_marker)
        completion_marker_valid, completion_marker_reason = _validate_completion_marker(marker_path, ledger_path)
        if args.require_completion_marker and not completion_marker_valid:
            print(completion_marker_reason)
            return 2
    if not ledger_path.exists():
        print(f"ledger not found yet: {ledger_path} (run not complete) - gates ready, nothing to review.")
        return 0

    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    symbols = args.symbols or sorted(
        key for key, value in ledger.items()
        if isinstance(value, dict) and not str(key).startswith("_")
    )
    rows = iter_rows(
        ledger,
        Path(args.evidence_dir),
        symbols=symbols,
        include_no_trade=False,
        holdout_min_trades=args.holdout_min_trades,
    )

    cfg_c = cfg_s = loader = None
    if not args.skip_stress:
        cfg_c = build_config("conservative_max")
        cfg_s = build_config(args.stress_mode)
        loader = DataLoader(cfg_c.data_dir)

    review_rows = []
    for row in rows:
        if args.skip_stress:
            stress = {"stress": "SKIPPED", "provenance": ""}
        else:
            stress = _stress_regime(row, cfg_c, cfg_s, loader, args.collapse_frac)
        verdict, why = _final_verdict(row, stress, args.stress_mode)
        gate_failures = _gate_failures(row, stress, args.stress_mode)
        review_rows.append({
            **row,
            **stress,
            "gate_failures": gate_failures,
            "gate_failure_codes": [failure["code"] for failure in gate_failures],
            "final_verdict": verdict,
            "why": why,
        })

    counts = {}
    for row in review_rows:
        counts[row["final_verdict"]] = counts.get(row["final_verdict"], 0) + 1
    gate_failure_counts = {}
    gate_failure_code_counts = {}
    for row in review_rows:
        for failure in row.get("gate_failures", []) or []:
            gate = str(failure.get("gate", "") or "")
            code = str(failure.get("code", "") or "")
            if gate:
                gate_failure_counts[gate] = gate_failure_counts.get(gate, 0) + 1
            if code:
                gate_failure_code_counts[code] = gate_failure_code_counts.get(code, 0) + 1

    print(f"\n=== PROMOTION-READINESS REVIEW [{args.label}] engine={_git_head()} ===")
    print("granularity: symbol / timeframe / regime")
    print(f"stress mode: {args.stress_mode if not args.skip_stress else 'SKIPPED'}")
    print(
        f"{'symbol':10} {'tf':4} {'regime':9} {'strategy':32} {'stress':8} "
        f"{'holdout':12} {'hist':8} {'tf_pbo':>8} {'verdict':9}  why"
    )
    for row in review_rows:
        print(
            f"{row['symbol']:10} {row['timeframe']:4} {row['regime']:9} "
            f"{str(row['strategy_name'])[:32]:32} {row['stress']:8} "
            f"{row['holdout_status']:12} {row['historical_status']:8} "
            f"{str(row.get('pbo_timeframe')):>8} {row['final_verdict']:9}  {row['why']}"
        )
    print(
        f"\n-- {counts.get('PROMOTE', 0)} PROMOTE / {counts.get('REVIEW', 0)} REVIEW / "
        f"{counts.get('REJECT', 0)} REJECT of {len(review_rows)} regime winners --"
    )
    if gate_failure_counts:
        print("\nGate failure counts (non-exclusive):")
        for gate, count in sorted(gate_failure_counts.items(), key=lambda item: (-item[1], item[0])):
            print(f"  {gate}: {count}")

    out = REPO / "pm_outputs" / "experiments" / f"{args.label}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(
            {
                "engine_commit": _git_head(),
                "created_at": datetime.now().isoformat(),
                "label": args.label,
                "ledger": str(ledger_path),
                "evidence_dir": str(Path(args.evidence_dir)),
                "completion_marker": str(Path(args.completion_marker)) if args.completion_marker else "",
                "completion_marker_valid": completion_marker_valid,
                "completion_marker_reason": completion_marker_reason,
                "granularity": "symbol/timeframe/regime",
                "stress_skipped": bool(args.skip_stress),
                "stress_mode": args.stress_mode if not args.skip_stress else None,
                "holdout_min_trades": int(args.holdout_min_trades),
                "rows": review_rows,
                "counts": counts,
                "gate_failure_counts": gate_failure_counts,
                "gate_failure_code_counts": gate_failure_code_counts,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
