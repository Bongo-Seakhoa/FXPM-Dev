"""Probability of Backtest Overfitting (PBO) via Combinatorially-Symmetric Cross-Validation.

López de Prado's CSCV: given a performance matrix M[T observations x N configurations], split
T into S blocks, and over every C(S, S/2) IS/OOS partition pick the best config in-sample and
measure its out-of-sample rank. PBO = fraction of partitions where the IS-best config lands
below the OOS median — i.e. the selection process is overfit.

This is the measurement side of audit finding C3 (global false-discovery control). It is
engine-agnostic: feed it any returns matrix. The CLI builds one matrix from a single strategy's
param-grid backtests as a demonstration; the real use is the GLOBAL family matrix (all configs
across strategies/symbols) once the engine preserves per-candidate OOS returns.
"""
import argparse, hashlib, itertools, json, sys
from pathlib import Path
import numpy as np

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

from pm_core import DataLoader, FeatureComputer, Backtester, get_instrument_spec
from pm_strategies import StrategyRegistry
from benchmark import build_config, _git_head


def _block_perf(returns_block):
    """Per-config performance within a time block: Sharpe-like mean/std, robust to flat configs."""
    mean = returns_block.mean(axis=0)
    std = returns_block.std(axis=0)
    return np.divide(mean, std, out=np.zeros_like(mean), where=std > 1e-12)


def pbo_cscv(M, n_blocks=16, candidate_ids=None):
    """M: ndarray [T, N] of per-period returns. Returns PBO and the logit distribution.
    PBO = P(IS-best config ranks below OOS median)."""
    M = np.asarray(M, dtype=np.float64)
    if M.ndim != 2:
        raise ValueError("PBO matrix must be 2D [T observations x N configurations]")
    T, N = M.shape
    n_blocks = max(2, int(n_blocks))
    n_blocks -= n_blocks % 2
    if N < 2 or T < n_blocks * 2:
        return {"pbo": None, "reason": f"insufficient data T={T} N={N}", "n_combos": 0}
    ids = [str(value) for value in candidate_ids] if candidate_ids is not None else [str(i) for i in range(N)]
    if len(ids) != N:
        raise ValueError(f"candidate_ids length {len(ids)} does not match matrix columns {N}")
    edges = np.array_split(np.arange(T), n_blocks)
    blocks = [M[idx, :] for idx in edges]
    half = n_blocks // 2
    logits = []
    selected_counts = np.zeros(N, dtype=np.int64)
    selected_below_median = np.zeros(N, dtype=np.int64)
    selected_rank_sum = np.zeros(N, dtype=np.float64)
    all_rank_sum = np.zeros(N, dtype=np.float64)
    all_below_median = np.zeros(N, dtype=np.int64)
    for is_idx in itertools.combinations(range(n_blocks), half):
        is_set = set(is_idx)
        is_mat = np.vstack([blocks[b] for b in range(n_blocks) if b in is_set])
        oos_mat = np.vstack([blocks[b] for b in range(n_blocks) if b not in is_set])
        is_perf = _block_perf(is_mat)
        oos_perf = _block_perf(oos_mat)
        best = int(np.argmax(is_perf))
        # OOS relative rank of the IS-best config in [0,1]
        sorted_oos = np.sort(oos_perf)
        ranks = np.searchsorted(sorted_oos, oos_perf, side="left") / max(1, (N - 1))
        rank = float(ranks[best])
        w = min(max(rank, 1.0 / (N + 1)), 1.0 - 1.0 / (N + 1))
        logits.append(float(np.log(w / (1.0 - w))))
        selected_counts[best] += 1
        selected_rank_sum[best] += rank
        if rank < 0.5:
            selected_below_median[best] += 1
        all_rank_sum += ranks
        all_below_median += (ranks < 0.5).astype(np.int64)
    logits = np.array(logits)
    pbo = float((logits < 0).mean())
    n_combos = len(logits)
    candidate_stats = []
    for idx, candidate_id in enumerate(ids):
        selected = int(selected_counts[idx])
        stat = {
            "candidate_id": candidate_id,
            "matrix_column": idx,
            "selected_count": selected,
            "selection_rate": round(float(selected / n_combos), 4) if n_combos else 0.0,
            "all_oos_mean_rank": round(float(all_rank_sum[idx] / n_combos), 4) if n_combos else 0.0,
            "all_oos_below_median_rate": round(float(all_below_median[idx] / n_combos), 4) if n_combos else 0.0,
        }
        if selected > 0:
            stat["selected_oos_pbo"] = round(float(selected_below_median[idx] / selected), 4)
            stat["selected_oos_mean_rank"] = round(float(selected_rank_sum[idx] / selected), 4)
        else:
            stat["selected_oos_pbo"] = None
            stat["selected_oos_mean_rank"] = None
        candidate_stats.append(stat)
    return {"pbo": round(pbo, 4), "n_combos": n_combos, "n_configs": N, "n_obs": T,
            "logit_median": round(float(np.median(logits)), 4),
            "candidate_stats": candidate_stats}


def load_oos_manifest_matrix(manifest_path):
    """Load and verify an engine-emitted oos_candidate_matrix_v1 manifest."""
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format_version") != "oos_candidate_matrix_v1":
        raise ValueError(f"unsupported OOS manifest format: {manifest.get('format_version')}")
    matrix_path = Path(str(manifest.get("matrix_path", "")))
    if not matrix_path.exists():
        fallback = manifest_path.parent / matrix_path.name
        matrix_path = fallback if fallback.exists() else matrix_path
    if not matrix_path.exists():
        raise FileNotFoundError(f"OOS matrix not found: {manifest.get('matrix_path')}")
    with np.load(matrix_path) as data:
        returns = np.asarray(data["returns"], dtype=np.float32)
        candidate_ids = np.asarray(data["candidate_ids"]).astype(str)
    expected_shape = (manifest.get("matrix_contract") or {}).get("returns_shape")
    if expected_shape and list(returns.shape) != list(expected_shape):
        raise ValueError(f"OOS matrix shape mismatch: {returns.shape} != {expected_shape}")
    record_count = int(manifest.get("record_count", 0) or 0)
    if returns.ndim != 2 or returns.shape[1] != record_count or len(candidate_ids) != record_count:
        raise ValueError("OOS matrix candidate count mismatch")
    digest_payload = returns.tobytes() + json.dumps(
        candidate_ids.tolist(),
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha1(digest_payload).hexdigest()
    if digest != str(manifest.get("matrix_digest_sha1", "")):
        raise ValueError("OOS matrix digest mismatch")
    return returns, manifest, candidate_ids.tolist()


def build_returns_matrix(cfg, loader, strategy_name, symbol, tf, samples, seed):
    grid = StrategyRegistry.get(strategy_name).get_param_grid()
    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    rng = np.random.RandomState(seed)
    if len(combos) > samples:
        combos = [combos[i] for i in rng.choice(len(combos), samples, replace=False)]
    base = loader.load_symbol(symbol)
    df = loader.resample(base, tf)
    feats = FeatureComputer.compute_for_strategy(
        df, StrategyRegistry.get(strategy_name), symbol=symbol, timeframe=tf,
        regime_params_file=cfg.regime_params_file)
    spec = get_instrument_spec(symbol)
    bt = Backtester(cfg)
    cols = []
    for combo in combos:
        params = dict(zip(keys, combo))
        strat = StrategyRegistry.get(strategy_name, **params)
        sig = strat.generate_signals(feats, symbol)
        if not hasattr(sig, "abs") or sig.abs().sum() == 0:
            continue
        r = bt.run(feats, sig, symbol, strat, spec=spec, timeframe=tf)
        eq = np.asarray(r.get("equity_curve", []), dtype=np.float64)
        if eq.size < 50:
            continue
        rets = np.diff(eq) / np.where(eq[:-1] != 0, eq[:-1], 1.0)
        cols.append(rets)
    if len(cols) < 2:
        return None
    T = min(len(c) for c in cols)
    return np.column_stack([c[-T:] for c in cols])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", help="Path to an oos_candidate_matrix_v1 manifest from the engine")
    ap.add_argument("--strategy")
    ap.add_argument("--symbol", default="EURUSD")
    ap.add_argument("--tf", default="H4")
    ap.add_argument("--samples", type=int, default=32)
    ap.add_argument("--blocks", type=int, default=16)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--spread-mode", default="conservative_max")
    args = ap.parse_args()

    if args.manifest:
        M, manifest, candidate_ids = load_oos_manifest_matrix(args.manifest)
        res = pbo_cscv(M, n_blocks=args.blocks, candidate_ids=candidate_ids)
        symbol = manifest.get("symbol", Path(args.manifest).stem)
        res.update({
            "source": "oos_candidate_matrix_v1",
            "manifest": str(Path(args.manifest)),
            "symbol": symbol,
            "record_count": manifest.get("record_count"),
            "matrix_digest_sha1": manifest.get("matrix_digest_sha1"),
            "candidate_id_sample": candidate_ids[:5],
            "engine_commit": _git_head(),
        })
        print(f"\n=== PBO (CSCV) [manifest {symbol}] engine={res['engine_commit']} ===")
        for k in ("n_configs", "n_obs", "n_combos", "logit_median", "pbo"):
            print(f"  {k}: {res.get(k)}")
        if res.get("pbo") is not None:
            verdict = "HIGH overfit risk" if res["pbo"] > 0.5 else ("MODERATE" if res["pbo"] > 0.25 else "LOW")
            print(f"  interpretation: PBO={res['pbo']:.0%} selection overfit -> {verdict}")
        selected = [s for s in res.get("candidate_stats", []) if s.get("selected_count", 0) > 0]
        if selected:
            selected.sort(
                key=lambda item: (
                    -int(item.get("selected_count", 0) or 0),
                    float(item.get("selected_oos_pbo", 1.0) or 1.0),
                )
            )
            print("  top IS-selected candidates:")
            for stat in selected[:5]:
                print(
                    f"    col={stat['matrix_column']} selected={stat['selected_count']} "
                    f"sel_pbo={stat['selected_oos_pbo']} "
                    f"mean_oos_rank={stat['selected_oos_mean_rank']} "
                    f"id={stat['candidate_id'][:12]}"
                )
        out = REPO / "pm_outputs" / "experiments" / f"pbo_manifest_{symbol}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        json.dump(res, open(out, "w"), indent=2, default=str)
        print(f"\nwrote {out}")
        return

    if not args.strategy:
        ap.error("--strategy is required unless --manifest is supplied")

    cfg = build_config(args.spread_mode)
    loader = DataLoader(cfg.data_dir)
    M = build_returns_matrix(cfg, loader, args.strategy, args.symbol, args.tf,
                             args.samples, args.seed)
    if M is None:
        print(f"{args.strategy} {args.symbol} {args.tf}: too few tradeful configs for PBO")
        return
    res = pbo_cscv(M, n_blocks=args.blocks)
    res.update({"strategy": args.strategy, "symbol": args.symbol, "tf": args.tf,
                "spread_mode": args.spread_mode, "engine_commit": _git_head()})
    print(f"\n=== PBO (CSCV) [{args.strategy} {args.symbol} {args.tf}] engine={res['engine_commit']} ===")
    for k in ("n_configs", "n_obs", "n_combos", "logit_median", "pbo"):
        print(f"  {k}: {res.get(k)}")
    if res.get("pbo") is not None:
        verdict = "HIGH overfit risk" if res["pbo"] > 0.5 else ("MODERATE" if res["pbo"] > 0.25 else "LOW")
        print(f"  interpretation: PBO={res['pbo']:.0%} selection overfit -> {verdict}")
    out = REPO / "pm_outputs" / "experiments" / f"pbo_{args.strategy}_{args.symbol}_{args.tf}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    json.dump(res, open(out, "w"), indent=2, default=str)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
