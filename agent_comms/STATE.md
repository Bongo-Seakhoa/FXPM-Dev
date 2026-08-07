# FXPM Program State

Updated: 2026-07-12 late (Claude). Rewrite in place; keep to one page.

NOTE: coordination authority = tools/agent_coordination.py (OS-locked,
hash-chained; see coordination/README.md). agent_bus.py remains vetoed.
AGENT_COMMS.md = operator digest. This STATE.md = shared one-page truth.

MILESTONE / SCOPE AUTHORITY = docs/PROJECT_MILESTONES.md (created 2026-07-19
on operator instruction): the dependable M0-M10 tracker that defines "done",
holds exit criteria + parked scope, and shows how far we are. Update it on
every ticked milestone. We are at the M2->M3 boundary (promotion imminent).

Commits landed: c90b7da (coordination protocol), 169f7e6 (v3 evidence +
bounded journals), 701645a (ATOMIC GENERATION STAGING — contract pt 9 done).
Claude ledger-compaction lane review-requested (b13cf065): canonical ledger
months losslessly gzip-archived with sha256 proof, readers transparent.

## Objective
Production-ready FXPM, medium-to-high risk appetite (as calibrated ruin
tolerance, never weakened evidence), consistently profitable on live
markets. Demo validation precedes any funded exposure. Operator: Bongo.

## Phase
Repair + per-cell rebuild. NOT ready to optimize: regime provenance sidecar
absent; roster must be rebuilt per-cell (~84 symbols / 287 cells vs old
35/210).

## Landed 2026-07-12
- Per-cell SYMBOL|TF eligibility contract pts 1,2,5,6,7 (Codex): whole-symbol
  veto removed, active-TF-only loading, M5-spine removed, provenance
  preflight before workers, technical failures never overwrite ledger.
- get_bars_range poll-to-stability + segmented coverage v2 + pm_trade_power +
  pm_sample_dependence + pm_native_resolution (Codex).
- Bounded storage: manifest rotation/dedupe, ledger retention (Claude).
- pm_configs.json restored to HEAD 73-winner book; expired, evidence-only
  (Claude).
- docs/EVIDENCE_MINIMUMS_AND_BOUNDARIES_20260712.md (Claude).
- Agent protocol v1: agent_comms/ + tools/agent_bus.py (Claude).
- Full suite: 1639 passed / 0 failed / 5 skipped (Claude run, ~18:10).

## Open (ordered)
1. Codex L-0002/L-0003: finish per-cell activation contract + active-cell
   regime tuning/preflight; atomic generation promotion (CX-001: blocking).
   CC-002 review questions RESOLVED by CX-001.
2. Memory governor + remaining copy elimination; 8 GB OOM guardrail stands
   (blunt workers=1 clamp rejected by operator).
2b. Methodology decisions D1-D4 pending Codex cross-review (screening stage;
   walk-forward process validation; refit-to-present production book under
   probation; edge-decay retrain cadence). See AGENT_COMMS 2026-07-12 +
   docs/SCREENING_AND_BLEEDING_EDGE_METHODOLOGY_20260712.md.
3. Reacquire/re-activate per-cell roster (~287 cells).
4. pm_regime_tuner for every active cell (params + provenance sidecar).
5. One-symbol canary: prove feature computation + strategy search + nonempty
   artifact + peak-memory measurement.
6. Full optimization -> synchronized replay -> capital envelope calibration
   at operator ruin tolerance -> demo shadow.

## Hard rules in force
- Funded trading disabled until evidence gates + operator approval.
- fxpm_academic_research repo untouchable.
- No arbitrary thresholds; evidence or calibration only.
- All memory/storage bounded, zero accuracy cost.
