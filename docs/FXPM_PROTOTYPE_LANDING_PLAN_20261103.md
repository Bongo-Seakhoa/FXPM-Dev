# FxPM prototype landing plan

**Date:** 2026-11-21, pass 4.
**Author:** claude-pm, third PM seat.
**Baseline:** Product and test code at `4544e4f2a8`. BUS through E7375, including the operator directions relayed in E7367, E7368 and E7370.
**Status:** Plan. Landed work is committed and pushed. Open items below identify the remaining evidence or decision.

## 1. Goal and the test

The goal is a production-ready FxPM whose deployed Book makes money. Score proposals on net profit at both spread tiers. E7368 relays P75 as the minimum criterion for a candidate to win. The scoring method and evidence for adoption remain open.

E7367 relays that a trained cell with a winner may trade and a cell without a winner stays flat. There is no separate live validation stage in that direction. Live admission still needs an implemented route and the operator's choice of paper, demo or funded use.

## 2. Status by workstream

| Workstream | Status | Evidence |
|---|---|---|
| W1 Data base | M5 job-view consumer landed. Backfill, daily-like segment exclusion and EURZAR open; not rechecked in this pass. | E4710 |
| W2 Cost readiness | Cost diagnostics landed. One P75 and one P95 tier per symbol and timeframe. Commission is NOT_MODELED. | E4541, E6830 |
| W3 Evaluation edition (V19) | **Landed.** Eligible signals are evaluated independently, using net R and the same closed-bar regime label as live. | `7dc40929`, `182bd5ffec` |
| W4 One complete path, invented data | **Done.** One ordinary V19 one-period cell produced 300 durable attempt units, Raw, Winner, the per-cell Book marginal and a cold-verified commit. The outcome was synthetic NO_TRADE. The path defects found in the earlier proofs are fixed and landed. | Lane 366, E7318; `18d40993`, `847328c3`, `62c6ad43`, `12c3e0c2` |
| W5 One complete path, real data | **Fresh engineering direction relayed.** PMs must bind the first exact cell, native input and source window, record operator provenance and get Original Claude scope. Source330 ended before publication and remains closed. | E6922, E7314, E7367, E7371, E7373 |
| W6 Cost of a cell | The synthetic case took 1,034.5 s, including 162 s of attempt work and 802.8 s across the following phases. Generation-flag reuse landed. Exact-stream and source-only reuse remain parked pending bounded measurement and scope. | Lane 367 profile, `9078dd45`, E7327 |
| W7 One-pass route | Unit custody, cohorts and gated consumer landed switched off. Timeframe roster exists as a test-only recipe. Sharing a unit's ledger across regimes still lacks a two-regime witness. | `d2c0a5b0`, lanes 341, 343, 344 |
| W8 Development set | Ten symbols, one period, after acceptance of the first real cell. PMs select and bind the exact roster. Use the four-worker local default where the runtime supports it. | E7367, E7371 |
| W9 Grid | The full 2,400-cell Book family and V19 native-account fence are open. Current native development requires Route A. | E7163, E7318; `pm_historical_anchor_orchestration.py:3887` |
| W10 Live admission | The product route and admission policy are open. Promotion ends at the retired interval qualification path, and demo/funded entry refuses the unadopted policy. Root owns order-handling parity and account isolation design. Original Claude reviews. | E7367, E7368, E7371; `pm_forward_oos_replay_producer.py:5174`, `pm_live_readiness.py:83`, `pm_demo_readiness.py:1263` |
| W11 Research | Third owns scoring, search efficiency and research plans. Drawdown guidance is approximate. A stopping rule, winner method or order-handling change needs explicit adoption and its own evidence. | E7341, E7346, E7368–E7372 |
| Strategy 101 | Source landed, not production-qualified. | E6592 |
| Hygiene | Portable retirement and Winner fixtures landed. Third owns the read-only exact disposable-leaves packet. Protect active inputs and STOP, review and landing evidence. The earlier approved Git prune is complete. | Lanes 369–371, E6947–E6949, E7345, E7371 |

No FxPM run is active.

## 3. Order from here

1. **Bind one fresh native case** (W5). The scope names the exact cell, period and cutoff, verified source and cost pins, source window, ordinary V19 route and expected receipts. Confirm the named operator relay and Original peer scope before issuance.
2. **Run the one real V19 cell.** Codex uses the existing laptop route and records actual phase timings, results and custody. Require 300 durable units, Raw, Winner, the Book marginal and a cold-verifiable slot commit or an exact authenticated STOP. A valid NO_TRADE is a terminal outcome.
3. **Measure the remaining repeated work** (W6). Use retained evidence where it answers the question. Scope exact-stream verification and source-only reuse separately, while preserving provenance, publication and independent cold checks.
4. **Run the ten-symbol development set** (W8) after the first real result is accepted and the exact roster is bound. Use the four-worker local default and schedule it separately from any study rebuild that uses those workers.
5. **Complete the full Book and V19 native-account fence** (W9). Keep historical evidence and account state independently verifiable.
6. **Build live admission and mode selection** (W10). Connect the adopted Winner contract to promotion, prove handling parity and separate per-account budgets. The operator chooses paper, demo or funded before use.
7. **Review research proposals as later editions** (W11). Keep daily history windows plan-bound. Scope the scoring rule, order handling, hourly costs and early stopping independently where they change policy or execution.
8. **The 13-period study, once,** after landing.

The measurement, research design and architecture scopes can proceed independently of native execution. Schedule substantial laptop workloads against the four-worker default.

## 4. Working rules for this plan

- One period is the unit of every run, test and estimate. Two only for a hand-over check.
- No run is stopped by a clock. Expected outputs are written down before launch.
- Reuse facts already verified within the operation that owns them. Keep cold verification and source, authority, publication, risk and causal controls.
- Compare research methods on net profit at both spread tiers. Keep candidate eligibility and the evidence for adopting a method explicit.
- Route each review question to one Claude seat, not two.

## 5. Risks

- **Method.** On the study ledgers no grid is above zero beyond chance at both tiers. The largest measured levers are named in W11; none is in the engine yet.
- **Real-data defects past the search.** The synthetic proof covered one cell. Native input and account closure have separate evidence requirements.
- **Throughput.** One synthetic D1 cell supplies a timing baseline. Measure real phases before estimating the development and full Book schedule.
- **Policy and integration.** Winner adoption, retired promotion and account-aware order admission are open.
- **Storage.** Put generated payloads in ignored owned scratch. Apply the retention boundaries in E6947–E6949 to exact disposable leaves and keep unique evidence.

## 6. Change log

- 2026-11-21, pass 4: reconciled the synthetic V19 result, fresh engineering direction, hygiene and open live/scoring work to `4544e4f2a8` and E7367–E7375. E6899 retains the earlier pass.
- 2026-11-19, pass 3: Native315 exited and `fc3ef42a` landed lanes 316-328; the evaluation method became W9.
- 2026-11-19: re-baselined to `c85da1c5`; the earlier text is `git show e52fa02e:docs/FXPM_PROTOTYPE_LANDING_PLAN_20261103.md`.
