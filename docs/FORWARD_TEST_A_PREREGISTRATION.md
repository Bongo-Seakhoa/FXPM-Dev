# Forward Test A — pre-registration (v2)

**Written before the test runs. Any change after seeing a result permanently
consumes the window.** Agreed CPM-010 / XPM-012 / XPM-013 / CPM-012, 2026-08-11.

v2 supersedes v1 on two counts, both from XPM-013, both of which invalidated
what I originally wrote. They are recorded rather than quietly replaced.

## Correction 1 — a config-only cutoff does not work

v1 proposed pinning `evidence_as_of_utc = 2026-04-01`. **That is invalid.** The
active activation and calendar authority override the config value, so the run
would not actually be blinded. A must instead use **one authenticated
cutoff-prefix with a matching activation**:

| | |
|---|---|
| Selection cutoff | **2026-02-25T00:00Z** |
| Mandatory purge through | **2026-03-31** |
| Scoring window | **2026-04-01 → 2026-07-14** |

Use the existing authenticated prefix/activation path. **Build no alternate
pipeline for this.**

## Correction 2 — my thresholds were uncalibrated

v1 carried R2 decay < 0.60, R3 drawdown > 2×, R4 single cell > 25%, and a flat
150-trade floor. **All four are withdrawn.** I could justify their direction and
not their values, which is the same error I conceded on N ≥ 20 earlier. v2 reuses
limits the system already calibrates rather than inventing new constants.

## What A is

**A falsification gate. Reject-only.** A clean result means *not falsified* and
carries no estimate of current profitability.

**Hypothesis:** the frozen 2026-02-25 lineage does not collapse over
2026-04-01 → 2026-07-14 under unchanged costs and controls.

## Not-falsified requires all of

| # | Condition | Source |
|---|---|---|
| 1 | ≥ **100** portfolio positions | existing portfolio-replay minimum |
| 2 | Every claimed cell ≥ **15** trades | existing validation trade floor |
| 3 | Absolute PF **non-losing** | breakeven, not an invented bar |
| 4 | Frozen **20%** drawdown limit respected | existing replay DD limit |
| 5 | All existing risk limits respected | already configured |

**Any insufficiency or gate breach rejects the lineage.** Insufficiency is a
result, not a reason to retry.

## Strongest contrary evidence, recorded before the run

The cutoff discards **139 days 8 hours** of recency. A is therefore testing a
*deliberately stale* model, and **can reject a process that is currently
viable**. A rejection is evidence against this lineage, not proof the approach
is dead. A cannot authorize or estimate current profitability in either
direction.

## Stopping condition

One run. Once **any** forward metric is observed the window is consumed, and no
tuned successor may reuse it.

## Test B — the decisive one, and its real timeline

B freezes the July-14 lineage **before** acquiring or inspecting later data, then
scores on data nobody has seen.

| Milestone | Date |
|---|---|
| Valid forward days available today | **zero** |
| First valid day after purge | **2026-08-18** |
| 63-day preliminary checkpoint | ~**2026-10-19** (preliminary only) |
| 365-day credible horizon ends | **2027-08-17** |

B cannot start for a week, gives a first preliminary read in about ten weeks, and
reaches a credible one-year result in August 2027. That is the honest schedule
for certification, and it is not shortenable by any code change.

## Recorded before the run

- The frozen lineage, its backtest metrics, and the replay output are all
  captured before any human or agent reads the forward numbers.
- Costs and controls unchanged from the frozen commit.
