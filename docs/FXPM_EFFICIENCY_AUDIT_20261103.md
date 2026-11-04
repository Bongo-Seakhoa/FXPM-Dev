# FxPM efficiency audit, pass 1

**Date:** 2026-11-03
**Author:** claude-pm
**Authority used:** checkout at `41aaf281`, read-only inventories of the authoritative research path, unit measurements on this laptop.
**Status:** assessment and ranked recommendations. Nothing here changes code or policy. Figures labelled "measured" were timed today; figures labelled "derived" are arithmetic on counts read from source.

## 1. The operator's question

Where is work repeated that need not be, what belongs in memory versus storage, which loops and repeated functions dominate, is the prototype route still the best route, and where is a process more complicated than its job. The over-hashing habit of the two PMs is named explicitly.

## 2. Measured unit costs

| Operation | Measured |
|---|---|
| Canonical JSON of a 300-attempt plan body, then sha256 | 2 ms, 2 ms |
| `_frame_sha256` on a 100k-row, 7-column frame (pandas vectorised hash) | 11 ms |
| `_frame_sha256` on a 600k-row frame (an M5 prefix) | 166 ms |
| sha256 of the EURUSD M5 bars CSV, 118 MB | 320 ms |
| pandas read of that CSV, two columns | 2.3 s |
| pandas read of that CSV, all columns | 5.4 s |
| Canonical JSON of 50k derived bars as dicts, then sha256 | 245 ms, 213 ms |
| Canonical JSON plus sha256 of 10k trade records | 45 ms |
| Planner `_strategy_domains()`, the registry truth recomputed by every frozen-process verification | 605 ms per call, 100 domains |
| Import of the executor module, cold | 3.5 s |

Reading: a hash is cheap. Reading a CSV is ten to seventeen times the cost of hashing it, and serialising a large object to canonical JSON costs as much as hashing it. Over-hashing is not a CPU problem. Its cost is elsewhere: repeated file reads for re-verification, deep copies made so that something can be hashed, receipts that embed receipts, and the volume and brittleness of the machinery that produces them.

## 3. The volume of the machinery

| Module | Lines | sha256 mentions | canonical-bytes mentions | verify or reverify mentions |
|---|---:|---:|---:|---:|
| pm_authoritative_search_execution.py | 6,128 | 530 | 66 | 66 |
| pm_production_search_planner.py | 5,278 | 439 | 28 | 48 |
| pm_winner_raw_evidence.py | 5,964 | 671 | 39 | 11 |
| pm_winner_selection.py | 5,415 | 323 | 11 | 5 |
| pm_native_prefix_production.py | 4,671 | 275 | 22 | 25 |
| pm_historical_production_adapter.py | 6,675 | 645 | 56 | 73 |
| pm_historical_search_producer.py | 2,863 | 218 | 28 | 31 |
| pm_book_multiplicity.py | 4,215 | 467 | 24 | 7 |
| pm_core.py | 10,991 | 110 | 0 | 0 |
| pm_pipeline.py (legacy, not on the authoritative path) | 19,140 | 608 | 7 | 13 |

The product is 234,837 lines of `pm_*.py`. The PM charter recorded in August that 46 percent of the engine was authority machinery. The research path above alone carries over four thousand hash mentions. The frozen-process verifier is called from eleven sites across six modules; each call recomputes the registry truth.

## 4. What is recomputed, per cell and per attempt

Source map by the recomputation inventory (file:line in `pm_authoritative_search_execution.py` unless stated). One cell is symbol x timeframe x regime; 300 attempts per cell; four regime cells share a symbol and timeframe.

| Item | Once per cell | Per attempt | Per partition x tier | Shared across the four regimes? |
|---|---|---|---|---|
| Timeframe fit: prefix reopen, spread inputs, P75 and P95 probes, regime detector, deep copy and frame hash (1067 to 1205) | yes, re-run by `_reverify_fitted_timeframe` at the start of each cell (2743; 5159, 4164) | no | no | **no**, each regime cell re-runs it |
| Regime label surface (1137) | yes, inside the fit | read only | mask applied per partition x tier (2443) | no |
| Full-prefix features (`compute_for_strategy`, 2356) | **no** | **yes** (2678 to 2344) | deep-copied slice per partition x tier (2398) | only through a global 24-entry FIFO cache that returns copies |
| Strategy signals (2369) | **no** | **yes**, on the full prefix | sliced and masked per partition x tier (2401, 2443) | no |
| Partition slices (`_fold_partitions`, 1972) | no | yes, 4 folds x 3 slices, in the precommit pass (2136) and again at execution (2683) | each a `.copy(deep=True)` then `_frame_sha256` (1908, 1942) | no |
| Entry, stop and target precompute (pm_core 8651 to 8801) | no | no | **yes, inside every kernel run** | no |
| Backtest kernel runs | no | **24 per attempt** | 4 folds x 3 partitions x 2 tiers (2682 to 2688) | no |
| Cost-attestation arrays (2294) | no | 24 | per partition x tier | no |
| Attempt evidence JSON | no | written with fsync (3642) then fully re-read, re-hashed and re-parsed in the same run (3751, 4938, 5371) | | |

Derived arithmetic. Kernel runs per regime cell: 300 x 24 = 7,200. Per symbol and timeframe: 28,800. Per 28-day cycle over 2,400 cells: about 17 million kernel runs, before Route A. Feature and signal builds per symbol and timeframe: 300 x 4 = 1,200, where one would do. The one measured full cell took 54,581 seconds; the budget is about 1,000 seconds per cell.

Both tiers of a partition receive identical signals, which the code asserts (2703), and yet the slice, the mask and the cost arrays are rebuilt for each tier.

Python per-bar loops that run on every attempt's full prefix: `cci` and `hull_ma` rolling applies in pm_core (7166, 7240); plain loops in Supertrend (pm_strategies 1228), Fisher transform (2117), PivotBreakout (2565), InsideBarBreakout (3281), TurtleSoupReversal (3404), OBVDivergence (3660), PSAR (3832), Kaufman AMA (3959), VIDYA (4236), Heikin-Ashi (640), rolling percentile rank (340); and the per-signal stop loop in `Backtester.run` (8711 to 8801) whenever a strategy overrides stops or uses structural stops, which runs per partition x tier. The regime kernel is numba and runs once per cell.

Disk round trips inside one run: every attempt's evidence is written, fsynced, re-read and re-hashed; the current cross-binding is persisted twice and compared (4818, 4906); the feature disk cache is off by default.

## 5. Ranked findings

Ranked by expected gain against the per-cell budget, with risk.

1. **One timeframe fit per symbol and timeframe, not four.** The prefix reopen, probes, regime detector and frame hash are repeated for each regime cell. Gain: four to one on everything above the attempt loop. Risk: none to results, the four cells read the same surface today; the change is in who owns the fitted object.
2. **Features and signals once per configuration per symbol and timeframe, reused across the four regimes and both tiers.** This is the accepted one-traversal form (E2639, E2640) and the heart of the operator's symbol-first instinct. Gain: up to four to one on feature and signal time, and the removal of two of every three slice-copy-hash cycles. Risk: none to results when the regime mask is applied after generation, which is already how the executor gates.
3. **Route A collapses the kernel work.** Today the account kernel is the search engine: 24 runs per attempt. Under Route A the search runs on the cash-free opportunity tape, whose first-touch index is vectorised and whose chain is one linear pass, and the kernel runs only on the shortlist of five: 5 x 24 = 120 runs per regime cell instead of 7,200, sixty to one. The unknown is the per-evaluation cost of signals plus labelling at thousands of tuples; the percentage checkpoints (1, 2.5, 5, 10 percent of N_s) must be capped by a measured time budget per strategy, because N_s reaches 1.27 million for RTO.
4. **No disk round trips inside a run.** Attempt evidence should live in memory for the cell and be written once as the end-grid row plus the compact evidence a cold reopen needs. Writing, fsyncing, re-reading and re-hashing every attempt is the ceremony the operator described, and it also multiplies disk churn on a laptop that was paging this afternoon. Risk: the cold-reopen evidence must still be complete; the row's retained view contract and the compact evidence cover it.
5. **Stop copying to hash.** `_partition_slice` deep-copies then hashes; `_slice_full_prefix_strategy_surface` deep-copies features and signals per partition and tier then runs a full `.equals`; `Backtester.run` copies signals again. Use index views; hash once per attempt if a hash is needed at all. Gain: memory and time proportional to prefix size times 24. Risk: low; the kernel already receives arrays.
6. **Vectorise the per-bar Python loops that dominate signal generation.** Eleven strategies and two indicators loop in Python over a 600k-bar M5 prefix on every attempt; the structural-stop path loops per signal per kernel run. Measure which strategies dominate on the development set before touching any, then port the top ones to numba with byte-equal signal tests. Risk: medium, mitigated by byte-equality tests on retained fixtures.
7. **Verify artifacts at open and at close, never per attempt or per cell.** `reverify_activated_native_prefix_product` is called at cell start (via the re-fit), at finish and in six other modules; the frozen-process verifier recomputes a 600 ms registry truth at eleven call sites. Rule: one verification per artifact per run, at open; one at close; a receipt carries one sha per object; a receipt never re-hashes an embedded receipt; immutable frozen dataclasses are not re-serialised to prove they are unchanged. This is the hashing discipline the memory directive already states. Section 11 lists the sites, their per-cell multipliers and which of them are gates that stay.
8. **Feature cache semantics.** The global FIFO of 24 entries returns copies on hit and on put, and its key includes a hash of the data. Replace with a per-job cache keyed by feature set and prefix identity, holding one immutable frame per key, no copies, sized to the job and released with it.

## 6. Memory versus storage

- **In memory for the job:** the canonical M5 rows for the hull, the derived view, the feature surfaces per strategy feature set, the regime label surface, each configuration's signal vector and opportunity tape, the per-regime chains and leaderboards. All of it is deterministic from code and M5 and is cheaper to recompute next cycle than to verify from disk.
- **In storage, permanently:** canonical M5, the frozen spread profile, the end-grid rows, the retained view-contract bodies, and the compact per-cell evidence that a cold reopen needs.
- **Not in storage:** feature caches across cycles, per-attempt evidence, partition slices, spool copies of inputs. Every cycle extends the data, so a cross-cycle feature cache is invalidated by design and the verification needed to trust it costs more than the recompute.

## 7. Is the prototype route still the best route

Three routes were weighed on the numbers above.

- Keep the 300-attempt roster and add one-traversal sharing: the measured ceiling is a quarter of runtime, because the roster seeds variants per cell and only the defaults repeat. Not enough.
- Adaptive search through the account kernel: 24 kernel runs per evaluation at a 10 percent coverage target is infeasible for any domain above a few thousand tuples.
- Route A: adaptive search on the cash-free tape, chains per regime from one pass, kernel only on the shortlist of five. Kernel work falls sixty to one, feature and signal work falls four to one, and selection and qualification happen on different surfaces. Route A stands, with two conditions that are now explicit: a measured per-evaluation cost before the coverage checkpoints are set, and the byte-equality of the chain against the kernel, which lane 213 established.

## 8. Over-complication candidates, in order

1. Per-attempt evidence written, fsynced, re-read and re-hashed inside one run.
2. Four regime cells re-fitting one symbol and timeframe.
3. Deep copy, hash and equality on every partition x tier slice.
4. Eleven call sites re-verifying one frozen process, each recomputing the registry truth.
5. Receipts nested inside receipts with their own shas.
6. The 300-attempt precommitted roster as the search mechanism; it was a defensible precommitment for the funded path, not a search policy.
7. A global feature cache that copies on every hit.

None of these protects a result. Each exists to prove something that can be proved once.

## 9. Data gaps, the ladder

Operator direction 2026-11-03: first look at the source. The 33 no-bars cells (7 of them M5) get one MetaTrader re-pull with max bars unlimited and a restart, published as a new activation edition with provenance, same server first; a different server only with its own provenance and identity audit, never spliced into one series without a versioned change point. What the broker cannot supply is treated as an unavailable interval, like a weekend, with no invented prices, and the cell continues weakened and recorded as such. The 88 daily-like early-year cells are not a download problem; they take the resolver segment exclusion. Escalation is to the operator only after the source and the mitigation have both been tried. This is a one-time patch, not a stage in every job.

## 10. What happens next

1. W5.1 measurement on one native and one derived cell with stage timers: fit, features, signals, kernel, slices and copies, evidence I/O. It ranks findings 1 to 6 by real share.
2. W6.3 edition design adopts findings 1 to 5 and 7 as design rules; the per-evaluation cost from W5.1 sets the coverage caps.
3. A strategy vectorisation lane after W5.1 names the dominant strategies.
4. Council rule on hashing discipline, finding 7, recorded in the landing plan.

## 11. Hashing and re-verification inventory, pass 2

Read-only pass over the six research-path modules at `41aaf281`, spot-checked by hand at the ten largest sites. Letters: **E** `pm_authoritative_search_execution.py`, **R** `pm_winner_raw_evidence.py`, **S** `pm_authority_spool.py`, **P** `pm_production_search_planner.py`, **N** `pm_native_prefix_production.py`, **C** `pm_core.py`. Multipliers per regime cell: N = 300 attempts, F = 4 folds (`min_causal_folds`, pm_winner_selection.py:233), 3 partitions per fold, 2 tiers per partition.

Codex-PM asked (E4570) that the audit separate memoisation of immutable transforms from the fresh-fit and final-source gates that must stay. The last column does that. **Memoise** means the same bytes or the same frozen object are hashed again with no intervening write, so one digest can be carried. **Gate, once** means a real verification that belongs at one point per artifact per job (open or close), not per attempt, fold or regime. **Gate, keep** means a check whose freshness is the point and which stays as it is.

### 11.1 The twenty largest sites by estimated work per cell

| # | Site | What is hashed or re-verified | How often per cell | Class |
|---|---|---|---|---|
| 1 | R:4613 and 4619 into R:3988, 4023, 3915, 4231 | Source-observation receipt, one row per bar: file re-read and streamed sha, decode, canonical re-check, `_sha_json` twice, rows rebuilt, `hash_pandas_object`, per-bar `spread_admission` per tier | 3 partitions x F x N = 3,600 | Memoise: the receipt is immutable once published; verify at publish, carry the sha |
| 2 | R:806 and 814 into S:631, S:1079, S:1264 | Every `_put_ascii_json`: canonical bytes, hash while writing, read back and hash, hash the in-memory bytes again, plus a reopen of the generation-context file | about 21 artifacts per fold, so about 84 x N | Memoise: one hash at write, one readback at close |
| 3 | E:3654, 3681, 3771, 3802 | Whole staged attempt body, all lanes, trade lists and per-bar signal vectors, serialised and hashed four times (digest before write, digest during write, file digest at reopen, canonical digest of the parsed body) | 4 x N | Memoise: serialise once, hash once, readback once at close |
| 4 | E:2609 to 2614; R:508, 520 | Signal vector: `tolist`, sha, canonical JSON of the per-bar values; Raw re-hashes it and packs it in a per-bar `struct.pack` loop | 3 x F x N | Memoise: one digest per configuration |
| 5 | E:2254 to 2257, 2522; R:3884 to 3887, 4583 | Lane trade list, position events, axis and attestation, hashed on the E side and again on the R side, about four passes each | 6 lanes x F x N x 4 | Memoise: hash at creation, carry |
| 6 | E:1942 via E:2136 and E:2683 | `_frame_sha256` of a partition frame after a deep copy; `_fold_partitions` runs twice per attempt (precommit pass and execution pass) | 2 x 3 x F x N = 7,200 | Memoise: partitions are index ranges of one fitted frame; hash the frame once per cell, bind partitions by bounds |
| 7 | C:1474, 7690 (twice), 9162, 9022 and 9165 | Per `Backtester.run`: axis bytes per bar, both spread arrays, the observation series, the admission mask | 6 x F x N = 7,200 runs | Memoise: inputs are identical across the 24 runs of an attempt and across attempts of a partition |
| 8 | E:2294 to 2297 | Half-spread arrays and attestation rebuilt and re-hashed per partition and tier | 6 x F x N | Memoise: once per partition per cell |
| 9 | R:4809 | `_sha_json(attempt_ledger)`, a ledger over all attempts, so the cost grows with N squared | 2 x F x N | Memoise: hash the ledger once when it is sealed |
| 10 | E:2487, 2695; R:2418, 2419, 4954 | Source-contract identity, including the constant seed, rebuilt per call | about 6 x F x N, two to four times each | Memoise: a constant per cell |
| 11 | E:2214, 2235; R:1740 into R:1635, 1619 | `_sha_json(pair)` plus attestation rebuild and sha | 9 x F x N | Memoise |
| 12 | E:2535 to 2647; R:4428 | Seven provenance receipts with canonical round trips (2559, 2620, 2641); Raw re-hashes each | 7 x F x N | Memoise: one sha per receipt |
| 13 | E:2743 into E:1067, 571, 594, 616, 834, 897, 1205, 2765 | `_reverify_fitted_timeframe` is a full refit: native source CSV scan (N:1861), prefix CSV hashed about three times, fine product re-verified or re-produced, config files read and hashed twice, two whole-frame hashes | once per regime cell, so four per symbol and timeframe | Gate, once: one fit per symbol and timeframe per job, verified at open (finding 1) |
| 14 | P:2422, 2431, 2671 | Prefix CSV hashed and parsed again; whole-frame hash; source-frame hash | once per cell | Memoise within the job: the fit already proved these bytes |
| 15 | R:4189, 4214 | Fine native product verified and its frame opened, twice; the comment at R:4210 calls the second an independent publication reopen | once per cell | Gate, once: the final-source check at close stays; the duplicate inside one function does not |
| 16 | R:1659, 1685, 5545, 5549, 5550, 5762, 5764, 5816, 5817 | Precommitment body over all rows and the attempt ledger, hashed more than once each | once per cell, several passes | Memoise: seal once, carry |
| 17 | P:1937 into P:645, 402 to 404; P:2037 to 2044; P:2188 | Strategy source files hashed and the process body recomputed (the 600 ms registry truth, section 2); 300 canonical comparisons and 300 shas; plan receipt re-put | once per cell, eleven call sites across the path | Gate, once: one process identity per job at open |
| 18 | E:1719 to 1722, 1590 | Four canonical serialisations per attempt; four roster shas | N; once per cell | Memoise |
| 19 | E:2160, 2161, 2182; R:4007 to 4009 | Building and publishing the source-observation receipts, one row per bar; the body is hashed twice in publish | once per unique partition geometry | Gate, once, with the double hash removed |
| 20 | E:3316, 3317, 3323 (declared mode) | Declared unit file: `_verify_artifact_file` reads and hashes it, `_stream_file` reads and hashes it again, then a canonical re-compare | up to three reads per attempt | Memoise: one read |

### 11.2 Same object, no intervening write

These pairs hash or read the same bytes twice in a row and carry no new information.

- S:631 into S:1264 (via S:1079) into R:814: the artifact bytes are hashed three times at the moment they are written, with one full readback.
- E:3316 and 3317, and likewise E:2816 and 2817: the same file is read and hashed twice back to back.
- E:3654 and 3681: the body is serialised and hashed before the write and again during it. E:3771 and 3802: the file is hashed, then the parsed body is re-serialised and hashed.
- R:3915 and R:4231: the same source-observation body gets `_sha_json` twice. R:4007 and 4008: the same body is hashed twice in publish.
- E:2136 and E:2683: `_fold_partitions` runs twice for the same attempt, so the E:1942 frame hash runs twice per partition.
- E:5236 and E:5285: `_sha256(plan_body)` is computed twice on the non-declared path.
- E:782 and E:890: the config and broker bytes are hashed when read, then hashed again by the derive step.
- E:1067 and E:571: inside one fit the prefix CSV is opened and then re-verified at once.
- E:5159 then E:5160 into P:2422 and 2431: the reverify opens and hashes the frame, then the planner reopens and re-hashes it.
- R:4189 and R:4214: the fine native product is verified twice; the comment says this is intentional.
- R:5549 and 5550 against R:5762, 5764, 5816 and 5817: the precommitment and the attempt ledger are re-hashed.
- E:2695 and R:2419: the constant seed identity is recomputed for every partition and tier.
- R:4809: the same ledger is hashed again for every tier and fold.

### 11.3 What this changes in the ranked findings

- Finding 7 (verify at open and close only) now has its site list: rows 2, 3, 13, 15, 17, 19 and 20 are the gates to keep once; everything else in the table is memoisation of an immutable object.
- Rows 1, 6 and 7 are the three largest and are all per-bar work repeated per attempt. Under Route A the account kernel runs 120 times per regime cell instead of 7,200, which removes rows 7 and 8 almost entirely; rows 1 and 6 fall with finding 2 (one feature and signal surface per configuration).
- None of the memoisation rows changes a result. A carried digest is the same digest. The gates that stay are the fresh fit at job open, the process identity at open, and the final-source check at close.

### 11.4 Not established in this pass

- `build_trade_power_multiplicity_receipt(attempt_ledger)` at R:4458 runs 2 x F x N times and may hash the whole ledger; `pm_trade_power.py` was not read.
- The historical adapter's `freeze_observed_cell` and `complete_observed_cell` were not read; if they call `verify_source_observation_reference_closure` (R:4046) that adds 7 x F x N provenance file reads.
- `current_refit.verify_current_refit_search_artifacts_bytes_v2` (E:1718, per attempt) and `spool_bound_fitted_state` (E:2938) were not opened.
- C:9022 against C:9165 may hash the admission mask twice in one run; not confirmed.
- `_revalidate_read_pins` in `pm_explicit_derived_native.py:1261` re-hashes CSV files already hashed by `_DigestReader` (line 498); that module is off the native search path and belongs to the derived consumer review.
- Per-cell wall-clock shares remain unmeasured until W5.1 runs stage timers on real inputs. Codex-PM's caution (E4570) stands: private metadata timings are not real-data cost estimates, and the multipliers above are counts, not seconds.

## 12. Evidence

- Recomputation inventory and per-bar loop list: read-only pass over `pm_authoritative_search_execution.py`, `pm_core.py`, `pm_regime.py`, `pm_strategies.py`, `pm_production_search_planner.py` on 2026-11-03.
- Measurements: scratch scripts `measure_hash_costs.py` and `measure_hash_costs_2.py`, run 2026-11-03 on this laptop.
- Counts: `grep -c` over the research-path modules at `41aaf281`.
- Hashing inventory, pass 2: read-only pass over `pm_authoritative_search_execution.py`, `pm_winner_raw_evidence.py`, `pm_authority_spool.py`, `pm_production_search_planner.py`, `pm_native_prefix_production.py`, `pm_core.py` at `41aaf281` on 2026-11-03, the ten largest sites re-read by hand.
