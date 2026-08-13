# Bad idea register

**Purpose.** Record what was tried and failed, *and the condition that made it
fail*, so that a discarded idea can be revived when that condition materially
changes — without the program looping endlessly over ideas that are dead for
structural reasons.

An idea is not "bad". An idea is **bad under stated conditions**. A slow
function using the wrong library is a bad idea until a better library exists;
then it is the best idea available. The register exists so that we know which
is which, and can tell the difference without re-deriving it.

This covers **code, evidence and methodology alike**. A discarded measurement, a
withdrawn estimate and a rejected fold-weighting scheme are all entries here.

---

## The three states

| State | Meaning | To leave this state |
|---|---|---|
| **DORMANT** | Failed under a *contingent* condition that could plausibly change. | Evidence that the named revival trigger has actually fired. |
| **INTERRED** | Failed for a *structural* reason true under any plausible future condition. | Extraordinary: requires a PM ruling that the structural claim itself was wrong. |
| **SUPERSEDED** | Not wrong, but a strictly better mechanism now exists. | Only if the superseding mechanism is itself withdrawn. |

**The bar for INTERRED is deliberately high.** Interring is a claim about all
future states of the world, and most failures are not that. When in doubt,
DORMANT with an honest trigger. A register full of INTERRED entries is a
register someone stopped thinking about.

**The bar for revival is symmetric.** "Let us try it again" is not a trigger.
The named condition must be shown to have changed, with evidence, before the
work restarts. This is what stops the loop the operator warned about.

---

## What every entry must contain

1. **What was tried** — concrete enough to rebuild.
2. **What killed it** — the measurement or argument, with a reproduction path.
3. **The revival trigger** — the specific condition whose change would make it
   viable again. *An entry without this is not finished.* For INTERRED entries,
   state instead why no such condition exists.
4. **State** and the evidence reference (commit, bus id, or doc).

The revival trigger is the load-bearing field. Everything else is history; the
trigger is the part that does work later.

---

## The anti-convenience rule

The operator's standing constraint, recorded here because this register is
exactly where it would otherwise be quietly violated:

> **An efficiency change may never be justified by its speed benefit alone if
> it also reduces the quality of the search or the strength of the evidence.**

Every change is classified before it lands:

| Class | Definition | How it may be justified |
|---|---|---|
| **A — output-preserving** | Provably identical outputs. Bit-identical hashes, bit-identical metrics, identical error behaviour. | Speed alone is sufficient. |
| **B — output-changing** | Changes any produced number, decision, or evidence. | **Never on speed as the sole justification.** Approval requires correctness or out-of-sample evidence. Runtime and resource feasibility may *constrain* a choice among quality-equivalent or better designs; they may not *be* the argument. |

*(Amended from an outright ban on citing speed, after XPM-048: deployability is a
real constraint and making it unmentionable was overbroad. The protection that
matters — runtime pressure must not decide a quality question — survives in the
narrower form.)*

### Coverage is not a currency

An operator direction, recorded as a standing preference rather than a fixed
number, because it decides what "efficiency" is even allowed to mean here:

> **Never reductionist.** More coverage is always preferred — more of the
> hyperparameter field, more of the situations the system may face. Quality and
> accuracy lead; efficiency follows them.

This splits cheapening a search into two things that look alike on a stopwatch
and are opposites in kind:

| | What it does | Verdict |
|---|---|---|
| **Shrink the space** | Fewer candidates are *reachable*. Coverage falls. | Forbidden as an efficiency move. It is a quality decision wearing a speed argument. |
| **Navigate the space better** | The same or a wider **economically plausible** domain explored for fewer evaluations. | This is the target — on evidence, not assertion. |

**But reach is not free, and this is the correction that makes the rule usable**
(XPM-049). A larger hypothesis field raises multiplicity and spends validation
power; redundant or economically impossible regions are not coverage, they are
noise with a bigger denominator. That is not hypothetical — it is the mechanism
behind the June book, where both multiple-testing defences were inoperative and
157 rows survived on thin samples.

The two rules resolve, and both halves are binding:

> **Reach must never be traded for runtime** — the operator's direction.
> **Reach is bought with validation power** — so widen the economically
> *plausible* domain rather than the raw count, and pay for any widening in
> multiplicity accounting rather than assuming it is free.

Neither agent may cite runtime when arguing about reach, in either direction.

A navigator earns adoption only on a **precommitted** equivalent-or-wider
plausible domain, a fixed-compute comparison, untouched OOS/forward outcomes, and
PBO/DSR plus stability. Claiming the "true optimum is still found" is
inadmissible — it is unknowable. Widening the field or raising a budget likewise
needs a marginal-value hypothesis and a stop rule, not a free pass.

The operator's Bayesian/Optuna illustration was explicitly **illustrative, not
prescriptive**. No method is mandated; the property is.

**Consequence for Class A.** Pure overhead removal — machinery around the search
rather than the search itself — is unconstrained by any of this, because it
changes no output and touches no coverage. That is where every efficiency win in
this program has come from so far, and it is the safe place to keep looking.

**Worked example of a forbidden move**, stated by the operator: cutting the
hyperparameter budget from 200 to 50 and reporting it as an efficiency win. It
is a Class B change. It makes the search less likely to find the real best
parameters, so it must be argued — and measured — as a *quality* decision. That
it also runs faster is irrelevant and must not appear in its justification.

The inverse also holds: a Class B change that *improves* accuracy is legitimate
even if it costs time. Recency-weighted folds instead of equal weighting, or a
correction to gate minima, are Class B changes to be argued on whether they
produce a book that makes money out of sample — never on runtime.

**Class A is where efficiency work belongs.** Everything landed in this program
so far under the efficiency heading was Class A and was pinned by an equivalence
test that passed against unchanged code first.

### Why the convenient shortcut is worse than it looks

The obvious cost of cutting the trial budget is a weaker search. The larger cost
is that **it destroys the diagnostic signal**, and this program is the proof.

The run being slow was information. Chasing that signal instead of suppressing it
is what surfaced, in order: a per-bar Python hash loop that was 30% of every
backtest; per-bar drawdown and `safe_float` loops; an O(n) per-trade ATR fallback
measuring ~89× a normal backtest; a feature cache that deep-copies 174 ms on every
hit; a stale-cache defect returning a fifty-times-wrong ATR; an aliasing defect in
a sibling cache; and seven anti-forgery tests that had not executed for five
contract versions.

Had the budget been cut to 50 and the win declared, the run would have been
roughly four times faster, every one of those defects would still be in the code,
**and the symptom that led to them would be gone.** The program would have been
slower, less accurate, and blind — while reporting an efficiency improvement.

That is the real argument against convenience: not that it trades quality for
speed, but that it spends the evidence you needed to find the actual problem.

---

## Register

### Interred

**BI-1 — `min()` clamp on the frozen-path stress swap** · claude · CPM-022
Proposed clamping `stress_swap = min(stress_swap, base_swap)` to force cost-stress
monotonicity after the H1 canary died on "trade 23 improves under stress".
*Killed by:* it forces monotonicity by discarding the disagreement rather than
resolving it. The frozen path would have stopped failing without anyone learning
why, and a genuinely mispriced stress swap would be silently floored to the base.
*No revival condition:* masking a diagnostic signal is wrong under every
condition. Superseded by E25 (`c5c42cc`), which removes the cause.

**BI-2 — caching a Series in `DataFrame.attrs`** · claude · `5b0180c` draft
*Killed by:* pandas compares `attrs` dicts with `==`, so a Series stored there
makes `obj.attrs == attrs` evaluate to a Series and **any** later `pd.concat` on
that frame raises. Caught by its own test before shipping.
*No revival condition:* this is pandas' comparison semantics, not a version quirk.

**BI-3 — feature-cache identity as `id(frame) + len`** · claude · `5b0180c`, reverted `c50758c`
*Killed by:* a same-length in-place OHLC edit returned a stale ATR — 0.000406
against a true 0.020400, fifty times wrong, silently, into stop distances (CX-084).
Invalid bar indices also drifted in both directions.
*No revival condition:* length is not identity for mutable content. Any future
cache here must be **content-bound**, which is a different idea (see BI-9).

**BI-4 — the 576M-backtest / "years" full-run cost model** · claude-pm · CPM-021, CPM-023, CPM-026
*Killed by:* it mixed the legacy Optuna path into the canonical executor, so the
run structure it multiplied by was not the one that runs. Withdrawn before use.
*No revival condition:* the arithmetic was over the wrong object. Any future
forecast must come from one measured end-to-end cell, not from scaling.

**BI-5 — config-only `evidence_as_of_utc` pinning for Forward Test A** · claude-pm · v1 pre-registration
*Killed by:* the active activation and calendar authority override the config
value, so the run would not actually be blinded. Corrected in v2 to an
authenticated cutoff-prefix.
*No revival condition:* config cannot outrank activation authority by design.

**BI-6 — reducing the hyperparameter budget to buy speed** · operator directive
Cutting `regime_hyperparam_max_combos` (200) to make runs faster.
*Killed by:* pre-emptively, as a Class B change dressed as Class A. Fewer trials
means a lower probability of finding the real best parameters; the speed gain is
inadmissible as justification.
*No revival condition as an efficiency measure.* A change to the trial budget may
only ever be proposed as a **quality** decision, with evidence about out-of-sample
outcomes — in which case it is a different entry, argued on different grounds.
*Note the asymmetry:* **raising** the budget, or widening the parameter field, is
not this entry and needs no revival — it runs with the operator's stated
preference and is limited only by whether the run completes. What is interred is
trading reach for runtime, not revisiting the number.

### Dormant

**BI-7 — fixed `--basetemp` in `pytest.ini`** · claude
Pinned a scratch dir to stop 138 temp trees landing in the repo root.
*Killed by:* a fixed basetemp is shared by every concurrent pytest process, so
two agents running suites at once wipe each other's temp files mid-run — Codex
measured 8 missing-temp-file failures with no failing assertion.
*Revival trigger:* **only one agent runs tests on this machine.** Then a pinned
basetemp is safe and the sprawl problem returns to being worth solving.

**BI-8 — `specialist_spokes_v2_max_validation_sharpe` 0.0 → 4.0** · claude · `d517d2c`, reverted `b2d0ba0`
An implausible-Sharpe-without-PBO guard.
*Killed by:* unreachable under `regime_scoring_model=legacy`, which production
uses. The claim that it rejected 157 rows was a replay against a dead gate.
*Revival trigger:* **the production scoring model stops being `legacy`.** The
guard is sound; only its reachability is broken.

**BI-9 — caching the ATR_14 fallback series** · claude
*Killed by:* BI-3's identity bug, and more fundamentally because
`FeatureComputer` supplies `ATR_14` on the canonical path, so the fallback must
be **zero-hit** — caching a path that should never execute buys nothing.
*Revival trigger:* **the fallback becomes a legitimate canonical path**, i.e. a
supported configuration where `ATR_14` is genuinely absent. Then it needs a
content-bound key, not an identity-bound one.

**BI-10 — removing the eager native-universe verification sweep** · claude (task 14) · superseded framing in XPM-036
Wanted manifest-trust to make cold verification cheap.
*Killed by:* a stale promotion receipt or a direct bars-path consumer would let
leaf mutation bypass runtime verification, so the sweep cannot be removed on the
current memo alone.
*Revival trigger:* **a code-version-bound semantic receipt at activation/promotion
exists, and every production bars consumer is forced through selected-leaf
verification.** This is E27's precondition, already agreed.

**BI-11 — `tools/agent_coordination.py`** (1,543 lines) · retired 2026-08-11
An OS-locked, hash-chained event ledger so two agents could prove neither had
tampered with the other's notes.
*Killed by:* single laptop, no attacker, against the operator's own
security-is-not-a-priority directive. Nothing imported it. The 326-line bus does
the same job.
*Revival trigger:* **a genuine multi-party trust boundary appears** — a third
party, a shared host, or an auditor who must verify the log without trusting us.
Preserved read-only as decision history.

### Stale framing

Register elements whose evidence has been withdrawn or superseded are flagged **in `RECONCILIATION_REGISTER.md` itself**, not duplicated here. Currently E15, E16, E17 and E7. An entry belongs in this file only when it is a failed implemented experiment, a rejected high-value hypothesis, or withdrawn evidence with a credible risk of being rebuilt.

## Protocol

1. **Any agent may add an entry.** Adding is cheap and always allowed; a failure
   nobody records will be repeated.
2. **DORMANT is the default.** INTERRED requires naming why no revival condition
   can exist, and a PM may challenge that claim.
3. **Reviving requires evidence the trigger fired**, posted to the bus, before
   any implementation work starts.
4. **Class A/B is declared in the commit message** of every change made for
   efficiency. A Class A claim must be backed by an equivalence test that passed
   against unchanged code first.
5. **Withdrawn evidence gets an entry too.** BI-4 is here precisely so nobody
   rebuilds a forecast on the same foundation.
