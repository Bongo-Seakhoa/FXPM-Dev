# F113: The "aggressive now, conservative when larger" scaling theory is not implemented — risk profile is static

## Category
Architecture / Risk (scaling readiness)

## Severity
Medium (High relative to the operator's stated $22→$10k scaling plan)

## Confidence
High

## Evidence
- The live risk profile is a **static config**: `position.risk_per_trade_pct 0.5`, `max_risk_pct 25`,
  `secondary_trade_max_risk_pct 50`, breakers `daily 50 / weekly 60 / open-dd 50` (`config.json:149-166,285`).
  None are a function of balance.
- The risk-scalar stack that *could* implement equity/vol-aware derating is wired but **disabled**:
  `live_risk_scalars_mode` is shadow/null (`audit_evidence_summary.json`), and `pm_main.py:4614-4630` only
  *logs* the would-be scalar in shadow mode (`live target unchanged`). Its context already carries
  `account_equity`, `account_peak_equity`, `target_annual_vol`, `open_exposure_pct`
  (`pm_main.py:4599-4613`) — i.e., the inputs for a scaling regime exist but are not applied.
- There is **no account-size regime switch** (no micro/intermediate/full-safety/preservation/recovery mode)
  anywhere in the live path; `live_min_account_equity 20` is a single hard floor, not a regime ladder.

## Explanation
The operator's thesis is explicit and reasonable: run hot at $22 to grow, then let conservative controls
"become meaningful" near $10k. But nothing in the code performs that transition. The aggressive caps/breakers
are constants; reaching $10k will not tighten them. The mechanism that would do it (the risk-scalar stack with
vol-targeting and drawdown throttle) is present but in shadow mode, so today it changes nothing and at $10k it
will still change nothing unless someone manually enables it and the operator manually re-tunes the caps.

So the scaling plan currently depends on the operator *remembering* to re-tune at the right time — an unowned
manual step, not an implemented regime. That is the gap worth flagging, not the high risk at $22 itself.

## Trading Impact
- At $22: behavior is as intended (aggressive) — acceptable per the growth thesis.
- During scaling: caps stay at micro-account width; at intermediate balances ($1k–$10k) the 25% cap / 50–75%
  breakers are no longer "intentional aggression," they are simply oversized and untracked.
- Transition risk: the moment the min-lot floor stops binding (mid-scaling), the static 0.5% target suddenly
  becomes the real risk while the wide caps/breakers remain — an unmanaged regime change.

## Suggested Fix or Experiment
1. Implement **explicit account-size regimes** keyed on equity (e.g. micro <$200 growth, intermediate
   $200–$2k, full-safety >$2k, plus a drawdown-recovery mode), each with its own `risk_per_trade_pct`,
   `max_risk_pct`, secondary cap, and breaker set.
2. Or, enable the risk-scalar stack out of shadow mode with a vol-target + equity-tier schedule and validate it.
3. At minimum, add a startup banner: current equity → active regime → effective caps/breakers, so the
   transition is visible and not silently manual.

## Estimated Effort
Medium

## Risk of Change
Low–Medium (config/regime layer; validate before live)

## Validation Plan
Codex/Claude: define the regime table, then replay the signal ledger at $22 / $1k / $5k / $10k and confirm the
effective caps and block rates change as intended across regimes.

## Partner Review
Pending (Codex). Reframes Claude F102/F104 in scale-aware terms; complements Codex F003.
