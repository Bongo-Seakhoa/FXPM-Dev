# Audit Questions

Last updated: 2026-06-17 by Codex, incorporating operator answers.

## Answered / Assumed

- The approximately USD 22 live account is intentionally aggressive micro-account growth mode.
- High percentage risk at USD 22 is not automatically a defect.
- The audit should flag code correctness, measurement, methodology, execution, accounting, hidden ruin, and scaling-transition problems.
- No full optimizer, no live trade command, and no production credential edits were permitted or performed.
- The 50k validation geometry is implemented correctly, but it is a selection surface, not pristine post-selection OOS.
- No explicit Toronto metric was found.
- FBS demo specs are treated as matching FBS real live conditions unless broker read-only parity checks later prove otherwise.
- All 100 configured symbols remain intentionally eligible in micro-account mode if they have valid trading parameters.
- The current hard cap remains 25% in micro-account mode.

## Resolved Operator Questions

| Question | Operator Answer | Audit Interpretation / Implementation Direction |
|---|---|---|
| What equity thresholds should define micro-account, intermediate, full safety, capital preservation, and drawdown recovery modes? | Not a major concern; the philosophy is that most of the account should be working at any given time. | Do not implement regimes as restrictive off-switches by default. If regimes are added, use them first for measurement, reporting, and gradual scaling, not to idle the account. Suggested default: keep micro-growth behavior active at the current USD 22 level; make larger-account thresholds configurable and replay-tested before enforcing. |
| Should the live per-trade hard cap stay at 25%, or should replay test 8-10%? | The hard cap should stay at 25%. | Keep `position.max_risk_pct=25.0` as the live micro-account policy. Any 8-10% cap analysis should be audit/shadow only, used to compare opportunity cost vs drawdown reduction, not to change live behavior without separate approval. |
| Should a post-selection audit initially be non-gating measurement or an immediate promotion gate? | Immediate promotion gate if it is found to be valuable and correct, meaning it leads to better trading outcomes. | Implement in two steps: first prove value on historical/replay evidence without changing live selections; then make it a promotion gate once it demonstrably improves profitability/drawdown/recovery. The end state can be a hard gate, but only after evidence shows it improves outcomes. |
| What validation trade-count floor is acceptable: 20, 30, or strategy/timeframe-specific? | D1 timeframes naturally have fewer trades; take Codex recommendation so all are aligned. | Use strategy/timeframe-specific floors rather than one blunt number. Recommended starting policy: D1/low-frequency cells require a lower floor plus stronger R/MAE/stability evidence; intraday cells should generally require at least 20 validation trades; high-frequency/noisy cells should require 30+ or stronger deflated evidence. |
| Are FBS real-account commission, swap, and margin terms exactly represented by current config/specs? | Yes, demo specs match the real live conditions. | Treat current FBS demo-derived specs as production-valid. Still keep a read-only broker parity check on the roadmap because it protects against future broker/spec drift, not because current specs are presumed wrong. |
| Which high-notional symbols are intentionally allowed in micro-account mode despite frequent min-lot blocks? | All 100 configured symbols, provided they have valid trading parameters. | Do not remove high-notional symbols solely because they frequently hit min-lot blocks. Instead, record tradeability/block rates and let valid signals through when the live cap permits them. |
| What closed-trade live history should be authoritative for profit contribution by risk bucket? | Context matters. For demo performance analysis, use the latest demo run prior to the current run because it should contain comprehensive information. | Use the latest completed demo/evaluation run before the current live run as the primary performance source for risk-bucket analysis. Current live logs should be used for live execution/risk behavior, not as the sole profitability sample until enough closed trades accrue. |
| After MAE/MFE is added, which drawdown bucket should control promotion: MAE/R, time underwater, recovery duration, or a composite? | The code has enough context; Codex should check for conflicts with the current methodology. | Use a composite, not a single drawdown field. Recommended promotion control: MAE/R as the main trade-pain measure, recovery duration/time-underwater as secondary persistence measures, and final expectancy/PF/R-multiple as the override that allows profitable pain to survive. This aligns with the current methodology because it avoids filtering drawdown unless it is unpaid by expectancy/recovery. |

## Final Working Decisions For Implementation

- Keep the USD 22 account in aggressive micro-account growth mode.
- Keep the live hard cap at 25%.
- Keep all 100 valid-parameter symbols eligible.
- Do not make account-size regimes suppress trade activity by default; use them first for measurement and scaling visibility.
- Add post-selection audit as a candidate hard gate only after replay proves it improves net profitability, drawdown, recovery, and robustness.
- Use timeframe-aware validation trade-count floors instead of one universal threshold.
- Use the latest completed demo/evaluation run before the current run as the main historical performance evidence for risk-bucket analysis.
- Use a composite high-drawdown control after MAE/MFE exists: MAE/R + recovery/time-underwater + expectancy/PF/R-multiple.

## Partner Review

- All finding-level cross-reviews are closed: Claude Code agreed with Codex F001-F005, and Codex agreed with Claude Code F101-F113.

### Claude Code cross-review of the resolved answers (2026-06-17)
Claude Code **agrees with all eight resolutions and the Final Working Decisions**, with two refinements:

1. **Q4 (validation floor) — pin it to the right insertion point.** The floor's highest leverage is *not*
   storage eligibility; it is the gate that lets validation Sharpe drive selection. Apply the higher
   (Sharpe-selection) floor specifically at `fx_val_sharpe_override` / `specialist_spokes_v2_max_validation_sharpe`
   (pm_core.py:597,621; pm_pipeline.py:6360-6372). Below the floor, fall back to robustness/train-anchored
   selection. Suggest leaning intraday M5/M15 to **30** (not 20), since that is exactly where the F111/F002
   small-sample Sharpe>10 inflation concentrates; D1 stays at ~15; H1/H4 ~20–25. Keeps D1 viable, hardens
   intraday — fully consistent with Codex's table.

2. **Q8 (high-DD promotion control) — endorse the expectancy/PF/R override as the profitable-pain safeguard.**
   Codex's composite (MAE/R primary, recovery/time-underwater secondary, expectancy/PF/R override) is the
   correct shape. The override is the load-bearing part: it is what stops the gate from filtering deep-MAE
   trades that are *paid for* by expectancy — matching the operator's "don't remove profitable pain"
   principle. Keep time-underwater a **derate only** (never a standalone hard gate) to protect D1/long-hold
   winners. No conflict with the bleeding-edge/validation methodology (same validation surface).

Net: no disagreements. The two delegated questions are answered; implementation order is the Net priority
stack (MAE/MFE+slippage capture → $22 min-lot-aware replay → timeframe floor + post-selection measurement →
MAE/R gate once proven → metric consistency).
