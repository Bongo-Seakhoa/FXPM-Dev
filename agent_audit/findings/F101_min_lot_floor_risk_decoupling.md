# F101: Broker min-lot floor decouples per-trade risk from the configured target on a small account

## Category
Risk management / Execution

## Severity
Critical

## Confidence
High (code + live decision-log evidence; corroborated by Codex `data/signal_risk_events.csv`)

## Evidence
- `pm_position.py:436-525` — `calculate_position_size`: when risk-based volume < `min_lot`, it either
  clamps **up** to min-lot (if `allow_min_lot_risk_clamp` and actual risk ≤ `max_risk_pct`) or skips.
- `pm_main.py:4899-4987` — live path: `volume_raw = target_risk_amount / loss_per_lot`; if below
  `min_vol`, `volume = min_vol`; recompute `actual_risk_pct`; block only if `> max_risk_pct` (25%).
- `last_actionable_log.json` (XAGUSD): `target_risk_pct 0.35` → `actual_risk_pct 2.71%`,
  `requested_volume 0.00129` forced to `0.01`, `risk_cap_reason: "broker min-lot floor"`.
- Codex `agent_audit/data/signal_risk_events.csv`: **83** `BLOCKED_MIN_LOT_EXCEEDS_CAP` rows. Examples:
  DE30 H4 target 2.5% → **actual 362%**; GBPUSD D1 → **95%**; PLATINUM H4 → **98%**; US30 D1 → **67%**.
  Executed FX/crypto trades ran **1.3–6.4% actual vs 0.5% target** (e.g. GBPCAD 6.41%, EURCHF 4.66%).
- Account context: live balance ~US$22 (operator), `position.risk_per_trade_pct 0.5`, `risk_basis equity`.

## Explanation
Per-trade risk = `risk_pct × equity`. At $22 × 0.5% = **$0.11**, far below one min-lot's loss-at-stop
on any instrument. The system then either skips the trade or clamps to 0.01 lots, at which point the
*actual* risk is whatever 0.01 lots happens to cost — typically **3–6% on FX/crypto and 67–362% on
indices/metals**. The configured 0.5% target is effectively inert; the lot floor sets the real risk.

## Trading Impact
- Realized per-trade risk on $22 is ~6–12× the intended target for tradeable instruments.
- Risk becomes **balance-dependent**: the same winner that risked ~1% on the $170 demo risks ~6%+ on $22.
- Index/metal/stock signals are almost always blocked → most of the 100-symbol universe is dead weight
  on this account (see F103), and the few executable trades are oversized relative to intent.

## Suggested Fix or Experiment
1. Treat the account as min-lot-constrained: restrict the **live universe** to instruments whose 0.01-lot
   loss-at-stop ≤ target risk fraction of equity (F103), and/or
2. Add an explicit **`effective_min_risk_pct` reconciliation**: if the min-lot actual risk exceeds e.g.
   2–3× target, skip rather than silently clamp (today the only backstop is the 25% hard cap — F104).
3. Until the balance supports min-lot sizing at the intended risk, recognize the system cannot honor 0.5%.

## Estimated Effort
Small (universe restriction) / Medium (sizing reconciliation)

## Risk of Change
Low

## Validation Plan
Codex: recompute, per live symbol, the 0.01-lot loss-at-stop ÷ $22 using `broker_specs.json` + typical
winner SL distances; confirm which symbols are ever tradeable within `max_risk_pct`.

## Partner Review
Pending (Codex). Cross-evidence already aligns via `signal_risk_events.csv`.
