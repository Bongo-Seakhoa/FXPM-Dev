# F103: The 100-symbol live universe (indices, metals, stocks) is mismatched to a $22 account

## Category
Risk management / Portfolio construction

## Severity
High

## Confidence
High (empirical block evidence)

## Evidence
- `agent_audit/data/audit_evidence_summary.json` — `config.symbol_count: 100`; live winner cells include
  DE30, US30, US100, US500, JP225 (indices); XAUUSD/XAUEUR/XAUGBP/PLATINUM (metals); TESLA, PEPSICO (stocks).
- `agent_audit/data/signal_risk_events.csv` — index/metal min-lot trades are blocked with implied account
  risk far above any sane cap: DE30 **362%**, PLATINUM **98%**, GBPUSD-D1 **95%**, US30 **67%**.
- `broker_specs.json` — indices/metals have large `contract_size`/`tick_value`, so 0.01 lot is a large
  notional relative to $22; FX majors (`tick_value 1.0`, contract 100k) are the only low-notional set.

## Explanation
A 0.01-lot position on a $24,000-priced index (DE30) or a $1,676 metal (PLATINUM) carries a loss-at-stop
that dwarfs a $22 account. Those signals can never pass the `max_risk_pct` cap (F104), so they are
perpetually `BLOCKED_MIN_LOT_EXCEEDS_CAP` — pure wasted compute and log noise — while creating a false
impression of breadth. The genuinely tradeable universe at $22 is a handful of FX/crypto pairs whose
0.01-lot risk lands in the (still oversized) 3–6% band.

## Trading Impact
- Most of the discovered "edge" across 100 symbols is unreachable on this account.
- Diversification is illusory: realized trading concentrates in a few low-notional pairs.
- Signal/log throughput is dominated by un-actionable blocked entries.

## Suggested Fix or Experiment
1. Define a **balance-aware tradeable universe**: include a symbol only if `0.01-lot loss-at-stop ≤
   max_risk_pct × equity` for typical winner SL distances. At $22 this likely reduces to select FX/crypto.
2. Surface a startup summary: "N of 100 symbols tradeable at current equity" so the operator sees reality.
3. Re-evaluate the universe whenever the balance changes materially (it gates which winners matter).

## Estimated Effort
Small–Medium

## Risk of Change
Low (it removes only entries that are already being rejected)

## Validation Plan
Codex: emit the per-symbol tradeability table at $22 and confirm it matches the EXECUTED vs
BLOCKED split in `signal_risk_events.csv`.

## Partner Review
Pending (Codex).
