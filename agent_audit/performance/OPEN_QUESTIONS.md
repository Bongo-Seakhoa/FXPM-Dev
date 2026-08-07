# Open Questions

| Question | Why It Matters | Current Status |
|---|---|---|
| Is the current `pm_configs.json` a completed production ledger or a mid-reoptimization/intermediate state? | The current live book is exactly one promoted winner, and it loses in corrected $10k replay. If the ledger is interim, the right action is to let optimization finish; if it is final, it is not deployable. | Unresolved; must be answered before live use. |
| Should the system add a hard validation trade-count floor or candidate-count deflation before promotion? | FORD was promoted on 16 validation trades at PF 1.77, then inverted to PF 0.66 over 166 replay trades. This is concrete sparse-selection failure. | Open; now high priority. |
| Should strategy/regime compatibility be enforced or penalized? | The sole promoted winner is a trend strategy in `CHOP`, which is structurally suspicious and replay-negative. | Open; needs shadow test before objective mutation. |
| Which high-MAE buckets are profitable pain versus bad risk after a clean winner book exists? | This determines whether filters/exits improve net profit or merely reduce profitable aggression. | Tooling exists; current FORD replay is bad-risk evidence, but broad classification needs a real promoted book. |
| How much adverse entry slippage appears by symbol/session/strategy? | Execution leakage can erase edge. | Fields are now in actionable logs; needs future fill data. |
| Should MAE/MFE enter Optuna scoring directly or only post-selection triage? | Direct objective use can help robustness but can over-penalize strategies that recover well. | Queued after clean-book replay. |
| Should exit/TP slippage attribution be added next? | Entry slippage is now measured; exit fill leakage can still distort profit attribution. | Open; requires live close/order-result path inspection and focused tests. |
| Do consumers branch on `trade_record_schema_version` and `actionable_schema_version`? | Missing schema must mean "pre-instrumentation," not zero MAE/MFE or zero slippage. | Open follow-up from Claude review. |
