# Rejected Fixes

| Candidate | Reason Rejected For This Pass |
|---|---|
| Starting-balance optimization or balance sweep | Prompt requires USD 10,000 canonical baseline only. |
| Full optimization rerun | Prompt explicitly forbids full optimizer runs. |
| Live account connection/trade placement | Prompt and audit guardrails forbid live trading during this implementation pass. |
| Immediate risk-throttle redesign | The user explicitly reframed risk away from conventional risk reduction; instrumentation and correctness came first. |
| Immediate MAE/MFE-based gating | New fields must first be populated in bounded replay/live logs; gating before evidence could remove profitable pain. |
| Production credential/config edit | Not required for the performance implementation prompt and not safe under no-live-trading guardrails. |

