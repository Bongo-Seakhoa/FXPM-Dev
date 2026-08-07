# Audit Assumptions

- `AGENT_NAME=Codex`.
- `PARTNER_AGENT` is the other local audit agent.
- Pre-existing modifications to `config.json` and `pm_configs.json` are user/partner state and must not be reverted.
- Existing artifacts and logs are acceptable evidence sources.
- Full optimization is out of scope.
- External research is optional and only required for methodology claims that cannot be grounded in repo evidence.
- No explicit `Toronto` metric was found in the initial search; the audit will record this as a negative finding unless later evidence appears.
- Corrective follow-up framing: the approximately $22 account is intentionally in aggressive capital-growth mode. High percentage risk is not automatically a defect. The second-pass audit flags only issues that indicate code/methodology inconsistency, distorted measurements, unrewarded drawdown, hidden execution/accounting risk, or poor transition logic toward larger-account safety modes.
- Account-size regimes to assess: micro-account growth mode, intermediate account mode, full safety mode, capital-preservation mode, and drawdown recovery mode. Absence of explicit regimes is a design gap only if it causes inconsistent behavior, measurement distortion, or scaling failure.
