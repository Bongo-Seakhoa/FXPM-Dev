# F001: Live account switch updates the MT5 identity pin, not the terminal login

## Category
MT5 execution safety / Account routing

## Severity
Medium

## Confidence
High

## Evidence
- `git diff -- config.json pm_configs.json` shows the direct account-target change in `config.json`: `expected_login` changed from `106165989` to `28829971`, and `expected_server` changed from `FBS-Demo` to `FBS-Real`.
- `agent_audit/data/audit_evidence_summary.json` records `mt5.login_is_blank=true`, `password_is_blank=true`, and `server_is_blank=true`; the config pins `expected_login=28829971`, `expected_server=FBS-Real`, `expected_currency=USD`.
- `pm_mt5.py:376-399` only calls `mt5.login()` when login, password, and server are provided. With blank credentials, the bot uses the already-open MT5 terminal session.
- `pm_live_readiness.py:304-334` compares actual MT5 account login/server/currency against the configured expected values and reports fail-closed mismatches.
- Focused validation passed: 245 pytest tests and 11 subtests, including `tests/test_mt5_connector.py` and `tests/test_profitability_readiness_blueprint.py`.

## Explanation
The account switch is implemented as an identity pin. That is a good safety design because secrets are not stored and the bot should refuse to trade if the terminal is on the wrong account. It does not, by itself, log the terminal into FBS Real. The operator must ensure the MT5 terminal session is already logged into account `28829971` on `FBS-Real` before any live `--trade` run.

The `pm_configs.json` diff also contains ledger-completeness and live-observability updates. Those are not needed for the account target swap and should be treated as generated/pre-existing state, not the mechanism that routes orders to the real account.

## Trading Impact
- Positive: the bot is pinned to the intended real account and should block if the active terminal is still on demo.
- Operational caveat: if the terminal is not already logged into FBS Real, the config will not switch it automatically.
- Audit caveat: the account swap was not only a clean `config.json` two-line diff in the working tree because `pm_configs.json` also changed.

## Suggested Fix or Experiment
1. Keep credentials blank, but add a documented preflight: read `account_info()` and print actual login/server/equity before enabling live mode.
2. Treat `config.json` expected account fields as the source of truth for routing; avoid editing `pm_configs.json` for account switching.
3. Add an operator checklist item: MT5 terminal must show `FBS-Real / 28829971 / USD` before `pm_main.py --trade`.

## Validation Plan
- Run readiness in a read-only/no-trade mode with mocked or demo `account_info()` to confirm mismatch blocks.
- Do not validate by placing live trades.

## Partner Review
Pending.

