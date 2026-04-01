# FxPM 1.4 Settings Reference

This file documents the current `config.json` schema and the way the PM uses those settings today.

Use this as the reference for the live codebase. The checked-in `config.json` may contain deployment-specific values that differ from the code defaults documented here.

---

## 1. Root-Level Settings

| Key | Type | Description |
|---|---|---|
| `pipeline` | object | Optimization, backtest, live-loop, and production schedule controls |
| `position` | object | Live position sizing and optional order-management helpers |
| `mt5` | object | MetaTrader 5 connection settings |
| `broker_specs_path` | string | Path to broker spec cache JSON |
| `instrument_spec_defaults` | object | Fallback defaults for symbols missing explicit specs |
| `instrument_specs` | object | Per-symbol static fallback specs used mainly outside live broker resolution |
| `symbols` | list | Instruments to optimize and trade |

---

## 2. Pipeline Settings

### 2.1 Paths and dataset controls

| Key | Type | Default | Description |
|---|---|---:|---|
| `data_dir` | string | `"./data"` | Historical data directory. Expects root M5 CSV files such as `EURUSD_M5.csv`. |
| `output_dir` | string | `"./pm_outputs"` | Optimization summaries and trade artifact directory. Runtime logs are not stored here. |
| `max_bars` | int | `500000` | Max bars loaded per symbol for research/optimization. |

### 2.2 Train/validation split

| Key | Type | Default | Description |
|---|---|---:|---|
| `train_pct` | float | `80.0` | Percentage of bars used for training. |
| `val_pct` | float | `30.0` | Informational field retained for visibility; effective validation is derived from split logic. |
| `overlap_pct` | float | `10.0` | Shared overlap between train and validation windows. |

Current split behavior:

- Training: start to `train_pct`
- Validation: `(train_pct - overlap_pct)` to end
- The overlap is intentional in the current PM methodology

### 2.3 Capital and base risk

| Key | Type | Default | Description |
|---|---|---:|---|
| `initial_capital` | float | `10000.0` | Starting capital for backtests and analytics. |
| `risk_per_trade_pct` | float | `1.0` | Base backtest/optimization risk percentage. Live sizing also mirrors this unless overridden in `position`. |

### 2.4 Cost modeling

| Key | Type | Default | Description |
|---|---|---:|---|
| `use_spread` | bool | `true` | Apply spread cost in backtests. |
| `use_commission` | bool | `true` | Apply commission per lot in backtests. |
| `use_slippage` | bool | `true` | Apply slippage modeling for stop-based exits. |
| `slippage_pips` | float | `0.5` | Slippage in pips for modeled stop exits. |

### 2.5 Optimization controls

| Key | Type | Default | Description |
|---|---|---:|---|
| `max_param_combos` | int | `150` | Max parameter combinations per strategy/timeframe in the optimization path. |
| `optimization_max_workers` | int | `2` | Parallel worker count for optimization. |
| `timeframes` | list | `["M5","M15","M30","H1","H4","D1"]` | Timeframes evaluated during optimization. |
| `optuna_use_val_in_objective` | bool | `false` | If enabled, Optuna objective includes validation data. Kept off by default to reduce holdout leakage risk. |

### 2.6 Legacy weighted-score thresholds

These mainly matter when `scoring_mode = "pm_weighted"`.

| Key | Type | Default | Description |
|---|---|---:|---|
| `min_trades` | int | `25` | Minimum trades for consideration. |
| `min_robustness` | float | `0.2` | Minimum robustness threshold in legacy weighted mode. |
| `min_win_rate` | float | `45.0` | Minimum win rate in legacy weighted mode. |
| `min_profit_factor` | float | `1.2` | Minimum profit factor in legacy weighted mode. |
| `min_sharpe` | float | `0.5` | Minimum Sharpe ratio in legacy weighted mode. |
| `max_drawdown` | float | `15.0` | Maximum drawdown in legacy weighted mode. |

### 2.7 Primary scoring and FX validation controls

| Key | Type | Default | Description |
|---|---|---:|---|
| `scoring_mode` | string | `"fx_backtester"` | Primary scoring methodology. Recommended active mode. |
| `fx_opt_min_trades` | int | `15` | Minimum training trades during parameter search. |
| `fx_val_min_trades` | int | `15` | Minimum validation trades required. |
| `fx_val_max_drawdown` | float | `20.0` | Maximum allowed validation drawdown. |
| `fx_val_sharpe_override` | float | `0.3` | Validation Sharpe threshold that can override weaker robustness in some paths. |
| `fx_selection_top_k` | int | `5` | Top strategy/timeframe candidates retained for validation. |
| `fx_opt_top_k` | int | `5` | Top parameter sets retained per strategy before validation. |
| `fx_gap_penalty_lambda` | float | `0.7` | Train-to-validation gap penalty strength. |
| `fx_robustness_boost` | float | `0.15` | Reward weight for strategies that generalize better. |
| `fx_min_robustness_ratio` | float | `0.85` | Minimum validation-to-training robustness ratio in code defaults. |

### 2.8 Score weights

| Key | Type | Default | Description |
|---|---|---:|---|
| `score_weights` | object | see code | Composite weights for metrics such as Sharpe, PF, return, drawdown, and trade count. |

### 2.9 Regime optimization

| Key | Type | Default | Description |
|---|---|---:|---|
| `use_regime_optimization` | bool | `true` | Enable regime-aware optimization. |
| `regime_min_train_trades` | int | `25` | Minimum training trades per regime bucket. |
| `regime_min_val_trades` | int | `15` | Minimum validation trades per regime bucket. |
| `regime_freshness_decay` | float | `0.85` | Penalty/decay for stale signals. |
| `regime_chop_no_trade` | bool | `false` | If true, CHOP can force no-trade behavior. |
| `regime_params_file` | string | `"regime_params.json"` | Tuned regime parameter file path. |
| `regime_enable_hyperparam_tuning` | bool | `true` | Enable regime-aware hyperparameter tuning. |
| `regime_hyperparam_top_k` | int | `3` | Top-K strategies to tune per regime. |
| `regime_hyperparam_max_combos` | int | `150` | Max parameter combinations per regime-tuned strategy. |

### 2.10 Regime winner profitability gates

| Key | Type | Default | Description |
|---|---|---:|---|
| `regime_min_val_profit_factor` | float | `1.0` | Minimum validation PF for a regime winner to be stored. |
| `regime_min_val_return_pct` | float | `0.0` | Minimum validation return percentage for a regime winner. |

### 2.11 Pre-tuning eligibility gates

| Key | Type | Default | Description |
|---|---|---:|---|
| `train_min_profit_factor` | float | `0.5` | Lenient pre-tuning training PF screen. |
| `train_min_return_pct` | float | `-30.0` | Lenient pre-tuning training return screen. |
| `train_max_drawdown` | float | `60.0` | Lenient pre-tuning training drawdown ceiling. |

These gates act before tuning and should remain materially looser than the final winner gates.

### 2.12 Exceptional validation overrides

| Key | Type | Default | Description |
|---|---|---:|---|
| `exceptional_val_profit_factor` | float | `1.3` | Validation PF needed to rescue a weak-train candidate. |
| `exceptional_val_return_pct` | float | `2.0` | Validation return needed to rescue a weak-train candidate. |

### 2.13 Live bars and signal gating

| Key | Type | Default | Description |
|---|---|---:|---|
| `live_bars_count` | int | `1500` | Bars loaded per timeframe during live trading. |
| `live_min_bars` | int | `300` | Minimum bars required to evaluate a timeframe live. |
| `actionable_score_margin` | float | `0.9` | Minimum score threshold for a signal to be actionable. |
| `min_trade_risk_pct` | float | `0.1` | Minimum non-zero risk required for a trade to be placed. |

### 2.14 Live spread-quality overlay

| Key | Type | Default | Description |
|---|---|---:|---|
| `execution_spread_filter_enabled` | bool | `true` | Enable live spread-quality gating. |
| `execution_spread_min_edge_mult` | float | `1.5` | Block when `ATR < min_edge_mult x spread`. |
| `execution_spread_spike_mult` | float | `2.0` | Block when spread exceeds this multiple of rolling median spread. |
| `execution_spread_penalty_start_mult` | float | `0.5` | Start soft penalties when `spread / ATR` exceeds this ratio. |

`execution_spread_spike_mult` is the spread-spike blocker threshold.

### 2.15 Dual-trade same-symbol controls

| Key | Type | Default | Description |
|---|---|---:|---|
| `allow_d1_plus_lower_tf` | bool | `true` | Allow one D1 trade plus one lower-timeframe trade on the same symbol. |
| `d1_secondary_risk_multiplier` | float | `1.0` | Risk multiplier for the secondary trade. |
| `secondary_trade_max_risk_pct` | float | `1.0` | Hard risk cap for the secondary trade. |
| `max_combined_risk_pct` | float | `3.0` | Combined same-symbol cap for the D1 + lower-timeframe pair. |

### 2.16 Production retrain schedule

These fields replaced the old rolling retrain-validity approach for production scheduling.

| Key | Type | Default | Description |
|---|---|---:|---|
| `production_retrain_mode` | string | `"auto"` | `auto`, `notify`, or `off`. |
| `production_retrain_interval_weeks` | int | `2` | Interval between scheduled retrain dates. |
| `production_retrain_weekday` | string | `"sunday"` | Scheduled retrain weekday. |
| `production_retrain_time` | string | `"00:01"` | Scheduled retrain time in `HH:MM`. |
| `production_retrain_anchor_date` | string | `"2026-03-29"` | Anchor date for the calendar schedule. Must match the configured weekday. |
| `production_retrain_poll_seconds` | int | `60` | Loop poll interval for schedule checks. |

Important:

- The PM no longer documents `retrain_periods` as an active production control.
- Production readiness is now governed by the fixed calendar schedule plus artifact/config invalidation.

---

## 3. Position Settings

These settings live under `"position"` and primarily affect live trading.

### 3.1 Core sizing

| Key | Type | Default | Description |
|---|---|---:|---|
| `risk_per_trade_pct` | float | `1.0` | Live base risk percentage. |
| `max_position_size` | float | `0.0` | Hard lot-size ceiling. `0.0` means no extra cap beyond broker max. |
| `min_position_size` | float | `0.01` | Hard lot-size floor. |
| `risk_basis` | string | `"balance"` | Live sizing basis. Valid active options are `balance` and `equity`. |
| `max_risk_pct` | float | `5.0` | Hard per-trade risk cap after overlays and sizing logic. |
| `risk_tolerance_pct` | float | `2.0` | Advisory tolerance field. Currently not a major live decision driver. |
| `auto_widen_sl` | bool | `true` | Auto-widen stop loss to meet broker minimum stop distance when needed. |

### 3.2 Optional trailing helpers

| Key | Type | Default | Description |
|---|---|---:|---|
| `use_trailing_stop` | bool | `false` | Enable trailing-stop helper. |
| `trailing_stop_pips` | float | `0.0` | Trailing distance. |
| `trailing_activation_pips` | float | `0.0` | Profit threshold to start trailing. |

### 3.3 Optional breakeven helpers

| Key | Type | Default | Description |
|---|---|---:|---|
| `use_breakeven_stop` | bool | `false` | Enable breakeven helper. |
| `breakeven_trigger_pips` | float | `0.0` | Profit threshold to move stop to breakeven. |
| `breakeven_offset_pips` | float | `1.0` | Offset above entry once breakeven is triggered. |

### 3.4 Optional scaling helpers

| Key | Type | Default | Description |
|---|---|---:|---|
| `allow_scaling` | bool | `false` | Allow scale-ins. |
| `max_scale_ins` | int | `3` | Max number of additions. |
| `scale_in_pct` | float | `50.0` | Size of each scale-in as percentage of the original trade. |

### 3.5 Optional duration and mirrored cost fields

| Key | Type | Default | Description |
|---|---|---:|---|
| `max_trade_duration_bars` | int | `0` | Force-close trade after N bars. `0` disables time-based close. |
| `use_spread` | bool | `true` | Mirrors spread modeling preference into live helper code paths. |
| `use_slippage` | bool | `true` | Mirrors slippage modeling preference into live helper code paths. |
| `slippage_pips` | float | `0.5` | Mirrored slippage setting. |

Important:

- These trailing/breakeven/scaling settings exist, but they are not the default primary exit engine of the PM.
- The active PM still centers on strategy entry logic plus SL/TP and live execution safeguards.

---

## 4. MT5 Settings

| Key | Type | Default | Description |
|---|---|---:|---|
| `login` | int | `0` | MT5 account number. `0` means use the current logged-in session. |
| `password` | string | `""` | MT5 password. |
| `server` | string | `""` | MT5 server name. |
| `path` | string | `""` | Explicit terminal path if auto-detection is insufficient. |
| `timeout` | int | `60000` | Connection timeout in milliseconds. |
| `portable` | bool | `false` | Launch terminal in portable mode. |

---

## 5. Instrument Specifications

### 5.1 Shared defaults

| Key | Type | Default | Description |
|---|---|---:|---|
| `broker_specs_path` | string | `"broker_specs.json"` | Cached broker specs file path. |
| `instrument_spec_defaults.commission_per_lot` | float | `7.0` | Default commission fallback. |

### 5.2 Per-symbol fields

| Field | Type | Description |
|---|---|---|
| `pip_position` | int | Pip decimal position for the symbol. |
| `pip_value` | float | Pip value per standard lot. |
| `spread_avg` | float | Average spread used mainly in backtests/fallback modeling. |
| `min_lot` | float | Minimum lot size. |
| `max_lot` | float | Maximum lot size. |
| `commission_per_lot` | float | Commission per standard lot. |
| `swap_long` | float | Informational long swap field. |
| `swap_short` | float | Informational short swap field. |

Live trading prefers broker metadata from MT5 when available, with these static values used as fallbacks.

---

## 6. Symbols

The checked-in repo currently configures `62` symbols across 7 asset groups:

- Major FX: `7`
- Cross FX: `21`
- Exotic FX: `8`
- Metals: `2`
- Energy: `3`
- Indices: `11`
- Crypto: `10`

To add a symbol:

1. Add it to `symbols`
2. Add or inherit a fallback spec in `instrument_specs` if needed
3. Ensure MT5 can resolve the broker symbol
4. Ensure historical data exists or can be fetched

---

## 7. Practical Notes

- Runtime logs are written to `logs/`, not `output_dir`.
- `config.json` is the intended PM source of truth, but some defensive code defaults still exist for robustness.
- The active live sizing basis is either `balance` or `equity`; `free_margin` is not an active documented option.
- `risk_tolerance_pct` is currently much less operationally important than:
  - `pipeline.risk_per_trade_pct`
  - `position.risk_per_trade_pct`
  - `position.risk_basis`
  - `position.max_risk_pct`
  - `pipeline.min_trade_risk_pct`
  - `pipeline.execution_spread_*`
