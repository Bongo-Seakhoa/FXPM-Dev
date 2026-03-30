# FxPM 1.4 Settings Reference

Complete reference for all `config.json` settings. This file documents every configurable parameter, its purpose, valid ranges, and impact on system behavior.

---

## Table of Contents

1. [Pipeline Settings](#1-pipeline-settings)
2. [Position Management](#2-position-management)
3. [MT5 Connection](#3-mt5-connection)
4. [Instrument Specifications](#4-instrument-specifications)
5. [Symbols List](#5-symbols-list)

---

## 1. Pipeline Settings

All settings under the `"pipeline"` key control optimization, backtesting, scoring, and live trading behavior.

### 1.1 Data & Paths

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `data_dir` | string | `"./data"` | Directory containing M5 CSV price data files (e.g., `EURUSD_M5.csv`). |
| `output_dir` | string | `"./pm_outputs"` | Directory for optimization summaries and trade JSON logs. |

### 1.2 Train/Validation Split

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `train_pct` | float | `80.0` | 50-95 | Percentage of data used for training. |
| `val_pct` | float | `30.0` | 10-50 | Informational; actual validation window is derived from `train_pct` and `overlap_pct`. |
| `overlap_pct` | float | `10.0` | 0-20 | Overlap between train and validation windows. The overlap zone (train_pct - overlap_pct to train_pct) is shared, preventing hard boundary artifacts. |

**How the split works:** Training = bars 0 to `train_pct`%. Validation = bars (`train_pct` - `overlap_pct`)% to 100%. The overlap region is seen by both train and val, providing a smoother transition.

### 1.3 Backtest Capital & Risk

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `initial_capital` | float | `10000.0` | >0 | Starting account balance for backtests (USD). |
| `risk_per_trade_pct` | float | `1.0` | 0.1-5.0 | Base risk per trade as a percentage of equity. This is the foundation for position sizing. |

### 1.4 Cost Modeling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `use_spread` | bool | `true` | Apply spread cost on entry. Uses per-symbol `spread_avg` from `instrument_specs`. |
| `use_commission` | bool | `true` | Apply commission per lot. Set `false` for spread-only accounts. |
| `use_slippage` | bool | `true` | Apply slippage on SL exits (market orders). TP exits are limit fills and never incur slippage. |
| `slippage_pips` | float | `0.5` | Slippage in pips applied to SL exits only. |

**Note:** Swap costs (`swap_long`/`swap_short` in instrument_specs) are defined but NOT applied in backtests. This simplification has <5% impact for H4 and below; D1 multi-week holds may see up to 20% impact.

### 1.5 Optimization Controls

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `max_param_combos` | int | `150` | 10-500 | Maximum parameter combinations tested per strategy per timeframe. Higher = more thorough but slower. |
| `optimization_max_workers` | int | `2` | 1-8 | Parallel workers for optimization. `1` = sequential. Higher uses more CPU/RAM. |
| `timeframes` | list | `["M5","M15","M30","H1","H4","D1"]` | - | Timeframes to evaluate during optimization. Each symbol tests all strategies across all listed timeframes. |
| `retrain_periods` | list | `[14,30,60,90,120]` | - | Lookback periods (days) to evaluate. The optimizer picks the best retrain period per strategy. |
| `max_bars` | int | `500000` | >1000 | Maximum bars to load per symbol. 5 years of M5 data is ~500k bars. |
| `optuna_use_val_in_objective` | bool | `false` | - | If `true`, Optuna's trial objective includes validation metrics. **Not recommended** as it can overfit to the holdout split. |

### 1.6 Evaluation Thresholds (pm_weighted mode)

These thresholds are used when `scoring_mode` = `"pm_weighted"`:

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `min_trades` | int | `25` | 5-100 | Minimum trades required for a backtest to be considered valid. |
| `min_robustness` | float | `0.2` | 0-1.0 | Minimum robustness ratio (val_score / train_score). |
| `min_win_rate` | float | `45.0` | 30-70 | Minimum win rate (%). |
| `min_profit_factor` | float | `1.2` | 1.0-3.0 | Minimum profit factor (gross_profit / gross_loss). |
| `min_sharpe` | float | `0.5` | 0-3.0 | Minimum Sharpe ratio. |
| `max_drawdown` | float | `15.0` | 5-50 | Maximum allowed drawdown (%). |

### 1.7 Scoring Mode & FX Backtester Settings

| Key | Type | Default | Options | Description |
|-----|------|---------|---------|-------------|
| `scoring_mode` | string | `"fx_backtester"` | `"fx_backtester"`, `"pm_weighted"` | Scoring methodology. `fx_backtester` uses the 3-layer generalization scoring system (recommended). `pm_weighted` uses legacy weighted composite scoring. |

**FX Backtester Scoring Parameters** (used when `scoring_mode` = `"fx_backtester"`):

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `fx_opt_min_trades` | int | `15` | 5-50 | Minimum trades during param search (train only). |
| `fx_val_min_trades` | int | `15` | 5-50 | Minimum validation trades required. |
| `fx_val_max_drawdown` | float | `20.0` | 5-50 | Maximum allowed validation drawdown (%). |
| `fx_val_sharpe_override` | float | `0.3` | 0-2.0 | If validation Sharpe exceeds this, strategy can bypass robustness threshold. |
| `fx_selection_top_k` | int | `5` | 1-20 | Top-K strategy/timeframe candidates to validate. Reduces compute by skipping low-potential candidates. |
| `fx_opt_top_k` | int | `5` | 1-20 | Top-K parameter combos to validate per strategy. |
| `fx_gap_penalty_lambda` | float | `0.7` | 0-1.0 | Penalty strength for train-to-val score gaps. Higher values penalize overfitting more aggressively. `0.5`-`0.7` recommended. |
| `fx_robustness_boost` | float | `0.15` | 0-0.5 | Weight given to robustness ratio in the composite score. Higher values reward strategies that generalize well. |
| `fx_min_robustness_ratio` | float | `0.8` | 0.5-1.0 | Minimum val_score/train_score ratio for validation pass. `0.8` means val must achieve at least 80% of train quality. |

### 1.8 Score Weights

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `score_weights` | object | See below | Weights for composite score calculation. Must sum to 1.0. |

Default weights:
```json
{
  "sharpe": 0.25,
  "profit_factor": 0.20,
  "win_rate": 0.15,
  "total_return": 0.15,
  "max_drawdown": 0.15,
  "trade_count": 0.10
}
```

### 1.9 Regime Optimization

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `use_regime_optimization` | bool | `true` | Enable regime-aware optimization. When `true`, strategies are optimized separately for each market regime (TREND, RANGE, BREAKOUT, CHOP). |
| `regime_min_train_trades` | int | `25` | Minimum trades per regime bucket in training. Regimes with fewer trades are skipped. |
| `regime_min_val_trades` | int | `15` | Minimum trades per regime bucket in validation. |
| `regime_freshness_decay` | float | `0.85` | Decay multiplier for stale timeframe signals. Applied when signal bar age exceeds expected freshness. |
| `regime_chop_no_trade` | bool | `false` | If `true`, enforces hard no-trade when in CHOP regime with no winner. If `false`, allows fallback strategies. |
| `regime_params_file` | string | `"regime_params.json"` | Path to tuned regime detector parameters (thresholds, hysteresis). |
| `regime_enable_hyperparam_tuning` | bool | `true` | Enable hyperparameter tuning during regime optimization. Uses Optuna to tune strategy params per regime. |
| `regime_hyperparam_top_k` | int | `3` | Top-K strategies to tune per regime during screening phase. |
| `regime_hyperparam_max_combos` | int | `150` | Maximum parameter combinations to test per strategy per regime. |

### 1.10 Regime Winner Profitability Gates

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `regime_min_val_profit_factor` | float | `1.0` | Minimum validation profit factor for a regime winner to be stored. Prevents "best loser" strategies from being selected. |
| `regime_min_val_return_pct` | float | `0.0` | Minimum validation return % for a regime winner. `0.0` = breakeven minimum. |
| `regime_allow_losing_winners` | bool | `false` | If `true`, allows strategies with PF < 1 to be stored as regime winners. Not recommended. |
| `regime_no_winner_marker` | string | `"NO_TRADE"` | Strategy name used when no valid winner exists for a regime. |

### 1.11 Pre-Tuning Eligibility Gates

These gates filter strategies **before** Optuna hyperparameter tuning, based on training results with **default (untuned) parameters**. Their purpose is to save compute by skipping truly catastrophic strategies. Thresholds should be lenient since strategies improve significantly after tuning.

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `train_min_profit_factor` | float | `0.5` | 0.1-1.5 | Minimum training profit factor with default params. Strategies below this are skipped. `0.5` = very lenient (only reject terrible), `0.85` = moderate, `0.95` = strict (may reject too many). |
| `train_min_return_pct` | float | `-30.0` | -50 to 5 | Minimum training return %. **Value is in percentage points**, not decimal. `-10.0` allows up to 10% loss. `0.0` = breakeven-or-better. |
| `train_max_drawdown` | float | `60.0` | 10-80 | Maximum training drawdown %. Strategies exceeding this are skipped. `20.0` = moderate, `60.0` = lenient. |

**Important:** These run on **default parameters** (before Optuna tuning). A strategy with PF=0.7 on defaults may become PF=1.8 after tuning. Set thresholds conservatively (lenient) to avoid rejecting good candidates prematurely. The real quality enforcement happens post-tuning via the regime winner profitability gates (Section 1.10).

### 1.12 Weak-Train Exceptional Validation

When a strategy has weak training metrics (PF < 1.0 or negative return), it can still be selected as a regime winner if its **validation** metrics are exceptionally strong. This handles strategies that happen to underperform on the specific training window but generalize well.

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `exceptional_val_profit_factor` | float | `1.3` | 1.0-3.0 | Minimum validation PF required to override weak training. Must also pass `exceptional_val_return_pct` and have 2x `regime_min_val_trades`. |
| `exceptional_val_return_pct` | float | `2.0` | 0-20 | Minimum validation return % required to override weak training. |

**Example:** A strategy with train PF=0.8 (weak) but val PF=1.5, val return=3%, and 30+ val trades would be allowed through the exceptional validation path.

### 1.13 Live Trading Settings

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `live_bars_count` | int | `1500` | 300-5000 | Bars loaded per timeframe during live trading. More bars = more accurate indicators but slower processing. |
| `live_min_bars` | int | `300` | 100-1000 | Minimum bars required to evaluate a timeframe in live trading. Timeframes with fewer bars are skipped. |
| `actionable_score_margin` | float | `0.9` | 0.0-1.0 | Minimum composite score (0-1) for a signal to be actionable. Signals below this are logged but not executed. `0.95` = strict, `0.9` = moderate, `0.8` = lenient. |

### 1.14 Winners-Only Risk Policy

Live trading uses a **winners-only** risk policy. Only validated regime winners may trade. No fallback to "best train" when validation fails.

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `min_trade_risk_pct` | float | `0.1` | 0.01-1.0 | Minimum non-zero risk for any placed trade. Trades calculated below this are skipped. |

Risk per trade is determined by `position.risk_per_trade_pct`, capped by `position.max_risk_pct`.

### 1.15 Dual-Trade D1 + Lower-TF

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `allow_d1_plus_lower_tf` | bool | `true` | Allow up to 2 concurrent trades per symbol: one D1 + one lower-timeframe. |
| `d1_secondary_risk_multiplier` | float | `1.0` | Risk multiplier for the second (non-D1) trade when D1 is already open. |
| `secondary_trade_max_risk_pct` | float | `1.0` | Hard cap for the secondary (non-D1) trade's risk. |
| `max_combined_risk_pct` | float | `3.0` | Maximum combined risk per symbol (D1 + lower-TF together). |

---

## 2. Position Management

All settings under the `"position"` key control position sizing and trade management in live trading.

### 2.1 Core Sizing

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `risk_per_trade_pct` | float | `1.0` | Risk per trade as % of equity (mirrors pipeline setting for live). |
| `max_position_size` | float | `0.0` | Maximum lot size. `0` = no limit (uses broker `max_lot`). |
| `min_position_size` | float | `0.01` | Minimum lot size (micro lot). |
| `risk_basis` | string | `"balance"` | Account metric for risk calculation. Options: `"balance"`, `"equity"`, `"free_margin"`. |
| `max_risk_pct` | float | `5.0` | Maximum total portfolio risk (%). |
| `risk_tolerance_pct` | float | `2.0` | Risk tolerance threshold for position size warnings. |
| `auto_widen_sl` | bool | `true` | If `true`, automatically widens SL to meet broker's minimum stop distance (`stops_level`). |

### 2.2 Trailing Stop

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `use_trailing_stop` | bool | `false` | Enable trailing stop loss. |
| `trailing_stop_pips` | float | `0.0` | Trailing distance in pips. |
| `trailing_activation_pips` | float | `0.0` | Profit in pips before trailing activates. |

### 2.3 Breakeven Stop

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `use_breakeven_stop` | bool | `false` | Enable breakeven stop (move SL to entry after trigger). |
| `breakeven_trigger_pips` | float | `0.0` | Profit in pips to trigger breakeven move. |
| `breakeven_offset_pips` | float | `1.0` | Pips above entry to place breakeven SL (covers spread). |

### 2.4 Scaling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `allow_scaling` | bool | `false` | Allow adding to winning positions. |
| `max_scale_ins` | int | `3` | Maximum number of scale-in additions. |
| `scale_in_pct` | float | `50.0` | Size of each scale-in as % of original position. |

### 2.5 Trade Duration & Costs

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_trade_duration_bars` | int | `0` | Force-close after N bars. `0` = no limit. |
| `use_spread` | bool | `true` | Apply spread cost (mirrors pipeline setting). |
| `use_slippage` | bool | `true` | Apply slippage (mirrors pipeline setting). |
| `slippage_pips` | float | `0.5` | Slippage in pips (mirrors pipeline setting). |

---

## 3. MT5 Connection

Settings under `"mt5"` for MetaTrader 5 terminal connection.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `login` | int | `0` | MT5 account number. Set to your broker account ID. |
| `password` | string | `""` | MT5 account password. |
| `server` | string | `""` | MT5 server name (e.g., `"ICMarkets-Demo"`). |
| `path` | string | `""` | Path to MT5 terminal executable (e.g., `"C:/Program Files/MetaTrader 5/terminal64.exe"`). Leave empty for default installation. |
| `timeout` | int | `60000` | Connection timeout in milliseconds. |
| `portable` | bool | `false` | Launch MT5 in portable mode. |

**Security note:** Do not commit `config.json` with real credentials to version control. Use environment variables or a separate `.env` file for production deployments.

---

## 4. Instrument Specifications

### 4.1 Defaults

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `broker_specs_path` | string | `"broker_specs.json"` | Path to broker-provided instrument specs (auto-populated from MT5). |
| `instrument_spec_defaults.commission_per_lot` | float | `7.0` | Default commission per standard lot (USD) when not specified per symbol. |

### 4.2 Per-Symbol Specs

Each entry in `"instrument_specs"` defines instrument properties used by the backtester and position sizer:

| Field | Type | Description |
|-------|------|-------------|
| `pip_position` | int | Decimal place of pip. `4` for EURUSD (0.0001), `2` for USDJPY (0.01), `0` for indices. |
| `pip_value` | float | Value per pip per standard lot (USD). `10.0` for EURUSD, `9.0` for USDJPY. |
| `spread_avg` | float | Average spread in pips. Used in backtest cost modeling. |
| `min_lot` | float | Minimum lot size (usually `0.01`). |
| `max_lot` | float | Maximum lot size (usually `100.0`). |
| `commission_per_lot` | float | Commission per standard lot (USD). `0.0` for spread-only instruments. |
| `swap_long` | float | Daily swap rate for long positions (points). Informational only (not applied in backtest). |
| `swap_short` | float | Daily swap rate for short positions (points). Informational only (not applied in backtest). |

**Note:** When MT5 is connected, live trading auto-fetches instrument specs from the broker, overriding these static values. These config values are used as fallbacks for backtesting when MT5 is not available.

---

## 5. Symbols List

The `"symbols"` array defines which instruments to optimize and trade. Currently configured with 44 instruments across 5 asset classes:

- **Major FX (7):** EURUSD, GBPUSD, USDJPY, USDCHF, USDCAD, AUDUSD, NZDUSD
- **Cross FX (21):** AUDNZD, EURGBP, EURJPY, GBPJPY, AUDJPY, EURAUD, EURCHF, EURCAD, EURNZD, GBPAUD, GBPCAD, GBPCHF, CADJPY, NZDJPY, AUDCAD, AUDCHF, CADCHF, CHFJPY, NZDCAD, NZDCHF, GBPNZD
- **Exotic FX (6):** USDNOK, USDMXN, USDSGD, USDZAR, USDPLN, USDSEK
- **Indices (6):** US100, US30, DE30, EU50, UK100, JP225
- **Crypto (4):** ETHUSD, XRPUSD, TONUSD, BTCETH
- **Metals (2):** XAUUSD, XAGUSD (when listed in symbols)

To add a new symbol: add it to `"symbols"`, add its spec to `"instrument_specs"`, and place its M5 CSV data file in `data_dir`.

---

## Quick Tuning Guide

**Conservative setup** (lower risk, fewer trades):
```json
"risk_per_trade_pct": 0.5,
"max_risk_pct": 2.0,
"actionable_score_margin": 0.95,
"fx_gap_penalty_lambda": 0.7,
"min_trades": 30
```

**Moderate setup** (balanced):
```json
"risk_per_trade_pct": 1.0,
"max_risk_pct": 5.0,
"actionable_score_margin": 0.9,
"fx_gap_penalty_lambda": 0.5,
"min_trades": 25
```

**Aggressive setup** (higher risk, more trades):
```json
"risk_per_trade_pct": 2.0,
"max_risk_pct": 8.0,
"actionable_score_margin": 0.8,
"fx_gap_penalty_lambda": 0.3,
"min_trades": 15
```
