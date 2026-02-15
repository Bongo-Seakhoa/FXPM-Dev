# FX Portfolio Manager - Stateful Optimization Ledger

**Version:** 3.3  
**Date:** February 1, 2026

---

## Overview

The Portfolio Manager now uses a **stateful optimization ledger** (`pm_configs.json`) that:

1. **Skips valid configs** - Re-running optimization only processes symbols that need it
2. **Persists incrementally** - Saves after each symbol (never loses progress)
3. **Uses atomic writes** - Config file is never corrupted, even on interruption
4. **Preserves existing configs** - Failed optimizations don't destroy prior results

---

## CLI Usage

### Default Behavior (Incremental)

```bash
python pm_main.py --optimize
```

- Loads existing `pm_configs.json`
- For each symbol:
  - If config is **valid** (not expired, validated) → **SKIP** with message
  - If config is **expired/missing/invalid** → **OPTIMIZE**
- Saves progress after each symbol

Example output:
```
SKIP EURUSD: valid until 2026-02-14 (13 days remaining)
SKIP GBPUSD: valid until 2026-02-12 (11 days remaining)
OPTIMIZE USDJPY: expired 3 days ago
OPTIMIZE AUDUSD: missing
```

### Force Re-optimization (Overwrite)

```bash
python pm_main.py --optimize --overwrite
```

- Ignores validity checks
- Re-optimizes ALL symbols
- Only replaces configs after successful optimization

### Other Commands

```bash
python pm_main.py --status                # Show portfolio status
python pm_main.py --trade                 # Live trading
python pm_main.py --trade --paper         # Paper trading
python pm_main.py --trade --auto-retrain  # With auto-retraining
```

---

## Config File Format

Each entry in `pm_configs.json` contains:

```json
{
  "EURUSD": {
    "symbol": "EURUSD",
    "strategy_name": "EMACrossoverStrategy",
    "timeframe": "H1",
    "parameters": {"fast_period": 10, "slow_period": 20},
    "retrain_days": 14,
    "composite_score": 75.5,
    "robustness_ratio": 0.85,
    "is_validated": true,
    "validation_reason": "passed all checks",
    "optimized_at": "2026-02-01T10:30:00",
    "valid_until": "2026-02-15T10:30:00",
    "train_metrics": {...},
    "val_metrics": {...},
    "regime_configs": {...}
  }
}
```

### Validity Rules

A config is **valid** if ALL of the following are true:
1. `is_validated` is `true`
2. `valid_until` exists and is in the future
3. `parameters` exist and are non-empty

### Expiry Duration

Controlled by:
- `retrain_days` in each config (set during optimization)
- `optimization_valid_days` in PipelineConfig (default: 14)

---

## Implementation Details

### ConfigLedger Class (`pm_pipeline.py`)

```python
class ConfigLedger:
    """Stateful optimization ledger for pm_configs.json."""
    
    def load(self) -> int:
        """Load existing configs from file."""
        
    def update_symbol(self, symbol: str, config: SymbolConfig) -> bool:
        """Update a single symbol's config and save atomically."""
        
    def has_valid_config(self, symbol: str) -> Tuple[bool, str]:
        """Check if a symbol has a valid (non-expired) config."""
        
    def should_optimize(self, symbol: str, overwrite: bool) -> Tuple[bool, str]:
        """Determine if a symbol should be optimized."""
        
    def get_symbols_to_optimize(self, symbols: List[str], overwrite: bool):
        """Partition symbols into those needing optimization and those to skip."""
```

### Atomic Write Pattern

```python
def _atomic_save(self):
    temp_path = self.filepath.with_suffix('.json.tmp')
    
    # Write to temp file
    with open(temp_path, 'w') as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    
    # Atomic rename
    temp_path.replace(self.filepath)
```

This ensures the config file is never corrupted, even if the process is killed mid-write.

---

## Behavior Matrix

| Scenario | `--optimize` | `--optimize --overwrite` |
|----------|--------------|--------------------------|
| Config valid | SKIP | OPTIMIZE |
| Config expired | OPTIMIZE | OPTIMIZE |
| Config invalid | OPTIMIZE | OPTIMIZE |
| Config missing | OPTIMIZE | OPTIMIZE |
| Optimization fails | Keep existing | Keep existing |
| Process interrupted | Progress saved | Progress saved |

---

## Edge Cases

### 1. `pm_configs.json` Missing

Treated as empty - optimizes all symbols from scratch.

### 2. `pm_configs.json` Corrupted

Fails fast with clear error:
```
RuntimeError: Corrupted JSON in pm_configs.json: ...
Please fix or remove the file manually.
```

### 3. Optimization Fails for a Symbol

- Does NOT delete/overwrite previous config
- Logs: `FAILED {symbol}: keeping existing config`
- Previous valid/expired config remains intact

### 4. Concurrent Runs

Not officially supported. If two processes run simultaneously, last-write-wins applies. Consider file locking for production.

---

## Logging

The ledger provides explicit, auditable logging:

```
INFO: Loaded 37 existing configs from pm_configs.json
INFO: SKIP EURUSD: valid until 2026-02-14 (13 days remaining)
INFO: SKIP GBPUSD: valid until 2026-02-12 (11 days remaining)
INFO: OPTIMIZE USDJPY: expired 3 days ago
INFO: OPTIMIZE AUDUSD: missing
...
INFO: SAVED USDJPY to pm_configs.json (atomic)
INFO: SAVED AUDUSD to pm_configs.json (atomic)
```

---

## Quality Guarantees

1. **No silent destruction** - Existing configs are never lost unless user explicitly deletes the file
2. **Resumable** - Interrupting and restarting continues from where it left off
3. **Atomic writes** - Config file is always valid JSON
4. **Backward compatible** - Existing config files work without modification
5. **Explicit overwrite** - Must use `--overwrite` flag to re-optimize valid configs

---

## Testing Checklist

- [x] Skip valid config (future expiry)
- [x] Optimize expired config
- [x] Optimize missing config
- [x] Optimize invalid (not validated) config
- [x] Incremental persistence after each symbol
- [x] Atomic write (temp + rename)
- [x] Reload from file preserves all data
- [x] Overwrite mode ignores validity
- [x] Failed optimization keeps existing config
- [x] Statistics calculation (valid/expired/invalid counts)
