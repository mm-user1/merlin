# Adding a New Strategy

This guide explains how to convert a PineScript strategy to Python and integrate it into Merlin.

## Overview: PineScript to Python Workflow

1. **Receive PineScript file** with expected results at the end
2. **Create strategy directory** with required files
3. **Define config.json** with parameter schema (camelCase)
4. **Create params dataclass** mapping PineScript inputs
5. **Implement strategy logic** in Python
6. **Run tests** and validate against expected PineScript results
7. **Strategy auto-registers** - no manual edits needed

## Step 1: Understand the PineScript File

PineScript files should include expected results at the end:

```pine
// Reference test results:
// Test Configuration:
// CSV File: ./data/raw/"OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv"
// Date range: from 2025-06-01 to 2025-10-01

// Parameters:
// RSI length = 16
// Stoch length = 16
// ...

// Expected Results:
// ├─ Net Profit:        113.26%
// ├─ Max Drawdown:      10.99%
// ├─ Total Trades:      52
```

**Key points:**
- Blocks marked `// skip start` to `// skip end` contain Pine-specific code (date filtering, tables) - use project's built-in functionality instead
- Parameter names in Pine (`rsiLen`, `stochLen`) become camelCase in Python
- Expected results are your validation target (±5% tolerance is acceptable)

## Step 2: Create Strategy Directory

```bash
mkdir -p src/strategies/s05_mystrategy
touch src/strategies/s05_mystrategy/__init__.py
touch src/strategies/s05_mystrategy/config.json
touch src/strategies/s05_mystrategy/strategy.py
```

The `__init__.py` file should be empty or contain:
```python
from .strategy import S05MyStrategy
```

## Step 3: Define config.json

Create parameter schema matching PineScript inputs:

```json
{
  "id": "s05_mystrategy",
  "name": "S05 My Strategy",
  "version": "v01",
  "description": "Brief description of strategy logic",
  "parameters": {
    "rsiLen": {
      "type": "int",
      "label": "RSI Length",
      "default": 14,
      "min": 2,
      "max": 100,
      "step": 1,
      "group": "Indicators",
      "optimize": { "enabled": true, "min": 5, "max": 50, "step": 2 }
    },
    "threshold": {
      "type": "float",
      "label": "Entry Threshold",
      "default": 0.5,
      "min": 0.0,
      "max": 1.0,
      "step": 0.1,
      "group": "Entry",
      "optimize": { "enabled": false }
    }
  }
}
```

**Parameter types:** `int`, `float`, `bool`, `select` (with `options` array)

**Naming rules:**
- Use camelCase: `rsiLen`, `closeCountLong`, `stopLongMaxPct`
- Match PineScript input names exactly
- Group related parameters for UI organization

### Bool Parameters in Optimization

Bool parameters are optimized as categorical values (`True` / `False`) in Start page.

Use one of these patterns:

1. **All bool combinations are valid**  
Do nothing special. Define bool params normally in `parameters`.

2. **Some bool combinations are invalid**  
Declare rules in `config.json` under `optimization_rules.bool_groups`.

Example (`at_least_one_true`):

```json
{
  "optimization_rules": {
    "bool_groups": [
      {
        "params": ["useCloseCount", "useTBands"],
        "mode": "at_least_one_true"
      }
    ]
  }
}
```

What this does:
- Invalid combo (`false`, `false`) is excluded from optimizer search space.
- Valid combos remain available.
- Coverage mode minimum/recommended trials are computed from the filtered search space.

Important notes:
- Rules apply to **optimized** bool params in the group.
- If a bool in the group is fixed (not optimized), its fixed value is used when validating combinations.
- If your strategy should allow all states (including all-off), do not add a bool group rule.

### Parameter Dependencies (`depends_on`)

Numeric (or other) parameters can declare a dependency on a bool parameter using `"depends_on"`. When the parent bool is `false`, the dependent parameter is **skipped entirely** during optimization — it is not suggested to Optuna and not stored in trial results. This reduces the effective search space and avoids wasting trials on meaningless values.

Example from S03:

```json
{
  "useCloseCount": {
    "type": "bool",
    "label": "Use Close Count",
    "default": true,
    "group": "Entry Filters",
    "optimize": { "enabled": true }
  },
  "closeCountLong": {
    "type": "int",
    "label": "Close Count Long",
    "default": 7,
    "min": 1,
    "max": 50,
    "step": 1,
    "group": "Entry Filters",
    "depends_on": "useCloseCount",
    "optimize": { "enabled": true, "min": 2, "max": 7, "step": 1 }
  }
}
```

What this does:
- When `useCloseCount = true`: `closeCountLong` is suggested normally by Optuna.
- When `useCloseCount = false`: `closeCountLong` is skipped (not suggested, not stored in trial params).
- Reduces search space dimensionality for trials where the feature is disabled.
- Works with both optimized and fixed parent bools.

Rules:
- `depends_on` must reference a **bool** parameter that exists in `parameters`.
- Can be a single string (`"depends_on": "useTBands"`) or a list (`"depends_on": ["useBool1", "useBool2"]`). When a list is given, **all** parents must be `true` for the dependent to be active.
- Combines naturally with `bool_groups`: the group rule controls which bool combos are valid, while `depends_on` controls which numeric params are active for each combo.

## Step 4: Create Params Dataclass

```python
from dataclasses import dataclass
from typing import Any, Dict, Optional
import pandas as pd

@dataclass
class S05Params:
    """S05 strategy parameters - camelCase matching PineScript."""
    rsiLen: int = 14
    threshold: float = 0.5
    riskPerTrade: float = 2.0
    contractSize: float = 0.01
    initialCapital: float = 100.0
    commissionPct: float = 0.05
    startDate: Optional[pd.Timestamp] = None
    endDate: Optional[pd.Timestamp] = None

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[pd.Timestamp]:
        if value in (None, ""):
            return None
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "S05Params":
        payload = payload or {}
        return cls(
            rsiLen=int(payload.get("rsiLen", cls.rsiLen)),
            threshold=float(payload.get("threshold", cls.threshold)),
            riskPerTrade=float(payload.get("riskPerTrade", cls.riskPerTrade)),
            contractSize=float(payload.get("contractSize", cls.contractSize)),
            initialCapital=float(payload.get("initialCapital", cls.initialCapital)),
            commissionPct=float(payload.get("commissionPct", cls.commissionPct)),
            startDate=cls._parse_timestamp(payload.get("startDate")),
            endDate=cls._parse_timestamp(payload.get("endDate")),
        )
```

**Rules:**
- Field names stay camelCase
- Do NOT add `to_dict()` - use `dataclasses.asdict(params)` instead
- `from_dict()` maps directly without snake_case fallbacks

## Step 5: Implement Strategy Class

```python
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd

from core import metrics
from core.backtest_engine import StrategyResult, TradeRecord, build_forced_close_trade
from strategies.base import BaseStrategy

class S05MyStrategy(BaseStrategy):
    STRATEGY_ID = "s05_mystrategy"
    STRATEGY_NAME = "S05 My Strategy"
    STRATEGY_VERSION = "v01"

    @staticmethod
    def run(df: pd.DataFrame, params: Dict[str, Any], trade_start_idx: int = 0) -> StrategyResult:
        p = S05Params.from_dict(params)

        if df.empty:
            return StrategyResult(trades=[], equity_curve=[], balance_curve=[], timestamps=[])

        # Get price data
        close = df["Close"]
        high = df["High"]
        low = df["Low"]

        # Calculate indicators (use indicators/ package)
        from indicators.oscillators import rsi
        rsi_values = rsi(close, p.rsiLen)

        # Initialize state variables
        balance = p.initialCapital
        position = 0  # 1=long, -1=short, 0=flat
        trades: List[TradeRecord] = []
        equity_curve: List[float] = []
        balance_curve: List[float] = []
        timestamps: List[pd.Timestamp] = []

        # Bar-by-bar simulation
        for i in range(trade_start_idx, len(df)):
            # Your entry/exit logic here
            # ...

            # Force-close any open position at the final bar (required for all modes).
            if i == len(df) - 1 and position != 0:
                trade, gross_pnl, exit_commission, _ = build_forced_close_trade(
                    position=position,
                    entry_time=entry_time,
                    exit_time=df.index[i],
                    entry_price=entry_price,
                    exit_price=close.iat[i],
                    size=position_size,
                    entry_commission=entry_commission,
                    commission_rate=p.commissionPct,
                    commission_is_pct=True,
                )
                if trade:
                    trades.append(trade)
                    balance += gross_pnl - exit_commission - entry_commission
                position = 0
                position_size = 0.0
                entry_price = np.nan
                entry_commission = 0.0
                entry_time = None

            # Track equity
            timestamps.append(df.index[i])
            equity_curve.append(balance)
            balance_curve.append(balance)

        # Build result
        result = StrategyResult(
            trades=trades,
            equity_curve=equity_curve,
            balance_curve=balance_curve,
            timestamps=timestamps,
        )

        # Compute and attach all declared metrics to result
        # This automatically handles the intersection of calculated metrics
        # with StrategyResult's declared fields (no manual assignment needed)
        metrics.enrich_strategy_result(result, initial_balance=p.initialCapital)
        return result
```

**Note on metrics:** `enrich_strategy_result()` calculates BasicMetrics and
AdvancedMetrics, then attaches only the metrics that StrategyResult declares
as fields. Additional metrics (like `win_rate`, `sortino_ratio`) are available
in Optuna optimization results but are not exposed in single-backtest output
by design.

**Key patterns from existing strategies:**
- Pre-extract NumPy arrays from DataFrame columns before the loop (e.g., `close_arr = df["Close"].to_numpy()`) for faster element access
- Use `trade_start_idx` to skip warmup bars
- Create `TradeRecord` for each closed trade
- Track `equity_curve`, `balance_curve`, `timestamps`
- Calculate metrics at the end using `core.metrics`

## Step 6: Test and Validate

Create a test file:

```python
# tests/test_s05_mystrategy.py
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.backtest_engine import load_data, prepare_dataset_with_warmup
from strategies.s05_mystrategy.strategy import S05MyStrategy, S05Params

def test_s05_matches_pine_expected():
    """Validate against PineScript expected results."""
    data_path = Path(__file__).parent.parent / "data" / "raw" / "OKX_LINKUSDT.P, 15 2025.05.01-2025.11.20.csv"
    df = load_data(str(data_path))

    params = {
        "rsiLen": 14,
        "threshold": 0.5,
        # ... match PineScript parameters
        "startDate": "2025-06-01",
        "endDate": "2025-10-01",
    }

    df_prepared, trade_start_idx = prepare_dataset_with_warmup(
        df,
        pd.Timestamp("2025-06-01", tz="UTC"),
        pd.Timestamp("2025-10-01", tz="UTC"),
        warmup_bars=1000
    )

    result = S05MyStrategy.run(df_prepared, params, trade_start_idx)

    # Expected from PineScript (±5% tolerance)
    assert abs(result.net_profit_pct - 113.26) < 113.26 * 0.05
    assert abs(result.max_drawdown_pct - 10.99) < 10.99 * 0.05
    assert result.total_trades == 52  # Exact match for trade count
```

Run tests:
```bash
pytest tests/test_s05_mystrategy.py -v
```

## Step 7: Auto-Registration

Strategies are auto-discovered. Ensure:
- `config.json` exists with valid `id` field
- `strategy.py` defines class with `STRATEGY_ID`, `STRATEGY_NAME`, `STRATEGY_VERSION`, and static `run()` method

The strategy will appear in UI dropdown after server restart.

## Reference: S04 StochRSI Example

See `src/strategies/s04_stochrsi/` for a complete working example:
- `config.json` - Parameter schema with optimization ranges
- `strategy.py` - Full implementation with StochRSI calculation

## Common Pitfalls

| Problem | Solution |
|---------|----------|
| Results don't match Pine | Check warmup bars, date filtering, commission calculation |
| Parameters not showing in UI | Verify config.json syntax, check browser console |
| Strategy not discovered | Ensure both config.json and strategy.py exist |
| snake_case parameters | Use camelCase everywhere: `rsiLen` not `rsi_len` |
| Missing trades | Check `trade_start_idx` usage, verify entry conditions |
| Wasted trials from invalid bool states | Add `optimization_rules.bool_groups` (e.g., `at_least_one_true`) |
| Numeric params explored when feature is off | Add `"depends_on": "boolParamName"` to skip dependent params when parent bool is false |

## Architecture Guarantees

- Frontend renders parameters automatically from `config.json`
- Optimization includes parameters via config-driven schemas
- Core layers stay strategy-agnostic
- Adding new strategies requires only config + strategy module (no UI/core edits)
