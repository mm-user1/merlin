# Phase 2.2.1 Strategy Loop Performance Improvement Report

## Summary

This update optimized the main per-bar loops in Merlin's three production strategies:

- `src/strategies/s01_trailing_ma/strategy.py`
- `src/strategies/s03_reversal_v10/strategy.py`
- `src/strategies/s04_stochrsi/strategy.py`

The goal was to remove repeated pandas scalar accessor overhead from the hottest execution path in Merlin without changing strategy logic, timestamps, trades, metrics, or Optuna behavior.

The update was implemented as a mechanical refactor only. No trading rules, indicator formulas, metric calculations, optimization flow, storage behavior, or persistence logic were changed.

## Problem Solved

Before this update, the strategy hot loops accessed scalar values on every bar through repeated pandas calls such as:

- `series.iat[i]`
- `df.index[i]`

These accesses are correct, but they are expensive in tight Python loops. They add repeated bounds checks, pandas dispatch overhead, and Python call overhead at the innermost layer of the backtest path.

This mattered because these loops run:

- for every bar
- for every trial
- for every WFA window
- across multiple workers

After the Optuna journal RAM migration, these strategy loops became one of Merlin's most important remaining CPU bottlenecks.

## What Was Changed

### 1. Numeric series are extracted once before the loop

In all three strategies, the relevant pandas Series are now converted once via:

- `to_numpy(copy=False)`

Examples:

- close/high/low arrays
- precomputed indicator arrays
- rolling min/max arrays

This removes repeated pandas scalar access inside the loop while keeping the same underlying values.

### 2. Timestamps were kept as `pd.Timestamp`

To preserve exact strategy behavior, timestamps were **not** converted to raw numpy datetime values.

Instead, each strategy now uses:

- `times = list(df.index)` or `timestamps_index = list(df.index)`

This was an intentional safety decision. Several strategy paths rely on normal `pd.Timestamp` behavior, including:

- timestamp comparisons
- trade entry/exit timestamp persistence
- `total_seconds()` arithmetic in S01

This preserves Merlin's existing timestamp semantics exactly.

### 3. Last-bar checks were normalized

The repeated condition:

- `i == len(df) - 1`

was replaced with a precomputed last-bar index based on the extracted timestamp list length. This is a small cleanup only and does not change behavior.

## Files Changed

### Production code

- `src/strategies/s01_trailing_ma/strategy.py`
- `src/strategies/s03_reversal_v10/strategy.py`
- `src/strategies/s04_stochrsi/strategy.py`

### New regression coverage

- `tests/test_strategy_loop_regression.py`

### Report

- `docs/phase_2-2-1_improve-strategy-loop-performance_report.md`

## Key Logic and Safety Constraints

This update was intentionally constrained to preserve Merlin behavior exactly.

### What was preserved

- loop order
- all branch conditions
- all NaN checks
- all calculations
- all indicator inputs
- all entry/exit rules
- all trade timestamps
- all trade prices
- all position sizing logic
- all equity and balance curves
- all attached metrics

### What was not changed

- Optuna optimization code
- WFA orchestration
- metrics module
- indicator formulas
- storage or DB logic
- multiprocess worker behavior

### Why this implementation is safe

The refactor changes only how already-computed values are read inside the loop:

- before: repeated scalar pandas access
- after: one-time array extraction, then direct array indexing

That is a low-risk change as long as timestamp semantics are preserved. The implementation explicitly preserved them.

## Exact Parity Verification

Before editing the strategy files, exact reference signatures were captured for fixed S01/S03/S04 runs using Merlin's current datasets and canonical test parameters.

After the refactor, the same runs were repeated and the full output signatures matched exactly.

Verified reference signatures:

- `S01`: `190c9f1cfe5222cff6086ec912a375bf42993b625a26d241c199c5d2a8098166`
- `S03`: `0c2b2d9d70bf906e85227b5530ade1472c6021ba3c036dbafea4cc0c55632226`
- `S04`: `2006d2bb158b39122d5aada0a29406b6da28def594b5b5b63dc41cc4302fdfa8`

Each signature covers:

- full trade list
- trade entry/exit timestamps
- trade prices and PnL
- full equity curve
- full balance curve
- full timestamp series
- key result metrics

This confirms that the refactor preserved Merlin output exactly for those reference runs.

## New Regression Protection

A new regression test file was added:

- `tests/test_strategy_loop_regression.py`

It validates:

1. exact output signature parity for S01, S03, and S04
2. preservation of `pd.Timestamp` objects in strategy result timestamps
3. preservation of `pd.Timestamp` entry/exit times in trades

This protects the exact behavior that was at risk during loop optimization.

## Reference Test Results

### Focused strategy/regression suite

Command:

- `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_regression_s01.py tests/test_s03_reversal_v10.py tests/test_s04_stochrsi.py tests/test_strategy_loop_regression.py -q`

Result:

- `24 passed`

### Full Merlin suite

Command:

- `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest -q`

Result:

- `253 passed`
- `8 warnings`

The warnings were existing Optuna experimental-feature warnings and not related to this update.

## Performance Validation

A direct benchmark was run against Merlin-style access patterns on the sample dataset with about `19,584` rows.

Measured access-pattern speedups:

- S01-style scalar access block: `8.28x`
- S03-style scalar access block: `12.74x`
- S04-style scalar access block: `10.59x`

Measured timings:

- `s01_iat`: `5.6407s`
- `s01_arr`: `0.6811s`
- `s03_iat`: `6.5281s`
- `s03_arr`: `0.5124s`
- `s04_iat`: `8.5948s`
- `s04_arr`: `0.8115s`

Interpretation:

- the accessor overhead was real and substantial
- replacing repeated `.iat[i]` calls with pre-extracted arrays is a meaningful performance improvement
- total whole-trial speedup will depend on the strategy and the relative cost of indicator generation, but the hot-loop improvement itself is clearly significant

## Errors or Issues Encountered

Two minor issues occurred during implementation and verification:

### 1. New signature regression test initially used a mismatched hash payload

The first version of `tests/test_strategy_loop_regression.py` hashed a slightly different payload than the standalone parity-verification script, so the expected hashes did not match.

This was a test-only issue. The production strategy outputs were already correct. The test helper was corrected to use the exact same payload structure.

### 2. Multiprocessing tests required execution outside the sandbox

Inside the sandbox, Windows `multiprocessing.Manager()` initialization raised:

- `PermissionError: [WinError 5] Access is denied`

This is an environment restriction, not a Merlin regression. The full suite was rerun outside the sandbox and passed completely.

## Robustness and Future-Proofing Assessment

This update is robust because:

1. it is a mechanical refactor, not a logic rewrite
2. timestamp semantics were preserved intentionally
3. exact-output parity was validated before and after the change
4. new regression coverage now guards the refactored path
5. the full Merlin suite passed after the change

This update is future-proof enough for its scope because:

- it uses standard pandas `to_numpy(copy=False)` access, which is explicit and maintainable
- it does not rely on unsafe shared-memory or dtype tricks
- it avoids numpy datetime behavior changes by keeping timestamps as `pd.Timestamp`

## Final Conclusion

The phase 2.2.1 strategy loop optimization was implemented successfully.

It achieves its intended target:

- remove repeated pandas scalar accessor overhead from Merlin's hottest strategy loops
- preserve exact Merlin behavior and results
- improve the CPU efficiency of trial execution without changing strategy logic

Verification outcome:

- exact pre/post reference signatures matched
- focused strategy suite passed
- full Merlin suite passed
- benchmark evidence confirms the hot-loop performance improvement is real

Final status:

- update implemented
- correctness verified
- regression protection added
- no unresolved errors remain
