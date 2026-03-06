# Phase 2-1 R2 Metric Update Report

## 1. Summary

This update replaces Merlin's old `consistency_score` semantics with a new
signed `R²` equity-shape metric.

Before this update:
- `consistency_score` meant **percentage of profitable months**
- the metric range was **`[0, 100]`**
- UI labels and score bounds treated it like a percent metric

After this update:
- `consistency_score` means **signed `R²` of the equity curve**
- the metric range is **`[-1, 1]`**
- the metric is calculated from **`equity_curve`**
- the score sign comes from **correlation / regression direction**
- the magnitude comes from **`R²`**

This implementation follows the agreed `v1` decision:
- reuse the existing `consistency_score` field
- do **not** introduce daily resampling yet
- keep current UI naming style where practical (`Consist` table header, `Consistency` / `Consistency Score` labels)
- display the metric as a normal decimal value, not as a percent


## 2. Targets Of The Update

The implementation targeted the following problems in the original proposal and
in Merlin's previous metric:

1. The old consistency metric did not measure equity-curve shape at all.
2. The initial R² proposal used an unsafe sign rule based on `first/last`.
3. Raw score bounds and UI formatting still assumed a percent metric.
4. Old saved `score_config` payloads with `consistency: 0..100` bounds could
   silently distort composite score normalization after the semantic switch.


## 3. What Was Changed

### Core metric logic

Updated:
- `src/core/metrics.py`
- `src/core/backtest_engine.py`

Changes:
- removed the old monthly-profitable-months consistency calculation
- added `_calculate_r2_consistency(equity_curve)`
- switched `calculate_advanced()` to compute consistency from
  `result.equity_curve`
- updated the `StrategyResult.consistency_score` comment to reflect the new
  semantics

### Score normalization and backend defaults

Updated:
- `src/core/optuna_engine.py`
- `src/ui/server_services.py`

Changes:
- changed default consistency bounds from `0..100` to `-1..1`
- changed display name from `Consistency %` to `Consistency`
- added legacy bounds migration:
  if an old config still contains `{"consistency": {"min": 0, "max": 100}}`,
  Merlin now upgrades it to `{"min": -1, "max": 1}` before score normalization

This closes a real compatibility risk for:
- old queue items
- old saved UI state
- old study configs

### UI and formatting

Updated:
- `src/ui/static/js/ui-handlers.js`
- `src/ui/static/js/optuna-results-ui.js`
- `src/ui/static/js/optuna-ui.js`
- `src/ui/static/js/results-format.js`
- `src/ui/static/js/analytics.js`
- `src/ui/templates/index.html`

Changes:
- removed percent semantics from labels and formatting
- kept the compact table header `Consist`
- changed consistency display in Optuna results to **2 decimals**
- changed score bounds inputs for consistency to `step="0.01"`
- changed constraint input for consistency to `step="0.01"`
- changed the helper text from `Profitable months %` to
  `Signed R² of equity curve`
- added UI-side migration for legacy `0..100` consistency bounds

### Tests and regression baseline

Updated:
- `tests/test_metrics.py`
- `tests/test_score_normalization.py`
- `tests/test_server.py`
- `data/baseline/s01_metrics.json`

Changes:
- added direct unit tests for the new signed `R²` implementation
- updated score-normalization tests to use the new metric scale
- added a regression test for legacy score-config bounds migration in the
  server config builder
- refreshed the S01 baseline consistency value to the new semantic meaning


## 4. Final Metric Logic

### Formula

The implemented metric is:

```text
consistency_score = sign(corr(x, y)) * corr(x, y)^2
```

where:
- `x` = bar index `0..N-1`
- `y` = `log(equity)` if **all** equity values are strictly positive
- otherwise `y` = raw `equity`

### Why this version was chosen

1. **Sign is based on trend direction**, not endpoint comparison.
   This prevents incorrect positive scores for curves that mostly trend down
   but finish slightly above the start.

2. **`log(equity)` is used when possible**.
   This avoids unfairly penalizing smooth compounded growth for being curved in
   raw equity space.

3. **`equity_curve` is used instead of `balance_curve`**.
   This keeps the metric focused on the actual path of marked-to-market equity.

4. **No daily resampling in `v1`**.
   This was intentionally left out to keep the implementation simple,
   transparent, and aligned with Merlin's current per-study bar-level behavior.

### Edge-case behavior

- fewer than 3 points -> `None`
- non-finite values (`NaN`, `Inf`) -> `None`
- flat valid curve -> `0.0`
- perfect upward linear trend in transformed space -> `+1.0`
- perfect downward linear trend in transformed space -> `-1.0`


## 5. Problems Solved By This Update

### Problem 1: wrong metric meaning

Solved.

The old `consistency_score` did not measure equity shape. The new metric now
does what the feature is intended to do: it measures smoothness and directional
linearity of the equity path.

### Problem 2: unsafe sign rule

Solved.

The implementation does **not** use:

```text
sign = eq[-1] >= eq[0]
```

It uses correlation sign instead, which is mathematically appropriate for this
metric.

### Problem 3: compounded growth being unfairly penalized

Solved.

The implementation uses `log(equity)` when possible, so smooth compounded
growth can still score highly.

### Problem 4: old percent-based normalization would corrupt composite score

Solved.

Both backend score normalization and server/UI config handling now migrate old
`consistency` bounds `0..100` to the new `-1..1` range.

### Problem 5: UI still presenting the metric as a percent

Solved.

The UI no longer renders consistency as `%`, and the Optuna results table now
shows the value with 2 decimal places.


## 6. Reference Result For S01 Baseline

The recorded S01 regression baseline was updated from:

```text
consistency_score = 66.66666666666666
```

to:

```text
consistency_score = 0.7939342470631295
```

This confirms the semantic switch from the old profitable-months percentage to
the new signed `R²` consistency metric.


## 7. Reference Tests

All tests were run with:

```text
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe
```

### Test command 1

```text
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest -q tests/test_metrics.py tests/test_score_normalization.py tests/test_regression_s01.py
```

Result:
- `51 passed`

### Test command 2

```text
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest -q tests/test_multiprocess_score.py
```

Result:
- `2 passed`
- `3 warnings`

Notes:
- warnings were Optuna `ExperimentalWarning` messages for `multivariate`
  sampling
- no test failures occurred

### Test command 3

```text
C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest -q tests/test_server.py -k "optuna_sanitize_defaults or optuna_coverage_mode_parsed or optuna_score_config_migrates_legacy_consistency_bounds"
```

Result:
- `3 passed`


## 8. Added Test Coverage

The update added direct coverage for:

- perfect compounded growth -> `+1.0`
- perfect compounded decline -> `-1.0`
- same shape, different scale -> same score
- flat curve -> `0.0`
- non-positive equity fallback to raw equity
- endpoint-positive but trend-negative shape -> negative score
- invalid input -> `None`
- legacy `0..100` score bounds migration in normalization
- legacy `0..100` score bounds migration in server config sanitization


## 9. Errors And Issues

### Final code/test status

No implementation errors remained after the final patch set.

### Non-blocking notes

1. Historical DB rows and studies still contain old `consistency_score`
   semantics. That is expected and accepted for this branch.
2. Existing persisted queue/study configs may still physically store old
   `0..100` consistency bounds, but the new migration logic now upgrades them
   during use.
3. Daily resampling was intentionally **not** added in `v1`.


## 10. Final Outcome

This update now gives Merlin a compact equity-shape metric that:

- measures smoothness / linearity of the equity path
- distinguishes upward vs downward trend direction
- avoids the original sign bug
- avoids percent-based UI confusion
- keeps score normalization correct under the new semantic meaning
- remains simple enough for reliable `v1` use

The implementation is complete, tested, and consistent with the agreed
specification for this phase.
