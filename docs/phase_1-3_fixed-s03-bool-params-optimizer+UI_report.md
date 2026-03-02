# Phase 1-3 Report: S03 Bool Params Fix (Optimizer + UI)

Date: 2026-03-02  
Project: Merlin (`./+Merlin/+Merlin-GH/`)

## 1) Goal of this update

This update addressed two linked problems in a robust, reusable way:

1. `bool` optimizer parameters were rendered as numeric `From/To/Step` controls in Start page UI (misleading and semantically incorrect).
2. S03 had an invalid boolean combination (`useCloseCount = false` and `useTBands = false`) that produced no-trade trials and wasted optimizer budget.

Target behavior:

- Bool params are treated as categorical options (`True/False`) in optimizer UI.
- Invalid boolean combinations can be excluded declaratively from strategy config (not hardcoded in optimizer).
- Optimizer, coverage mode, and queue-compatible config flow remain stable.

## 2) Problems solved

### Problem A: Bool UI was incorrect

- Before: bool optimizer rows were auto-falling into numeric range input branch.
- Result: user saw fake numeric controls for bools (`0..100 step 0.1`), while optimizer ignored them.

### Problem B: Invalid S03 combo was sampled

- Before: optimizer sampled both-off state for S03 bool pair.
- Result: trials with 0 trades / 0% useful signal, budget waste, noisy search.

## 3) What was implemented

## 3.1 UI: bool optimizer controls now categorical

File: `src/ui/static/js/strategy-config.js`

- Added dedicated bool branch in `createOptimizerRow(...)`.
- Bool params now use `createSelectOptions(...)` with options `[true, false]`.
- `selectAllByDefault` enabled for bool params, so optimization starts with both states available.
- Bool labels are rendered as `True` / `False`.

## 3.2 UI config collector/validator: bool options are now passed explicitly

File: `src/ui/static/js/ui-handlers.js`

- Added helper functions:
  - `readSelectedOptimizerOptionValues(...)`
  - `parseBoolOptionValue(...)`
- Updated validation:
  - bool params now require at least one selected option (same rule as other categorical params).
- Updated config building:
  - bool selected options are serialized into `param_ranges` as categorical values (`[true, false]` subset).
- Updated generic optimizer range collector similarly.

## 3.3 Coverage hints in UI now respect bool-group rules

File: `src/ui/static/js/optuna-ui.js`

- Added bool parsing + selected bool option readers.
- Added support for strategy-level bool-group coverage adjustment.
- Implemented rule-aware block size adjustment for mode `at_least_one_true`.
- Coverage hint (minimum and block multiples) now matches effective search space when bool-group rules are active.

## 3.4 Optimizer core: declarative bool-group constraints (future-proof)

File: `src/core/optuna_engine.py`

- Added reusable helpers:
  - `_coerce_bool_value(...)`
  - `_normalize_bool_choices(...)`
  - `_extract_bool_group_rules(...)`
- Bool params now support UI overrides in `param_ranges` (`values` subset).
- Added declarative bool-group transformation stage in search-space build:
  - `_apply_bool_group_rules(...)`
  - `_build_bool_group_surrogate_name(...)`
- Supported mode: `at_least_one_true`.
- For multi-bool groups, optimizer creates a surrogate categorical axis containing only valid combinations.
- Added decode logic in `_prepare_trial_parameters(...)` to map surrogate token back to real bool params before strategy execution.
- Improved bool casting in `_cast_param_value(...)` to avoid `bool("false") == True` pitfalls.

## 3.5 Strategy config: S03 declares invalid bool combo rule

File: `src/strategies/s03_reversal_v10/config.json`

- Added:

```json
"optimization_rules": {
  "bool_groups": [
    {
      "params": ["useCloseCount", "useTBands"],
      "mode": "at_least_one_true"
    }
  ]
}
```

This removes `both off` from optimizer search space without hardcoding strategy id in engine logic.

## 3.6 Tests added/updated

File: `tests/test_coverage_startup.py`

Added tests:

1. `test_s03_bool_group_rule_reduces_invalid_combo_in_search_space`
2. `test_s03_bool_group_rule_updates_coverage_minimum`
3. `test_s03_bool_group_surrogate_is_decoded_to_real_bool_params`

These verify:

- `both off` is excluded.
- effective S03 coverage minimum is updated (`33` instead of `44` when both bools are enabled with full options).
- surrogate categorical bool-group axis decodes correctly into real strategy bool params.

## 4) Key logic summary

1. Strategy config can declare invalid bool combinations using `optimization_rules.bool_groups`.
2. Optimizer reads these rules during search-space construction.
3. For affected bool groups:
   - invalid combinations are removed;
   - valid combinations become sampler-visible via constrained choices.
4. For coverage mode:
   - startup coverage uses the transformed search space, so minimum/recommended counts align with valid combinations only.
5. UI now correctly represents bool optimization as categorical choices and sends selected bool values to backend.

## 5) Files changed

- `src/ui/static/js/strategy-config.js`
- `src/ui/static/js/ui-handlers.js`
- `src/ui/static/js/optuna-ui.js`
- `src/core/optuna_engine.py`
- `src/strategies/s03_reversal_v10/config.json`
- `tests/test_coverage_startup.py`
- `docs/phase_1-3_fixed-s03-bool-params-optimizer+UI_report.md` (this report)

## 6) Reference tests executed

Interpreter used (as requested):

`C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

Commands run:

1. `python.exe -m pytest -q tests/test_coverage_startup.py tests/test_naming_consistency.py tests/test_s03_reversal_v10.py`
   - Result: `29 passed`
2. `python.exe -m pytest -q tests/test_server.py`
   - Result: `44 passed`
3. `python.exe -m pytest -q tests/test_walkforward.py`
   - Result: `10 passed`

Total: `83 passed`, `0 failed`.

Observed warning:

- Optuna `NSGAIIISampler` experimental warning (existing known warning, not introduced by this patch).

## 7) Errors encountered during implementation

- No runtime/test failures after final patch set.
- One intermediate patch-application mismatch occurred during editing (`apply_patch` context mismatch), resolved by re-reading file slice and applying precise hunks.

## 8) Safety and compatibility assessment

- No DB migration required.
- No API contract break observed in server tests.
- Queue/scheduler compatibility preserved: bool selections are serialized in `param_ranges` and consumed by engine.
- Backward compatibility:
  - strategies without `optimization_rules` are unaffected.
  - bool params in existing strategies now render correctly as categorical controls in optimizer UI.
- Coverage mode consistency improved because UI and backend now both account for valid bool-group combinations.

## 9) Final status

Update fully implemented and verified against targeted and integration-relevant tests.  
Core objective achieved: S03 invalid bool combo is excluded in optimizer flow, and bool optimizer UI is now semantically correct and consistent with backend behavior.
