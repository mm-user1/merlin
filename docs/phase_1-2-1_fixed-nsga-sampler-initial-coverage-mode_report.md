# Phase 1-2-1 Report: Fixed NSGA Initial Coverage Integration

## Goal of this update

Fix a critical integration issue between `coverage_mode` and NSGA samplers:

- `coverage_mode` enqueues deterministic initial trials via `study.enqueue_trial(...)`.
- In Optuna 4.6.0, these enqueued trials can complete without NSGA generation metadata.
- NSGA parent selection operates on generation-tagged trials.
- Result: initial coverage trials were executed but could be invisible to NSGA evolution.

Target behavior after fix:

- For `coverage_mode + nsga2/nsga3`, enqueued coverage trials must be explicitly visible as `generation=0`.
- For TPE and other samplers, behavior must remain unchanged.
- The fix must work in both single-process and multi-process execution paths.

---

## What was changed

### 1) Core fix in Optuna engine

File: `src/core/optuna_engine.py`

Added a new internal method:

- `OptunaOptimizer._mark_coverage_generation_for_nsga(trial)`

Logic:

1. Exit unless `coverage_mode=True`.
2. Exit unless sampler is `nsga2` or `nsga3`.
3. Read trial system attrs from Optuna storage.
4. Exit unless trial is an enqueued fixed trial (`fixed_params` exists).
5. If generation key is missing, set it to `0`:
   - `NSGAIISampler._GENERATION_KEY` for NSGA-II
   - `NSGAIIISampler._GENERATION_KEY` for NSGA-III

Safety:

- Defensive checks for missing trial/storage internals.
- Wrapped with fail-safe `try/except` and debug log (no optimization abort).
- No behavior changes for non-coverage, non-NSGA paths.

### 2) Applied fix in both objective execution paths

File: `src/core/optuna_engine.py`

Calls added at the start of:

- `_objective(...)` (single-process path)
- `_objective_for_worker(...)` (multi-process worker path)

This guarantees generation marking happens before parameter suggestions for every evaluated trial.

---

## Why this solution was chosen

Chosen approach:

- Keep existing `enqueue_trial` flow.
- Add targeted generation tagging only for NSGA + coverage mode.

Reasons:

- Minimal and local patch.
- Preserves current architecture (no custom sampler wrappers needed).
- Avoids external dependencies.
- Keeps TPE coverage flow unchanged and stable.
- Works for both NSGA-II and NSGA-III.
- Keeps code concise and easy to reason about.

---

## Problems solved

1. **Coverage trials now participate in NSGA generation model**
   - Enqueued coverage trials are no longer generation-less for NSGA.
   - They are explicitly tagged as `generation=0`.

2. **No wasted coverage warmup for NSGA**
   - NSGA can use these completed trials as the initial generation pool for parent selection.

3. **Consistency across execution modes**
   - Same behavior in single-process and multi-process optimization.

---

## Tests added and updated

File: `tests/test_coverage_startup.py`

Added 3 tests:

1. `test_nsga2_coverage_marks_enqueued_trial_as_generation_zero`
2. `test_nsga3_coverage_marks_enqueued_trial_as_generation_zero`
3. `test_nsga_coverage_marker_is_noop_when_coverage_mode_disabled`

These tests validate:

- NSGA-II coverage enqueued trial gets generation key `= 0`.
- NSGA-III coverage enqueued trial gets generation key `= 0`.
- Marker is inactive when coverage mode is disabled.

---

## Reference test results

Interpreter used (as requested):

- `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

Executed commands:

1. `python -m pytest tests/test_coverage_startup.py -q`
   - Result: `11 passed`

2. `python -m pytest -q`
   - Result: `230 passed`

Warnings observed:

- Existing Optuna experimental warnings (`NSGAIIISampler`, `multivariate` TPE), expected and non-blocking.

---

## Reliability and regression assessment

### Reliability

- Fix is narrowly scoped and conditional.
- Only affects `coverage_mode + nsga2/nsga3`.
- Graceful fallback on internal API anomalies (debug log, no crash).

### Regression risk

- Low.
- Full suite passed after patch.
- No changes to search-space generation, queue, WFA wiring, UI, DB schema, or TPE startup logic.

### Future-proof notes

- Current Optuna internals are used via storage/trial internals for generation tagging.
- This is currently the smallest robust fix for Optuna 4.6.0 behavior.
- If a future Optuna version changes internal storage/trial interfaces, the defensive fallback prevents hard failure; tests should be rerun after Optuna upgrades.

---

## Errors encountered during implementation

- No implementation errors.
- No failing tests after patch.
- Only expected Optuna experimental warnings were present.

---

## Final status

Update completed successfully.

- NSGA initial coverage now correctly enters generation model as `generation=0`.
- TPE and other existing behavior remains unchanged.
- Tests confirm correctness and no regressions.

---

## Follow-up UI update (Coverage Mode UX polish)

This follow-up patch improved Start page UX for Coverage Mode without changing optimization core logic.

### Goals

- Simplify naming.
- Make coverage hint immediately actionable.
- Auto-fill a practical default when Coverage Mode is enabled.

### What was changed

Files:

- `src/ui/templates/index.html`
- `src/ui/static/js/optuna-ui.js`

Changes:

1. Coverage label simplified:
   - From: `Coverage mode (stratified LHS)`
   - To: `Coverage mode`

2. Coverage hint text simplified:
   - Previous verbose diagnostic sentence was removed.
   - Hint now shows the first 4 block targets in compact form:
     - Example: `44 / 88 / 132 / 176`
   - Color behavior:
     - Red if current initial trials are below minimum 1-block coverage.
     - Neutral gray otherwise.

3. Auto-fill on enabling Coverage Mode:
   - On user check of `Coverage mode`, `Initial trials` is auto-set to `3 * block_size`.
   - Auto-fill is intentionally guarded:
     - It runs only when current `Initial trials` is default/empty (`20` or blank/invalid).
     - It does not overwrite already custom user values.

4. Recommendation baseline aligned to practical default:
   - Coverage `recommended` value now maps to `3 * block_size` (used in tooltip/help context).

### Why this is safe

- Scope is UI-only (text + UI helper behavior).
- No backend schema changes.
- No change in queue payload fields or optimization execution contracts.
- Existing `coverage_mode` wiring remains unchanged.

### Reference test run after UI patch

Interpreter:

- `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

Commands:

1. `python -m pytest tests/test_coverage_startup.py -q`
   - Result: `11 passed`

2. `python -m pytest tests/test_server.py::test_optuna_coverage_mode_parsed tests/test_walkforward.py::test_run_optuna_on_window_forwards_coverage_mode -q`
   - Result: `2 passed`

Warnings:

- Existing Optuna experimental warning for `NSGAIIISampler` (expected, non-blocking).

---

## Analytics sidebar parity update (focused mode: Initial trials)

Added `Initial` to the Analytics focused sidebar (`Optuna Settings`) so it matches the Results sidebar behavior.

### Goal

- Show `Initial trials` in Analytics focused mode.
- Include `(coverage)` suffix when coverage mode was enabled for that study.
- Keep behavior backward-compatible for old studies with missing fields.

### What changed

Files:

- `src/ui/server_routes_analytics.py`
- `src/ui/static/js/analytics.js`
- `tests/test_server.py`

Backend (`/api/analytics/summary`):

- Extended `optuna_settings` payload with:
  - `warmup_trials`
  - `coverage_mode`
- Fallback chain for `warmup_trials`:
  1. `config_json.optuna_config.warmup_trials`
  2. `config_json.n_startup_trials`
  3. `config_json.optuna_config.sampler_config.n_startup_trials`
- Fallback for `coverage_mode`:
  1. `config_json.optuna_config.coverage_mode`
  2. `config_json.coverage_mode`
- Missing values remain `None` (no forced defaults).

Frontend (Analytics focus sidebar):

- Added `formatInitialLabel(settings)` helper.
- Added `Initial` row in focused `optunaRows`.
- Render behavior:
  - `N (coverage)` when `warmup_trials` is present and `coverage_mode=true`
  - `N` when `warmup_trials` is present and coverage is false/unknown
  - `-` when `warmup_trials` is missing

Tests:

- Updated `test_analytics_summary_includes_focus_settings_payload` to assert:
  - `warmup_trials` and `coverage_mode` are present for configured studies
  - both are `None` for studies without these fields

### Validation

Interpreter:

- `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

Commands:

1. `python -m pytest tests/test_server.py -q`
   - Result: `44 passed`

2. `python -m pytest tests/test_server.py::test_optuna_coverage_mode_parsed tests/test_walkforward.py::test_run_optuna_on_window_forwards_coverage_mode tests/test_coverage_startup.py -q`
   - Result: `13 passed`

Warnings:

- Existing Optuna experimental warning for `NSGAIIISampler` (expected, non-blocking).

### Notes

- No DB migration required.
- No changes to optimization logic or queue execution.
- Update is scoped to analytics summary payload + focused sidebar rendering only.

### Errors encountered

- No errors during implementation.
- No regressions detected in the referenced tests.

---

## Additional UI refinement (block click-to-fill + tooltip removal)

Second UX refinement was applied to Coverage Mode hints in Start page.

### What changed

File:

- `src/ui/static/js/optuna-ui.js`

Changes:

1. Removed tooltip-only warning text from coverage hint.
   - The hint no longer relies on `title` hover behavior.

2. Coverage block values are now clickable.
   - The hint renders block targets as interactive values (e.g. `44 / 88 / 132 / 176`).
   - Clicking a value sets `Initial trials` to that number immediately.
   - After click, the same UI refresh path is triggered via an `input` event.

3. Added a safety guard for auto-fill on Coverage checkbox enable.
   - Auto-fill to `3 * block_size` now runs only for trusted user-originated checkbox events.
   - Programmatic change events (e.g. queue/form restore flows) do not trigger auto-overwrite.

### Why this is safe

- UI-only change; backend and optimization core remain unchanged.
- Existing queue/run payload contracts are untouched.
- The guard reduces risk of unintended value mutation during non-user form updates.

### Validation

Interpreter:

- `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

Commands:

1. `python -m pytest tests/test_coverage_startup.py -q`
   - Result: `11 passed`

2. `python -m pytest tests/test_server.py::test_optuna_coverage_mode_parsed tests/test_walkforward.py::test_run_optuna_on_window_forwards_coverage_mode -q`
   - Result: `2 passed`

Warnings:

- Existing Optuna experimental warning for `NSGAIIISampler` (expected, non-blocking).
