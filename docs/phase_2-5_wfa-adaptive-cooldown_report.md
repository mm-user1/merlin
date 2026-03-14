# Phase 2.5 Report: WFA Adaptive Configurable Cooldown

## Scope

This update implements `Adaptive Cooldown` for Merlin WFA with the following agreed boundaries:

- Cooldown is available only for `WFA Adaptive`.
- `WFA Fixed` behavior was left unchanged.
- Default behavior without the cooldown checkbox remains exactly the current behavior.
- Cooldown is triggered only after adaptive `CUSUM` or `Drawdown` triggers.
- Cooldown days are included in adaptive annualization/WFE, because capital is idle during cooldown and does not earn.
- `Presets` were intentionally not changed in this update.
- `IS => OOS gap` was intentionally not implemented in this update.

## Goals Solved

The update solves these practical and technical problems:

1. It allows the user to pause adaptive re-optimization after a degradation trigger instead of immediately continuing the rolling adaptive cycle.
2. It prevents overstating adaptive WFE by counting cooldown time in elapsed OOS time for annualization.
3. It keeps legacy queue items and existing `queue.json` compatible by applying safe defaults when cooldown fields are absent.
4. It exposes the new setting consistently across the main form, queue runs, results view, analytics view, API payloads, and stored study metadata.

## Implemented Changes

### 1. Core adaptive WFA logic

Updated `src/core/walkforward_engine.py`.

Added new WFA config fields:

- `cooldown_enabled: bool = False`
- `cooldown_days: int = 15`

Added new per-window metadata:

- `cooldown_days_applied`
- `oos_elapsed_days`

Added adaptive cooldown resolution logic:

- new helper `_resolve_adaptive_roll_end(...)`
- applies cooldown only when:
  - adaptive trigger actually fired
  - trigger type is `cusum` or `drawdown`
  - cooldown is enabled
  - cooldown days are positive

Adaptive execution changes:

- window still truncates OOS at the actual trigger point
- next adaptive shift uses `actual OOS + cooldown`
- cooldown does not apply to `inactivity` or `max_period`
- stored `oos_actual_days` remains the true traded OOS duration
- stored `oos_elapsed_days` becomes `traded OOS + cooldown`

Annualization/WFE changes:

- adaptive WFE now prefers `oos_elapsed_days`
- if `oos_elapsed_days` is absent, it falls back to prior behavior (`oos_actual_days`)
- this preserves correctness for old studies and for non-cooldown adaptive windows

### 2. Persistence and schema

Updated `src/core/storage.py`.

Study-level persistence now stores:

- `cooldown_enabled`
- `cooldown_days`

Window-level persistence now stores:

- `cooldown_days_applied`
- `oos_elapsed_days`

Schema migration support was added for existing databases, so old DBs receive the new columns automatically.

### 3. Backend request handling

Updated `src/ui/server_routes_run.py`.

Added request parsing for:

- `wf_cooldown_enabled`
- `wf_cooldown_days`

Backend rules:

- cooldown is forcibly disabled when `adaptive_mode` is off
- cooldown days are normalized and bounded
- cooldown settings are propagated into:
  - `WFConfig`
  - saved config payload
  - runtime optimization state

### 4. Main UI

Updated `src/ui/templates/index.html` and `src/ui/static/js/ui-handlers.js`.

Implemented UI as agreed:

- inside `Adaptive Re-Optimization`
- first row before `Max OOS (days)`
- checkbox + label `Cooldown (days):` + integer input
- default checkbox: off
- default input value: `15`
- input is disabled until checkbox is enabled

Removed the unused `Behavior (Phase 2)` block from the form.

### 5. Queue support and backward compatibility

Updated `src/ui/static/js/queue.js`.

Queue integration now:

- saves cooldown fields into queue items
- restores cooldown fields from queue items
- passes cooldown fields when queued WFA runs are executed
- safely defaults legacy queue items to:
  - `cooldownEnabled = false`
  - `cooldownDays = 15`

Important compatibility result:

- existing `queue.json` items without cooldown fields continue to work
- new queue items preserve cooldown settings through queue API round-trips

### 6. Results and analytics

Updated:

- `src/ui/templates/results.html`
- `src/ui/static/js/results-controller.js`
- `src/ui/static/js/results-tables.js`
- `src/ui/static/js/wfa-results-ui.js`
- `src/ui/server_routes_analytics.py`
- `src/ui/static/js/analytics.js`
- `src/ui/server_routes_data.py`

Results/UI behavior:

- results sidebar shows `Cooldown (days)` only for adaptive studies with cooldown enabled
- adaptive window headers show cooldown metadata when cooldown was actually applied
- window detail API now exposes cooldown window metadata

Analytics behavior:

- analytics summary now includes cooldown settings in `wfa_settings`
- focused study settings render cooldown when enabled

## Reliability and Compatibility Notes

The update was designed to preserve existing behavior:

- `WFA Fixed` was not altered
- `WFA Adaptive` without cooldown remains current behavior
- legacy studies without cooldown metadata still load correctly
- legacy queue items without cooldown fields still run correctly
- adaptive WFE stays backward-compatible through field fallback logic

## Reference Tests

Interpreter used:

- `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe`

Executed test commands:

1. `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_adaptive_wfa.py tests/test_storage.py tests/test_server.py -q`
   - Result: `81 passed`

2. `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_walkforward.py -q`
   - Result inside sandbox: `10 passed, 1 failed`
   - Failure reason: Windows sandbox permission issue on multiprocessing queue creation (`WinError 5`), not a logic failure in Merlin

3. `C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe -m pytest tests/test_walkforward.py::test_run_optuna_on_window_multiprocess_uses_in_memory_worker_csv -q`
   - Result outside sandbox: `1 passed`

## New Test Coverage Added

Added or extended tests for:

- adaptive annualization using elapsed OOS days
- cooldown not applying to inactivity trigger
- adaptive window shift after cooldown-triggered CUSUM event
- DB schema presence for cooldown columns
- study/window metadata persistence for cooldown fields
- queue API round-trip preservation of cooldown metadata
- walkforward route parsing of cooldown form fields
- analytics summary payload including cooldown settings

## Errors Encountered

There were no implementation logic errors remaining after fixes.

One environment-specific issue occurred during verification:

- multiprocessing walkforward test failed inside sandbox with `PermissionError: [WinError 5]`
- the same test passed when rerun outside sandbox
- this indicates an execution-environment restriction, not a regression in the Merlin code

## Final Outcome

The adaptive cooldown update is implemented end-to-end and is ready for use.

It is:

- consistent with the agreed product behavior
- backward-compatible for queue and legacy studies
- safe for existing non-cooldown WFA usage
- correctly annualized for idle capital during cooldown
- covered by targeted engine, storage, server, queue, and analytics tests
